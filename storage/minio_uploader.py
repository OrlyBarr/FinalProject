"""
storage/minio_uploader.py
Reads bus position data and uploads it to MinIO (local S3 data lake).

Uploads:
  - buses_with_nearest_stops.json  →  raw/bus-positions/YYYY/MM/DD/HH/
  - bus_positions.json             →  raw/bus-positions/YYYY/MM/DD/HH/ (fallback)

Can be run:
  - Standalone:  python3 storage/minio_uploader.py
  - From Airflow: called by dag_realtime_ingestion
"""

import json
import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

# ── Allow running from project root ──────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("minio_uploader")

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
S3_BUCKET        = os.getenv("S3_BUCKET_NAME",   "israel-transit-lake")

PROJECT_ROOT = Path(__file__).parent.parent


def get_s3_client():
    """Return a boto3 S3 client pointed at MinIO."""
    import boto3
    from botocore.config import Config

    endpoint = MINIO_ENDPOINT
    # Inside Docker: minio:9000 — outside: localhost:9000
    if os.getenv("MINIO_ENDPOINT") is None and _is_local():
        endpoint = "http://localhost:9000"

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _is_local() -> bool:
    """True when running outside Docker (on the host)."""
    return not os.path.exists("/.dockerenv")


def ensure_bucket(s3):
    """Create bucket if it doesn't exist."""
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=S3_BUCKET)
        log.info(f"Created bucket: {S3_BUCKET}")


def upload_json(s3, data: list, prefix: str, label: str) -> str:
    """Serialize data to JSON and upload to MinIO under a time-partitioned key."""
    now = datetime.now(timezone.utc)
    key = (
        f"{prefix}/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"{label}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body,
                  ContentType="application/json")
    log.info(f"✅ Uploaded {len(data)} records → s3://{S3_BUCKET}/{key}")
    return key


def load_local_json(filename: str) -> list:
    """Load a JSON file from the project root. Returns [] if missing/invalid."""
    path = PROJECT_ROOT / filename
    if not path.exists():
        log.warning(f"File not found: {path}")
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # some files store {"buses": [...]}
            for v in data.values():
                if isinstance(v, list):
                    return v
        return []
    except Exception as e:
        log.error(f"Failed to parse {filename}: {e}")
        return []


def fetch_live_bus_data() -> list:
    """Fetch fresh bus positions from Hasadna SIRI API."""
    try:
        import requests
        r = requests.get(
            "https://open-bus-stride-api.hasadna.org.il/siri_vehicle_locations/list",
            params={"limit": 500, "order_by": "recorded_at_time desc"},
            timeout=15,
        )
        r.raise_for_status()
        records = r.json()
        log.info(f"Fetched {len(records)} live vehicle positions from Hasadna SIRI")
        return [
            {
                "vehicle_id":    str(rec.get("id", "")),
                "route_id":      str(rec.get("siri_snapshot_id", "")),
                "trip_id":       str(rec.get("siri_ride_stop_id", "")),
                "lat":           rec.get("lat"),
                "lon":           rec.get("lon"),
                "bearing":       rec.get("bearing"),
                "velocity":      rec.get("velocity"),
                "timestamp":     rec.get("recorded_at_time"),
                "source":        "hasadna-siri",
            }
            for rec in records
            if rec.get("lat") is not None and rec.get("lon") is not None
        ]
    except Exception as e:
        log.error(f"Live fetch failed: {e}")
        return []


def run_upload():
    """Main entry point — upload current data to MinIO."""
    log.info("=== MinIO Uploader starting ===")

    s3 = get_s3_client()
    ensure_bucket(s3)

    uploaded = []

    # 1. Upload buses_with_nearest_stops.json (enriched data → processed/)
    enriched = load_local_json("buses_with_nearest_stops.json")
    if enriched:
        key = upload_json(s3, enriched, "processed/bus-positions", "buses_enriched")
        uploaded.append(key)
    else:
        log.warning("buses_with_nearest_stops.json empty/missing — skipping enriched upload")

    # 2. Upload raw bus positions
    raw = load_local_json("bus_positions.json") or fetch_live_bus_data()
    if raw:
        key = upload_json(s3, raw, "raw/bus-positions", "buses_raw")
        uploaded.append(key)

    # 3. Fetch fresh live data and upload (always, as raw snapshot)
    live = fetch_live_bus_data()
    if live:
        key = upload_json(s3, live, "raw/bus-positions", "buses_live")
        uploaded.append(key)
        # Also save locally for the bot API
        out = PROJECT_ROOT / "bus_positions.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(live, f, ensure_ascii=False, indent=2)
        log.info(f"Refreshed local bus_positions.json ({len(live)} records)")

    log.info(f"=== Upload complete — {len(uploaded)} objects written to MinIO ===")
    return uploaded


# ── Airflow-callable wrapper ──────────────────────────────────────────────────
def airflow_upload_task(**context):
    """Called from Airflow PythonOperator."""
    # Inside Airflow containers MINIO_ENDPOINT is set to http://minio:9000
    keys = run_upload()
    context["ti"].xcom_push(key="minio_keys", value=keys)
    return len(keys)


if __name__ == "__main__":
    keys = run_upload()
    print(f"\nDone. Uploaded {len(keys)} files:")
    for k in keys:
        print(f"  s3://{S3_BUCKET}/{k}")
