"""
storage/traffic_to_minio.py
Direct HERE Traffic → MinIO writer. Bypasses Kafka entirely.

Writes traffic flow data for all of Israel to MinIO bucket 'TRAFIC-DATA'
(or the bucket name in env TRAFFIC_BUCKET) under:
  raw/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>.json
  processed/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>.json

Usage:
  python3 storage/traffic_to_minio.py           # fetch + upload
  python3 storage/traffic_to_minio.py --dry-run # fetch only, no upload

Can also be called from Airflow as traffic_to_minio_task(**context).
"""

import json
import os
import sys
import logging
import argparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

IL_TZ = ZoneInfo("Asia/Jerusalem")  # UTC+2 winter / UTC+3 summer (DST-aware)
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("traffic_to_minio")

# ── Configuration ─────────────────────────────────────────────────────────────
HERE_API_KEY   = os.getenv("HERE_API_KEY", "")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
# MinIO requires lowercase bucket names. The bucket visible in the UI as
# "TRAFIC-DATA" is stored internally as "trafic-data" by MinIO.
_raw_bucket    = os.getenv("TRAFFIC_BUCKET", "trafic-data")
TRAFFIC_BUCKET = _raw_bucket.lower()   # enforce lowercase for S3-compatible API

# Israel bounding box — same tiles as traffic_producer.py
ISRAEL_TILES = [
    ("galil_west",   32.7, 33.3, 34.9, 35.3),
    ("galil_east",   32.7, 33.3, 35.3, 35.55),
    ("haifa",        32.5, 32.9, 34.9, 35.15),
    ("shomron",      32.1, 32.5, 34.9, 35.35),
    ("tel_aviv",     31.9, 32.2, 34.7, 34.95),
    ("center",       31.7, 32.1, 34.8, 35.1),
    ("jerusalem",    31.6, 31.95, 34.9, 35.35),
    ("south_west",   30.8, 31.6,  34.4, 34.9),
    ("beer_sheva",   30.5, 31.0,  34.5, 35.1),
    ("negev",        29.5, 30.5,  34.6, 35.2),
    ("eilat",        29.4, 29.8,  34.9, 35.05),
]

CONGESTION_MAP = {1: "free", 2: "minor", 3: "moderate", 4: "heavy", 5: "blocked"}


# ── HERE API fetch ────────────────────────────────────────────────────────────

def fetch_tile(session, tile_name: str, lat_min, lat_max, lon_min, lon_max) -> list:
    bbox = f"{lon_min},{lat_min},{lon_max},{lat_max}"
    try:
        resp = session.get(
            "https://data.traffic.hereapi.com/v7/flow",
            params={
                "apiKey":              HERE_API_KEY,
                "in":                  f"bbox:{bbox}",
                "locationReferencing": "shape",
                "lang":                "en-US",
            },
            timeout=20,
        )
        if resp.status_code == 401:
            log.error("HERE API: Invalid API key (401) — check HERE_API_KEY in .env")
            return []
        if resp.status_code == 429:
            log.warning("HERE API: Rate limit (429) — too many requests")
            return []
        resp.raise_for_status()
        results = resp.json().get("results", [])
        log.info(f"  Tile {tile_name}: {len(results)} segments from HERE")
        return [_normalize(r, tile_name) for r in results if r]
    except Exception as e:
        log.warning(f"  Tile {tile_name} failed: {e}")
        return []


def _normalize(result: dict, tile_name: str) -> dict:
    now          = datetime.now(IL_TZ)
    location     = result.get("location", {})
    description  = location.get("description", "")
    shape        = location.get("shape", {})
    links        = shape.get("links", [{}])
    first_link   = links[0] if links else {}
    points       = first_link.get("points", [{}])
    mid          = points[len(points) // 2] if points else {}

    cf           = result.get("currentFlow", {})
    speed        = cf.get("speed", 0)
    free_flow    = cf.get("freeFlow", 0)
    jam_factor   = cf.get("jamFactor", 0)
    confidence   = cf.get("confidence", 0)
    traversability = cf.get("traversability", "open")

    cong_raw  = cf.get("congestion", {})
    cong_val  = cong_raw.get("value", 1) if isinstance(cong_raw, dict) else 1
    congestion = CONGESTION_MAP.get(cong_val, "unknown")

    speed_ratio = round(speed / free_flow, 3) if free_flow > 0 else 0
    delay_min   = round((60 / speed - 60 / free_flow), 2) if speed > 0 and free_flow > 0 else 0

    if jam_factor >= 8:   severity = "critical"
    elif jam_factor >= 6: severity = "severe"
    elif jam_factor >= 4: severity = "moderate"
    elif jam_factor >= 2: severity = "minor"
    else:                 severity = "free"

    return {
        "segment_id":     result.get("id", ""),
        "tile_name":      tile_name,
        "description":    description,
        "road_name":      description.split("/")[0].strip() if "/" in description else description,
        "lat":            mid.get("lat"),
        "lon":            mid.get("lng"),
        "speed_kmh":      speed,
        "free_flow_kmh":  free_flow,
        "speed_ratio":    speed_ratio,
        "jam_factor":     jam_factor,
        "congestion":     congestion,
        "severity":       severity,
        "confidence":     confidence,
        "traversability": traversability,
        "is_congested":   jam_factor >= 4,
        "is_blocked":     traversability == "closed" or jam_factor >= 9,
        "delay_min_per_km": max(delay_min, 0),
        "hour_of_day":    now.hour,
        "day_of_week":    now.strftime("%A"),
        "recorded_at":    now.isoformat(),
        "source":         "here_traffic_v7",
    }


def fetch_all_israel() -> list:
    """Fetch traffic for all Israel tiles. Returns flat list of records."""
    if not HERE_API_KEY:
        log.error("HERE_API_KEY is not set in .env — cannot fetch real traffic data")
        log.info("Falling back to sample data for demonstration")
        return _sample_traffic_data()

    import requests
    session = requests.Session()
    session.headers["Accept"] = "application/json"

    all_records = []
    for tile in ISRAEL_TILES:
        name, lat_min, lat_max, lon_min, lon_max = tile
        records = fetch_tile(session, name, lat_min, lat_max, lon_min, lon_max)
        all_records.extend(records)

    log.info(f"Total segments fetched: {len(all_records)}")

    if not all_records:
        log.warning("HERE returned 0 records — using sample data")
        return _sample_traffic_data()

    return all_records


def _sample_traffic_data() -> list:
    """Generate realistic sample traffic data for all Israel tiles."""
    import random
    now  = datetime.now(IL_TZ)
    hour = now.hour
    # Rush hours have more congestion
    is_rush = 7 <= hour <= 9 or 16 <= hour <= 19

    SAMPLE_ROADS = [
        ("tel_aviv",  "Ayalon Highway",  32.07, 34.79),
        ("tel_aviv",  "Geha Road",            32.08, 34.85),
        ("tel_aviv",  "Begin Road",           32.06, 34.78),
        ("tel_aviv",  "Ibn Gabirol Street",         32.08, 34.78),
        ("tel_aviv",  "Dizengoff Street",      32.08, 34.77),
        ("center",    "Route 1",                31.97, 34.90),
        ("center",    "Route 4",                31.95, 34.87),
        ("center",    "Route 40",              31.80, 34.72),
        ("jerusalem", "Route 1 Jerusalem", 31.79, 35.20),
        ("jerusalem", "Begin Boulevard",    31.77, 35.21),
        ("haifa",     "Route 2 Haifa",    32.82, 34.99),
        ("haifa",     "Route 22",              32.78, 35.02),
        ("galil_west","Route 85",              32.90, 35.12),
        ("south_west","Route 6",                31.50, 34.78),
        ("beer_sheva","Route 40 South",  31.25, 34.80),
    ]

    records = []
    for i, (tile, desc, lat, lon) in enumerate(SAMPLE_ROADS):
        jam = random.uniform(4, 9) if is_rush else random.uniform(0, 5)
        jam = round(jam, 1)
        free_flow = random.uniform(80, 120)
        ratio     = max(0.1, 1 - jam / 12)
        speed     = round(free_flow * ratio, 1)

        if jam >= 8:   cong, sev = "blocked",  "critical"
        elif jam >= 6: cong, sev = "heavy",    "severe"
        elif jam >= 4: cong, sev = "moderate", "moderate"
        elif jam >= 2: cong, sev = "minor",    "minor"
        else:          cong, sev = "free",     "free"

        records.append({
            "segment_id":       f"SAMPLE_{i:04d}",
            "tile_name":        tile,
            "description":      desc,
            "road_name":        desc.split("/")[0].strip(),
            "lat":              lat + random.uniform(-0.01, 0.01),
            "lon":              lon + random.uniform(-0.01, 0.01),
            "speed_kmh":        speed,
            "free_flow_kmh":    round(free_flow, 1),
            "speed_ratio":      round(ratio, 3),
            "jam_factor":       jam,
            "congestion":       cong,
            "severity":         sev,
            "confidence":       round(random.uniform(0.7, 1.0), 2),
            "traversability":   "closed" if jam >= 9.5 else "open",
            "is_congested":     jam >= 4,
            "is_blocked":       jam >= 9.5,
            "delay_min_per_km": round(max(0, (60 / speed - 60 / free_flow)), 2) if speed > 0 else 0,
            "hour_of_day":      now.hour,
            "day_of_week":      now.strftime("%A"),
            "recorded_at":      now.isoformat(),
            "source":           "sample_data",
        })

    log.info(f"Generated {len(records)} sample traffic records (rush_hour={is_rush})")
    return records


# ── MinIO upload ──────────────────────────────────────────────────────────────

def get_minio_client():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
        log.info(f"Bucket '{bucket}' exists")
    except Exception:
        s3.create_bucket(Bucket=bucket)
        log.info(f"Created bucket '{bucket}'")


def upload_records(s3, records: list, prefix: str, label: str) -> str:
    """Upload records as a single JSON file under time-partitioned path."""
    now = datetime.now(IL_TZ)
    partition = (
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}"
    )
    key  = f"{prefix}/{partition}/{label}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    body = json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")

    s3.put_object(
        Bucket=TRAFFIC_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata={
            "record_count": str(len(records)),
            "source":       records[0].get("source", "here") if records else "unknown",
            "tile_count":   str(len({r["tile_name"] for r in records})),
        },
    )
    log.info(f"  Uploaded {len(records)} records → s3://{TRAFFIC_BUCKET}/{key}")
    return f"s3://{TRAFFIC_BUCKET}/{key}"


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> dict:
    log.info("=" * 60)
    log.info(f"Traffic → MinIO  |  bucket={TRAFFIC_BUCKET}  |  endpoint={MINIO_ENDPOINT}")
    log.info(f"HERE_API_KEY={'SET (' + HERE_API_KEY[:8] + '...)' if HERE_API_KEY else 'NOT SET — will use sample data'}")
    log.info("=" * 60)

    # 1. Fetch
    records = fetch_all_israel()
    if not records:
        log.error("No traffic records fetched — nothing to upload")
        return {"records": 0, "uploaded": []}

    log.info(f"Fetched {len(records)} records total")

    if dry_run:
        log.info("DRY RUN — skipping MinIO upload")
        # Print a sample
        for r in records[:3]:
            print(json.dumps(r, ensure_ascii=False, indent=2))
        return {"records": len(records), "uploaded": []}

    # 2. Connect + ensure bucket exists
    try:
        s3 = get_minio_client()
        ensure_bucket(s3, TRAFFIC_BUCKET)
    except Exception as e:
        log.error(f"Cannot connect to MinIO at {MINIO_ENDPOINT}: {e}")
        log.error("Make sure Docker is running: docker-compose up -d minio")
        return {"records": len(records), "uploaded": [], "error": str(e)}

    uploaded = []

    # 3. Upload raw records
    try:
        key = upload_records(s3, records, "raw", "traffic_raw")
        uploaded.append(key)
    except Exception as e:
        log.error(f"Raw upload failed: {e}")

    # 4. Upload processed (with ETL enrichment)
    try:
        from etl.traffic_transformer import TrafficTransformer
        transformer = TrafficTransformer()
        processed = []
        for r in records:
            try:
                processed.append(transformer.transform(r))
            except Exception as ex:
                log.warning(f"Transform error: {ex}")

        if processed:
            key = upload_records(s3, processed, "processed", "traffic_processed")
            uploaded.append(key)
            log.info(f"Processed {len(processed)} records with ETL transformer")
    except ImportError:
        log.warning("ETL transformer not available — skipping processed upload")

    # 5. Summary
    log.info("=" * 60)
    log.info(f"Done. {len(uploaded)} files written to bucket '{TRAFFIC_BUCKET}':")
    for k in uploaded:
        log.info(f"  {k}")

    # Tile breakdown
    from collections import Counter
    tile_counts = Counter(r["tile_name"] for r in records)
    log.info("Tile breakdown:")
    for tile, count in sorted(tile_counts.items()):
        log.info(f"  {tile:20s}: {count} segments")

    congestion_counts = Counter(r["congestion"] for r in records)
    log.info(f"Congestion: {dict(congestion_counts)}")

    return {"records": len(records), "uploaded": uploaded}


def traffic_to_minio_task(**context):
    """Airflow PythonOperator callable."""
    result = run(dry_run=False)
    if context.get("ti"):
        context["ti"].xcom_push(key="traffic_records", value=result["records"])
        context["ti"].xcom_push(key="traffic_files",   value=result.get("uploaded", []))
    return result["records"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch HERE traffic data → MinIO TRAFIC-DATA")
    parser.add_argument("--dry-run", action="store_true", help="Fetch only, do not upload")
    parser.add_argument("--bucket",  default=None, help=f"Override bucket name (default: {TRAFFIC_BUCKET})")
    args = parser.parse_args()

    if args.bucket:
        TRAFFIC_BUCKET = args.bucket

    result = run(dry_run=args.dry_run)
    sys.exit(0 if result.get("uploaded") or args.dry_run else 1)
