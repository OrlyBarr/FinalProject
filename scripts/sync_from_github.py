#!/usr/bin/env python3
"""
scripts/sync_from_github.py
Import latest data from the data-buffer/ directory (populated by GitHub Actions)
into MinIO, after a git pull has already fetched the latest snapshot.

Called by resilient_pipeline_run.sh Step 0 on VM wake-up.
Only imports a snapshot if its collected_at timestamp is newer than the last
successful sync (tracked in data-buffer/.last_sync).

Paths written to MinIO (bucket: israel-transit-lake):
  raw/bus-positions/year=.../month=.../day=.../hour=.../github_sync_<ts>.json
  raw/traffic-data/year=.../month=.../day=.../hour=.../github_sync_<ts>.json

Usage:
  python3 scripts/sync_from_github.py
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("sync_from_github")

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(os.getenv("PROJECT", "/home/local_admin/finalproject"))
DATA_BUFFER    = BASE_DIR / "data-buffer"
LAST_SYNC_FILE = DATA_BUFFER / ".last_sync"
LOG_FILE       = Path(os.getenv("LOG", "/home/local_admin/finalproject/pipeline_host.log"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")
BUCKET         = os.getenv("S3_BUCKET_NAME",    "israel-transit-lake")


def _log(msg: str) -> None:
    log.info(msg)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(f"[sync_from_github] {msg}\n")
    except Exception:
        pass


def _read_json(path: Path):
    """Read a JSON file; return None on any error."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        _log(f"Cannot read {path}: {e}")
        return None


def _get_last_sync_ts() -> str:
    """Return the ISO timestamp of the last successful sync, or empty string."""
    if LAST_SYNC_FILE.exists():
        try:
            return LAST_SYNC_FILE.read_text(encoding="utf-8").strip()
        except Exception:
            pass
    return ""


def _save_last_sync_ts(ts: str) -> None:
    try:
        LAST_SYNC_FILE.write_text(ts, encoding="utf-8")
    except Exception as e:
        _log(f"Could not write .last_sync: {e}")


def _get_s3():
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


def _ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        try:
            s3.create_bucket(Bucket=BUCKET)
            _log(f"Created bucket '{BUCKET}'")
        except Exception as e:
            _log(f"Could not create bucket: {e}")


def _upload(s3, records: list, prefix: str, ts_label: str) -> bool:
    """Upload records JSON to a time-partitioned MinIO path. Returns True on success."""
    if not records:
        _log(f"  {prefix}: 0 records — skipping upload")
        return True
    now = datetime.now(timezone.utc)
    key = (
        f"{prefix}/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"github_sync_{ts_label}.json"
    )
    body = json.dumps(records, ensure_ascii=False).encode("utf-8")
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=body,
            ContentType="application/json",
            Metadata={"record_count": str(len(records)), "source": "github-actions-sync"},
        )
        _log(f"  Uploaded {len(records)} records → s3://{BUCKET}/{key}")
        return True
    except Exception as e:
        _log(f"  MinIO upload failed for {prefix}: {e}")
        return False


def main() -> int:
    _log("=== sync_from_github start ===")

    # ── Sanity checks ─────────────────────────────────────────────────────────
    if not DATA_BUFFER.is_dir():
        _log(f"data-buffer directory not found: {DATA_BUFFER} — nothing to sync")
        return 0

    meta_path    = DATA_BUFFER / "meta.json"
    buses_path   = DATA_BUFFER / "buses_latest.json"
    traffic_path = DATA_BUFFER / "traffic_latest.json"

    meta = _read_json(meta_path)
    if meta is None:
        _log("meta.json missing or unreadable — cannot determine freshness, skipping")
        return 0

    collected_at: str = meta.get("collected_at", "")
    if not collected_at:
        _log("meta.json has no collected_at — skipping")
        return 0

    # ── Freshness check ───────────────────────────────────────────────────────
    last_sync = _get_last_sync_ts()
    if last_sync and collected_at <= last_sync:
        _log(
            f"Snapshot not newer than last sync "
            f"(collected_at={collected_at}, last_sync={last_sync}) — skipping"
        )
        return 0

    _log(f"New snapshot detected: collected_at={collected_at} (last_sync={last_sync or 'never'})")
    _log(f"  bus_count={meta.get('bus_count', '?')}, traffic_count={meta.get('traffic_count', '?')}")

    # ── Load data files ───────────────────────────────────────────────────────
    buses   = _read_json(buses_path)   or []
    traffic = _read_json(traffic_path) or []

    if not buses and not traffic:
        _log("Both buses and traffic are empty — skipping MinIO upload")
        # Still mark as synced so we don't retry the same empty snapshot
        _save_last_sync_ts(collected_at)
        return 0

    # ── Connect to MinIO ──────────────────────────────────────────────────────
    try:
        s3 = _get_s3()
        _ensure_bucket(s3)
    except Exception as e:
        _log(f"Cannot connect to MinIO ({MINIO_ENDPOINT}): {e}")
        _log("Will retry next pipeline run")
        return 1

    # Safe timestamp label for filenames (colons → underscores)
    ts_label = collected_at.replace(":", "_").replace("+", "_")

    # ── Upload ────────────────────────────────────────────────────────────────
    ok_buses   = _upload(s3, buses,   "raw/bus-positions", ts_label)
    ok_traffic = _upload(s3, traffic, "raw/traffic-data",  ts_label)

    if ok_buses and ok_traffic:
        _save_last_sync_ts(collected_at)
        _log(f"Sync complete: {len(buses)} buses, {len(traffic)} traffic segments imported")
    else:
        _log("Partial upload failure — .last_sync NOT updated; will retry next run")
        return 1

    _log("=== sync_from_github done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
