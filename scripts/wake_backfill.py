#!/usr/bin/env python3
"""
wake_backfill.py - Run after VM resume from sleep.
Detects the gap since last data collection and backfills from Stride API.
Usage: python3 scripts/wake_backfill.py
"""
import json
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("wake_backfill")

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
IL_TZ = ZoneInfo("Asia/Jerusalem")

# Force localhost for host-side scripts (Docker uses minio:9000)
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET         = os.getenv("S3_BUCKET_NAME", "israel-transit-lake")
STRIDE_URL     = "https://open-bus-stride-api.hasadna.org.il"

import boto3
from botocore.config import Config as BotoConfig
import urllib.request
import urllib.parse


def get_s3():
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS, aws_secret_access_key=MINIO_SECRET,
        config=BotoConfig(signature_version="s3v4", connect_timeout=5, read_timeout=30))


def get_last_upload_time(s3):
    """Find the most recent file upload time in MinIO."""
    now = datetime.now(IL_TZ)
    for day_offset in range(2):
        d = now - timedelta(days=day_offset)
        prefix = f"raw/bus-positions/year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        try:
            r = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=500)
            objs = [o for o in r.get("Contents", []) if o["Key"].endswith(".json")]
            if objs:
                latest = max(objs, key=lambda x: x["LastModified"])
                return latest["LastModified"].replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return datetime.now(timezone.utc) - timedelta(hours=1)


def stride_fetch(endpoint, params, timeout=30):
    """Generic Stride API fetch."""
    url = f"{STRIDE_URL}/{endpoint}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
            return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"  Stride {endpoint} failed: {e}")
        return []


def fetch_bus_positions(from_dt, to_dt):
    """Fetch bus positions from Stride siri_vehicle_locations."""
    records = stride_fetch("siri_vehicle_locations/list", {
        "recorded_at_time_from": from_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "recorded_at_time_to": to_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "limit": 500,
        "order_by": "recorded_at_time desc",
    })
    # Transform to our format
    buses = []
    for r in records:
        buses.append({
            "vehicle_id": r.get("siri_ride__vehicle_ref", ""),
            "lat": r.get("lat"),
            "lon": r.get("lon"),
            "bearing": r.get("bearing"),
            "velocity": r.get("velocity"),
            "line_ref": r.get("siri_route__line_ref"),
            "operator_ref": r.get("siri_route__operator_ref"),
            "route_id": r.get("siri_route__id"),
            "recorded_at": r.get("recorded_at_time"),
            "source": "stride_backfill",
        })
    return buses


def fetch_rides(from_dt, to_dt):
    """Fetch ride data for alerts/delays."""
    return stride_fetch("siri_rides/list", {
        "scheduled_start_time_from": from_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "scheduled_start_time_to": to_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "limit": 200,
        "order_by": "scheduled_start_time desc",
    })


def upload_records(s3, records, prefix, label, ts):
    """Upload records to MinIO with proper time partitioning."""
    il_time = ts.astimezone(IL_TZ)
    key = (
        f"{prefix}/"
        f"year={il_time.year}/month={il_time.month:02d}/"
        f"day={il_time.day:02d}/hour={il_time.hour:02d}/"
        f"{label}_{il_time.strftime('%Y%m%d_%H%M%S')}_backfill.json"
    )
    body = json.dumps(records, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json",
                  Metadata={"record_count": str(len(records)), "source": "wake_backfill"})
    return key


def main():
    log.info("=" * 60)
    log.info("  Wake Backfill - checking for data gaps")
    log.info("=" * 60)

    s3 = get_s3()
    now = datetime.now(timezone.utc)
    last_upload = get_last_upload_time(s3)
    gap_minutes = (now - last_upload).total_seconds() / 60

    log.info(f"  Last data: {last_upload.isoformat()}")
    log.info(f"  Now:       {now.isoformat()}")
    log.info(f"  Gap:       {gap_minutes:.0f} minutes")

    if gap_minutes < 5:
        log.info("  No significant gap - pipeline is current!")
        return

    if gap_minutes > 720:
        log.warning(f"  Gap too large ({gap_minutes:.0f} min). Limiting to 12 hours.")
        last_upload = now - timedelta(hours=12)
        gap_minutes = 720

    log.info(f"  Backfilling {gap_minutes:.0f} minutes of data...")

    # Split into 15-minute chunks
    chunk_size = timedelta(minutes=15)
    current = last_upload
    total_buses = 0
    total_rides = 0
    chunks_done = 0

    while current < now:
        chunk_end = min(current + chunk_size, now)
        chunk_label = f"{current.strftime('%H:%M')}-{chunk_end.strftime('%H:%M')}"

        # Bus positions
        buses = fetch_bus_positions(current, chunk_end)
        if buses:
            key = upload_records(s3, buses, "raw/bus-positions", "buses", chunk_end)
            total_buses += len(buses)
            log.info(f"  [{chunk_label}] {len(buses)} bus positions uploaded")

        # Rides/alerts
        rides = fetch_rides(current, chunk_end)
        if rides:
            key = upload_records(s3, rides, "raw/service-alerts", "alerts", chunk_end)
            total_rides += len(rides)
            log.info(f"  [{chunk_label}] {len(rides)} ride records uploaded")

        current = chunk_end
        chunks_done += 1

    log.info(f"")
    log.info(f"  === Backfill complete ===")
    log.info(f"  Buses:  {total_buses} records")
    log.info(f"  Rides:  {total_rides} records")
    log.info(f"  Chunks: {chunks_done}")


if __name__ == "__main__":
    main()
