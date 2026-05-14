#!/usr/bin/env python3
"""
wake_backfill.py - Runs every 15 min via cron.
Scans the last 24h hour-by-hour, detects gaps, and backfills from Stride API.
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


def find_gaps(s3):
    """Scan last 24h hour-by-hour and return list of (start, end) gaps."""
    now = datetime.now(IL_TZ)
    gaps = []
    gap_start = None

    for hours_ago in range(24, 0, -1):
        check_time = now - timedelta(hours=hours_ago)
        prefix = (
            f"raw/bus-positions/year={check_time.year}/"
            f"month={check_time.month:02d}/day={check_time.day:02d}/"
            f"hour={check_time.hour:02d}/"
        )
        try:
            r = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
            has_data = r.get("KeyCount", 0) > 0
        except Exception:
            has_data = False

        if not has_data:
            if gap_start is None:
                gap_start = check_time.replace(minute=0, second=0, microsecond=0)
        else:
            if gap_start is not None:
                gap_end = check_time.replace(minute=0, second=0, microsecond=0)
                gaps.append((gap_start, gap_end))
                gap_start = None

    # Close trailing gap
    if gap_start is not None:
        gap_end = now.replace(minute=0, second=0, microsecond=0)
        gaps.append((gap_start, gap_end))

    return gaps


def stride_fetch(endpoint, params, timeout=30):
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
    from_utc = from_dt.astimezone(timezone.utc)
    to_utc = to_dt.astimezone(timezone.utc)
    records = stride_fetch("siri_vehicle_locations/list", {
        "recorded_at_time_from": from_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "recorded_at_time_to": to_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "limit": 500,
        "order_by": "recorded_at_time desc",
    })
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
    from_utc = from_dt.astimezone(timezone.utc)
    to_utc = to_dt.astimezone(timezone.utc)
    return stride_fetch("siri_rides/list", {
        "scheduled_start_time_from": from_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "scheduled_start_time_to": to_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "limit": 200,
        "order_by": "scheduled_start_time desc",
    })


def upload_records(s3, records, prefix, label, ts):
    il_time = ts if ts.tzinfo and str(ts.tzinfo) == "Asia/Jerusalem" else ts.astimezone(IL_TZ)
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
    log.info("  Wake Backfill - scanning last 24h for gaps")
    log.info("=" * 60)

    s3 = get_s3()
    gaps = find_gaps(s3)

    if not gaps:
        log.info("  No gaps found in last 24h - all good!")
        return

    log.info(f"  Found {len(gaps)} gap(s):")
    for i, (gs, ge) in enumerate(gaps):
        duration = (ge - gs).total_seconds() / 60
        log.info(f"    Gap {i+1}: {gs.strftime('%d/%m %H:%M')} -> {ge.strftime('%d/%m %H:%M')} ({duration:.0f} min)")

    total_buses = 0
    total_rides = 0
    total_chunks = 0

    for gap_start, gap_end in gaps:
        duration = (gap_end - gap_start).total_seconds() / 60
        if duration < 10:
            log.info(f"  Skipping tiny gap ({duration:.0f} min)")
            continue
        if duration > 720:
            log.warning(f"  Gap too large ({duration:.0f} min), limiting to 12h")
            gap_start = gap_end - timedelta(hours=12)

        log.info(f"  Backfilling {gap_start.strftime('%d/%m %H:%M')} -> {gap_end.strftime('%d/%m %H:%M')}...")

        chunk_size = timedelta(minutes=15)
        current = gap_start
        while current < gap_end:
            chunk_end = min(current + chunk_size, gap_end)
            chunk_label = f"{current.strftime('%H:%M')}-{chunk_end.strftime('%H:%M')}"

            buses = fetch_bus_positions(current, chunk_end)
            if buses:
                upload_records(s3, buses, "raw/bus-positions", "buses", chunk_end)
                total_buses += len(buses)
                log.info(f"    [{chunk_label}] {len(buses)} bus positions")

            rides = fetch_rides(current, chunk_end)
            if rides:
                upload_records(s3, rides, "raw/service-alerts", "alerts", chunk_end)
                total_rides += len(rides)
                log.info(f"    [{chunk_label}] {len(rides)} ride records")

            current = chunk_end
            total_chunks += 1

    log.info(f"")
    log.info(f"  === Backfill complete ===")
    log.info(f"  Buses:  {total_buses} records")
    log.info(f"  Rides:  {total_rides} records")
    log.info(f"  Chunks: {total_chunks}")


if __name__ == "__main__":
    main()
