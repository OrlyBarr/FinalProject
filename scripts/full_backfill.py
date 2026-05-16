#!/usr/bin/env python3
"""
full_backfill.py - One-time script to fill ALL missing days from Stride API.
"""
import json, os, sys, logging, time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("full_backfill")

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

# operator_ref → operator_name (kept in sync with storage/direct_to_minio.py)
_OPERATOR_NAMES = {
    "3": "Dan", "4": "Egged", "5": "Egged", "6": "Egged Taavura",
    "7": "Metropoline", "14": "Nateev Express", "15": "Nateev Express",
    "16": "Kavim", "18": "Galim", "23": "Superbus", "25": "Egged",
    "31": "Nadan", "32": "KBS", "34": "Afikim", "37": "Electra Afikim",
    "42": "Malam", "44": "GB Tours", "52": "Gush Dan Bus",
    "40": "Kavim Hatichon", "135": "Tnufa",
}

import boto3
from botocore.config import Config as BotoConfig
import urllib.request, urllib.parse

def get_s3():
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS, aws_secret_access_key=MINIO_SECRET,
        config=BotoConfig(signature_version="s3v4", connect_timeout=5, read_timeout=30))

def day_has_data(s3, dt):
    prefix = f"raw/bus-positions/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    try:
        r = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
        return r.get("KeyCount", 0) > 0
    except:
        return False

def stride_fetch(endpoint, params, timeout=30, retries=3):
    url = f"{STRIDE_URL}/{endpoint}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"accept": "application/json"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read())
                return data if isinstance(data, list) else []
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
            else:
                log.warning(f"  Stride {endpoint} failed after {retries} retries: {e}")
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
    out = []
    for r in records:
        op_ref = r.get("siri_route__operator_ref")
        op_id = str(op_ref).strip() if op_ref not in (None, "") else ""
        out.append({
            "vehicle_id": r.get("siri_ride__vehicle_ref", ""),
            "lat": r.get("lat"), "lon": r.get("lon"),
            "bearing": r.get("bearing"), "velocity": r.get("velocity"),
            "line_ref": r.get("siri_route__line_ref"),
            "operator_ref": op_ref,
            "operator_id": op_id,
            "operator_name": _OPERATOR_NAMES.get(op_id, "Unknown"),
            "route_id": r.get("siri_route__id"),
            "recorded_at": r.get("recorded_at_time"),
            "source": "stride_backfill",
        })
    return out

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
    il_time = ts.astimezone(IL_TZ)
    key = (f"{prefix}/year={il_time.year}/month={il_time.month:02d}/"
           f"day={il_time.day:02d}/hour={il_time.hour:02d}/"
           f"{label}_{il_time.strftime('%Y%m%d_%H%M%S')}_backfill.json")
    body = json.dumps(records, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json",
                  Metadata={"record_count": str(len(records)), "source": "full_backfill"})
    return key

def backfill_day(s3, day_dt, day_num, total_days):
    day_str = day_dt.strftime("%d/%m/%Y")
    log.info(f"")
    log.info(f"  [{day_num}/{total_days}] Backfilling {day_str}...")

    day_start = day_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    chunk_size = timedelta(hours=1)  # 1-hour chunks to reduce API calls
    current = day_start
    day_buses = 0
    day_rides = 0

    while current < day_end:
        chunk_end = min(current + chunk_size, day_end)
        hour_label = f"{current.strftime('%H:%M')}-{chunk_end.strftime('%H:%M')}"

        buses = fetch_bus_positions(current, chunk_end)
        if buses:
            upload_records(s3, buses, "raw/bus-positions", "buses", chunk_end)
            day_buses += len(buses)

        rides = fetch_rides(current, chunk_end)
        if rides:
            upload_records(s3, rides, "raw/service-alerts", "alerts", chunk_end)
            day_rides += len(rides)

        current = chunk_end
        time.sleep(0.3)  # be nice to Stride API

    log.info(f"    -> {day_str}: {day_buses} buses, {day_rides} rides")
    return day_buses, day_rides

def main():
    log.info("=" * 60)
    log.info("  FULL BACKFILL - filling all missing days")
    log.info("=" * 60)

    s3 = get_s3()

    # Build list of all days from March 14 to May 7
    start_date = datetime(2026, 3, 15, tzinfo=IL_TZ)
    end_date = datetime(2026, 5, 8, tzinfo=IL_TZ)

    # Find missing days
    missing = []
    current = start_date
    while current < end_date:
        if not day_has_data(s3, current):
            missing.append(current)
        current += timedelta(days=1)

    if not missing:
        log.info("  No missing days found!")
        return

    log.info(f"  Found {len(missing)} missing days:")
    for d in missing:
        log.info(f"    - {d.strftime('%d/%m/%Y (%A)')}")

    total_buses = 0
    total_rides = 0

    for i, day in enumerate(missing, 1):
        b, r = backfill_day(s3, day, i, len(missing))
        total_buses += b
        total_rides += r

    log.info(f"")
    log.info(f"  {'=' * 40}")
    log.info(f"  FULL BACKFILL COMPLETE")
    log.info(f"  Days filled:  {len(missing)}")
    log.info(f"  Bus records:  {total_buses}")
    log.info(f"  Ride records: {total_rides}")
    log.info(f"  {'=' * 40}")

if __name__ == "__main__":
    main()
