#!/usr/bin/env python3
"""
fix_backfill_processed.py - Copy backfilled raw data to processed paths
AND backfill train positions from Stride (operator_ref=2).
One-time script.
"""
import json, os, sys, logging, time
from datetime import datetime, timedelta, timezone

sys.stdout = __import__('io').TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("fix_processed")

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
IL_TZ = ZoneInfo("Asia/Jerusalem")

MINIO_ENDPOINT = "http://localhost:9000"
BUCKET = "israel-transit-lake"
STRIDE_URL = "https://open-bus-stride-api.hasadna.org.il"

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
        aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin123",
        config=BotoConfig(signature_version="s3v4", connect_timeout=5, read_timeout=30))

def day_has_data(s3, prefix_template, dt):
    prefix = prefix_template.format(y=dt.year, m=f"{dt.month:02d}", d=f"{dt.day:02d}")
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
                log.warning(f"  Stride failed: {e}")
                return []

def upload_json(s3, records, key):
    body = json.dumps(records, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json",
                  Metadata={"record_count": str(len(records)), "source": "backfill_fix"})

def backfill_day_processed(s3, day_dt, day_num, total):
    day_str = day_dt.strftime("%d/%m/%Y")
    log.info(f"  [{day_num}/{total}] {day_str}...")

    day_start = day_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    buses_total = 0
    trains_total = 0

    for hour in range(24):
        h_start = day_start + timedelta(hours=hour)
        h_end = h_start + timedelta(hours=1)
        h_start_utc = h_start.astimezone(timezone.utc)
        h_end_utc = h_end.astimezone(timezone.utc)

        il_time = h_start
        base_path = f"year={il_time.year}/month={il_time.month:02d}/day={il_time.day:02d}/hour={il_time.hour:02d}"
        ts_str = il_time.strftime('%Y%m%d_%H0000')

        # 1. Bus positions -> processed/bus-positions/
        buses = stride_fetch("siri_vehicle_locations/list", {
            "recorded_at_time_from": h_start_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "recorded_at_time_to": h_end_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "limit": 500,
            "order_by": "recorded_at_time desc",
        })
        if buses:
            processed_buses = []
            for r in buses:
                op_ref = r.get("siri_route__operator_ref")
                op_id = str(op_ref).strip() if op_ref not in (None, "") else ""
                processed_buses.append({
                    "vehicle_id": r.get("siri_ride__vehicle_ref", ""),
                    "lat": r.get("lat"), "lon": r.get("lon"),
                    "bearing": r.get("bearing"), "velocity": r.get("velocity"),
                    "line_ref": r.get("siri_route__line_ref"),
                    "operator_ref": op_ref,
                    "operator_id": op_id,
                    "operator_name": _OPERATOR_NAMES.get(op_id, "Unknown"),
                    "recorded_at": r.get("recorded_at_time"),
                    "source": "stride_backfill",
                })
            key = f"processed/bus-positions/{base_path}/buses_{ts_str}_backfill.json"
            upload_json(s3, processed_buses, key)
            buses_total += len(processed_buses)

        # 2. Train positions (operator_ref=2) -> processed/train-positions/ AND raw/train-positions/
        trains = stride_fetch("siri_vehicle_locations/list", {
            "siri_routes__operator_ref": 2,
            "recorded_at_time_from": h_start_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "recorded_at_time_to": h_end_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "limit": 500,
            "order_by": "recorded_at_time desc",
        })
        if trains:
            processed_trains = [{
                "vehicle_id": r.get("siri_ride__vehicle_ref", ""),
                "lat": r.get("lat"), "lon": r.get("lon"),
                "bearing": r.get("bearing"), "velocity": r.get("velocity"),
                "line_ref": r.get("siri_route__line_ref"),
                "operator_ref": 2,
                "recorded_at": r.get("recorded_at_time"),
                "source": "stride_backfill",
            } for r in trains]
            for pfx in ["processed/train-positions", "raw/train-positions"]:
                key = f"{pfx}/{base_path}/trains_{ts_str}_backfill.json"
                upload_json(s3, processed_trains, key)
            trains_total += len(processed_trains)

        time.sleep(0.2)

    log.info(f"    -> {day_str}: {buses_total} buses, {trains_total} trains")
    return buses_total, trains_total

def main():
    log.info("=" * 60)
    log.info("  FIX BACKFILL — filling processed/ paths + train data")
    log.info("=" * 60)

    s3 = get_s3()

    # Find days missing from processed/bus-positions AND processed/train-positions
    start_date = datetime(2026, 3, 15, tzinfo=IL_TZ)
    end_date = datetime(2026, 5, 13, tzinfo=IL_TZ)

    missing = []
    current = start_date
    while current < end_date:
        bus_ok = day_has_data(s3, "processed/bus-positions/year={y}/month={m}/day={d}/", current)
        train_ok = day_has_data(s3, "processed/train-positions/year={y}/month={m}/day={d}/", current)
        if not bus_ok or not train_ok:
            missing.append(current)
        current += timedelta(days=1)

    if not missing:
        log.info("  No missing days in processed/!")
        return

    log.info(f"  Found {len(missing)} days missing from processed/:")
    for d in missing:
        log.info(f"    - {d.strftime('%d/%m/%Y (%A)')}")

    total_buses = 0
    total_trains = 0

    for i, day in enumerate(missing, 1):
        b, t = backfill_day_processed(s3, day, i, len(missing))
        total_buses += b
        total_trains += t

    log.info("")
    log.info(f"  {'=' * 40}")
    log.info(f"  FIX COMPLETE")
    log.info(f"  Days fixed:    {len(missing)}")
    log.info(f"  Bus records:   {total_buses}")
    log.info(f"  Train records: {total_trains}")
    log.info(f"  {'=' * 40}")

if __name__ == "__main__":
    main()
