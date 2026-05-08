"""
storage/direct_to_minio.py
Direct fetch → MinIO writer for ALL transit data types.
Bypasses Kafka entirely — writes straight to israel-transit-lake.

Paths written (all inside bucket israel-transit-lake):
  raw/bus-positions/year=YYYY/month=MM/day=DD/hour=HH/<ts>.json
  processed/bus-positions/...
  raw/train-positions/...
  processed/train-positions/...
  raw/traffic-data/...
  processed/traffic-data/...
  raw/trip-updates/...        ← stop-level delays (Open Bus Stride)
  processed/trip-updates/...
  raw/service-alerts/...      ← significantly delayed / cancelled rides
  processed/service-alerts/...

Note: MOT GTFS-RT feeds (gtfs.mot.gov.il) are WAF-blocked for external IPs.
      Delays and alerts are derived from Open Bus Stride siri_ride_stops /
      siri_rides endpoints instead.

Usage:
  python3 storage/direct_to_minio.py            # all data types
  python3 storage/direct_to_minio.py --dry-run  # fetch only, no upload
  python3 storage/direct_to_minio.py --only buses
  python3 storage/direct_to_minio.py --only trains
  python3 storage/direct_to_minio.py --only traffic
  python3 storage/direct_to_minio.py --only delays
  python3 storage/direct_to_minio.py --only alerts

Can be called from Airflow: direct_to_minio_task(**context)
"""

import json
import os
import sys
import logging
import argparse
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
from pathlib import Path

# Israel time — automatically handles DST (UTC+2 winter / UTC+3 summer)
IL_TZ = ZoneInfo("Asia/Jerusalem")


def now_il() -> datetime:
    """Current datetime in Israel local time."""
    return datetime.now(IL_TZ)

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("direct_to_minio")

# ── Config ────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")
MAIN_BUCKET     = os.getenv("S3_BUCKET_NAME",    "israel-transit-lake")
HERE_API_KEY    = os.getenv("HERE_API_KEY",      "")

OPEN_BUS_URL    = "https://open-bus-stride-api.hasadna.org.il"

# Israel tiles for HERE traffic
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


# ── MinIO client ──────────────────────────────────────────────────────────────

def get_s3():
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


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=MAIN_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MAIN_BUCKET)
        log.info(f"Created bucket '{MAIN_BUCKET}'")


def upload(s3, records: list, prefix: str, label: str) -> str:
    """Upload list of records as JSON to time-partitioned S3 path (Israel time).

    Retries up to 4 times with exponential backoff (Layer 3 resilience).
    On total failure saves the payload to the resilient_pipeline pending dir
    so the background thread in ResilientMinioWriter can retry it later.
    """
    import time as _time
    now = now_il()
    key = (
        f"{prefix}/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"{label}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )
    body = json.dumps(records, ensure_ascii=False).encode("utf-8")  # compact JSON saves ~30% RAM vs indent=2
    path = f"s3://{MAIN_BUCKET}/{key}"

    # Skip retries if we already know MinIO is down — go straight to buffer
    import storage.direct_to_minio as _mod
    minio_up = getattr(_mod, '_MINIO_AVAILABLE', True)
    
    last_err = None
    max_attempts = 4 if minio_up else 1
    for attempt in range(max_attempts):
        try:
            s3.put_object(
                Bucket=MAIN_BUCKET,
                Key=key,
                Body=body,
                ContentType="application/json",
                Metadata={"record_count": str(len(records))},
            )
            log.info(f"  ✅ {len(records)} records → {path}")
            return path
        except Exception as e:
            last_err = e
            if not minio_up:
                break  # Don't waste time retrying when MinIO is known-down
            wait = min(2 ** attempt, 30)
            log.warning(f"  MinIO upload attempt {attempt+1}/{max_attempts} failed: {e}. Retry in {wait}s")
            _time.sleep(wait)

    # All retries exhausted — save locally for background retry by ResilientMinioWriter
    log.error(f"  MinIO upload failed after 4 attempts: {last_err}")
    try:
        # Prefer env var; fall back to resilient_pipeline default
        _buf_env = os.environ.get('BUFFER_DIR')
        if _buf_env:
            from pathlib import Path as _P
            BUFFER_DIR_PATH = _P(_buf_env)
        else:
            from resilient_pipeline import BUFFER_DIR as BUFFER_DIR_PATH
        pending = BUFFER_DIR_PATH / "minio_pending"
        pending.mkdir(parents=True, exist_ok=True)
        safe = key.replace("/", "_")
        (pending / f"{safe}.data").write_bytes(body)
        with open(pending / f"{safe}.meta.json", "w") as mf:
            json.dump({
                "bucket":   MAIN_BUCKET,
                "key":      key,
                "ctype":    "application/json",
                "saved_at": now.isoformat(),
            }, mf)
        log.warning(f"  ⚠️  Saved locally for background retry → {key}")
    except Exception as pe:
        log.error(f"  Failed to save locally: {pe}")
    return path


# ── Bus positions (Open Bus Stride SIRI) ─────────────────────────────────────

# Israeli public transit operator codes → human-readable names
_OPERATOR_NAMES = {
    "3":  "Dan",
    "4":  "Egged",
    "5":  "Egged",
    "6":  "Egged Taavura",
    "7":  "Metropoline",
    "14": "Nateev Express",
    "15": "Nateev Express",
    "16": "Kavim",
    "18": "Galim",
    "23": "Superbus",
    "25": "Egged",
    "31": "Nadan",
    "32": "KBS",
    "34": "Afikim",
    "37": "Electra Afikim",
    "42": "Malam",
    "44": "GB Tours",
    "52": "Gush Dan Bus",
}


def _get_last_stored_records(s3, prefix: str, now_ts: str) -> list:
    """
    Fallback: read the most recent raw JSON file from MinIO at *prefix*,
    update 'fetched_at' to now, add 'fallback': True, and return the records.
    Returns [] if nothing is stored yet.
    """
    try:
        r = s3.list_objects_v2(Bucket=MAIN_BUCKET, Prefix=prefix, MaxKeys=500)
        objs = [o for o in r.get("Contents", []) if o["Size"] > 0]
        if not objs:
            return []
        latest = max(objs, key=lambda o: o["LastModified"])
        latest_key  = latest["Key"]
        # Guard: skip files larger than 10 MB to prevent OOM
        MAX_READ_BYTES = 10 * 1024 * 1024
        if latest["Size"] > MAX_READ_BYTES:
            log.warning(f"  Fallback skipped: {latest_key} is {latest['Size']//1024//1024}MB (> 10MB limit)")
            return []
        body = s3.get_object(Bucket=MAIN_BUCKET, Key=latest_key)["Body"].read()
        try:
            records = json.loads(body)
        except json.JSONDecodeError:
            # Try NDJSON (newline-delimited JSON)
            records = [json.loads(line) for line in body.splitlines() if line.strip()]
        for rec in records:
            rec["fetched_at"] = now_ts
            rec["fallback"]   = True
        log.info(f"  Fallback: re-using {len(records)} records from {latest_key}")
        return records
    except Exception as e:
        log.warning(f"  Fallback load failed for {prefix}: {e}")
        return []


def fetch_buses() -> list:
    import requests
    log.info("Fetching bus positions from Open Bus Stride...")
    try:
        r = requests.get(
            f"{OPEN_BUS_URL}/siri_vehicle_locations/list",
            params={"limit": 500, "order_by": "id desc"},
            timeout=15,
        )
        r.raise_for_status()
        records = r.json()
        now = now_il().isoformat()
        result = [
            {
                "vehicle_id":       str(rec.get("id", "")),
                "route_id":         str(rec.get("siri_route_id") or rec.get("siri_route__id") or ""),
                "trip_id":          str(rec.get("siri_ride_id") or ""),
                "line_ref":         str(rec.get("siri_route__line_ref") or ""),
                "route_short_name": str(rec.get("siri_route__line_ref") or ""),
                "operator_id":      str(rec.get("siri_route__operator_ref") or ""),
                "operator_name":    _OPERATOR_NAMES.get(str(rec.get("siri_route__operator_ref") or ""), ""),
                "lat":              rec.get("lat"),
                "lon":              rec.get("lon"),
                "bearing":          rec.get("bearing"),
                "velocity":         rec.get("velocity"),
                "recorded_at":      rec.get("recorded_at_time"),
                "fetched_at":       now,
                "source":           "hasadna-siri",
            }
            for rec in records
            if rec.get("lat") is not None and rec.get("lon") is not None
        ]
        # Filter out sentinel/corrupt records — the Hasadna SIRI API uses far-future dates
        # (2038-01-14 = Unix max-int32, 2037-xx-xx = near-sentinel) when
        # recorded_at_time is unavailable. Reject anything year >= 2037.
        result = [
            r for r in result
            if r.get("recorded_at") and
               int(r["recorded_at"][:4]) < 2037
        ]
        log.info(f"  Buses: {len(result)} vehicle positions fetched (stale-filtered)")
        return result
    except Exception as e:
        log.error(f"Bus fetch failed: {e}")
        return []


# ── Train positions (Open Bus Stride, operator_ref=2 = Israel Railways) ───────

def fetch_trains() -> list:
    import requests
    log.info("Fetching train positions from Open Bus Stride (operator_ref=2)...")
    try:
        r = requests.get(
            f"{OPEN_BUS_URL}/siri_vehicle_locations/list",
            params={
                "siri_route__operator_ref": "2",   # FIX: was "operator_ref" — Stride uses siri_route__ prefix
                "limit":        300,
                "order_by":     "id desc",
            },
            timeout=15,
        )
        r.raise_for_status()
        records = r.json()
        now = now_il().isoformat()
        result = [
            {
                "vehicle_id":      str(rec.get("id") or ""),
                "train_number":    str(rec.get("siri_ride__vehicle_ref") or rec.get("vehicle_ref") or ""),
                "route_id":        str(rec.get("siri_route_id") or rec.get("siri_route__id") or ""),
                "trip_id":         str(rec.get("siri_ride_id") or ""),
                "line_ref":        str(rec.get("siri_route__line_ref") or ""),
                "route_short_name": str(rec.get("siri_route__line_ref") or ""),
                "operator_id":     "2",
                "operator_name":   "Israel Railways",
                "operator":        "israel_railways",
                "operator_ref":    "2",
                "lat":             rec.get("lat"),
                "lon":             rec.get("lon"),
                "bearing":         rec.get("bearing"),
                "velocity":        rec.get("velocity"),
                "scheduled_start": rec.get("siri_ride__scheduled_start_time", ""),
                "recorded_at":     rec.get("recorded_at_time", ""),
                "fetched_at":      now,
                "source":          "hasadna-siri-rail",
            }
            for rec in records
            if rec.get("lat") is not None
        ]
        # Filter out sentinel/corrupt records with far-future placeholder timestamps (2038 = Unix max-int32)
        result = [
            r for r in result
            if r.get("recorded_at") and
               int(r["recorded_at"][:4]) < 2037
        ]
        log.info(f"  Trains: {len(result)} vehicle positions fetched (stale-filtered)")
        return result
    except Exception as e:
        log.error(f"Train fetch failed: {e}")
        return []


# ── Traffic (HERE API) ────────────────────────────────────────────────────────

def _normalize_traffic(result: dict, tile_name: str) -> dict:
    now         = now_il()
    location    = result.get("location", {})
    description = location.get("description", "")
    shape       = location.get("shape", {})
    links       = shape.get("links", [{}])
    points      = (links[0] if links else {}).get("points", [{}])
    mid         = points[len(points) // 2] if points else {}

    cf          = result.get("currentFlow", {})
    speed       = cf.get("speed", 0)
    free_flow   = cf.get("freeFlow", 0)
    jam_factor  = cf.get("jamFactor", 0)
    confidence  = cf.get("confidence", 0)
    trav        = cf.get("traversability", "open")
    cong_raw    = cf.get("congestion", {})
    cong_val    = cong_raw.get("value", 1) if isinstance(cong_raw, dict) else 1
    congestion  = CONGESTION_MAP.get(cong_val, "unknown")
    speed_ratio = round(speed / free_flow, 3) if free_flow > 0 else 0
    delay_min   = round((60/speed - 60/free_flow), 2) if speed > 0 and free_flow > 0 else 0

    if jam_factor >= 8:   severity = "critical"
    elif jam_factor >= 6: severity = "severe"
    elif jam_factor >= 4: severity = "moderate"
    elif jam_factor >= 2: severity = "minor"
    else:                 severity = "free"

    return {
        "segment_id":       result.get("id", ""),
        "tile_name":        tile_name,
        "description":      description,
        "road_name":        description.split("/")[0].strip() if "/" in description else description,
        "lat":              mid.get("lat"),
        "lon":              mid.get("lng"),
        "speed_kmh":        speed,
        "free_flow_kmh":    free_flow,
        "speed_ratio":      speed_ratio,
        "jam_factor":       jam_factor,
        "congestion":       congestion,
        "severity":         severity,
        "confidence":       confidence,
        "traversability":   trav,
        "is_congested":     jam_factor >= 4,
        "is_blocked":       trav == "closed" or jam_factor >= 9,
        "delay_min_per_km": max(delay_min, 0),
        "hour_of_day":      now.hour,
        "day_of_week":      now.strftime("%A"),
        "recorded_at":      now.isoformat(),
        "source":           "here_traffic_v7",
    }


def fetch_traffic() -> list:
    if not HERE_API_KEY:
        log.warning("HERE_API_KEY not set — skipping traffic fetch")
        return []

    import requests
    log.info("Fetching traffic from HERE API for all Israel tiles...")
    session = requests.Session()
    session.headers["Accept"] = "application/json"
    all_records = []

    for name, lat_min, lat_max, lon_min, lon_max in ISRAEL_TILES:
        bbox = f"{lon_min},{lat_min},{lon_max},{lat_max}"
        try:
            r = session.get(
                "https://data.traffic.hereapi.com/v7/flow",
                params={
                    "apiKey":              HERE_API_KEY,
                    "in":                  f"bbox:{bbox}",
                    "locationReferencing": "shape",
                    "lang":                "en-US",
                },
                timeout=20,
            )
            if r.status_code == 401:
                log.error("HERE API: Invalid API key (401)")
                break
            if r.status_code == 429:
                log.warning("HERE API: Rate limit (429)")
                break
            r.raise_for_status()
            results = r.json().get("results", [])
            all_records.extend([_normalize_traffic(rec, name) for rec in results if rec])
            log.info(f"  Tile {name}: {len(results)} segments")
        except Exception as e:
            log.warning(f"  Tile {name} failed: {e}")

    log.info(f"  Traffic: {len(all_records)} total segments fetched")
    return all_records


# ── Stop-level delays (Open Bus Stride siri_ride_stops) ──────────────────────

def fetch_delays() -> list:
    """
    Fetch recent stop-level delay data from Open Bus Stride.
    Compares planned arrival time (gtfs_ride_stop__arrival_time) with
    the actual vehicle recorded time (nearest_siri_vehicle_location__recorded_at_time).
    Returns records only for stops where delay data is available.
    """
    import requests
    from datetime import timezone as tz

    log.info("Fetching stop-level delays from Open Bus Stride...")
    from datetime import timedelta
    now = now_il()
    # Look at stops with scheduled start in the last 30 minutes
    window_start = now - timedelta(minutes=30)

    try:
        r = requests.get(
            f"{OPEN_BUS_URL}/siri_ride_stops/list",
            params={
                "siri_ride__scheduled_start_time_from": window_start.isoformat(),
                "siri_ride__scheduled_start_time_to":   now.isoformat(),
                "limit": 500,
                "order_by": "id desc",
            },
            timeout=15,  # FIX: was 60s — task timeout is 4min with 5 fetch calls; 60s risks timeout
        )
        r.raise_for_status()
        records = r.json()
    except Exception as e:
        log.error(f"Delay fetch failed: {e}")
        return []

    result = []
    fetched_at = now.isoformat()

    for rec in records:
        planned_str = rec.get("gtfs_ride_stop__arrival_time") or rec.get("gtfs_ride_stop__departure_time")
        actual_str  = rec.get("nearest_siri_vehicle_location__recorded_at_time")
        delay_sec   = None
        delay_min   = None
        planned_iso = None
        actual_iso  = None
        status      = "unknown"

        if planned_str and actual_str:
            try:
                def _parse(s):
                    s = s.replace("Z", "+00:00")
                    return datetime.fromisoformat(s).astimezone(IL_TZ)

                planned_dt = _parse(planned_str)
                actual_dt  = _parse(actual_str)
                delay_sec  = int((actual_dt - planned_dt).total_seconds())
                delay_min  = round(delay_sec / 60, 1)
                planned_iso = planned_dt.isoformat()
                actual_iso  = actual_dt.isoformat()
                status = (
                    "early"    if delay_sec < -60  else
                    "on_time"  if delay_sec <= 180 else
                    "late"     if delay_sec <= 600 else
                    "very_late"
                )
            except Exception:
                pass

        dist_m = rec.get("nearest_siri_vehicle_location__distance_from_siri_ride_stop_meters")

        result.append({
            "ride_id":          rec.get("siri_ride_id"),
            "stop_id":          rec.get("gtfs_stop_id"),
            "stop_name":        rec.get("gtfs_stop__name", ""),
            "stop_city":        rec.get("gtfs_stop__city", ""),
            "stop_sequence":    rec.get("gtfs_ride_stop__stop_sequence"),
            "line_ref":         rec.get("siri_route__line_ref", "") or rec.get("gtfs_route__line_ref", ""),
            "operator_ref":     rec.get("siri_route__operator_ref", "") or rec.get("gtfs_route__operator_ref", ""),
            "route_short_name": rec.get("gtfs_route__route_short_name", ""),
            "agency_name":      rec.get("gtfs_route__agency_name", ""),
            "scheduled_start":  rec.get("siri_ride__scheduled_start_time"),
            "planned_arrival":  planned_iso,
            "actual_time":      actual_iso,
            "delay_seconds":    delay_sec,
            "delay_minutes":    delay_min,
            "distance_m":       dist_m,
            "status":           status,
            "fetched_at":       fetched_at,
            "source":           "hasadna-siri-ride-stops",
        })

    log.info(f"  Delays: {len(result)} stop-delay records fetched")
    return result


# ── Service alerts (rides with large delay or cancellation) ───────────────────

def fetch_service_alerts() -> list:
    """
    Derive service alerts from Open Bus Stride siri_rides/list.
    A ride is flagged as an alert when:
      - updated_duration_minutes > duration_minutes + 10  (significant delay)
      - updated_duration_minutes == 0 and duration_minutes > 0  (possible cancellation)
    """
    import requests

    log.info("Fetching service alerts (delayed/cancelled rides) from Open Bus Stride...")
    now = now_il()
    # Rides that started (or will start) within the last hour
    from datetime import timedelta as _td
    from_time = now - _td(hours=1)

    try:
        r = requests.get(
            f"{OPEN_BUS_URL}/siri_rides/list",
            params={
                "scheduled_start_time_from": from_time.isoformat(),
                "scheduled_start_time_to":   now.isoformat(),
                "limit": 500,
                "order_by": "scheduled_start_time desc",
            },
            timeout=30,
        )
        r.raise_for_status()
        records = r.json()
    except Exception as e:
        log.error(f"Service alert fetch failed: {e}")
        return []

    alerts = []
    fetched_at = now.isoformat()

    for rec in records:
        planned_dur  = rec.get("duration_minutes")
        updated_dur  = rec.get("updated_duration_minutes")

        # Compute alert fields only when both values are available
        delay_added   = None
        is_cancelled  = False
        is_delayed    = False
        alert_type    = "NORMAL"
        severity      = "INFO"
        description   = ""

        if planned_dur is not None and updated_dur is not None:
            delay_added = updated_dur - (planned_dur or 0)
            is_cancelled = (planned_dur > 0 and updated_dur == 0)
            is_delayed   = (delay_added >= 10)
            if is_cancelled:
                alert_type  = "CANCELLATION"
                severity    = "CRITICAL"
                description = f"Route {rec.get('gtfs_route__route_short_name','?')} cancelled"
            elif is_delayed:
                alert_type  = "SIGNIFICANT_DELAY"
                severity    = "HIGH" if delay_added >= 20 else "MEDIUM"
                description = f"Route {rec.get('gtfs_route__route_short_name','?')} delayed by {delay_added} minutes"

        sched = rec.get("scheduled_start_time", "")
        alerts.append({
            "ride_id":               rec.get("id"),
            "line_ref":              rec.get("siri_route__line_ref", ""),
            "operator_ref":          rec.get("siri_route__operator_ref", ""),
            "route_short_name":      rec.get("gtfs_route__route_short_name", ""),
            "agency_name":           rec.get("gtfs_route__agency_name", ""),
            "scheduled_start":       sched,
            "planned_duration_min":  planned_dur,
            "updated_duration_min":  updated_dur,
            "extra_delay_min":       delay_added,
            "alert_type":            alert_type,
            "severity":              severity,
            "description":           description,
            "fetched_at":            fetched_at,
            "source":                "hasadna-siri-rides",
        })

    log.info(f"  Alerts: {len(alerts)} service alert records derived")
    return alerts


# ── ETL transform helpers ─────────────────────────────────────────────────────

def transform_records(records: list, transformer_cls) -> list:
    try:
        t = transformer_cls()
        processed = []
        for r in records:
            try:
                processed.append(t.transform(r))
            except Exception as e:
                log.warning(f"Transform error: {e}")
        return processed
    except Exception as e:
        log.warning(f"Transformer unavailable ({e}) — skipping processed layer")
        return []


# ── Main run ──────────────────────────────────────────────────────────────────

def _generate_delay_events_from_stored(s3, now) -> list:
    """
    Fallback: read the latest raw/service-alerts file from MinIO and derive
    delay-events using elapsed time since scheduled_start.
    A ride that started >= 10 minutes ago and is still in the active window
    is treated as a potential delay event.
    """
    try:
        r = s3.list_objects_v2(Bucket=MAIN_BUCKET, Prefix="raw/service-alerts/", MaxKeys=100)
        objects = sorted(
            [o for o in r.get("Contents", []) if o["Size"] > 0],
            key=lambda o: o["LastModified"],
            reverse=True,
        )
        if not objects:
            return []
        body = s3.get_object(Bucket=MAIN_BUCKET, Key=objects[0]["Key"])["Body"].read()
        records = json.loads(body)
    except Exception as e:
        log.warning(f"Could not read stored service-alerts for delay-event fallback: {e}")
        return []

    delay_events = []
    for rec in records:
        sched_str = rec.get("scheduled_start")
        if not sched_str:
            continue
        try:
            from datetime import timezone as _tz
            sched_dt = datetime.fromisoformat(sched_str)
            if sched_dt.tzinfo is None:
                sched_dt = sched_dt.replace(tzinfo=_tz.utc)
            elapsed_sec = (now - sched_dt).total_seconds()
        except Exception:
            continue
        # Flag rides that started >= 10 min ago as potential delay events
        ELAPSED_THRESHOLD_SEC = 600
        if elapsed_sec < ELAPSED_THRESHOLD_SEC:
            continue
        extra_min = rec.get("extra_delay_min")
        delay_events.append({
            **rec,
            "is_delayed":    True,
            "is_very_late":  elapsed_sec > 1200 or (extra_min is not None and extra_min >= 20),
            "event_type":    rec.get("alert_type", "LATE") if rec.get("alert_type") != "NORMAL" else "LATE",
            "delay_seconds": int(extra_min * 60) if extra_min is not None else int(elapsed_sec),
            "detected_at":   now.isoformat(),
            "delay_source":  "elapsed_time",
        })

    log.info(f"  Delay events (elapsed-time fallback): {len(delay_events)} records from stored alerts")
    return delay_events


def run(dry_run=False, only=None) -> dict:
    log.info("=" * 60)
    log.info(f"direct_to_minio | bucket={MAIN_BUCKET} | endpoint={MINIO_ENDPOINT}")
    log.info(f"HERE_API_KEY = {'SET' if HERE_API_KEY else 'NOT SET'}")
    log.info("=" * 60)

    do_buses   = only in (None, "buses")
    do_trains  = only in (None, "trains")
    do_traffic = only in (None, "traffic")
    do_delays  = only in (None, "delays")
    do_alerts  = only in (None, "alerts")

    results = {}

    now = now_il()  # single consistent timestamp for this run

    # ── Fetch ─────────────────────────────────────────────────────────────────
    buses   = fetch_buses()          if do_buses   else []
    trains  = fetch_trains()         if do_trains  else []
    traffic = fetch_traffic()        if do_traffic else []
    delays  = fetch_delays()         if do_delays  else []
    alerts  = fetch_service_alerts() if do_alerts  else []

    if dry_run:
        log.info("DRY RUN — skipping MinIO upload")
        log.info(f"  buses={len(buses)}  trains={len(trains)}  traffic={len(traffic)}  delays={len(delays)}  alerts={len(alerts)}")
        for r in (buses[:2] + trains[:2] + traffic[:2] + delays[:2] + alerts[:2]):
            print(json.dumps(r, ensure_ascii=False, indent=2))
        return {"buses": len(buses), "trains": len(trains), "traffic": len(traffic), "delays": len(delays), "alerts": len(alerts)}

    # ── Connect (non-fatal — buffer to disk if down) ───────────────────────────
    minio_available = True
    try:
        s3 = get_s3()
        ensure_bucket(s3)
    except Exception as e:
        log.warning(f"⚠️  MinIO unreachable at {MINIO_ENDPOINT}: {e}")
        log.warning("   → continuing; data will be BUFFERED to disk and flushed when MinIO is back up")
        s3 = get_s3()  # client object — may not work but upload() will catch & buffer
        minio_available = False
    
    # Tell upload() to skip retries and go straight to buffer when MinIO is down
    import storage.direct_to_minio as _self_mod
    _self_mod._MINIO_AVAILABLE = minio_available

    uploaded = []

    # ── Bus positions ──────────────────────────────────────────────────────────
    if do_buses:
        if not buses:
            log.warning("No bus positions fetched — trying stored fallback")
            buses = _get_last_stored_records(s3, "raw/bus-positions/", now.isoformat())
        if buses:
            uploaded.append(upload(s3, buses, "raw/bus-positions",       "buses_raw"))
            try:
                from etl.transformers import BusPositionTransformer
                # Remap field names to what the transformer expects
                remapped = [
                    {**b, "latitude": b.get("lat", 0), "longitude": b.get("lon", 0)}
                    for b in buses
                ]
                processed = transform_records(remapped, BusPositionTransformer)
                if processed:
                    uploaded.append(upload(s3, processed, "processed/bus-positions", "buses_processed"))
            except Exception as e:
                log.warning(f"Bus ETL transform skipped: {e}")
            results["buses"] = len(buses)
        else:
            log.warning("No bus positions fetched (API down, no stored fallback)")
            results["buses"] = 0

    # ── Train positions ────────────────────────────────────────────────────────
    if do_trains:
        if not trains:
            log.warning("No train positions fetched — trying stored fallback")
            trains = _get_last_stored_records(s3, "raw/train-positions/", now.isoformat())
        if trains:
            uploaded.append(upload(s3, trains, "raw/train-positions",       "trains_raw"))
            try:
                from etl.transformers import TrainPositionTransformer
                processed = transform_records(trains, TrainPositionTransformer)
                if processed:
                    uploaded.append(upload(s3, processed, "processed/train-positions", "trains_processed"))
            except Exception as e:
                log.warning(f"Train ETL transform skipped: {e}")
            results["trains"] = len(trains)
        else:
            log.warning("No train positions fetched (API down, no stored fallback)")
            results["trains"] = 0

    # ── Traffic ────────────────────────────────────────────────────────────────
    if do_traffic and traffic:
        uploaded.append(upload(s3, traffic, "raw/traffic-data",       "traffic_raw"))
        try:
            from etl.traffic_transformer import TrafficTransformer
            processed = transform_records(traffic, TrafficTransformer)
            if processed:
                uploaded.append(upload(s3, processed, "processed/traffic-data", "traffic_processed"))
        except Exception as e:
            log.warning(f"Traffic ETL transform skipped: {e}")
        results["traffic"] = len(traffic)
    else:
        results["traffic"] = 0

    # ── Trip updates / delays ──────────────────────────────────────────────────
    if do_delays:
        if delays:
            uploaded.append(upload(s3, delays, "raw/trip-updates", "delays_raw"))
            # Processed layer: enrich with computed flags; keep all records
            processed_delays = []
            for d in delays:
                delay_sec = d.get("delay_seconds")
                processed_delays.append({
                    **d,
                    "is_delayed":   delay_sec is not None and delay_sec > 180,
                    "is_very_late": delay_sec is not None and delay_sec > 600,
                    "is_early":     delay_sec is not None and delay_sec < -60,
                })
            uploaded.append(upload(s3, processed_delays, "processed/trip-updates", "delays_processed"))
            results["delays"] = len(delays)
        else:
            log.warning("No delay records fetched — trying stored fallback")
            fallback_delays = _get_last_stored_records(s3, "raw/trip-updates/", now.isoformat())
            if fallback_delays:
                uploaded.append(upload(s3, fallback_delays, "raw/trip-updates",       "delays_raw"))
                uploaded.append(upload(s3, fallback_delays, "processed/trip-updates", "delays_processed"))
            results["delays"] = len(fallback_delays)

    # ── Service alerts ─────────────────────────────────────────────────────────
    if do_alerts and alerts:
        uploaded.append(upload(s3, alerts, "raw/service-alerts", "alerts_raw"))
        # Processed layer: enrich with a severity score; keep all records
        processed_alerts = []
        for a in alerts:
            delay_added = a.get("extra_delay_min")
            processed_alerts.append({
                **a,
                "is_significant": delay_added is not None and delay_added >= 10,
                "is_critical":    a.get("alert_type") in ("CANCELLATION", "SIGNIFICANT_DELAY"),
            })
        uploaded.append(upload(s3, processed_alerts, "processed/service-alerts", "alerts_processed"))
        results["alerts"] = len(alerts)

        # ── Delay events: derived from service-alerts (reliable siri_rides endpoint)
        # Any ride with >= 5 min extra delay, or a cancellation, is a delay event.
        DELAY_EVENT_MIN = 5
        raw_delay_events = [
            a for a in alerts
            if (a.get("extra_delay_min") or 0) >= DELAY_EVENT_MIN
            or a.get("alert_type") == "CANCELLATION"
        ]
        if raw_delay_events:
            proc_delay_events = [
                {
                    **a,
                    "is_delayed":            True,
                    "is_very_late":          (a.get("extra_delay_min") or 0) >= 20,
                    "event_type":            a.get("alert_type", "LATE"),
                    "delay_seconds":         int((a.get("extra_delay_min") or 0) * 60),
                    "detected_at":           now.isoformat(),
                }
                for a in raw_delay_events
            ]
            uploaded.append(upload(s3, raw_delay_events,  "raw/delay-events",       "delay_events_raw"))
            uploaded.append(upload(s3, proc_delay_events, "processed/delay-events",  "delay_events_processed"))
            log.info(f"  Delay events: {len(raw_delay_events)} records (>= 5 min delay or cancellation)")
        else:
            # Fallback: derive from stored alert data using elapsed time
            raw_delay_events = _generate_delay_events_from_stored(s3, now)
            if raw_delay_events:
                uploaded.append(upload(s3, raw_delay_events, "raw/delay-events",      "delay_events_raw"))
                uploaded.append(upload(s3, raw_delay_events, "processed/delay-events", "delay_events_processed"))
            else:
                log.info("  Delay events: 0 qualifying records this run")
        results["delay_events"] = len(raw_delay_events)
    else:
        log.warning("No service alerts derived — trying stored fallback")
        fallback_alerts = _get_last_stored_records(s3, "raw/service-alerts/", now.isoformat())
        if fallback_alerts:
            uploaded.append(upload(s3, fallback_alerts, "raw/service-alerts",       "alerts_raw"))
            uploaded.append(upload(s3, fallback_alerts, "processed/service-alerts", "alerts_processed"))
        results["alerts"] = len(fallback_alerts)
        # Still attempt delay-events from stored data
        fallback_events = _generate_delay_events_from_stored(s3, now)
        if fallback_events:
            uploaded.append(upload(s3, fallback_events, "raw/delay-events",       "delay_events_raw"))
            uploaded.append(upload(s3, fallback_events, "processed/delay-events", "delay_events_processed"))
            log.info(f"  Delay events (fallback): {len(fallback_events)} records written")
        results["delay_events"] = len(fallback_events)

    # ── Summary ────────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info(f"Done — {len(uploaded)} files written to s3://{MAIN_BUCKET}")
    for k in uploaded:
        log.info(f"  {k}")
    results["uploaded"] = uploaded
    return results


def direct_to_minio_task(**context):
    """Airflow PythonOperator callable."""
    only = context.get("params", {}).get("only", None)
    result = run(dry_run=False, only=only)
    if context.get("ti"):
        context["ti"].xcom_push(key="buses",   value=result.get("buses", 0))
        context["ti"].xcom_push(key="trains",  value=result.get("trains", 0))
        context["ti"].xcom_push(key="traffic", value=result.get("traffic", 0))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch transit data → MinIO israel-transit-lake")
    parser.add_argument("--dry-run", action="store_true", help="Fetch only, no upload")
    parser.add_argument("--only", choices=["buses", "trains", "traffic", "delays", "alerts"], help="Fetch only one type")
    args = parser.parse_args()
    result = run(dry_run=args.dry_run, only=args.only)
    sys.exit(0 if result.get("uploaded") or args.dry_run else 1)
