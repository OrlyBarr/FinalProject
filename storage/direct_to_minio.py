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
    """Upload list of records as JSON to time-partitioned S3 path (Israel time)."""
    now = now_il()
    key = (
        f"{prefix}/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"{label}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )
    body = json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(
        Bucket=MAIN_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata={"record_count": str(len(records))},
    )
    path = f"s3://{MAIN_BUCKET}/{key}"
    log.info(f"  ✅ {len(records)} records → {path}")
    return path


# ── Bus positions (Open Bus Stride SIRI) ─────────────────────────────────────

def fetch_buses() -> list:
    import requests
    log.info("Fetching bus positions from Open Bus Stride...")
    try:
        r = requests.get(
            f"{OPEN_BUS_URL}/siri_vehicle_locations/list",
            params={"limit": 500, "order_by": "recorded_at_time desc"},
            timeout=15,
        )
        r.raise_for_status()
        records = r.json()
        now = now_il().isoformat()
        result = [
            {
                "vehicle_id":   str(rec.get("id", "")),
                "route_id":     str(rec.get("siri_snapshot_id", "")),
                "trip_id":      str(rec.get("siri_ride_stop_id", "")),
                "line_ref":     str(rec.get("siri_route__line_ref") or rec.get("line_ref") or ""),
                "operator_ref": str(rec.get("siri_route__operator_ref") or ""),
                "lat":          rec.get("lat"),
                "lon":          rec.get("lon"),
                "bearing":      rec.get("bearing"),
                "velocity":     rec.get("velocity"),
                "recorded_at":  rec.get("recorded_at_time"),
                "fetched_at":   now,
                "source":       "hasadna-siri",
            }
            for rec in records
            if rec.get("lat") is not None and rec.get("lon") is not None
        ]
        # Filter out stale records — the Hasadna SIRI API uses far-future dates
        # (e.g. 2038-01-14 = Unix max-int32, 2037-xx-xx = near-sentinel) when
        # recorded_at_time is unavailable. Those records also have velocity=0 and
        # a frozen bearing, making them useless for analysis.
        current_year = now_il().year
        result = [
            r for r in result
            if r.get("recorded_at") and
               int(r["recorded_at"][:4]) <= current_year
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
                "operator_ref": "2",
                "limit":        300,
                "order_by":     "recorded_at_time desc",
            },
            timeout=15,
        )
        r.raise_for_status()
        records = r.json()
        now = now_il().isoformat()
        result = [
            {
                "train_number":    str(rec.get("siri_ride__vehicle_ref") or rec.get("vehicle_ref") or ""),
                "line_ref":        str(rec.get("siri_route__line_ref") or rec.get("line_ref") or ""),
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
        # Filter out stale records with far-future placeholder timestamps
        current_year = now_il().year
        result = [
            r for r in result
            if r.get("recorded_at") and
               int(r["recorded_at"][:4]) <= current_year
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
                "nearest_siri_vehicle_location__distance_from_siri_ride_stop_meters__lte": 200,
                "limit": 500,
                "order_by": "id desc",
            },
            timeout=20,
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
            timeout=20,
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

    # ── Connect ────────────────────────────────────────────────────────────────
    try:
        s3 = get_s3()
        ensure_bucket(s3)
    except Exception as e:
        log.error(f"Cannot connect to MinIO at {MINIO_ENDPOINT}: {e}")
        log.error("Make sure Docker is running: docker-compose up -d minio")
        return {"error": str(e)}

    uploaded = []

    # ── Bus positions ──────────────────────────────────────────────────────────
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
        log.warning("No bus positions fetched")
        results["buses"] = 0

    # ── Train positions ────────────────────────────────────────────────────────
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
        log.warning("No train positions fetched")
        results["trains"] = 0

    # ── Traffic ────────────────────────────────────────────────────────────────
    if traffic:
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

        # ── Delay events: subset where delay >= 5 minutes ─────────────────────
        # delay_seconds may be None when vehicle telemetry is unavailable.
        # Fall back to elapsed time since scheduled_start as an approximation:
        # if a ride was scheduled >5 min ago and no actual_time recorded, it is
        # potentially delayed.
        DELAY_EVENT_THRESHOLD_SEC = 300  # 5 minutes

        def _effective_delay(d):
            """Return best-estimate delay in seconds, or 0 if unknown."""
            ds = d.get("delay_seconds")
            if ds is not None:
                return ds
            # Approximate: time elapsed since scheduled start without arrival
            if d.get("actual_time") is None:
                sched = d.get("scheduled_start")
                if sched:
                    try:
                        from datetime import datetime, timezone
                        sched_dt = datetime.fromisoformat(sched)
                        if sched_dt.tzinfo is None:
                            sched_dt = sched_dt.replace(tzinfo=timezone.utc)
                        elapsed = (now - sched_dt).total_seconds()
                        return max(0, elapsed)
                    except Exception:
                        pass
            return 0

        raw_delay_events = [
            d for d in delays
            if _effective_delay(d) >= DELAY_EVENT_THRESHOLD_SEC
        ]
        proc_delay_events = [
            {
                **d,
                "is_delayed":   True,
                "is_very_late": _effective_delay(d) > 600,
                "event_type":   "VERY_LATE" if _effective_delay(d) > 600 else "LATE",
                "effective_delay_seconds": _effective_delay(d),
                "detected_at":  now.isoformat(),
            }
            for d in raw_delay_events
        ]
        if raw_delay_events:
            uploaded.append(upload(s3, raw_delay_events,  "raw/delay-events",       "delay_events_raw"))
            uploaded.append(upload(s3, proc_delay_events, "processed/delay-events",  "delay_events_processed"))
            log.info(f"  Delay events: {len(raw_delay_events)} records (>= 5 min late)")
        results["delay_events"] = len(raw_delay_events)
    else:
        log.warning("No delay records fetched")
        results["delays"] = 0
        results["delay_events"] = 0

    # ── Service alerts ─────────────────────────────────────────────────────────
    if alerts:
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
    else:
        log.warning("No service alerts derived")
        results["alerts"] = 0

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
