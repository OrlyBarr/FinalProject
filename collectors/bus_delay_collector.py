"""
Bus Delay Collector
====================
Fetches real-time delay data from the Open Bus Stride API
and publishes to Kafka topic: bus-delays

Data sources:
  - SIRI SM (real-time vehicle monitoring) דרך Stride API
  - Automatic comparison between actual time and scheduled time

Usage:
  python bus_delay_collector.py
  python bus_delay_collector.py --once   # ריצה חד פעמית (לבדיקה)
"""

import json
import time
import logging
import argparse
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

IL_TZ = ZoneInfo("Asia/Jerusalem")  # UTC+2 winter / UTC+3 summer (DST-aware)

import requests
from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_BUS_DELAYS,
    STRIDE_API_BASE,
    BUS_POLL_INTERVAL_SECONDS,
    BUS_FETCH_LIMIT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BUS-COLLECTOR] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ─── Stride API client ────────────────────────────────────────────────────────

def fetch_siri_vehicle_locations(limit: int = BUS_FETCH_LIMIT) -> list[dict]:
    """
    Fetches current vehicle locations with actual delay.
    endpoint: /siri_vehicle_monitoring/list
    Key fields:
      - recorded_at_time       : זמן הדיווח בפועל
      - aimed_arrival_time     : זמן הגעה מתוכנן
      - expected_arrival_time  : זמן הגעה צפוי (לאחר עדכון)
      - delay                  : עיכוב בשניות (שדה מחושב ע"י Stride)
      - line_ref               : מזהה קו
      - operator_ref           : מזהה חברה מפעילה
    """
    url = f"{STRIDE_API_BASE}/siri_vehicle_monitoring/list"
    params = {
        "limit": limit,
        "get_count": "false",
        "order_by": "id desc",   # מהיר יותר מ-recorded_at_time
    }
    try:
        resp = requests.get(url, params=params, timeout=8)  # fail fast
        resp.raise_for_status()
        items = resp.json()
        # סינון לגוש דן ותל אביב בלבד
        filtered = [
            v for v in items
            if v.get("lat") and v.get("lon")
            and 31.97 <= float(v["lat"]) <= 32.19
            and 34.73 <= float(v["lon"]) <= 34.93
        ]
        log.info(f"Vehicle monitoring: {len(filtered)}/{len(items)} in Gush Dan")
        return filtered
    except requests.Timeout:
        log.warning("Stride API timed out (>8s) — skipping vehicle monitoring cycle")
        return []
    except requests.RequestException as e:
        log.error(f"Stride API error (vehicle monitoring): {e}")
        return []


def fetch_ride_stops_with_delay(limit: int = BUS_FETCH_LIMIT) -> list[dict]:
    """
    Fetches stop data with computed delay (actual vs planned).
    endpoint: /gtfs_ride_stop/list
    Key fields:
      - gtfs_stop_id           : מזהה תחנה
      - planned_arrival_time   : זמן הגעה מתוכנן
      - actual_arrival_time    : זמן הגעה בפועל (null אם טרם הגיע)
      - delay_arrival          : עיכוב בשניות (שלילי = מוקדם)
    """
    url = f"{STRIDE_API_BASE}/gtfs_ride_stop/list"
    params = {
        "limit": limit,
        "get_count": "false",
        "order_by": "-actual_arrival_time",
    }
    try:
        resp = requests.get(url, params=params, timeout=8)  # fail fast
        resp.raise_for_status()
        return resp.json()
    except requests.Timeout:
        log.warning("Stride API timed out (>8s) — skipping ride stops cycle")
        return []
    except requests.RequestException as e:
        log.error(f"Stride API error (ride stops): {e}")
        return []


# ─── Normalization ────────────────────────────────────────────────────────────

def normalize_vehicle_record(raw: dict) -> dict:
    """
    Maps an API record to a unified format for Kafka.
    """
    aimed    = raw.get("aimed_arrival_time")
    expected = raw.get("expected_arrival_time")

    # מחשב עיכוב בדקות אם אין שדה מוכן
    delay_seconds = raw.get("delay")
    if delay_seconds is None and aimed and expected:
        try:
            from dateutil import parser as dtparser
            t_aimed    = dtparser.parse(aimed)
            t_expected = dtparser.parse(expected)
            delay_seconds = int((t_expected - t_aimed).total_seconds())
        except Exception:
            delay_seconds = None

    return {
        "source":          "siri_vehicle_monitoring",
        "transport_type":  "bus",
        "collected_at":    datetime.now(IL_TZ).isoformat(),
        # זיהוי
        "vehicle_ref":     raw.get("vehicle_ref"),
        "line_ref":        raw.get("line_ref"),
        "operator_ref":    raw.get("operator_ref"),
        "journey_ref":     raw.get("dated_vehicle_journey_ref"),
        # Stop
        "stop_point_ref":  raw.get("stop_point_ref"),
        "stop_order":      raw.get("order"),
        # זמנים
        "recorded_at":     raw.get("recorded_at_time"),
        "aimed_arrival":   aimed,
        "expected_arrival": expected,
        # עיכוב
        "delay_seconds":   delay_seconds,
        "delay_minutes":   round(delay_seconds / 60, 1) if delay_seconds is not None else None,
        # Location
        "lat":             raw.get("lat"),
        "lon":             raw.get("lon"),
    }


def normalize_ride_stop_record(raw: dict) -> dict:
    """
    Maps a gtfs_ride_stop record to a unified format.
    """
    delay_sec = raw.get("delay_arrival")
    return {
        "source":         "gtfs_ride_stop",
        "transport_type": "bus",
        "collected_at":   datetime.now(IL_TZ).isoformat(),
        # Identification
        "gtfs_ride_id":   raw.get("gtfs_ride_id"),
        "gtfs_stop_id":   raw.get("gtfs_stop_id"),
        # Times
        "planned_arrival":  raw.get("planned_arrival_time"),
        "actual_arrival":   raw.get("actual_arrival_time"),
        # Delay
        "delay_seconds":  delay_sec,
        "delay_minutes":  round(delay_sec / 60, 1) if delay_sec is not None else None,
        "is_early":       delay_sec < 0 if delay_sec is not None else None,
    }


# ─── Kafka producer ───────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def publish_records(producer: KafkaProducer, records: list[dict], topic: str):
    published = 0
    for record in records:
        # Key: combination of line_ref + stop (for partition distribution)
        key = f"{record.get('line_ref', 'unknown')}_{record.get('stop_point_ref') or record.get('gtfs_stop_id', '')}"
        producer.send(topic, key=key, value=record)
        published += 1
    producer.flush()
    return published


# ─── Main loop ────────────────────────────────────────────────────────────────

def collect_once(producer: KafkaProducer) -> int:
    total = 0

    # 1) Vehicle monitoring (עיכוד נוכחי לפי מיקום רכב)
    raw_vehicles = fetch_siri_vehicle_locations()
    vehicle_records = [normalize_vehicle_record(r) for r in raw_vehicles]
    # מסנן רק רכבים עם עיכוד ידוע
    delayed = [r for r in vehicle_records if r["delay_seconds"] is not None]
    if delayed:
        n = publish_records(producer, delayed, KAFKA_TOPIC_BUS_DELAYS)
        log.info(f"Vehicle monitoring: {n} records published (out of {len(raw_vehicles)} fetched)")
        total += n

    # 2) Ride stops (עיכוד לפי עצירה)
    raw_stops = fetch_ride_stops_with_delay()
    stop_records = [normalize_ride_stop_record(r) for r in raw_stops
                    if r.get("actual_arrival_time")]  # רק עצירות שהתרחשו
    if stop_records:
        n = publish_records(producer, stop_records, KAFKA_TOPIC_BUS_DELAYS)
        log.info(f"Ride stops: {n} records published (out of {len(raw_stops)} fetched)")
        total += n

    return total


def run(once: bool = False):
    log.info(f"Starting Bus Delay Collector → topic: {KAFKA_TOPIC_BUS_DELAYS}")
    producer = create_producer()
    try:
        while True:
            start = time.time()
            count = collect_once(producer)
            elapsed = time.time() - start
            log.info(f"Cycle complete: {count} total records in {elapsed:.1f}s")

            if once:
                break
            sleep_time = max(0, BUS_POLL_INTERVAL_SECONDS - elapsed)
            log.info(f"Sleeping {sleep_time:.0f}s until next cycle...")
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        log.info("Stopped by user")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()
    run(once=args.once)