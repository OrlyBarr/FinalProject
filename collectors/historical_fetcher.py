"""
Historical Delay Fetcher
=========================
Fetches historical (batch) delay data for buses and trains
and publishes to dedicated Kafka topics for historical data.

Usage:
  python historical_fetcher.py --days 7
  python historical_fetcher.py --days 30 --transport bus
  python historical_fetcher.py --days 7  --transport train
  python historical_fetcher.py --from 2025-01-01 --to 2025-01-31

Data will be saved to:
  - Kafka: bus-delays-historical / train-delays-historical
  - MinIO: after being consumed by delay_kafka_consumer.py
"""

import json
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone, date
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

IL_TZ = ZoneInfo("Asia/Jerusalem")  # UTC+2 winter / UTC+3 summer (DST-aware)
from typing import Iterator

import requests
from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_BUS_HISTORICAL,
    KAFKA_TOPIC_TRAIN_HISTORICAL,
    STRIDE_API_BASE,
    RAIL_API_BASE,
    HISTORICAL_DAYS_BACK,
    HISTORICAL_BATCH_SIZE,
    MONITORED_TRAIN_ROUTES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [HISTORICAL] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

RAIL_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.rail.co.il/",
}


# ─── Bus historical (Stride API) ─────────────────────────────────────────────

def iter_historical_bus_delays(
    date_from: datetime,
    date_to: datetime,
    batch_size: int = HISTORICAL_BATCH_SIZE,
) -> Iterator[dict]:
    """
    Returns an iterator over historical bus delay data.
    
    Uses gtfs_ride_stop with date filter.
    Returns records with actual_arrival_time (stops that actually occurred).
    """
    offset = 0
    date_from_str = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_to_str   = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info(f"Fetching bus history: {date_from_str} → {date_to_str}")

    while True:
        url = f"{STRIDE_API_BASE}/gtfs_ride_stop/list"
        params = {
            "limit":                  batch_size,
            "offset":                 offset,
            "get_count":              "false",
            "actual_arrival_time_from": date_from_str,
            "actual_arrival_time_to":   date_to_str,
            "order_by":               "actual_arrival_time",
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            records = resp.json()
        except requests.RequestException as e:
            log.error(f"Stride API error at offset {offset}: {e}")
            break

        if not records:
            log.info(f"Bus history: no more records at offset {offset}")
            break

        for raw in records:
            delay_sec = raw.get("delay_arrival")
            yield {
                "source":         "gtfs_ride_stop_historical",
                "transport_type": "bus",
                "collected_at":   datetime.now(IL_TZ).isoformat(),
                "gtfs_ride_id":   raw.get("gtfs_ride_id"),
                "gtfs_stop_id":   raw.get("gtfs_stop_id"),
                "planned_arrival":  raw.get("planned_arrival_time"),
                "actual_arrival":   raw.get("actual_arrival_time"),
                "delay_seconds":    delay_sec,
                "delay_minutes":    round(delay_sec / 60, 1) if delay_sec is not None else None,
                "is_early":         delay_sec < 0 if delay_sec is not None else None,
            }

        log.info(f"Bus history: fetched {len(records)} records (offset {offset})")
        offset += batch_size

        # avoid rate limiting
        time.sleep(0.5)


def iter_historical_bus_vehicle_monitoring(
    date_from: datetime,
    date_to: datetime,
    batch_size: int = HISTORICAL_BATCH_SIZE,
) -> Iterator[dict]:
    """
    Historical vehicle monitoring data from SIRI.
    Allows viewing location + delay over time.
    """
    offset = 0
    date_from_str = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_to_str   = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info(f"Fetching vehicle monitoring history: {date_from_str} → {date_to_str}")

    while True:
        url = f"{STRIDE_API_BASE}/siri_vehicle_monitoring/list"
        params = {
            "limit":                batch_size,
            "offset":               offset,
            "get_count":            "false",
            "recorded_at_time_from": date_from_str,
            "recorded_at_time_to":   date_to_str,
            "order_by":             "recorded_at_time",
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            records = resp.json()
        except requests.RequestException as e:
            log.error(f"Stride API error (vehicle monitoring) at offset {offset}: {e}")
            break

        if not records:
            break

        for raw in records:
            delay = raw.get("delay")
            yield {
                "source":          "siri_vehicle_monitoring_historical",
                "transport_type":  "bus",
                "collected_at":    datetime.now(IL_TZ).isoformat(),
                "vehicle_ref":     raw.get("vehicle_ref"),
                "line_ref":        raw.get("line_ref"),
                "operator_ref":    raw.get("operator_ref"),
                "journey_ref":     raw.get("dated_vehicle_journey_ref"),
                "stop_point_ref":  raw.get("stop_point_ref"),
                "recorded_at":     raw.get("recorded_at_time"),
                "aimed_arrival":   raw.get("aimed_arrival_time"),
                "expected_arrival": raw.get("expected_arrival_time"),
                "delay_seconds":   delay,
                "delay_minutes":   round(delay / 60, 1) if delay is not None else None,
                "lat":             raw.get("lat"),
                "lon":             raw.get("lon"),
            }

        log.info(f"Vehicle monitoring history: fetched {len(records)} records (offset {offset})")
        offset += batch_size
        time.sleep(0.5)


# ─── Train historical (Rail API) ─────────────────────────────────────────────

def iter_historical_train_delays(
    date_from: date,
    date_to: date,
) -> Iterator[dict]:
    """
    Returns an iterator over historical train delays.
    Iterates over each day in the range and all defined routes.
    """
    current = date_from
    while current <= date_to:
        date_str = current.strftime("%d/%m/%Y")
        log.info(f"Fetching train delays for date: {date_str}")

        for route in MONITORED_TRAIN_ROUTES:
            origin = route["origin"]
            dest   = route["destination"]

            url = f"{RAIL_API_BASE}/Plan/GetRoutes"
            params = {
                "OId": origin,
                "TId": dest,
                "Date": date_str,
                "Hour": "0",
                "isGoing": "1",
                "SavedName": "",
            }
            try:
                resp = requests.get(url, params=params, headers=RAIL_HEADERS, timeout=15)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as e:
                log.error(f"Rail API error {origin}→{dest} on {date_str}: {e}")
                time.sleep(2)
                continue

            if data.get("MessageType") != 2:
                continue

            route_data = data.get("Data", {})

            # Delays by station
            for raw in (route_data.get("Delays") or []):
                try:
                    delay_min = int(raw.get("Min", 0))
                except ValueError:
                    delay_min = 0

                yield {
                    "source":            "rail_api_delays_historical",
                    "transport_type":    "train",
                    "collected_at":      datetime.now(IL_TZ).isoformat(),
                    "train_number":      raw.get("Train"),
                    "station_id":        raw.get("Station"),
                    "route_origin":      origin,
                    "route_destination": dest,
                    "delay_date":        raw.get("Date"),
                    "delay_time":        raw.get("Time"),
                    "delay_minutes":     delay_min,
                    "delay_seconds":     delay_min * 60,
                    "is_delayed":        delay_min > 0,
                }

            # Trips and stops (for analyzing delay patterns)
            for route_info in (route_data.get("Routes") or []):
                for train in (route_info.get("Train") or []):
                    train_no = train.get("Trainno")
                    for stop in (train.get("StopStations") or []):
                        yield {
                            "source":            "rail_api_stops_historical",
                            "transport_type":    "train",
                            "collected_at":      datetime.now(IL_TZ).isoformat(),
                            "train_number":      train_no,
                            "station_id":        stop.get("StationId"),
                            "route_origin":      origin,
                            "route_destination": dest,
                            "arrival_time":      stop.get("ArrivalTime"),
                            "departure_time":    stop.get("DepartureTime"),
                            "platform":          stop.get("Platform"),
                            "fetch_date":        date_str,
                        }

            time.sleep(0.8)  # rate limiting

        current += timedelta(days=1)


# ─── Kafka publisher ──────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def stream_to_kafka(producer: KafkaProducer, iterator: Iterator[dict], topic: str, log_every: int = 1000):
    count = 0
    for record in iterator:
        key = f"{record.get('train_number') or record.get('line_ref') or 'unknown'}"
        producer.send(topic, key=key, value=record)
        count += 1
        if count % log_every == 0:
            log.info(f"[{topic}] Published {count} records so far...")
            producer.flush()
    producer.flush()
    log.info(f"[{topic}] Done: {count} total records published")
    return count


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Historical delay fetcher")
    parser.add_argument(
        "--transport",
        choices=["bus", "train", "both"],
        default="both",
        help="Which transport to fetch (default: both)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=HISTORICAL_DAYS_BACK,
        help=f"How many days back (default: {HISTORICAL_DAYS_BACK})",
    )
    parser.add_argument("--from", dest="date_from", help="תאריך התחלה YYYY-MM-DD")
    parser.add_argument("--to",   dest="date_to",   help="תאריך סיום YYYY-MM-DD")
    args = parser.parse_args()

    # Calculate date range
    now = datetime.now(IL_TZ)
    if args.date_from:
        dt_from = datetime.fromisoformat(args.date_from).replace(tzinfo=IL_TZ)
    else:
        dt_from = now - timedelta(days=args.days)

    if args.date_to:
        dt_to = datetime.fromisoformat(args.date_to).replace(tzinfo=IL_TZ)
    else:
        dt_to = now

    log.info(f"Historical fetch: {dt_from.date()} → {dt_to.date()}, transport={args.transport}")

    producer = create_producer()
    try:
        if args.transport in ("bus", "both"):
            log.info("=== Starting BUS historical fetch ===")

            total = stream_to_kafka(
                producer,
                iter_historical_bus_delays(dt_from, dt_to),
                KAFKA_TOPIC_BUS_HISTORICAL,
            )
            log.info(f"Bus ride stops: {total} records")

            total = stream_to_kafka(
                producer,
                iter_historical_bus_vehicle_monitoring(dt_from, dt_to),
                KAFKA_TOPIC_BUS_HISTORICAL,
            )
            log.info(f"Bus vehicle monitoring: {total} records")

        if args.transport in ("train", "both"):
            log.info("=== Starting TRAIN historical fetch ===")

            total = stream_to_kafka(
                producer,
                iter_historical_train_delays(dt_from.date(), dt_to.date()),
                KAFKA_TOPIC_TRAIN_HISTORICAL,
            )
            log.info(f"Train delays: {total} records")

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()