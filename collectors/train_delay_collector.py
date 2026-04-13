"""
Train Delay Collector
======================
שולף נתוני עיכובים בזמן אמת מה-API של רכבת ישראל
ומפרסם ל-Kafka topic: train-delays

מקור הנתונים:
  - API לא-רשמי של rail.co.il (מתועד ע"י OpenTrainCommunity)
  - שדה Delays: עיכוד בדקות לפי תחנה
  - שדה TrainPositions: מיקום רכבת + עיכוד נוכחי

הרצה:
  python train_delay_collector.py
  python train_delay_collector.py --once
"""

import json
import time
import logging
import argparse
from datetime import datetime, timezone, date

import requests
from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRAIN_DELAYS,
    RAIL_API_BASE,
    TRAIN_POLL_INTERVAL_SECONDS,
    MONITORED_TRAIN_ROUTES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRAIN-COLLECTOR] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# Headers נדרשים לגישה ל-API של רכבת ישראל
RAIL_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.rail.co.il/",
}


# ─── Rail API client ──────────────────────────────────────────────────────────

def fetch_route_delays(origin: str, destination: str, travel_date: str = None) -> dict:
    """
    שולף נתוני מסלול + עיכובים לנסיעה ספציפית.
    
    endpoint: /Plan/GetRoutes
    פרמטרים:
      OId   : תחנת מוצא (מזהה 4 ספרות)
      TId   : תחנת יעד
      Date  : תאריך (DD/MM/YYYY)
      Hour  : שעה (0 = כל היום)
      
    Response מכיל:
      Delays[]         : עיכובים לפי תחנה ורכבת
      TrainPositions[] : מיקום + עיכוד נוכחי לכל רכבת פעילה
    """
    if travel_date is None:
        travel_date = date.today().strftime("%d/%m/%Y")

    url = f"{RAIL_API_BASE}/Plan/GetRoutes"
    params = {
        "OId":    origin,
        "TId":    destination,
        "Date":   travel_date,
        "Hour":   "0",
        "isGoing": "1",
        "SavedName": "",
    }
    try:
        resp = requests.get(url, params=params, headers=RAIL_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("MessageType") == 2:  # success
            return data.get("Data", {})
        log.warning(f"Rail API returned non-success for {origin}→{destination}: {data.get('Message')}")
        return {}
    except requests.RequestException as e:
        log.error(f"Rail API error ({origin}→{destination}): {e}")
        return {}


# ─── Normalization ────────────────────────────────────────────────────────────

def normalize_delay_record(raw_delay: dict, route_meta: dict) -> dict:
    """
    ממפה רשומת עיכוד מה-API לפורמט אחיד.
    
    raw_delay שדות:
      Station : מזהה תחנה
      Date    : תאריך YYYYMMDD
      Time    : שעה HHMMSS
      Train   : מספר רכבת
      Min     : עיכוד בדקות (מחרוזת "0006" = 6 דקות)
    """
    delay_str = raw_delay.get("Min", "0")
    try:
        delay_min = int(delay_str)
    except ValueError:
        delay_min = 0

    return {
        "source":         "rail_api_delays",
        "transport_type": "train",
        "collected_at":   datetime.now(timezone.utc).isoformat(),
        # זיהוי
        "train_number":   raw_delay.get("Train"),
        "station_id":     raw_delay.get("Station"),
        # מסלול
        "route_origin":      route_meta.get("origin"),
        "route_destination": route_meta.get("destination"),
        # זמנים
        "delay_date":     raw_delay.get("Date"),
        "delay_time":     raw_delay.get("Time"),
        # עיכוד
        "delay_minutes":  delay_min,
        "delay_seconds":  delay_min * 60,
        "is_delayed":     delay_min > 0,
    }


def normalize_position_record(raw_pos: dict, route_meta: dict) -> dict:
    """
    ממפה רשומת מיקום רכבת לפורמט אחיד.
    
    raw_pos שדות:
      TrainNumber    : מספר רכבת
      CurrentStation : תחנה נוכחית
      NextStation    : תחנה הבאה
      DifMin         : עיכוד נוכחי בדקות
      DifType        : סוג עיכוד (1=מאחר, 2=מוקדם, 0=בזמן)
    """
    dif_min  = raw_pos.get("DifMin", 0)
    dif_type = raw_pos.get("DifType", 0)

    # DifType: 1=עיכוד, 2=מוקדם
    if dif_type == 2:
        dif_min = -abs(dif_min)

    return {
        "source":         "rail_api_positions",
        "transport_type": "train",
        "collected_at":   datetime.now(timezone.utc).isoformat(),
        # זיהוי
        "train_number":      str(raw_pos.get("TrainNumber")),
        "current_station":   raw_pos.get("CurrentStation"),
        "next_station":      raw_pos.get("NextStation"),
        # מסלול
        "route_origin":      route_meta.get("origin"),
        "route_destination": route_meta.get("destination"),
        # עיכוד
        "delay_minutes":  dif_min,
        "delay_seconds":  dif_min * 60,
        "delay_type":     {0: "on_time", 1: "delayed", 2: "early"}.get(dif_type, "unknown"),
        "is_delayed":     dif_type == 1,
        "is_early":       dif_type == 2,
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
        key = f"{record.get('train_number', 'unknown')}_{record.get('station_id') or record.get('current_station', '')}"
        producer.send(topic, key=key, value=record)
        published += 1
    producer.flush()
    return published


# ─── Main loop ────────────────────────────────────────────────────────────────

def collect_once(producer: KafkaProducer) -> int:
    total = 0

    for route in MONITORED_TRAIN_ROUTES:
        origin = route["origin"]
        dest   = route["destination"]

        data = fetch_route_delays(origin, dest)
        if not data:
            continue

        # 1) עיכובים לפי תחנה
        raw_delays = data.get("Delays", []) or []
        delay_records = [
            normalize_delay_record(d, route)
            for d in raw_delays
            if int(d.get("Min", 0)) > 0  # רק עיכובים ממשיים
        ]
        if delay_records:
            n = publish_records(producer, delay_records, KAFKA_TOPIC_TRAIN_DELAYS)
            log.info(f"Route {origin}→{dest}: {n} delay records published")
            total += n

        # 2) מיקומי רכבות נוכחיים
        raw_positions = data.get("TrainPositions", []) or []
        position_records = [
            normalize_position_record(p, route)
            for p in raw_positions
        ]
        if position_records:
            n = publish_records(producer, position_records, KAFKA_TOPIC_TRAIN_DELAYS)
            log.info(f"Route {origin}→{dest}: {n} position records published")
            total += n

        # המתנה קצרה בין מסלולים למניעת rate limiting
        time.sleep(1)

    return total


def run(once: bool = False):
    log.info(f"Starting Train Delay Collector → topic: {KAFKA_TOPIC_TRAIN_DELAYS}")
    log.info(f"Monitoring {len(MONITORED_TRAIN_ROUTES)} routes")
    producer = create_producer()
    try:
        while True:
            start = time.time()
            count = collect_once(producer)
            elapsed = time.time() - start
            log.info(f"Cycle complete: {count} total records in {elapsed:.1f}s")

            if once:
                break
            sleep_time = max(0, TRAIN_POLL_INTERVAL_SECONDS - elapsed)
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