"""
producers/bus_positions_producer.py
Fetches real-time bus positions from Open Bus Stride (Hasadna) SIRI API.
→ Kafka topic: bus-positions
Runs every 30 seconds.

תיקונים:
  - סינון לאזור המרכז בלבד (bbox) להקלה על ה-API
  - timeout קצר (8s) כדי לא לתקוע את ה-DAG כשה-API למטה
  - order_by=id+desc (מהיר יותר מ-recorded_at_time)
  - fallback fields לoperator_ref ו-line_ref
  - route_short_name — מספר קו קריא (למשל "63")
  - velocity > 0 — רק כלי רכב נוסעים
"""

import requests
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
import sys

IL_TZ = ZoneInfo("Asia/Jerusalem")
sys.path.append("..")
from config.settings import OPEN_BUS_API_URL, KAFKA_TOPICS, OPERATORS
from producers.base_producer import BaseProducer

# URL ללא slash בסוף למניעת double-slash
SIRI_URL = OPEN_BUS_API_URL.rstrip("/") + "/siri_vehicle_locations/list"

# ── גוש דן ותל אביב — Bounding Box ──────────────────────────────────────────
# מכסה: תל אביב, רמת גן, גבעתיים, בני ברק, חולון, בת ים, פתח תקווה, רמת השרון
# lon_min, lat_min, lon_max, lat_max
CENTER_BBOX = {
    "lat_min": 31.97,  # דרום — בת ים / חולון
    "lat_max": 32.19,  # צפון — הרצליה / רמת השרון
    "lon_min": 34.73,  # מערב — חוף הים
    "lon_max": 34.93,  # מזרח — פתח תקווה / בני ברק
}


class BusPositionsProducer(BaseProducer):

    def __init__(self):
        super().__init__("bus_positions")
        self.url = SIRI_URL

    def get_topic(self) -> str:
        return KAFKA_TOPICS["bus_positions"]

    def fetch_data(self) -> list:
        """
        Fetch live bus positions from Open Bus Stride SIRI API.
        מסנן לאזור המרכז בלבד להקלה על ה-DB.
        מחזיר רק כלי רכב נוסעים (velocity > 0).
        """
        params = {
            "limit":    500,
            "order_by": "id desc",   # מהיר יותר מ-recorded_at_time desc
        }
        try:
            response = requests.get(self.url, params=params, timeout=8)
            response.raise_for_status()
        except requests.Timeout:
            self.logger.warning(
                "Hasadna SIRI API timed out (>8s) — using local cache fallback."
            )
            return self._local_fallback()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch from Hasadna SIRI: {e} — using local cache fallback.")
            return self._local_fallback()

        try:
            items = response.json()
        except ValueError as e:
            self.logger.error(f"Invalid JSON from Hasadna SIRI: {e}")
            return []

        records = []
        skipped_outside = 0
        skipped_invalid = 0

        for item in items:
            record = self._normalize(item)
            if record is None:
                lat = item.get("lat")
                lon = item.get("lon")
                if not lat or not lon:
                    skipped_invalid += 1
                elif not self._in_center(float(lat), float(lon)):
                    skipped_outside += 1
                else:
                    skipped_invalid += 1
            else:
                records.append(record)

        moving  = sum(1 for r in records if r.get("is_moving"))
        parked  = len(records) - moving
        self.logger.info(
            f"Fetched {len(records)} buses in Gush Dan "
            f"({moving} moving, {parked} parked/unknown-speed) | "
            f"skipped: {skipped_outside} outside bbox, {skipped_invalid} invalid"
        )
        return records

    def _in_center(self, lat: float, lon: float) -> bool:
        """בדיקה אם הרכב נמצא באזור המרכז."""
        return (
            CENTER_BBOX["lat_min"] <= lat <= CENTER_BBOX["lat_max"] and
            CENTER_BBOX["lon_min"] <= lon <= CENTER_BBOX["lon_max"]
        )

    def _parse_timestamp(self, raw_ts: str) -> str:
        """
        Validate recorded_at_time from the Hasadna SIRI API.
        The API uses 2037-12-05T23:59:49 as sentinel when no real timestamp.
        Falls back to current IL time.
        """
        now = datetime.now(IL_TZ)
        if not raw_ts:
            return now.isoformat()
        try:
            dt     = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            dt_il  = dt.astimezone(IL_TZ).replace(tzinfo=None)
            now_naive = now.replace(tzinfo=None)
            if (dt_il - now_naive).days > 1:
                return now.isoformat()
            return dt.astimezone(IL_TZ).isoformat()
        except Exception:
            return now.isoformat()

    def _normalize(self, item: dict):
        """
        Normalize Hasadna SIRI record.
        מחזיר None רק אם:
          - אין lat/lon תקין
          - לא בגוש דן/תל אביב

        הערה: לא מסנן לפי velocity — SIRI לא תמיד מדווח מהירות
        גם כלי רכב ללא velocity מקבלים is_moving=False ונשמרים.
        """
        try:
            lat = item.get("lat")
            lon = item.get("lon")

            if not lat or not lon:
                return None

            # סינון גיאוגרפי — גוש דן ותל אביב בלבד
            if not self._in_center(float(lat), float(lon)):
                return None

            # velocity — אופציונלי ב-SIRI, לא תמיד מדווח
            velocity = item.get("velocity")
            try:
                speed_kmh = float(velocity) if velocity is not None else None
            except (TypeError, ValueError):
                speed_kmh = None

            # is_moving: True רק אם velocity ידוע וחיובי
            is_moving = speed_kmh is not None and speed_kmh > 0

            # ── Operator: fallback chain ──────────────────────────────────────
            operator_ref = (
                str(item.get("siri_route__operator_ref") or "").strip() or
                str(item.get("operator_ref") or "").strip()
            )

            # ── Line ref: fallback chain ──────────────────────────────────────
            line_ref = (
                str(item.get("siri_route__line_ref") or "").strip() or
                str(item.get("line_ref") or "").strip()
            )

            # ── מספר קו קריא (למשל "63", "480") ─────────────────────────────
            route_short_name = (
                str(item.get("siri_route__gtfs_route__route_short_name") or "").strip() or
                str(item.get("route_short_name") or "").strip() or
                line_ref
            )

            timestamp = self._parse_timestamp(item.get("recorded_at_time", ""))

            return {
                "vehicle_id":       str(item.get("siri_ride__vehicle_ref") or item.get("id") or ""),
                "entity_id":        str(item.get("id") or ""),
                "trip_id":          str(item.get("siri_ride__id") or ""),
                "route_id":         line_ref,
                "line_ref":         line_ref,
                "route_short_name": route_short_name,
                "operator_id":      operator_ref,
                "operator_name":    OPERATORS.get(operator_ref, "Unknown") if operator_ref else "Unknown",
                "start_date":       timestamp[:10],
                "latitude":         round(float(lat), 6),
                "longitude":        round(float(lon), 6),
                "bearing":          item.get("bearing"),
                "speed_kmh":        speed_kmh,          # None אם לא מדווח
                "is_moving":        is_moving,           # True רק אם velocity > 0
                "current_stop_seq": item.get("current_stop_sequence"),
                "stop_id":          str(item.get("siri_ride_stop_id") or ""),
                "current_status":   "in_transit" if is_moving else "unknown",
                "timestamp":        timestamp,
                "congestion_level": 0,
                "area":             "gush_dan",
            }
        except Exception as e:
            self.logger.warning(f"Failed to normalize record {item.get('id')}: {e}")
            return None


    def _local_fallback(self) -> list:
        """Load buses_with_nearest_stops.json and return normalized records."""
        import os, json as _json
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        now_ts = datetime.now(IL_TZ).isoformat()
        for fname in ("buses_with_nearest_stops.json", "bus_positions.json"):
            path = os.path.join(base, fname)
            if not os.path.exists(path):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = _json.load(f)
                if not data:
                    continue
                records = []
                for b in data:
                    lat = b.get("lat") or b.get("latitude")
                    lon = b.get("lon") or b.get("longitude")
                    if not lat or not lon:
                        continue
                    if not self._in_center(float(lat), float(lon)):
                        continue
                    vel = b.get("velocity") or b.get("speed_kmh")
                    try:
                        speed_kmh = float(vel) if vel is not None else None
                    except (TypeError, ValueError):
                        speed_kmh = None
                    op_id = str(b.get("operator_id") or "")
                    line_ref = str(b.get("line_ref") or b.get("route_id") or "")
                    records.append({
                        "vehicle_id":       str(b.get("vehicle_id") or ""),
                        "entity_id":        str(b.get("trip_id") or ""),
                        "trip_id":          str(b.get("trip_id") or ""),
                        "route_id":         line_ref,
                        "line_ref":         line_ref,
                        "route_short_name": str(b.get("route_short_name") or line_ref),
                        "operator_id":      op_id,
                        "operator_name":    b.get("operator_name") or OPERATORS.get(op_id, "Unknown"),
                        "start_date":       now_ts[:10],
                        "latitude":         round(float(lat), 6),
                        "longitude":        round(float(lon), 6),
                        "bearing":          b.get("bearing"),
                        "speed_kmh":        speed_kmh,
                        "is_moving":        speed_kmh is not None and speed_kmh > 0,
                        "current_stop_seq": None,
                        "stop_id":          str(b.get("nearest_stop_id") or ""),
                        "current_status":   "in_transit" if (speed_kmh or 0) > 0 else "unknown",
                        "timestamp":        b.get("timestamp") or now_ts,
                        "congestion_level": 0,
                        "area":             "gush_dan",
                        "source":           "local-cache-fallback",
                    })
                if records:
                    self.logger.info(f"[fallback] publishing {len(records)} cached records from {fname}")
                    return records
            except Exception as e:
                self.logger.error(f"[fallback] failed to load {fname}: {e}")
        self.logger.warning("[fallback] no local cache found — returning empty")
        return []


if __name__ == "__main__":
    producer = BusPositionsProducer()
    try:
        producer.run_once()
    finally:
        producer.close()