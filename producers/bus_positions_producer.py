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
                "Hasadna SIRI API timed out (>8s) — skipping this cycle. "
                "API may be down or overloaded."
            )
            return []
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch from Hasadna SIRI: {e}")
            return []

        try:
            items = response.json()
        except ValueError as e:
            self.logger.error(f"Invalid JSON from Hasadna SIRI: {e}")
            return []

        records = []
        skipped_parked   = 0
        skipped_outside  = 0
        skipped_invalid  = 0

        for item in items:
            record = self._normalize(item)
            if record is None:
                # קבע סיבת דחייה ללוג
                lat = item.get("lat")
                lon = item.get("lon")
                vel = item.get("velocity")
                if not lat or not lon:
                    skipped_invalid += 1
                elif not self._in_center(float(lat), float(lon)):
                    skipped_outside += 1
                elif vel is None or float(vel) <= 0:
                    skipped_parked += 1
                else:
                    skipped_invalid += 1
            else:
                records.append(record)

        self.logger.info(
            f"Fetched {len(records)} moving buses in center Israel | "
            f"skipped: {skipped_parked} parked, "
            f"{skipped_outside} outside center, "
            f"{skipped_invalid} invalid"
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
        מחזיר None אם:
          - אין lat/lon
          - לא באזור המרכז
          - velocity <= 0 (חונה)
        """
        try:
            lat = item.get("lat")
            lon = item.get("lon")

            if not lat or not lon:
                return None

            # סינון גיאוגרפי — מרכז ישראל בלבד
            if not self._in_center(float(lat), float(lon)):
                return None

            # רק כלי רכב נוסעים
            velocity = item.get("velocity")
            if velocity is None or float(velocity) <= 0:
                return None

            # ── Operator: fallback chain ──────────────────────────────────────
            # siri_route__operator_ref — שדה ראשי מה-Stride API
            # operator_ref             — fallback ברמת ה-item
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
                "speed_kmh":        float(velocity),   # velocity > 0 מובטח
                "current_stop_seq": item.get("current_stop_sequence"),
                "stop_id":          str(item.get("siri_ride_stop_id") or ""),
                "current_status":   "in_transit",
                "timestamp":        timestamp,
                "congestion_level": 0,
                "area":             "center",           # תיוג לסינון
            }
        except Exception as e:
            self.logger.warning(f"Failed to normalize record {item.get('id')}: {e}")
            return None


if __name__ == "__main__":
    producer = BusPositionsProducer()
    try:
        producer.run_once()
    finally:
        producer.close()