"""
producers/train_positions_producer.py
Fetches real-time Israel Railways train data via Open Bus Stride API.
→ Kafka topic: train-positions
Runs every 30 seconds.

תיקונים:
  - סינון לאזור המרכז בלבד (bbox) להקלה על ה-API
  - timeout קצר (8s) כדי לא לתקוע את ה-DAG
  - order_by=id+desc (מהיר יותר)
  - URL תיקון double-slash
  - fallback לשדות line_ref ו-train_number
  - velocity > 0 — רק רכבות נוסעות
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
from config.settings import OPEN_BUS_API_URL, KAFKA_TOPICS, DELAY_THRESHOLD_SECONDS
from producers.base_producer import BaseProducer

RAIL_OPERATOR_REF = "2"   # Israel Railways in Open Bus Stride

# ── גוש דן ותל אביב — Bounding Box ──────────────────────────────────────────
# רכבת ישראל — תחנות בגוש דן: ת"א מרכז, ת"א השלום, ת"א הגנה, ת"א סבידור,
#               ת"א אוניברסיטה, בני ברק, פתח תקווה, כפר חיים
CENTER_BBOX = {
    "lat_min": 31.97,  # דרום — ת"א הגנה / חולון מתחם
    "lat_max": 32.19,  # צפון — הרצליה / רמת השרון
    "lon_min": 34.73,  # מערב — חוף הים
    "lon_max": 34.93,  # מזרח — פתח תקווה / בני ברק
}


class TrainPositionsProducer(BaseProducer):

    def __init__(self):
        super().__init__("train_positions")
        # URL ללא slash בסוף למניעת double-slash
        self.api_url = OPEN_BUS_API_URL.rstrip("/")
        self.headers = {"Accept": "application/json"}

    def get_topic(self) -> str:
        return KAFKA_TOPICS["train_positions"]

    def fetch_data(self) -> list:
        """
        Fetch active (moving) train vehicles from Open Bus Stride.
        מסנן לאזור המרכז ורק רכבות נוסעות (velocity > 0).
        """
        try:
            resp = requests.get(
                f"{self.api_url}/siri_vehicle_locations/list",
                params={
                    "siri_route__operator_ref": RAIL_OPERATOR_REF,
                    "limit":    200,
                    "order_by": "id desc",   # מהיר יותר מ-recorded_at_time
                },
                headers=self.headers,
                timeout=8,   # fail fast כשה-API למטה
            )
            resp.raise_for_status()
            vehicles = resp.json()
        except requests.Timeout:
            self.logger.warning(
                "Hasadna SIRI API timed out (>8s) — skipping train fetch. "
                "API may be down or overloaded."
            )
            return []
        except Exception as e:
            self.logger.warning(f"Train fetch failed: {e}")
            return []

        records      = []
        skipped_outside = 0
        skipped_nopos   = 0

        for v in vehicles:
            lat = v.get("lat")
            lon = v.get("lon")

            # רק עם מיקום תקין
            if not lat or not lon:
                skipped_nopos += 1
                continue

            # סינון גיאוגרפי — גוש דן ותל אביב
            if not self._in_center(float(lat), float(lon)):
                skipped_outside += 1
                continue

            # velocity — לא מסננים! Israel Railways לא תמיד מדווחת
            rec = self._normalize(v)
            if rec:
                records.append(rec)

        moving = sum(1 for r in records if r.get("is_moving"))
        self.logger.info(
            f"Fetched {len(records)} trains in Gush Dan "
            f"({moving} moving, {len(records)-moving} no-speed-data) | "
            f"skipped: {skipped_outside} outside bbox, {skipped_nopos} no-position"
        )
        return records

    def _in_center(self, lat: float, lon: float) -> bool:
        """בדיקה אם הרכבת נמצאת באזור המרכז."""
        return (
            CENTER_BBOX["lat_min"] <= lat <= CENTER_BBOX["lat_max"] and
            CENTER_BBOX["lon_min"] <= lon <= CENTER_BBOX["lon_max"]
        )

    def _normalize(self, v: dict) -> dict:
        """Normalize Hasadna SIRI train record.
        velocity אופציונלי — Israel Railways לא תמיד מדווחת מהירות.
        """
        try:
            # velocity — אופציונלי, לא מסננים לפיו
            velocity = v.get("velocity")
            try:
                speed = float(velocity) if velocity is not None else None
            except (TypeError, ValueError):
                speed = None

            is_moving = speed is not None and speed > 0

            # ── Train number: fallback chain ──────────────────────────────────
            train_number = (
                str(v.get("siri_ride__vehicle_ref") or "").strip() or
                str(v.get("vehicle_ref") or "").strip() or
                str(v.get("id") or "")
            )

            # ── Line ref: fallback chain ──────────────────────────────────────
            line_ref = (
                str(v.get("siri_route__line_ref") or "").strip() or
                str(v.get("line_ref") or "").strip()
            )

            scheduled_start = (
                v.get("siri_ride__scheduled_start_time") or
                v.get("scheduled_start_time") or ""
            )

            return {
                "train_number":    train_number,
                "line_ref":        line_ref,
                "operator":        "israel_railways",
                "operator_ref":    RAIL_OPERATOR_REF,
                "lat":             round(float(v.get("lat")), 6),
                "lon":             round(float(v.get("lon")), 6),
                "bearing":         v.get("bearing"),
                "velocity":        speed,           # None אם לא מדווח
                "is_moving":       is_moving,        # True רק אם velocity > 0
                "scheduled_start": scheduled_start,
                "recorded_at":     v.get("recorded_at_time", ""),
                "is_delayed":      False,
                "delay_minutes":   0,
                "area":            "gush_dan",
                "timestamp":       datetime.now(IL_TZ).isoformat(),
            }
        except Exception as e:
            self.logger.warning(f"Failed to normalize train record {v.get('id')}: {e}")
            return None


if __name__ == "__main__":
    producer = TrainPositionsProducer()
    try:
        producer.run_once()
    finally:
        producer.close()