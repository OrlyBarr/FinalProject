"""
producers/train_positions_producer.py
Fetches real-time Israel Railways train data via Open Bus Stride API.
→ Kafka topic: train-positions
Runs every 30 seconds.

Data source:
  Open Bus Stride (Hasadna) — operator_ref=2 (Israel Railways)
  https://open-bus-stride-api.hasadna.org.il

Note: israelrail.azurewebsites.net was taken over, www.rail.co.il is blocked by Cloudflare.
"""

import requests
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
import sys

IL_TZ = ZoneInfo("Asia/Jerusalem")  # UTC+2 winter / UTC+3 summer (DST-aware)
sys.path.append("..")
from config.settings import OPEN_BUS_API_URL, KAFKA_TOPICS, DELAY_THRESHOLD_SECONDS
from producers.base_producer import BaseProducer

RAIL_OPERATOR_REF = "2"   # Israel Railways in Open Bus Stride


class TrainPositionsProducer(BaseProducer):

    def __init__(self):
        super().__init__("train_positions")
        self.api_url = OPEN_BUS_API_URL
        self.headers = {"Accept": "application/json"}

    def get_topic(self) -> str:
        return KAFKA_TOPICS["train_positions"]

    def fetch_data(self) -> list:
        """Fetch active (moving) train vehicles from Open Bus Stride."""
        try:
            resp = requests.get(
                f"{self.api_url}/siri_vehicle_locations/list",
                params={
                    "operator_ref": RAIL_OPERATOR_REF,
                    "limit":        200,
                    "order_by":     "recorded_at_time desc",
                },
                headers=self.headers,
                timeout=15,
            )
            resp.raise_for_status()
            vehicles = resp.json()

            total = len(vehicles)
            # Only include trains that have a valid position AND are moving
            records = [
                self._normalize(v) for v in vehicles
                if v.get("lat")
                and v.get("velocity") is not None
                and float(v.get("velocity", 0)) > 0
            ]
            self.logger.info(
                f"Fetched {len(records)} moving train vehicles from Open Bus Stride "
                f"(skipped {total - len(records)} parked/no-position vehicles)"
            )
            return records
        except Exception as e:
            self.logger.warning(f"Train fetch failed: {e}")
            return []

    def _normalize(self, v: dict) -> dict:
        return {
            "train_number":     str(v.get("siri_ride__vehicle_ref") or v.get("vehicle_ref") or ""),
            "line_ref":         str(v.get("siri_route__line_ref") or v.get("line_ref") or ""),
            "operator":         "israel_railways",
            "operator_ref":     RAIL_OPERATOR_REF,
            "lat":              v.get("lat"),
            "lon":              v.get("lon"),
            "bearing":          v.get("bearing"),
            "velocity":         v.get("velocity"),
            "scheduled_start":  v.get("siri_ride__scheduled_start_time", ""),
            "recorded_at":      v.get("recorded_at_time", ""),
            "is_delayed":       False,
            "delay_minutes":    0,
            "timestamp":        int(datetime.now(IL_TZ).timestamp()),
        }


if __name__ == "__main__":
    producer = TrainPositionsProducer()
    try:
        producer.run_once()
    finally:
        producer.close()