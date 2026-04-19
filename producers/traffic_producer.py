"""
producers/traffic_producer.py
Fetches real-time traffic flow data from HERE Traffic API for all of Israel.
→ Kafka topic: traffic-data
Runs every 5 minutes.

HERE Traffic Flow API v7:
  https://traffic.ls.hereapi.com/traffic/6.3/flow.json
  Returns traffic flow segments with current speed vs free-flow speed.

Coverage: Israel bounding box (29.4–33.4 lat, 34.2–35.9 lon)
  Split into 9 tiles (3x3 grid) to cover the whole country.
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
from config.settings import HERE_API_KEY, KAFKA_TOPICS
from producers.base_producer import BaseProducer


# גוש דן ותל אביב בלבד — שני tiles ממוקדים
# (name,          lat_min, lat_max, lon_min, lon_max)
ISRAEL_TILES = [
    ("tel_aviv",  31.97,   32.19,   34.73,   34.93),  # תל אביב + גוש דן
    ("center",    31.80,   32.00,   34.75,   34.95),  # מרכז — רמת גן, גבעתיים, חולון, בת ים
]

# HERE congestion level mapping
CONGESTION_MAP = {
    1: "free",
    2: "minor",
    3: "moderate",
    4: "heavy",
    5: "blocked",
}


class TrafficProducer(BaseProducer):

    def __init__(self):
        super().__init__("traffic")
        self.api_key = HERE_API_KEY
        self.headers = {"Accept": "application/json"}

    def get_topic(self) -> str:
        return KAFKA_TOPICS["traffic_data"]

    def fetch_data(self) -> list:
        """Fetch traffic flow for all Israel tiles."""
        if not self.api_key:
            self.logger.error("HERE_API_KEY not set — skipping traffic fetch")
            return []

        all_records = []
        for tile in ISRAEL_TILES:
            name, lat_min, lat_max, lon_min, lon_max = tile
            records = self._fetch_tile(name, lat_min, lat_max, lon_min, lon_max)
            all_records.extend(records)
            self.logger.info(f"Tile {name}: {len(records)} segments")

        self.logger.info(f"Total traffic segments fetched: {len(all_records)}")
        return all_records

    def _fetch_tile(self, tile_name: str, lat_min: float, lat_max: float,
                    lon_min: float, lon_max: float) -> list:
        """Fetch traffic flow for a single geographic tile."""
        bbox = f"{lon_min},{lat_min},{lon_max},{lat_max}"
        try:
            resp = requests.get(
                "https://data.traffic.hereapi.com/v7/flow",
                params={
                    "apiKey":      self.api_key,
                    "in":          f"bbox:{bbox}",
                    "locationReferencing": "shape",
                    "lang":        "en-US",  # road names in English
                },
                headers=self.headers,
                timeout=20,
            )
            if resp.status_code == 401:
                self.logger.error("HERE API: Invalid API key")
                return []
            if resp.status_code == 429:
                self.logger.warning("HERE API: Rate limit reached")
                return []
            resp.raise_for_status()

            data = resp.json()
            results = data.get("results", [])
            return [self._normalize(r, tile_name) for r in results if r]

        except requests.RequestException as e:
            self.logger.warning(f"Tile {tile_name} fetch failed: {e}")
            return []
        except Exception as e:
            self.logger.warning(f"Tile {tile_name} parse error: {e}")
            return []

    def _normalize(self, result: dict, tile_name: str) -> dict:
        """Normalize a HERE traffic flow result into a flat record."""
        now = datetime.now(IL_TZ)

        # Location info
        location    = result.get("location", {})
        description = location.get("description", "")
        shape       = location.get("shape", {})
        links       = shape.get("links", [{}])
        first_link  = links[0] if links else {}
        points      = first_link.get("points", [{}])
        mid_point   = points[len(points) // 2] if points else {}

        # Flow info
        current_flow = result.get("currentFlow", {})
        speed        = current_flow.get("speed", 0)          # km/h current
        free_flow    = current_flow.get("freeFlow", 0)        # km/h free flow
        jam_factor   = current_flow.get("jamFactor", 0)       # 0-10
        confidence   = current_flow.get("confidence", 0)      # 0-1
        traversability = current_flow.get("traversability", "open")

        # Congestion level
        congestion_raw = current_flow.get("congestion", {})
        if isinstance(congestion_raw, dict):
            cong_value = congestion_raw.get("value", 1)
        else:
            cong_value = 1
        congestion = CONGESTION_MAP.get(cong_value, "unknown")

        # Speed ratio (0=blocked, 1=free flow)
        speed_ratio = round(speed / free_flow, 3) if free_flow > 0 else 0

        return {
            "segment_id":       first_link.get("id", ""),     # FIX: HERE v7 segment ID lives in location.shape.links[].id, not result.id
            "tile_name":        tile_name,
            "description":      description,
            "lat":              mid_point.get("lat"),
            "lon":              mid_point.get("lng"),
            "speed_kmh":        speed,
            "free_flow_kmh":    free_flow,
            "speed_ratio":      speed_ratio,
            "jam_factor":       jam_factor,          # 0=free, 10=blocked
            "congestion":       congestion,           # free/minor/moderate/heavy/blocked
            "confidence":       confidence,
            "traversability":   traversability,
            "is_congested":     jam_factor >= 4,
            "is_blocked":       traversability == "closed" or jam_factor >= 9,
            "delay_minutes":    self._estimate_delay(speed, free_flow),
            "road_name":        description.split("/")[0].strip() if "/" in description else description,
            "timestamp":        int(now.timestamp()),
            "recorded_at":      now.isoformat(),
            "hour_of_day":      now.hour,
            "day_of_week":      now.strftime("%A"),
        }

    def _estimate_delay(self, speed: float, free_flow: float) -> float:
        """Estimate delay in minutes per km based on speed vs free flow."""
        if not speed or not free_flow or speed <= 0:
            return 0
        # time_per_km = 60/speed (minutes) vs 60/free_flow (minutes)
        delay = (60 / speed) - (60 / free_flow)
        return round(max(delay, 0), 2)


if __name__ == "__main__":
    producer = TrafficProducer()
    try:
        producer.run_once()
    finally:
        producer.close()