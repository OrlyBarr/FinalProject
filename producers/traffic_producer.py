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
from zoneinfo import ZoneInfo
import sys

IL_TZ = ZoneInfo("Asia/Jerusalem")  # UTC+2 winter / UTC+3 summer (DST-aware)
sys.path.append("..")
from config.settings import HERE_API_KEY, KAFKA_TOPICS
from producers.base_producer import BaseProducer


# Israel bounding box split into tiles — Israeli territory only (excludes Jordan, Lebanon, Syria)
# Eastern border: 35.55 (west of Sea of Galilee) — excludes Jordan
# Northern border: 33.3 — excludes Lebanon/Syria
ISRAEL_TILES = [
    # (name,          lat_min, lat_max, lon_min, lon_max)
    ("galil_west",    32.7,    33.3,    34.9,    35.3),   # Western Galilee
    ("galil_east",    32.7,    33.3,    35.3,    35.55),  # Eastern Galilee + Sea of Galilee
    ("haifa",         32.5,    32.9,    34.9,    35.15),  # Haifa and Krayot
    ("shomron",       32.1,    32.5,    34.9,    35.35),  # Samaria + Jezreel Valley
    ("tel_aviv",      31.9,    32.2,    34.7,    34.95),  # Greater Tel Aviv (Gush Dan)
    ("center",        31.7,    32.1,    34.8,    35.1),   # Central Israel
    ("jerusalem",     31.6,    31.95,   34.9,    35.35),  # Jerusalem
    ("south_west",    30.8,    31.6,    34.4,    34.9),   # Shephelah + Northern Negev
    ("beer_sheva",    30.5,    31.0,    34.5,    35.1),   # Beer Sheva
    ("negev",         29.5,    30.5,    34.6,    35.2),   # Southern Negev
    ("eilat",         29.4,    29.8,    34.9,    35.05),  # Eilat
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