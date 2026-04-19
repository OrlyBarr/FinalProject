"""
producers/bus_positions_producer.py
Fetches real-time bus positions from Open Bus Stride (Hasadna) SIRI API.
→ Kafka topic: bus-positions
Runs every 30 seconds.

Primary source: https://open-bus-stride-api.hasadna.org.il/siri_vehicle_locations/list
(MOT GTFS-RT at gtfs.mot.gov.il is currently down — returns HTML error pages)
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
from config.settings import OPEN_BUS_API_URL, KAFKA_TOPICS, OPERATORS
from producers.base_producer import BaseProducer

# Hasadna SIRI endpoint for live vehicle locations
SIRI_URL = f"{OPEN_BUS_API_URL}/siri_vehicle_locations/list"


class BusPositionsProducer(BaseProducer):

    def __init__(self):
        super().__init__("bus_positions")
        self.url = SIRI_URL

    def get_topic(self) -> str:
        return KAFKA_TOPICS["bus_positions"]

    def fetch_data(self) -> list:
        """
        Fetch live bus positions from Open Bus Stride SIRI API.
        Returns list of normalized vehicle position dicts.
        Uses short timeout (8s) to fail fast when API is down.
        """
        params = {
            "limit":    500,
            "order_by": "id desc",   # id desc מהיר יותר מ-recorded_at_time desc
        }
        try:
            response = requests.get(self.url, params=params, timeout=8)  # fail fast
            response.raise_for_status()
        except requests.Timeout:
            self.logger.warning("Hasadna SIRI API timed out (>8s) — skipping this cycle")
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
        skipped = 0
        for item in items:
            record = self._normalize(item)
            if record:
                records.append(record)
            else:
                skipped += 1

        self.logger.info(
            f"Fetched {len(records)} moving bus positions from Hasadna SIRI "
            f"(skipped {skipped} parked/invalid vehicles)"
        )
        return records

    def _parse_timestamp(self, raw_ts: str) -> str:
        """
        Validate recorded_at_time from the Hasadna SIRI API.
        The API uses 2037-12-05T23:59:49 (Unix ≈ 2^31, a 32-bit int sentinel)
        when the vehicle has not reported a real timestamp.
        Fall back to current UTC time in that case.
        """
        now = datetime.now(IL_TZ)
        if not raw_ts:
            return now.isoformat()
        try:
            dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            dt_il = dt.astimezone(IL_TZ).replace(tzinfo=None)
            now_naive = now.replace(tzinfo=None)
            # Any timestamp more than 1 day in the future is a sentinel value
            if (dt_il - now_naive).days > 1:
                return now.isoformat()
            return dt.astimezone(IL_TZ).isoformat()
        except Exception:
            return now.isoformat()

    def _normalize(self, item: dict) -> dict:
        """Normalize Hasadna SIRI vehicle location to flat dict.
        Only returns records for actively moving vehicles (velocity > 0).

        Stride API field reference (confirmed from live data):
          siri_route__operator_ref                    — operator numeric ID (e.g. "6" for NTA)
          siri_route__line_ref                        — line reference string
          siri_route__gtfs_route__route_short_name    — human-readable line number (e.g. "63")
          siri_ride__vehicle_ref                      — vehicle ID
          siri_ride__id                               — ride/trip ID
        """
        try:
            lat = item.get("lat")
            lon = item.get("lon")
            # Basic Israel bounds check
            if not lat or not lon:
                return None
            if not (29.5 <= lat <= 33.3 and 34.2 <= lon <= 35.9):
                return None

            # Skip parked / stationary buses — only include moving vehicles
            velocity = item.get("velocity")
            if velocity is None or float(velocity) <= 0:
                return None

            # ── Operator: try siri_route__ first, then top-level fallback ────
            operator_ref = (
                str(item.get("siri_route__operator_ref") or "").strip() or
                str(item.get("operator_ref") or "").strip()
            )

            # ── Line ref: try siri_route__ first, then top-level fallback ───
            line_ref = (
                str(item.get("siri_route__line_ref") or "").strip() or
                str(item.get("line_ref") or "").strip()
            )

            # ── Human-readable route number (e.g. "63", "480") ───────────────
            route_short_name = (
                str(item.get("siri_route__gtfs_route__route_short_name") or "").strip() or
                str(item.get("route_short_name") or "").strip() or
                line_ref  # fallback to line_ref if no short name
            )

            timestamp = self._parse_timestamp(item.get("recorded_at_time", ""))

            return {
                "vehicle_id":       str(item.get("siri_ride__vehicle_ref") or item.get("id") or ""),
                "entity_id":        str(item.get("id") or ""),
                "trip_id":          str(item.get("siri_ride__id") or ""),
                "route_id":         line_ref,
                "line_ref":         line_ref,
                "route_short_name": route_short_name,      # מספר קו קריא, למשל "63"
                "operator_id":      operator_ref,
                "operator_name":    OPERATORS.get(operator_ref, "Unknown") if operator_ref else "Unknown",
                "start_date":       timestamp[:10],
                "latitude":         round(float(lat), 6),
                "longitude":        round(float(lon), 6),
                "bearing":          item.get("bearing"),
                "speed_kmh":        float(item.get("velocity") or 0),  # velocity > 0 מובטח
                "current_stop_seq": item.get("current_stop_sequence"),
                "stop_id":          str(item.get("siri_ride_stop_id") or ""),
                "current_status":   "in_transit",          # velocity > 0 מובטח כאן
                "timestamp":        timestamp,
                "congestion_level": 0,
            }
        except Exception as e:
            self.logger.warning(f"Failed to normalize Hasadna record {item.get('id')}: {e}")
            return None


if __name__ == "__main__":
    producer = BusPositionsProducer()
    try:
        producer.run_once()
    finally:
        producer.close()