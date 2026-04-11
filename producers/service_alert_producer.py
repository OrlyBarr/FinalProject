"""
producers/service_alerts_producer.py
Fetches service alerts (disruptions, cancellations) from MOT GTFS-RT.
→ Kafka topic: service-alerts
Runs every 2 minutes.

Data source: https://gtfs.mot.gov.il/gtfsfiles/ServiceAlerts.pb
Contains: alerts about disruptions affecting routes/stops/agencies.
"""

import requests
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone
import sys
sys.path.append("..")
from config.settings import GTFS_RT_ALERTS_URL, KAFKA_TOPICS
from producers.base_producer import BaseProducer

# Alert cause codes → Hebrew/English labels
CAUSE_MAP = {
    1: "unknown_cause",         2: "other_cause",
    3: "technical_problem",     4: "strike",
    5: "demonstration",         6: "accident",
    7: "holiday",               8: "weather",
    9: "maintenance",           10: "construction",
    11: "police_activity",      12: "medical_emergency",
}

EFFECT_MAP = {
    1: "no_service",            2: "reduced_service",
    3: "significant_delays",    4: "detour",
    5: "additional_service",    6: "modified_service",
    7: "other_effect",          8: "unknown_effect",
    9: "stop_moved",            10: "no_effect",
    11: "accessibility_issue",
}


class ServiceAlertsProducer(BaseProducer):

    def __init__(self):
        super().__init__("service_alerts")
        self.url = GTFS_RT_ALERTS_URL

    def get_topic(self) -> str:
        return KAFKA_TOPICS["service_alerts"]

    def fetch_data(self) -> list:
        try:
            response = requests.get(self.url, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch ServiceAlerts: {e}")
            return []

        # Guard: MOT server sometimes returns an HTML error page (IP/WAF block)
        # instead of a binary protobuf — detect it early for a clear error message.
        if response.headers.get("Content-Type", "").startswith("text/html"):
            self.logger.error(
                f"MOT ServiceAlerts feed returned HTML instead of protobuf — "
                f"IP may be blocked by WAF or service is unavailable "
                f"(HTTP {response.status_code}, {len(response.content)} bytes). "
                f"Check https://gtfs.mot.gov.il/ for status."
            )
            return []

        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(response.content)
        except Exception as e:
            self.logger.error(f"Protobuf parse error: {e}")
            return []

        records = []
        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue
            normalized = self._normalize(entity.id, entity.alert, feed.header.timestamp)
            if normalized:
                records.append(normalized)

        self.logger.info(f"Parsed {len(records)} service alerts")
        return records

    def _normalize(self, entity_id: str, alert, feed_timestamp: int) -> dict:
        # Extract affected entities
        affected_routes = []
        affected_stops  = []
        affected_trips  = []
        affected_agencies = []

        for informed in alert.informed_entity:
            if informed.route_id:
                affected_routes.append(informed.route_id)
            if informed.stop_id:
                affected_stops.append(informed.stop_id)
            if informed.trip.trip_id:
                affected_trips.append(informed.trip.trip_id)
            if informed.agency_id:
                affected_agencies.append(informed.agency_id)

        # Extract active period
        active_start = None
        active_end   = None
        if alert.active_period:
            active_start = alert.active_period[0].start
            active_end   = alert.active_period[0].end if alert.active_period[0].end else None

        # Extract translated text
        header_text = self._get_translation(alert.header_text)
        desc_text   = self._get_translation(alert.description_text)
        url_text    = self._get_translation(alert.url)

        return {
            "alert_id":          entity_id,
            "cause":             CAUSE_MAP.get(alert.cause, "unknown"),
            "effect":            EFFECT_MAP.get(alert.effect, "unknown"),
            "header_text":       header_text,
            "description_text":  desc_text,
            "url":               url_text,
            "severity":          self._calc_severity(alert.effect),
            "active_start":      active_start,
            "active_end":        active_end,
            "affected_routes":   list(set(affected_routes))[:20],
            "affected_stops":    list(set(affected_stops))[:20],
            "affected_trips":    list(set(affected_trips))[:20],
            "affected_agencies": list(set(affected_agencies)),
            "routes_count":      len(set(affected_routes)),
            "stops_count":       len(set(affected_stops)),
            "feed_timestamp":    feed_timestamp,
        }

    def _get_translation(self, translated_string, lang="he") -> str:
        """Extract Hebrew text from GTFS-RT TranslatedString, fallback to English."""
        if not translated_string.translation:
            return ""
        for t in translated_string.translation:
            if t.language == lang:
                return t.text
        # fallback: first available
        return translated_string.translation[0].text if translated_string.translation else ""

    def _calc_severity(self, effect_code: int) -> str:
        """Map effect code to severity label."""
        if effect_code in (1, 3):    return "high"    # no_service / significant_delays
        if effect_code in (2, 4, 6): return "medium"  # reduced_service / detour / modified
        return "low"


if __name__ == "__main__":
    producer = ServiceAlertsProducer()
    try:
        producer.run_once()
    finally:
        producer.close()