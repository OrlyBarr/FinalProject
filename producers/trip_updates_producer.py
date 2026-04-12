"""
producers/trip_updates_producer.py
Fetches real-time trip updates (delays, cancellations) from MOT GTFS-RT.
→ Kafka topic: trip-updates
Runs every 60 seconds.

Data source: https://gtfs.mot.gov.il/gtfsfiles/TripUpdates.pb
Contains: stop-time updates, departure/arrival delays per trip.
"""

import requests
from google.transit import gtfs_realtime_pb2
import sys
sys.path.append("..")
from config.settings import GTFS_RT_BUS_URL, KAFKA_TOPICS, DELAY_THRESHOLD_SECONDS
from producers.base_producer import BaseProducer


class TripUpdatesProducer(BaseProducer):

    def __init__(self):
        super().__init__("trip_updates")
        self.url = GTFS_RT_BUS_URL

    def get_topic(self) -> str:
        return KAFKA_TOPICS["trip_updates"]

    def fetch_data(self) -> list:
        try:
            response = requests.get(self.url, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch TripUpdates: {e}")
            return []

        # Guard: MOT server sometimes returns an HTML error page (IP/WAF block)
        # instead of a binary protobuf — detect it early for a clear error message.
        if response.headers.get("Content-Type", "").startswith("text/html"):
            self.logger.error(
                f"MOT TripUpdates feed returned HTML instead of protobuf — "
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
            if not entity.HasField("trip_update"):
                continue
            tu = entity.trip_update
            normalized = self._normalize(entity.id, tu, feed.header.timestamp)
            records.extend(normalized)

        delayed = sum(1 for r in records if r.get("is_delayed"))
        cancelled = sum(1 for r in records if r.get("is_cancelled"))
        self.logger.info(
            f"Parsed {len(records)} stop-time updates | "
            f"delayed={delayed} cancelled={cancelled}"
        )
        return records

    def _normalize(self, entity_id: str, trip_update, feed_timestamp: int) -> list:
        """
        Each trip_update can have multiple stop_time_updates.
        Returns one record per stop with delay data.
        """
        trip = trip_update.trip
        records = []

        for stu in trip_update.stop_time_update:
            arrival_delay   = stu.arrival.delay   if stu.HasField("arrival")   else None
            departure_delay = stu.departure.delay if stu.HasField("departure") else None
            arrival_time    = stu.arrival.time     if stu.HasField("arrival")   else None
            departure_time  = stu.departure.time   if stu.HasField("departure") else None

            # Use whichever delay we have
            delay_seconds = departure_delay if departure_delay is not None else arrival_delay

            records.append({
                "trip_id":          trip.trip_id,
                "route_id":         trip.route_id,
                "direction_id":     trip.direction_id,
                "start_date":       trip.start_date,
                "start_time":       trip.start_time,
                "stop_sequence":    stu.stop_sequence,
                "stop_id":          stu.stop_id,
                "arrival_delay":    arrival_delay,
                "departure_delay":  departure_delay,
                "delay_seconds":    delay_seconds,
                "arrival_time":     arrival_time,
                "departure_time":   departure_time,
                "schedule_relationship": self._schedule_rel(stu.schedule_relationship),
                "is_delayed":       delay_seconds is not None and delay_seconds >= DELAY_THRESHOLD_SECONDS,
                "is_cancelled":     trip.schedule_relationship == 3,  # CANCELED
                "feed_timestamp":   feed_timestamp,
            })
        return records

    def _schedule_rel(self, code: int) -> str:
        return {0: "scheduled", 1: "skipped", 2: "no_data", 5: "unscheduled"}.get(code, "unknown")


if __name__ == "__main__":
    producer = TripUpdatesProducer()
    try:
        producer.run_once()
    finally:
        producer.close()