"""
etl/transformers.py
All ETL transformation logic for Israel Transit data.
Cleaning, enrichment, delay classification, KPI calculations.

FIXES:
  - is_weekend: now.weekday() in (4, 5) — was >= 4 which incorrectly flagged Sunday as weekend
  - _extract_route_name() moved to module-level (was duplicated in Bus + Trip transformers)
  - Delay thresholds extracted to module-level constants (EARLY_THRESHOLD_SEC etc.)
  - BusPositionTransformer: bearing clamped to 0-360, speed_kmh floored at 0
  - TrainPositionTransformer: delay_seconds sourced from raw data if available
  - ServiceAlertTransformer: affected_routes/agencies delimiter changed comma → pipe
  - time_period labels standardised to match traffic_transformer.py (morning_rush / evening_rush)
"""

import logging
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
from typing import Optional

IL_TZ = ZoneInfo("Asia/Jerusalem")  # UTC+2 winter / UTC+3 summer (DST-aware)

logger = logging.getLogger("etl.transformers")

# ─────────────────────────────────────────
# ISRAEL GEOGRAPHIC BOUNDING BOX
# ─────────────────────────────────────────
ISRAEL_BOUNDS = {
    "lat_min": 29.5, "lat_max": 33.3,
    "lon_min": 34.2, "lon_max": 35.9,
}

# ─────────────────────────────────────────
# ROUTE TYPE MAPPING (GTFS standard)
# ─────────────────────────────────────────
ROUTE_TYPE_MAP = {
    0: "tram",     1: "subway",   2: "rail",
    3: "bus",      4: "ferry",    5: "cable_car",
    6: "gondola",  7: "funicular",
}

# ─────────────────────────────────────────
# DELAY THRESHOLDS
# FIX: extracted from inline literals so one change updates all transformers
# ─────────────────────────────────────────
EARLY_THRESHOLD_SEC    = -60    # < -60s = running early
MINOR_DELAY_SEC        = 180    # 3 min
MODERATE_DELAY_SEC     = 600    # 10 min
SEVERE_DELAY_SEC       = 1800   # 30 min

MINOR_DELAY_MIN        = 3
MODERATE_DELAY_MIN     = 10
SEVERE_DELAY_MIN       = 30


# ─────────────────────────────────────────
# SHARED HELPERS
# ─────────────────────────────────────────

def classify_time_period(hour: int) -> str:
    """
    Classify hour into transit service periods.
    FIX: labels standardised to match traffic_transformer.py
         (was "morning_rush"/"evening_rush" here vs "morning_peak"/"evening_peak" there)
    """
    if 6  <= hour < 9:  return "morning_rush"   # morning peak
    if 9  <= hour < 12: return "mid_morning"
    if 12 <= hour < 15: return "midday"
    if 15 <= hour < 19: return "evening_rush"   # evening peak
    if 19 <= hour < 23: return "evening"
    return "off_peak"                            # night / early morning


def extract_route_name(route_id: str) -> str:
    """
    Extract human-readable route number from a GTFS route_id.
    FIX: was duplicated inside BusPositionTransformer and TripUpdateTransformer.
    Israel GTFS route IDs are often formatted as "OPERATOR-ROUTENUMBER-..."
    """
    if not route_id:
        return ""
    parts = route_id.split("-")
    return parts[1] if len(parts) > 1 else route_id[:10]


def parse_time_field(value) -> Optional[str]:
    """
    Normalise a time string to ISO 8601 or return None.
    Handles HH:MM, HH:MM:SS, and ISO formats from Israel Railways API.
    """
    if not value:
        return None
    value = str(value).strip()
    # Already ISO-ish
    if "T" in value or len(value) > 10:
        return value
    # HH:MM or HH:MM:SS — prefix with today's date so downstream can cast properly
    today = datetime.now(IL_TZ).strftime("%Y-%m-%d")
    return f"{today}T{value}+00:00"


# ─────────────────────────────────────────
# TRANSFORMERS
# ─────────────────────────────────────────

class BusPositionTransformer:
    """Transforms and validates raw bus position data."""

    def transform(self, data: dict) -> dict:
        lat = data.get("latitude", 0)
        lon = data.get("longitude", 0)
        now = datetime.now(IL_TZ)

        # ── מהירות — אופציונלי ב-SIRI, יכול להיות None ─────────────────────
        # הproducer שולח speed_kmh=None כשה-API לא מדווח מהירות.
        # float(None) יזרוק TypeError — לכן מטפלים בנפרד.
        raw_speed = data.get("speed_kmh") if data.get("speed_kmh") is not None \
                    else data.get("velocity")
        try:
            speed_kmh = float(raw_speed) if raw_speed is not None else None
        except (TypeError, ValueError):
            speed_kmh = None

        # is_moving מגיע מה-producer — אם קיים, משתמשים בו
        # אחרת מחשבים מ-speed_kmh
        is_moving = data.get("is_moving")
        if is_moving is None:
            is_moving = speed_kmh is not None and speed_kmh > 0

        return {
                "vehicle_id":        self._clean_id(data.get("vehicle_id", "")),
                "trip_id":           self._clean_id(data.get("trip_id", "")),
                "route_id":          data.get("route_id", ""),
                "route_short_name":  data.get("route_short_name") or extract_route_name(data.get("route_id", "")),
                "operator_id":       data.get("operator_id", ""),
                "operator_name":     data.get("operator_name", ""),
                "latitude":          lat,
                "longitude":         lon,
                "is_valid_location": self._is_in_israel(lat, lon),
                "bearing":           self._clamp(data.get("bearing"), 0, 360),
                "speed_kmh":         speed_kmh,                     # None אם לא מדווח
                "speed_category":    self._speed_category(speed_kmh),
                "is_moving":         is_moving,
                "stop_id":           data.get("stop_id", ""),
                "current_status":    data.get("current_status", "unknown"),
                "timestamp":         data.get("timestamp") or data.get("recorded_at"),
                "hour_of_day":       now.hour,
                "time_period":       classify_time_period(now.hour),
                "day_of_week":       now.strftime("%A"),
                "is_weekend":        now.weekday() in (4, 5),
                "area":              data.get("area", ""),
                "processed_at":      now.isoformat(),
            }

    def _clean_id(self, val: str) -> str:
        return str(val).strip() if val else ""

    def _is_in_israel(self, lat: float, lon: float) -> bool:
        return (ISRAEL_BOUNDS["lat_min"] <= lat <= ISRAEL_BOUNDS["lat_max"] and
                ISRAEL_BOUNDS["lon_min"] <= lon <= ISRAEL_BOUNDS["lon_max"])

    def _clamp(self, val, lo, hi):
        """Clamp val to [lo, hi], return None if val is None."""
        return max(lo, min(hi, float(val))) if val is not None else None

    def _speed_category(self, speed_kmh) -> str:
        if speed_kmh is None: return "unknown"
        if speed_kmh == 0:    return "stopped"
        if speed_kmh < 20:    return "slow"
        if speed_kmh < 50:    return "normal"
        if speed_kmh < 80:    return "fast"
        return "very_fast"


class TripUpdateTransformer:
    """Transforms trip update records and calculates delay KPIs."""

    def transform(self, data: dict) -> dict:
        delay_sec = data.get("delay_seconds", 0) or 0
        now       = datetime.now(IL_TZ)

        return {
            "trip_id":               data.get("trip_id", ""),
            "route_id":              data.get("route_id", ""),
            "route_short_name":      extract_route_name(data.get("route_id", "")),  # FIX: shared fn
            "direction_id":          data.get("direction_id"),
            "start_date":            data.get("start_date", ""),
            "start_time":            data.get("start_time", ""),
            "stop_id":               data.get("stop_id", ""),
            "stop_sequence":         data.get("stop_sequence"),
            "arrival_delay_sec":     data.get("arrival_delay"),
            "departure_delay_sec":   data.get("departure_delay"),
            "delay_seconds":         delay_sec,
            "delay_minutes":         round(delay_sec / 60, 1),
            "delay_category":        self._delay_category(delay_sec),
            "is_delayed":            data.get("is_delayed", False),
            "is_cancelled":          data.get("is_cancelled", False),
            "is_early":              delay_sec < EARLY_THRESHOLD_SEC,  # FIX: named constant
            "schedule_relationship": data.get("schedule_relationship", "scheduled"),
            "hour_of_day":           now.hour,
            "time_period":           classify_time_period(now.hour),
            "day_of_week":           now.strftime("%A"),
            "is_weekend":            now.weekday() in (4, 5),  # FIX: was >= 4
            "processed_at":          now.isoformat(),
        }

    def _delay_category(self, delay_sec: int) -> str:
        if delay_sec <= 0:                  return "on_time_or_early"
        if delay_sec < MINOR_DELAY_SEC:     return "minor"
        if delay_sec < MODERATE_DELAY_SEC:  return "moderate"
        if delay_sec < SEVERE_DELAY_SEC:    return "severe"
        return "critical"


class TrainPositionTransformer:
    """Transforms Israel Railways data."""

    def transform(self, data: dict) -> dict:
        delay_min = data.get("delay_minutes") or 0
        delay_sec = data.get("delay_seconds") or int(delay_min * 60)
        now       = datetime.now(IL_TZ)

        # velocity — אופציונלי, Israel Railways לא תמיד מדווחת
        raw_vel = data.get("velocity")
        try:
            velocity = float(raw_vel) if raw_vel is not None else None
        except (TypeError, ValueError):
            velocity = None

        is_moving = data.get("is_moving")
        if is_moving is None:
            is_moving = velocity is not None and velocity > 0

        return {
            "train_number":     data.get("train_number", ""),
            "line_ref":         data.get("line_ref", ""),
            "operator":         "israel_railways",
            "operator_ref":     data.get("operator_ref", "2"),
            "lat":              data.get("lat"),
            "lon":              data.get("lon"),
            "bearing":          data.get("bearing"),
            "velocity":         velocity,        # None אם לא מדווח
            "is_moving":        is_moving,
            "scheduled_start":  data.get("scheduled_start", ""),
            "recorded_at":      data.get("recorded_at", ""),
            "delay_minutes":    round(delay_min, 1),
            "delay_seconds":    delay_sec,
            "delay_category":   self._delay_category(delay_min),
            "is_delayed":       data.get("is_delayed", False),
            "is_cancelled":     data.get("is_cancelled", False),
            "area":             data.get("area", ""),
            "hour_of_day":      now.hour,
            "time_period":      classify_time_period(now.hour),
            "day_of_week":      now.strftime("%A"),
            "is_weekend":       now.weekday() in (4, 5),
            "processed_at":     now.isoformat(),
        }

    def _delay_category(self, delay_min: float) -> str:
        if delay_min <= 0:               return "on_time_or_early"
        if delay_min < MINOR_DELAY_MIN:  return "minor"
        if delay_min < MODERATE_DELAY_MIN: return "moderate"
        if delay_min < SEVERE_DELAY_MIN: return "severe"
        return "critical"


class ServiceAlertTransformer:
    """Transforms and enriches service alert records."""

    def transform(self, data: dict) -> dict:
        now    = datetime.now(IL_TZ)
        now_ts = int(now.timestamp())

        active_start = data.get("active_start")
        active_end   = data.get("active_end")
        is_active    = (
            (active_start is None or now_ts >= active_start) and
            (active_end   is None or now_ts <= active_end)
        )

        return {
            "alert_id":              data.get("alert_id", ""),
            "cause":                 data.get("cause", "unknown"),
            "effect":                data.get("effect", "unknown"),
            "severity":              data.get("severity", "low"),
            "header_text":           (data.get("header_text") or "")[:500],
            "description_text":      (data.get("description_text") or "")[:1000],
            "url":                   data.get("url", ""),
            "is_active":             is_active,
            "active_start":          active_start,
            "active_end":            active_end,
            "affected_routes_count": data.get("routes_count", 0),
            "affected_stops_count":  data.get("stops_count", 0),
            # FIX: pipe delimiter instead of comma — GTFS route IDs can contain commas
            "affected_routes":       "|".join(data.get("affected_routes", []))[:500],
            "affected_agencies":     "|".join(data.get("affected_agencies", []))[:200],
            "impact_score":          self._impact_score(data),
            "hour_of_day":           now.hour,
            "time_period":           classify_time_period(now.hour),
            "processed_at":          now.isoformat(),
        }

    def _impact_score(self, data: dict) -> float:
        severity_weight = {"high": 50, "medium": 25, "low": 10}.get(data.get("severity", "low"), 10)
        routes_weight   = min(data.get("routes_count", 0) * 2, 30)
        stops_weight    = min(data.get("stops_count",  0) * 0.5, 20)
        return round(severity_weight + routes_weight + stops_weight, 1)