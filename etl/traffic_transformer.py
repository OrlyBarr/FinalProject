"""
etl/traffic_transformer.py
Transforms and enriches HERE Traffic Flow records for storage.
"""

from datetime import datetime, timezone
import logging

logger = logging.getLogger("etl.traffic_transformer")

# Israel major road classification keywords
HIGHWAY_KEYWORDS   = ["כביש", "highway", "route", "road", "motorway", "freeway"]
URBAN_KEYWORDS     = ["רחוב", "שדרות", "street", "avenue", "blvd"]
INTERCHANGE_KEYWORDS = ["interchange", "צומת", "junction"]

# Congestion to numeric score
CONGESTION_SCORE = {
    "free":     0,
    "minor":    2,
    "moderate": 5,
    "heavy":    8,
    "blocked":  10,
    "unknown":  -1,
}

# Israel regions by lat/lon
ISRAEL_REGIONS = [
    ("north",     32.5, 33.4, 34.8, 36.0),
    ("haifa",     32.7, 32.9, 34.9, 35.1),
    ("tel_aviv",  31.9, 32.2, 34.7, 34.95),
    ("center",    31.7, 32.5, 34.7, 35.3),
    ("jerusalem", 31.6, 31.95, 35.0, 35.35),
    ("south",     29.4, 31.5, 34.2, 35.5),
    ("dead_sea",  31.0, 31.8, 35.3, 35.6),
]

REGION_NAMES_HE = {
    "north":     "צפון",
    "haifa":     "חיפה",
    "tel_aviv":  "תל אביב",
    "center":    "מרכז",
    "jerusalem": "ירושלים",
    "south":     "דרום",
    "dead_sea":  "ים המלח",
    "unknown":   "לא ידוע",
}


def classify_region(lat, lon) -> str:
    """Classify a GPS point into an Israel region."""
    if lat is None or lon is None:
        return "unknown"
    for name, lat_min, lat_max, lon_min, lon_max in ISRAEL_REGIONS:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return "unknown"


def classify_road_type(description: str) -> str:
    """Classify road type from description."""
    desc = description.lower()
    if any(k in desc for k in INTERCHANGE_KEYWORDS):
        return "interchange"
    if any(k in desc for k in HIGHWAY_KEYWORDS):
        return "highway"
    if any(k in desc for k in URBAN_KEYWORDS):
        return "urban"
    return "other"


def classify_time_period(hour: int) -> str:
    if 6 <= hour < 9:   return "morning_peak"
    if 9 <= hour < 12:  return "morning"
    if 12 <= hour < 15: return "afternoon"
    if 15 <= hour < 19: return "evening_peak"
    if 19 <= hour < 23: return "evening"
    return "night"


class TrafficTransformer:
    """Transforms HERE Traffic Flow records for S3/Redshift storage."""

    def transform(self, data: dict) -> dict:
        now = datetime.now(timezone.utc)

        lat = data.get("lat")
        lon = data.get("lon")
        jam_factor  = float(data.get("jam_factor", 0))
        speed       = float(data.get("speed_kmh", 0))
        free_flow   = float(data.get("free_flow_kmh", 0))
        congestion  = data.get("congestion", "unknown")
        description = data.get("description", "")
        hour        = data.get("hour_of_day", now.hour)

        region      = classify_region(lat, lon)
        road_type   = classify_road_type(description)
        time_period = classify_time_period(hour)
        cong_score  = CONGESTION_SCORE.get(congestion, -1)

        # Speed ratio: 1.0 = free flow, 0.0 = blocked
        speed_ratio = round(speed / free_flow, 3) if free_flow > 0 else 0

        # Severity label
        if jam_factor >= 8:   severity = "critical"
        elif jam_factor >= 6: severity = "severe"
        elif jam_factor >= 4: severity = "moderate"
        elif jam_factor >= 2: severity = "minor"
        else:                 severity = "free"

        return {
            # identifiers
            "segment_id":       data.get("segment_id", ""),
            "tile_name":        data.get("tile_name", ""),

            # location
            "lat":              lat,
            "lon":              lon,
            "road_name":        data.get("road_name", description[:100]),
            "description":      description[:200],
            "region":           region,
            "region_he":        REGION_NAMES_HE.get(region, "לא ידוע"),
            "road_type":        road_type,

            # traffic metrics
            "speed_kmh":        speed,
            "free_flow_kmh":    free_flow,
            "speed_ratio":      speed_ratio,
            "jam_factor":       jam_factor,
            "congestion":       congestion,
            "congestion_score": cong_score,
            "severity":         severity,
            "confidence":       float(data.get("confidence", 0)),
            "traversability":   data.get("traversability", "open"),

            # derived fields
            "is_congested":     jam_factor >= 4,
            "is_blocked":       data.get("is_blocked", False),
            "delay_min_per_km": float(data.get("delay_minutes", 0)),

            # time dimensions
            "hour_of_day":      hour,
            "day_of_week":      data.get("day_of_week", now.strftime("%A")),
            "time_period":      time_period,

            # metadata
            "recorded_at":      data.get("recorded_at", now.isoformat()),
            "processed_at":     now.isoformat(),
            "source":           "here_traffic_v7",
        }