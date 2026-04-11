"""
etl/traffic_transformer.py
Transforms and enriches HERE Traffic Flow records for storage.

FIXES:
  - float(data.get(...) or 0) guards against null values from HERE API
    (float(None) raised TypeError and crashed the entire batch)
  - time_period labels standardised: "morning_peak"→"morning_rush",
    "evening_peak"→"evening_rush" to match transformers.py
  - ISRAEL_REGIONS reordered with explicit comment: specific sub-regions
    must appear before their containing parent region, since classify_region()
    returns the first match
  - classify_road_type() comment added explaining .lower() + Hebrew keyword interaction
  - is_weekend uses weekday() in (4, 5) — was >= 4 (wrongly included Sunday)
"""

from datetime import datetime, timezone
import logging

logger = logging.getLogger("etl.traffic_transformer")

# Israel major road classification keywords
# NOTE: .lower() is called on the description before matching.
# Hebrew characters are unaffected by .lower(), so Hebrew keywords work correctly.
# If adding new English keywords, add them in lowercase only.
HIGHWAY_KEYWORDS     = ["כביש", "highway", "route", "road", "motorway", "freeway"]
URBAN_KEYWORDS       = ["רחוב", "שדרות", "street", "avenue", "blvd"]
INTERCHANGE_KEYWORDS = ["interchange", "צומת", "junction"]

# Congestion label → numeric score (-1 = unknown, excluded from averages)
CONGESTION_SCORE = {
    "free":     0,
    "minor":    2,
    "moderate": 5,
    "heavy":    8,
    "blocked":  10,
    "unknown":  -1,
}

# FIX: Reordered so specific sub-regions appear before their containing parent.
# classify_region() returns the first match — if "north" came before "haifa",
# Haifa coordinates would be classified as "north" silently.
# Order: most-specific first, broad catch-alls last.
ISRAEL_REGIONS = [
    ("haifa",     32.7, 32.9, 34.9, 35.1),   # inside "north" — must precede it
    ("tel_aviv",  31.9, 32.2, 34.7, 34.95),  # inside "center" — must precede it
    ("jerusalem", 31.6, 31.95, 35.0, 35.35),
    ("dead_sea",  31.0, 31.8, 35.3, 35.6),
    ("north",     32.5, 33.4, 34.8, 36.0),   # broad — after haifa
    ("center",    31.7, 32.5, 34.7, 35.3),   # broad — after tel_aviv
    ("south",     29.4, 31.5, 34.2, 35.5),
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
    """Classify a GPS point into an Israel region. Returns first match — order matters."""
    if lat is None or lon is None:
        return "unknown"
    for name, lat_min, lat_max, lon_min, lon_max in ISRAEL_REGIONS:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return "unknown"


def classify_road_type(description: str) -> str:
    """
    Classify road type from description string.
    description is lowercased before matching — Hebrew keywords are unaffected by .lower().
    """
    desc = (description or "").lower()
    if any(k in desc for k in INTERCHANGE_KEYWORDS):
        return "interchange"
    if any(k in desc for k in HIGHWAY_KEYWORDS):
        return "highway"
    if any(k in desc for k in URBAN_KEYWORDS):
        return "urban"
    return "other"


def classify_time_period(hour: int) -> str:
    """
    FIX: labels now match transformers.py
    Was "morning_peak"/"evening_peak" — changed to "morning_rush"/"evening_rush".
    """
    if 6  <= hour < 9:  return "morning_rush"   # was "morning_peak"
    if 9  <= hour < 12: return "mid_morning"
    if 12 <= hour < 15: return "afternoon"
    if 15 <= hour < 19: return "evening_rush"   # was "evening_peak"
    if 19 <= hour < 23: return "evening"
    return "night"


class TrafficTransformer:
    """Transforms HERE Traffic Flow records for S3/Redshift storage."""

    def transform(self, data: dict) -> dict:
        now = datetime.now(timezone.utc)

        lat = data.get("lat")
        lon = data.get("lon")

        # FIX: use `or 0` pattern to guard against explicit null from HERE API.
        # data.get("jam_factor", 0) returns None when the key exists but value is null,
        # so the default=0 fallback doesn't help — `or 0` handles both missing and null.
        jam_factor  = float(data.get("jam_factor")  or 0)
        speed       = float(data.get("speed_kmh")   or 0)
        free_flow   = float(data.get("free_flow_kmh") or 0)
        confidence  = float(data.get("confidence")  or 0)

        congestion  = data.get("congestion", "unknown")
        description = data.get("description", "")
        hour        = data.get("hour_of_day", now.hour)

        region      = classify_region(lat, lon)
        road_type   = classify_road_type(description)
        time_period = classify_time_period(hour)
        cong_score  = CONGESTION_SCORE.get(congestion, -1)

        # Speed ratio: 1.0 = free flow, 0.0 = blocked
        speed_ratio = round(speed / free_flow, 3) if free_flow > 0 else 0

        # Severity label derived from jam_factor
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
            "confidence":       confidence,
            "traversability":   data.get("traversability", "open"),

            # derived fields
            "is_congested":     jam_factor >= 4,
            "is_blocked":       data.get("is_blocked", False),
            "delay_min_per_km": float(data.get("delay_minutes") or 0),  # FIX: null guard

            # time dimensions
            "hour_of_day":      hour,
            "day_of_week":      data.get("day_of_week", now.strftime("%A")),
            "time_period":      time_period,
            "is_weekend":       now.weekday() in (4, 5),  # FIX: was >= 4 (wrongly included Sunday)

            # metadata
            "recorded_at":      data.get("recorded_at", now.isoformat()),
            "processed_at":     now.isoformat(),
            "source":           "here_traffic_v7",
        }