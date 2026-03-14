"""
config/settings.py
Central configuration for Israel Public Transit Monitoring Platform.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# KAFKA
# ─────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

KAFKA_TOPICS = {
    "bus_positions":  "bus-positions",    # real-time bus locations
    "train_positions": "train-positions", # train locations
    "trip_updates":   "trip-updates",     # trip updates (delays)
    "service_alerts": "service-alerts",   # service alerts
    "delay_events":   "delay-events",     # computed delay events
    "traffic_data":   "traffic-data",     # HERE traffic congestion data
    "errors":         "pipeline-errors",
}

KAFKA_PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
    "max_block_ms": 10000,
    "compression_type": "gzip",   # GTFS-RT payloads can be large
}

KAFKA_CONSUMER_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,
    "max_poll_records": 200,
    "session_timeout_ms": 30000,
}

# ─────────────────────────────────────────
# DATA SOURCES - Israel Transit APIs
# ─────────────────────────────────────────

# Ministry of Transport - GTFS Realtime (public, no API key required)
GTFS_RT_BUS_URL      = os.getenv("GTFS_RT_BUS_URL",
                        "https://gtfs.mot.gov.il/gtfsfiles/TripUpdates.pb")
GTFS_RT_VEHICLE_URL  = os.getenv("GTFS_RT_VEHICLE_URL",
                        "https://gtfs.mot.gov.il/gtfsfiles/VehiclePositions.pb")
GTFS_RT_ALERTS_URL   = os.getenv("GTFS_RT_ALERTS_URL",
                        "https://gtfs.mot.gov.il/gtfsfiles/ServiceAlerts.pb")

# HERE Traffic API
HERE_API_KEY         = os.getenv("HERE_API_KEY", "")

# Open Bus Stride - hasadna (public, REST API)
OPEN_BUS_API_URL     = os.getenv("OPEN_BUS_API_URL",
                        "https://open-bus-stride-api.hasadna.org.il")

# Israel Railways
# Israel Railways — both official APIs are unavailable:
# israelrail.azurewebsites.net — hijacked
# www.rail.co.il/apiinfo — blocked by Cloudflare
# Train data is available via Open Bus Stride with operator_ref=2
RAIL_API_URL         = os.getenv("RAIL_API_URL", "")   # disabled
RAIL_API_URL_ALT     = os.getenv("RAIL_API_URL_ALT",
                        "https://www.rail.co.il/apiinfo/api/Train/GetRouteTrainByStation")

MOT_API_KEY          = os.getenv("MOT_API_KEY", "")

# ─────────────────────────────────────────
# AWS / STORAGE
# ─────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET             = os.getenv("S3_BUCKET_NAME", "israel-transit-lake")

S3_PREFIXES = {
    "bus_positions":   "processed/bus-positions",
    "train_positions": "processed/train-positions",
    "trip_updates":    "processed/trip-updates",
    "service_alerts":  "processed/service-alerts",
    "delay_events":    "processed/delay-events",
    "traffic_data":    "processed/traffic-data",
}

USE_MINIO        = os.getenv("USE_MINIO", "false").lower() == "true"
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

# ─────────────────────────────────────────
# REDSHIFT
# ─────────────────────────────────────────
REDSHIFT_CONFIG = {
    "host":     os.getenv("REDSHIFT_HOST"),
    "port":     int(os.getenv("REDSHIFT_PORT", 5439)),
    "dbname":   os.getenv("REDSHIFT_DB", "transit_dw"),
    "user":     os.getenv("REDSHIFT_USER"),
    "password": os.getenv("REDSHIFT_PASSWORD"),
}
REDSHIFT_SCHEMA = "transit"

# ─────────────────────────────────────────
# ISRAEL TRANSIT CONSTANTS
# ─────────────────────────────────────────

# Transit operators - operator IDs in GTFS Israel
OPERATORS = {
    "dan":        "3",    # Dan
    "egged":      "5",    # Egged
    "metropoline": "14",  # Metropoline
    "kavim":      "21",   # Kavim
    "nateev_express": "25",  # Nateev Express
    "rail":       "2",    # Israel Railways
}

# Major train stations
MAJOR_TRAIN_STATIONS = {
    "2300":  "Tel Aviv - Center",
    "2820":  "Tel Aviv - HaShalom",
    "3400":  "Jerusalem - Yitzhak Navon",
    "3600":  "Haifa - Center HaShmira",
    "4600":  "Beer Sheva - Center",
    "3100":  "Netanya",
    "5000":  "Herzliya",
    "5010":  "Raanana B",
    "700":   "Hadera West",
    "1220":  "Nahariya",
}

# Delay threshold (in seconds) to classify as "delayed"
DELAY_THRESHOLD_SECONDS = 180    # 3 minutes
SEVERE_DELAY_SECONDS    = 600    # 10 minutes

# ─────────────────────────────────────────
# INGESTION INTERVALS (seconds)
# ─────────────────────────────────────────
FETCH_INTERVALS = {
    "bus_positions":  30,   # every 30 seconds
    "train_positions": 30,  # every 30 seconds
    "trip_updates":   60,   # every minute
    "service_alerts": 120,  # every 2 minutes
    "traffic_data":   300,  # every 5 minutes
}

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────
LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"