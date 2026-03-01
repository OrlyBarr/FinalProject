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
    "bus_positions":  "bus-positions",    # מיקומי אוטובוסים בזמן אמת
    "train_positions": "train-positions", # מיקומי רכבות
    "trip_updates":   "trip-updates",     # עדכוני נסיעה (איחורים)
    "service_alerts": "service-alerts",   # התראות שירות
    "delay_events":   "delay-events",     # אירועי איחור מחושבים
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

# משרד התחבורה - GTFS Realtime (ציבורי, ללא מפתח)
GTFS_RT_BUS_URL      = os.getenv("GTFS_RT_BUS_URL",
                        "https://gtfs.mot.gov.il/gtfsfiles/TripUpdates.pb")
GTFS_RT_VEHICLE_URL  = os.getenv("GTFS_RT_VEHICLE_URL",
                        "https://gtfs.mot.gov.il/gtfsfiles/VehiclePositions.pb")
GTFS_RT_ALERTS_URL   = os.getenv("GTFS_RT_ALERTS_URL",
                        "https://gtfs.mot.gov.il/gtfsfiles/ServiceAlerts.pb")

# Open Bus Stride - hasadna (ציבורי, REST API)
OPEN_BUS_API_URL     = os.getenv("OPEN_BUS_API_URL",
                        "https://open-bus-stride-api.hasadna.org.il")

# רכבת ישראל
RAIL_API_URL         = os.getenv("RAIL_API_URL",
                        "https://israelrail.azurewebsites.net/stations/GetStationBoard")

MOT_API_KEY          = os.getenv("MOT_API_KEY", "")

# ─────────────────────────────────────────
# AWS / STORAGE
# ─────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET             = os.getenv("S3_BUCKET_NAME", "israel-transit-lake")

S3_PREFIXES = {
    "bus_positions":   "raw/bus-positions",
    "train_positions": "raw/train-positions",
    "trip_updates":    "raw/trip-updates",
    "service_alerts":  "raw/service-alerts",
    "delay_events":    "processed/delay-events",
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

# מפעילי תחבורה - operator IDs in GTFS Israel
OPERATORS = {
    "dan":        "3",    # דן
    "egged":      "5",    # אגד
    "metropoline": "14",  # מטרופולין
    "kavim":      "21",   # קווים
    "nateev_express": "25",  # נתיב אקספרס
    "rail":       "2",    # רכבת ישראל
}

# תחנות רכבת מרכזיות
MAJOR_TRAIN_STATIONS = {
    "2300":  "תל אביב - מרכז",
    "2820":  "תל אביב - השלום",
    "3400":  "ירושלים - יצחק נבון",
    "3600":  "חיפה - מרכז השמיר",
    "4600":  "באר שבע - מרכז",
    "3100":  "נתניה",
    "5000":  "הרצליה",
    "5010":  "רעננה ב",
    "700":   "חדרה מערב",
    "1220":  "נהריה",
}

# סף איחור (בשניות) להגדרת "מאחר"
DELAY_THRESHOLD_SECONDS = 180    # 3 דקות
SEVERE_DELAY_SECONDS    = 600    # 10 דקות

# ─────────────────────────────────────────
# INGESTION INTERVALS (seconds)
# ─────────────────────────────────────────
FETCH_INTERVALS = {
    "bus_positions":  30,   # כל 30 שניות
    "train_positions": 30,  # כל 30 שניות
    "trip_updates":   60,   # כל דקה
    "service_alerts": 120,  # כל 2 דקות
}

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────
LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"