"""
Configuration for Israel Transit Delay Collectors
"""
import os
from dotenv import load_dotenv

# Load .env from the project root (one level up from collectors/)
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ─── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_BUS_DELAYS   = "bus-delays"
KAFKA_TOPIC_TRAIN_DELAYS = "train-delays"
KAFKA_TOPIC_BUS_HISTORICAL   = "bus-delays-historical"
KAFKA_TOPIC_TRAIN_HISTORICAL = "train-delays-historical"

# ─── Open Bus Stride API (Buses) ─────────────────────────────────────────
STRIDE_API_BASE = "https://open-bus-stride-api.hasadna.org.il"
# Seconds to wait between polling cycles
BUS_POLL_INTERVAL_SECONDS = 30
# Records to fetch per request — מוקטן ל-50 (גוש דן בלבד)
BUS_FETCH_LIMIT = 50

# Gush Dan bounding box — סינון גיאוגרפי בכל ה-collectors
GUSH_DAN_BBOX = {
    "lat_min": 31.97, "lat_max": 32.19,
    "lon_min": 34.73, "lon_max": 34.93,
}

# ─── Israel Railways API (Trains) ──────────────────────────────────────────────
RAIL_API_BASE = "https://www.rail.co.il/apiinfo/api"
# Polling interval for trains (changes less frequently)
TRAIN_POLL_INTERVAL_SECONDS = 60

# Monitored routes — can be extended
# Train station IDs: https://www.rail.co.il
MONITORED_TRAIN_ROUTES = [
    {"origin": "3700", "destination": "4100"},  # Tel Aviv Savidor → Haifa Central
    {"origin": "4100", "destination": "3700"},  # Haifa Central → Tel Aviv Savidor
    {"origin": "4900", "destination": "3700"},  # Hadera West → Tel Aviv Savidor
    {"origin": "3700", "destination": "3100"},  # Tel Aviv → Jerusalem Navon
]

# ─── Historical fetch ─────────────────────────────────────────────────────────
# Days back to fetch in each batch run
HISTORICAL_DAYS_BACK = 7
# Batch size for historical fetch
HISTORICAL_BATCH_SIZE = 500

# ─── Elasticsearch ────────────────────────────────────────────────────────────
ES_HOST  = os.getenv("ES_HOST", "localhost:9200")
ES_INDEX_BUS_DELAYS   = "bus-delays"
ES_INDEX_TRAIN_DELAYS = "train-delays"

# ─── MinIO ────────────────────────────────────────────────────────────────────
# Minio client expects "host:port" without scheme — strip http:// or https:// if present
_raw_minio = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ENDPOINT   = _raw_minio.removeprefix("https://").removeprefix("http://")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET_BUS_DELAYS   = "bus-delays-raw"
MINIO_BUCKET_TRAIN_DELAYS = "train-delays-raw"