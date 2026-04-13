"""
Configuration for Israel Transit Delay Collectors
"""
import os

# ─── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_BUS_DELAYS   = "bus-delays"
KAFKA_TOPIC_TRAIN_DELAYS = "train-delays"
KAFKA_TOPIC_BUS_HISTORICAL   = "bus-delays-historical"
KAFKA_TOPIC_TRAIN_HISTORICAL = "train-delays-historical"

# ─── Open Bus Stride API (אוטובוסים) ─────────────────────────────────────────
STRIDE_API_BASE = "https://open-bus-stride-api.hasadna.org.il"
# כמה שניות להמתין בין polling cycles
BUS_POLL_INTERVAL_SECONDS = 30
# כמה רשומות לשלוף בכל קריאה
BUS_FETCH_LIMIT = 100

# ─── Israel Railways API (רכבות) ──────────────────────────────────────────────
RAIL_API_BASE = "https://www.rail.co.il/apiinfo/api"
# polling interval לרכבות (שינויים פחות תכופים)
TRAIN_POLL_INTERVAL_SECONDS = 60

# תחנות לניטור — ניתן להרחיב
# מזהי תחנות רכבת: https://www.rail.co.il
MONITORED_TRAIN_ROUTES = [
    {"origin": "3600", "destination": "3700"},  # ת"א סבידור → חיפה מרכז
    {"origin": "3700", "destination": "3600"},  # חיפה מרכז → ת"א סבידור
    {"origin": "4900", "destination": "3700"},  # באר שבע → חיפה
    {"origin": "3600", "destination": "7000"},  # ת"א → ירושלים נבון
]

# ─── Historical fetch ─────────────────────────────────────────────────────────
# כמה ימים אחורה לשלוף בכל ריצת batch
HISTORICAL_DAYS_BACK = 7
# גודל batch לשליפה היסטורית
HISTORICAL_BATCH_SIZE = 500

# ─── Elasticsearch ────────────────────────────────────────────────────────────
ES_HOST  = os.getenv("ES_HOST", "localhost:9200")
ES_INDEX_BUS_DELAYS   = "bus-delays"
ES_INDEX_TRAIN_DELAYS = "train-delays"

# ─── MinIO ────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_BUS_DELAYS   = "bus-delays-raw"
MINIO_BUCKET_TRAIN_DELAYS = "train-delays-raw"