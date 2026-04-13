"""
Delay Kafka Consumer
=====================
Listens to Kafka delay topics and saves:
  - Elasticsearch: לחיפוש וניתוח בזמן אמת
  - MinIO: לשמירה היסטורית גולמית (Parquet/JSON)

Usage:
  python delay_kafka_consumer.py
  python delay_kafka_consumer.py --topics bus-delays train-delays
  python delay_kafka_consumer.py --es-only
  python delay_kafka_consumer.py --minio-only
"""

import io
import json
import gzip
import logging
import argparse
import threading
from datetime import datetime, timezone
from collections import defaultdict

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers
from minio import Minio
from minio.error import S3Error

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_BUS_DELAYS,
    KAFKA_TOPIC_TRAIN_DELAYS,
    KAFKA_TOPIC_BUS_HISTORICAL,
    KAFKA_TOPIC_TRAIN_HISTORICAL,
    ES_HOST,
    ES_INDEX_BUS_DELAYS,
    ES_INDEX_TRAIN_DELAYS,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET_BUS_DELAYS,
    MINIO_BUCKET_TRAIN_DELAYS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# Number of messages to buffer before flushing to ES/MinIO
ES_BULK_SIZE    = 200
MINIO_BULK_SIZE = 500


# ─── Elasticsearch ────────────────────────────────────────────────────────────

ES_INDEX_MAPPINGS = {
    "mappings": {
        "properties": {
            "source":          {"type": "keyword"},
            "transport_type":  {"type": "keyword"},
            "collected_at":    {"type": "date"},
            "delay_seconds":   {"type": "integer"},
            "delay_minutes":   {"type": "float"},
            "is_delayed":      {"type": "boolean"},
            "is_early":        {"type": "boolean"},
            # Bus fields
            "line_ref":        {"type": "keyword"},
            "operator_ref":    {"type": "keyword"},
            "vehicle_ref":     {"type": "keyword"},
            "stop_point_ref":  {"type": "keyword"},
            "gtfs_stop_id":    {"type": "keyword"},
            "aimed_arrival":   {"type": "date"},
            "expected_arrival": {"type": "date"},
            "actual_arrival":  {"type": "date"},
            "planned_arrival": {"type": "date"},
            "lat":             {"type": "float"},
            "lon":             {"type": "float"},
            # Train fields
            "train_number":    {"type": "keyword"},
            "station_id":      {"type": "keyword"},
            "route_origin":    {"type": "keyword"},
            "route_destination": {"type": "keyword"},
            "delay_type":      {"type": "keyword"},
        }
    },
    "settings": {
        "number_of_shards":   2,
        "number_of_replicas": 1,
    }
}


def setup_es(es: Elasticsearch):
    """יוצר indexes ב-ES אם לא קיימים."""
    for index_name in [ES_INDEX_BUS_DELAYS, ES_INDEX_TRAIN_DELAYS]:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=ES_INDEX_MAPPINGS)
            log.info(f"Created ES index: {index_name}")


def get_es_index(record: dict) -> str:
    transport = record.get("transport_type", "bus")
    return ES_INDEX_BUS_DELAYS if transport == "bus" else ES_INDEX_TRAIN_DELAYS


def bulk_index_to_es(es: Elasticsearch, records: list[dict]):
    """מבצע bulk index ל-Elasticsearch."""
    actions = [
        {
            "_index": get_es_index(record),
            "_source": record,
        }
        for record in records
    ]
    success, errors = helpers.bulk(es, actions, raise_on_error=False)
    if errors:
        log.warning(f"ES bulk errors: {len(errors)} failed out of {len(records)}")
    return success


# ─── MinIO ────────────────────────────────────────────────────────────────────

def setup_minio(minio: Minio):
    """יוצר buckets ב-MinIO אם לא קיימים."""
    for bucket in [MINIO_BUCKET_BUS_DELAYS, MINIO_BUCKET_TRAIN_DELAYS]:
        if not minio.bucket_exists(bucket):
            minio.make_bucket(bucket)
            log.info(f"Created MinIO bucket: {bucket}")


def get_minio_bucket(record: dict) -> str:
    transport = record.get("transport_type", "bus")
    return MINIO_BUCKET_BUS_DELAYS if transport == "bus" else MINIO_BUCKET_TRAIN_DELAYS


def save_batch_to_minio(minio: Minio, records: list[dict]):
    """
    שומר batch של רשומות ל-MinIO כ-gzipped JSONL.
    נתיב: transport_type/YYYY/MM/DD/HH/timestamp.jsonl.gz
    """
    # Group by transport_type and date
    by_bucket: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        bucket = get_minio_bucket(rec)
        by_bucket[bucket].append(rec)

    now = datetime.now(timezone.utc)
    path_prefix = now.strftime("%Y/%m/%d/%H")
    timestamp   = now.strftime("%Y%m%d_%H%M%S_%f")

    for bucket, recs in by_bucket.items():
        # JSONL → gzip
        jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in recs)
        compressed = gzip.compress(jsonl.encode("utf-8"))

        object_name = f"{path_prefix}/{timestamp}_{bucket}.jsonl.gz"
        minio.put_object(
            bucket,
            object_name,
            io.BytesIO(compressed),
            length=len(compressed),
            content_type="application/gzip",
        )
        log.info(f"MinIO: saved {len(recs)} records → s3://{bucket}/{object_name}")


# ─── Consumer ─────────────────────────────────────────────────────────────────

class DelayConsumer:
    def __init__(
        self,
        topics: list[str],
        use_es: bool = True,
        use_minio: bool = True,
    ):
        self.topics    = topics
        self.use_es    = use_es
        self.use_minio = use_minio
        self._buffer: list[dict] = []
        self._lock = threading.Lock()

        # Kafka
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="delay-consumer-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            max_poll_records=500,
        )

        # Elasticsearch
        if use_es:
            self.es = Elasticsearch([f"http://{ES_HOST}"])
            setup_es(self.es)

        # MinIO
        if use_minio:
            self.minio = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False,
            )
            setup_minio(self.minio)

    def _flush(self, force: bool = False):
        """מנקז את ה-buffer ל-ES ו-MinIO."""
        with self._lock:
            if not self._buffer:
                return
            if not force and len(self._buffer) < ES_BULK_SIZE:
                return

            records = list(self._buffer)
            self._buffer.clear()

        # ES
        if self.use_es and records:
            try:
                n = bulk_index_to_es(self.es, records)
                log.info(f"ES: indexed {n} records")
            except Exception as e:
                log.error(f"ES bulk index error: {e}")

        # MinIO
        if self.use_minio and records:
            try:
                save_batch_to_minio(self.minio, records)
            except S3Error as e:
                log.error(f"MinIO save error: {e}")

    def run(self):
        log.info(f"Consumer started, topics: {self.topics}")
        try:
            for msg in self.consumer:
                record = msg.value
                with self._lock:
                    self._buffer.append(record)

                # flush after each bulk
                if len(self._buffer) >= ES_BULK_SIZE:
                    self._flush()

        except KeyboardInterrupt:
            log.info("Stopping consumer...")
        finally:
            self._flush(force=True)
            self.consumer.close()
            log.info("Consumer stopped")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Delay Kafka Consumer")
    parser.add_argument(
        "--topics",
        nargs="+",
        default=[
            KAFKA_TOPIC_BUS_DELAYS,
            KAFKA_TOPIC_TRAIN_DELAYS,
            KAFKA_TOPIC_BUS_HISTORICAL,
            KAFKA_TOPIC_TRAIN_HISTORICAL,
        ],
        help="Kafka topics to listen on",
    )
    parser.add_argument("--es-only",    action="store_true", help="שמור רק ל-Elasticsearch")
    parser.add_argument("--minio-only", action="store_true", help="שמור רק ל-MinIO")
    args = parser.parse_args()

    use_es    = not args.minio_only
    use_minio = not args.es_only

    consumer = DelayConsumer(
        topics=args.topics,
        use_es=use_es,
        use_minio=use_minio,
    )
    consumer.run()


if __name__ == "__main__":
    main()