"""
storage/es_indexer.py
Kafka → Elasticsearch real-time indexer.
Consumes from Kafka topics and bulk-indexes records into Elasticsearch
so Kibana can visualize live transit data.

Indices created:
  - transit-bus-positions
  - transit-trip-updates
  - transit-train-positions
  - transit-service-alerts
"""

import json
import os
from typing import Optional
import logging
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

IL_TZ = ZoneInfo("Asia/Jerusalem")  # UTC+2 winter / UTC+3 summer (DST-aware)

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ES_HOST       = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# Map Kafka topic keys → Elasticsearch index names
TOPIC_INDEX_MAP = {
    "bus_positions":    "transit-bus-positions",
    "trip_updates":     "transit-trip-updates",
    "train_positions":  "transit-train-positions",
    "service_alerts":   "transit-service-alerts",
    "delay_events":     "transit-delay-events",
}

# Index settings applied at creation time (if index doesn't exist)
INDEX_SETTINGS = {
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
        "refresh_interval":   "5s",
    },
    "mappings": {
        "dynamic": "true",
        "properties": {
            "timestamp":     {"type": "date", "ignore_malformed": True},
            "recorded_at":   {"type": "date", "ignore_malformed": True},
            "latitude":      {"type": "float"},
            "longitude":     {"type": "float"},
            "location":      {"type": "geo_point"},
            "delay_minutes": {"type": "float"},
            "operator_name": {"type": "keyword"},
            "route_id":      {"type": "keyword"},
            "vehicle_id":    {"type": "keyword"},
            "line_ref":      {"type": "keyword"},
            "trip_id":       {"type": "keyword"},
            "stop_id":       {"type": "keyword"},
        }
    }
}


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST, request_timeout=10)


def ensure_index(es: Elasticsearch, index_name: str) -> None:
    """Create the index with mappings if it doesn't exist yet."""
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=INDEX_SETTINGS)
        log.info(f"Created Elasticsearch index: {index_name}")


def _add_geo_point(doc: dict) -> dict:
    """If lat/lon present, add a geo_point field for Kibana Maps."""
    lat = doc.get("latitude") or doc.get("lat")
    lon = doc.get("longitude") or doc.get("lon")
    if lat and lon:
        try:
            doc["location"] = {"lat": float(lat), "lon": float(lon)}
        except (TypeError, ValueError):
            pass
    return doc


def index_topic(topic_key: str, max_msgs: int = 2000, consumer_timeout_ms: int = 5000) -> int:
    """
    Consume up to `max_msgs` messages from a Kafka topic and bulk-index
    them into the matching Elasticsearch index.

    Returns the number of documents indexed.
    """
    from config.settings import KAFKA_TOPICS

    kafka_topic  = KAFKA_TOPICS[topic_key]
    index_name   = TOPIC_INDEX_MAP[topic_key]
    group_id     = f"es-indexer-{topic_key}"

    es = get_es_client()
    ensure_index(es, index_name)

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=consumer_timeout_ms,
        max_poll_records=500,
    )

    actions = []
    count   = 0
    for msg in consumer:
        if count >= max_msgs:
            break
        payload = msg.value
        # Payload is {"data": {...}, "fetched_at": "...", "topic": "..."}
        doc = payload.get("data", payload)
        doc["_indexed_at"] = datetime.now(IL_TZ).isoformat()
        doc = _add_geo_point(doc)
        actions.append({
            "_index": index_name,
            "_source": doc,
        })
        count += 1

    consumer.commit()
    consumer.close()

    if actions:
        success, errors = helpers.bulk(es, actions, raise_on_error=False)
        if errors:
            log.warning(f"ES bulk errors for {index_name}: {errors[:3]}")
        log.info(f"Indexed {success} docs into {index_name}")
        return success

    log.info(f"No messages to index from {kafka_topic}")
    return 0


def index_all_topics(max_msgs: int = 2000) -> dict:
    """Index all transit topics. Returns a summary dict."""
    results = {}
    for topic_key in TOPIC_INDEX_MAP:
        try:
            n = index_topic(topic_key, max_msgs=max_msgs)
            results[topic_key] = n
        except Exception as e:
            log.error(f"Failed to index {topic_key}: {e}")
            results[topic_key] = 0
    return results


def index_from_minio(topic_key: str, date_prefix: str = None,
                     max_files: int = 10, after_key: str = None) -> int:
    """
    Read processed JSON files from MinIO and index them into Elasticsearch.
    Use this to backfill historical data that was stored by the ETL DAG.

    Args:
        topic_key:   e.g. "bus_positions" reads from raw/bus-positions/
        date_prefix: e.g. "year=2026/month=03/day=01" to narrow the scan
        max_files:   max number of JSON files to process per call
        after_key:   only index files whose S3 key > after_key (skip already-indexed)
    """
    import boto3
    from botocore.client import Config

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
    MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    S3_BUCKET      = os.getenv("S3_BUCKET_NAME",   "israel-transit-lake")

    slug_map = {
        "bus_positions":   "raw/bus-positions",
        "trip_updates":    "raw/trip-updates",
        "train_positions": "raw/train-positions",
        "service_alerts":  "raw/service-alerts",
        "delay_events":    "raw/delay-events",
    }
    prefix     = slug_map.get(topic_key, f"raw/{topic_key}")
    index_name = TOPIC_INDEX_MAP[topic_key]
    if date_prefix:
        prefix = f"{prefix}/{date_prefix}"

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    es         = get_es_client()
    ensure_index(es, index_name)
    paginator  = s3.get_paginator("list_objects_v2")
    total_docs = 0
    files_done = 0

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if files_done >= max_files:
                break
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            try:
                body    = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
                records = json.loads(body)
                if isinstance(records, dict):
                    records = [records]
                actions = []
                for doc in records:
                    doc = _add_geo_point(doc)
                    doc["_minio_source"] = key
                    doc["_indexed_at"]   = datetime.now(IL_TZ).isoformat()
                    actions.append({"_index": index_name, "_source": doc})
                if actions:
                    success, _ = helpers.bulk(es, actions, raise_on_error=False)
                    total_docs += success
                    log.info(f"MinIO→ES: {key} → {success} docs → {index_name}")
                files_done += 1
            except Exception as e:
                log.error(f"MinIO→ES error on {key}: {e}")

    log.info(f"MinIO→ES done: {files_done} files, {total_docs} docs → {index_name}")
    return total_docs


# ─────────────────────────────────────────────────────────────────────────────
# Last-indexed tracking helpers (used by dag_es_indexer MinIO backfill)
# Stores the last-processed S3 key in a small Elasticsearch document so that
# each backfill run only processes *new* files, preventing duplicate indexing.
# ─────────────────────────────────────────────────────────────────────────────
_TRACKER_INDEX = "transit-indexer-state"


def get_last_indexed_key(topic_key: str, date_prefix: str) -> Optional[str]:
    """Return the S3 key of the last file indexed for this topic + date, or None."""
    es  = get_es_client()
    doc_id = f"{topic_key}_{date_prefix.replace('/', '_')}"
    try:
        res = es.get(index=_TRACKER_INDEX, id=doc_id, ignore=[404])
        return res.get("_source", {}).get("last_key") if res.get("found") else None
    except Exception:
        return None


def set_last_indexed_key(topic_key: str, date_prefix: str, last_key: str = "done") -> None:
    """Persist the last-indexed S3 key for this topic + date."""
    es  = get_es_client()
    doc_id = f"{topic_key}_{date_prefix.replace('/', '_')}"
    try:
        es.index(index=_TRACKER_INDEX, id=doc_id, document={"last_key": last_key, "updated_at": datetime.now(IL_TZ).isoformat()})
    except Exception as e:
        log.warning(f"Could not save indexer state: {e}")


if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else None
    if topic:
        n = index_topic(topic)
        print(f"Indexed {n} documents into transit-{topic.replace('_', '-')}")
    else:
        results = index_all_topics()
        print("Indexing complete:", results)
