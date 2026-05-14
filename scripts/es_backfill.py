#!/usr/bin/env python3
"""
es_backfill.py
Bulk-load all MinIO data from the last 7 days into Elasticsearch.
Called by start_es_and_backfill.sh
"""
import os, sys, time
sys.path.insert(0, "/home/local_admin/finalproject")

os.environ.setdefault("ELASTICSEARCH_HOST", "http://localhost:9200")
os.environ.setdefault("MINIO_ENDPOINT", "http://localhost:9000")
os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")
os.environ.setdefault("S3_BUCKET_NAME", "israel-transit-lake")

from storage.es_indexer import index_from_minio
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

IL_TZ = ZoneInfo("Asia/Jerusalem")
now = datetime.now(IL_TZ)

CATEGORIES = [
    "bus_positions",
    "train_positions",
    "trip_updates",
    "service_alerts",
    "delay_events",
    "traffic_data",
]

total = 0
for days_ago in range(7, -1, -1):
    day = (now - timedelta(days=days_ago)).strftime("year=%Y/month=%m/day=%d")
    print(f"\n--- {day} ---")
    for cat in CATEGORIES:
        try:
            n = index_from_minio(cat, date_prefix=day, max_files=500)
            total += n
            if n > 0:
                print(f"  {cat}: {n} docs")
        except Exception as e:
            print(f"  {cat}: ERROR - {e}")
        time.sleep(0.3)

print(f"\n{'='*40}")
print(f"  TOTAL: {total} documents indexed")
print(f"{'='*40}")
