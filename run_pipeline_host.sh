#!/bin/bash
# Run the full data pipeline from VM host (bypasses Docker networking issue)
# Run every 5-10 minutes via cron

cd /home/local_admin/finalproject
VENV=/home/local_admin/finalproject/venv/bin/python3
LOG=/home/local_admin/finalproject/pipeline_host.log

export MINIO_ENDPOINT='http://localhost:9000'
export MINIO_ACCESS_KEY='minioadmin'
export MINIO_SECRET_KEY='minioadmin123'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
export ELASTICSEARCH_HOST='http://localhost:9200'

echo "" >> $LOG
echo "=== Pipeline run: $(date) ===" >> $LOG

# Step 1: Fetch all data types → MinIO
echo "[1] direct_to_minio..." >> $LOG
$VENV storage/direct_to_minio.py >> $LOG 2>&1
echo "[1] Done" >> $LOG

# Step 2: Index new MinIO data → Elasticsearch
DATE_PREFIX="year=$(date +%Y)/month=$(date +%m)/day=$(date +%d)"
echo "[2] ES indexing for $DATE_PREFIX..." >> $LOG
$VENV -c "
import os, sys
os.environ['MINIO_ENDPOINT'] = 'http://localhost:9000'
os.environ['ELASTICSEARCH_HOST'] = 'http://localhost:9200'
os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
os.environ['MINIO_SECRET_KEY'] = 'minioadmin123'
sys.path.insert(0, '/home/local_admin/finalproject')
from storage.es_indexer import index_from_minio
date_prefix = '$DATE_PREFIX'
for t in ['bus_positions', 'train_positions', 'traffic_data', 'trip_updates', 'service_alerts']:
    n = index_from_minio(t, date_prefix=date_prefix)
    print(f'{t}: {n} docs')
" >> $LOG 2>&1
echo "[2] Done" >> $LOG

echo "Pipeline complete: $(date)" >> $LOG
