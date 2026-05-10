#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# Resilient pipeline — runs every 5 min via cron.
# 
# Always fetches data from external APIs (works as long as VM has internet).
# If MinIO/ES are DOWN, buffers data to local disk.
# When containers come back UP, automatically flushes the backlog.
#
# This is a 'store-and-forward' pattern — the same pattern Kafka uses
# at a deeper level, applied here at the file system layer for full
# system resilience.
# ─────────────────────────────────────────────────────────────────

cd /home/local_admin/finalproject

VENV=/home/local_admin/finalproject/venv/bin/python3
LOG=/home/local_admin/finalproject/pipeline_host.log
PROJECT=/home/local_admin/finalproject

# Persistent buffer (survives reboots — unlike /tmp)
export BUFFER_DIR='/home/local_admin/finalproject/buffer'

export MINIO_ENDPOINT='http://localhost:9000'
export MINIO_ACCESS_KEY='minioadmin'
export MINIO_SECRET_KEY='minioadmin123'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
export ELASTICSEARCH_HOST='http://localhost:9200'
export S3_BUCKET_NAME='israel-transit-lake'

TS=$(date '+%Y-%m-%d %H:%M:%S')

echo '' >> $LOG
echo "=== Pipeline run: $TS ===" >> $LOG

# ─── Health check ────────────────────────────────────────────────
MINIO_UP=0
ES_UP=0
if curl -sm 3 http://localhost:9000/minio/health/live > /dev/null 2>&1; then MINIO_UP=1; fi
if curl -sm 3 http://localhost:9200 > /dev/null 2>&1; then ES_UP=1; fi
echo "[STATUS] MinIO=$MINIO_UP, ES=$ES_UP" >> $LOG

PENDING_MINIO=$(ls $BUFFER_DIR/minio_pending/*.meta.json 2>/dev/null | wc -l)
PENDING_ES=$(ls $BUFFER_DIR/es_pending/*.json 2>/dev/null | wc -l)
echo "[BACKLOG] MinIO pending: $PENDING_MINIO files, ES pending: $PENDING_ES files" >> $LOG

# ─── Step 0: Sync GitHub buffer (catches data missed during sleep) ──
if [ -d "$PROJECT/.git" ]; then
  echo '[0] Syncing data from GitHub buffer...' >> $LOG
  cd $PROJECT && git pull origin main --quiet >> $LOG 2>&1
  $VENV $PROJECT/scripts/sync_from_github.py >> $LOG 2>&1
  cd $PROJECT
fi

# ─── Step 1: ALWAYS fetch from APIs (independent of containers) ──
echo '[1] Fetching from external APIs...' >> $LOG
$VENV storage/direct_to_minio.py >> $LOG 2>&1

# ─── Step 2: Flush MinIO backlog if MinIO is up ──────────────────
if [ $MINIO_UP -eq 1 ] && [ $PENDING_MINIO -gt 0 ]; then
  echo "[2] Flushing $PENDING_MINIO pending MinIO uploads..." >> $LOG
  $VENV $PROJECT/scripts/flush_buffer.py >> $LOG 2>&1
fi

# ─── Step 3: Index to ES if up ───────────────────────────────────
if [ $ES_UP -eq 1 ]; then
  echo '[3] Indexing MinIO → Elasticsearch...' >> $LOG
  DATE_PREFIX="year=$(date +%Y)/month=$(date +%m)/day=$(date +%d)"
  $VENV -c "
import os, sys
os.environ['MINIO_ENDPOINT'] = 'http://localhost:9000'
os.environ['ELASTICSEARCH_HOST'] = 'http://localhost:9200'
sys.path.insert(0, '/home/local_admin/finalproject')
from storage.es_indexer import index_from_minio
date_prefix = '$DATE_PREFIX'
total = 0
for t in ['bus_positions', 'train_positions', 'traffic_data', 'trip_updates', 'service_alerts']:
    try:
        n = index_from_minio(t, date_prefix=date_prefix)
        total += n
    except Exception as e:
        print(f'{t}: ERROR {e}')
print(f'Total ES docs indexed this run: {total}')
" >> $LOG 2>&1
else
  echo '[3] ES is DOWN — skipping indexing (data preserved in MinIO)' >> $LOG
fi

# ─── Step 4: Trim log file ───────────────────────────────────────
if [ -f $LOG ] && [ $(wc -l < $LOG) -gt 5000 ]; then
  tail -2000 $LOG > $LOG.tmp && mv $LOG.tmp $LOG
fi

echo "[DONE] Pipeline finished: $(date '+%H:%M:%S')" >> $LOG
