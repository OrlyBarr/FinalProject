#!/bin/bash
# ============================================================
# start_es_and_backfill.sh
# Run Tuesday evening to load all MinIO data into ES
# Then ES + dag_es_indexer stay running for real-time updates
# ============================================================
set -e

echo "========================================"
echo "  ES Backfill - $(date)"
echo "========================================"

# 1. Start ES
echo ""
echo "[1/4] Starting Elasticsearch..."
docker update --restart=unless-stopped elasticsearch 2>/dev/null || true
docker start elasticsearch 2>/dev/null || echo "ES already running"

# 2. Wait for ES to be ready (up to 3 minutes)
echo "[2/4] Waiting for ES to be ready..."
for i in $(seq 1 36); do
  if curl -s http://localhost:9200/_cluster/health 2>/dev/null | grep -q '"status"'; then
    echo "  ES is ready!"
    break
  fi
  if [ "$i" -eq 36 ]; then
    echo "ERROR: ES not ready after 3 min"
    exit 1
  fi
  sleep 5
done

# 3. Run full backfill from MinIO to ES
echo "[3/4] Loading all data from MinIO into ES..."
echo "  This may take 10-20 minutes..."
cd /home/local_admin/finalproject
python3 /home/local_admin/finalproject/scripts/es_backfill.py

# 4. Unpause indexer DAG for real-time updates
echo "[4/4] Unpausing dag_es_indexer for real-time..."
docker exec postgres psql -U airflow -d airflow -c \
  "UPDATE dag SET is_paused=false WHERE dag_id='dag_es_indexer';" 2>/dev/null || true

echo ""
echo "========================================"
echo "  DONE! ES is running + indexer active"
echo "  Kibana: http://localhost:5601"
echo "  New data will auto-index every 3 min"
echo "========================================"
