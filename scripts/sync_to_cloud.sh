#!/bin/bash
# ============================================================
#  sync_to_cloud.sh — MinIO → Elastic Cloud catch-up
#
#  Run this WHENEVER you want the cloud Elasticsearch to catch
#  up with everything collected into MinIO since the last run.
#
#  Plan for the demo:
#    • Tuesday evening : bash scripts/sync_to_cloud.sh
#    • Wednesday 15:00 : bash scripts/sync_to_cloud.sh   (again)
#
#  It is IDEMPOTENT — deterministic _id means running it twice
#  (even with overlapping data) never creates duplicates.
#  You do NOT need to pass any dates; it tracks the last run.
# ============================================================
set -e
cd "$(dirname "$0")/.."

LOG=/tmp/sync_to_cloud_$(date +%Y%m%d_%H%M%S).log
echo "▶ Sync MinIO → Elastic Cloud starting… (log: $LOG)"

source venv/bin/activate 2>/dev/null || true

MINIO_ENDPOINT_HOST=http://localhost:9000 \
  python3 scripts/reindex_minio_to_cloud_es.py --since-last 2>&1 | tee "$LOG"

echo ""
echo "✅ Done. Verify counts in Kibana:"
echo "   https://e09ede952136469abb765be45f5d1f43.eu-central-1.aws.cloud.es.io/app/dashboards"
