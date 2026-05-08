#!/bin/bash
# Quick status check for the resilient pipeline
BUFFER=/home/local_admin/finalproject/buffer
echo '🛡️  Resilient Pipeline Status'
echo '────────────────────────────'

MINIO_UP=$(curl -sm 3 http://localhost:9000/minio/health/live > /dev/null 2>&1 && echo 'UP ✅' || echo 'DOWN ❌')
ES_UP=$(curl -sm 3 http://localhost:9200 > /dev/null 2>&1 && echo 'UP ✅' || echo 'DOWN ❌')

echo "  MinIO:         $MINIO_UP"
echo "  Elasticsearch: $ES_UP"
echo ''

PENDING_COUNT=$(ls $BUFFER/minio_pending/*.meta.json 2>/dev/null | wc -l)
PENDING_SIZE=$(du -sh $BUFFER/minio_pending 2>/dev/null | cut -f1)
echo "  Pending in buffer: $PENDING_COUNT files ($PENDING_SIZE)"

if [ $PENDING_COUNT -gt 0 ]; then
  echo '  Oldest pending:'
  ls -t $BUFFER/minio_pending/*.meta.json 2>/dev/null | tail -1 | xargs -I{} basename {}
fi

echo ''
LAST_RUN=$(grep -E '^=== Pipeline run:' /home/local_admin/finalproject/pipeline_host.log 2>/dev/null | tail -1)
echo "  Last pipeline run: $LAST_RUN"
