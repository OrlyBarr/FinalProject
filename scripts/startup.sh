#!/bin/bash
# הרצי אחרי כל אתחול / boot של ה-VM
# Wakes up the entire stack + flushes any buffered data

cd /home/local_admin/finalproject

echo '🚀 Starting Israel Transit Platform...'
echo ''

# 1. Bring up containers
echo '[1/4] Starting Docker containers...'
docker compose up -d
sleep 30  # Give MinIO + ES time to start

# 2. Verify health
echo '[2/4] Checking services...'
for svc in 'http://localhost:9000/minio/health/live:MinIO'            'http://localhost:9200:Elasticsearch'            'http://localhost:5601/api/status:Kibana'; do
  url=${svc%%:*}; name=${svc##*:}
  if curl -sm 5 $url > /dev/null 2>&1; then
    echo "   ✅ $name UP"
  else
    echo "   ⚠️  $name not yet ready"
  fi
done

# 3. Start bot.py (watchdog will keep it alive)
echo '[3/4] Starting bot.py...'
if ! pgrep -f 'bot.py' > /dev/null; then
  nohup /home/local_admin/finalproject/venv/bin/python3 -u bot.py > bot.log 2>&1 &
  echo '   ✅ bot.py started'
fi

# 4. Flush any backlog from buffer
echo '[4/4] Flushing buffered data...'
PENDING=$(ls /home/local_admin/finalproject/buffer/minio_pending/*.meta.json 2>/dev/null | wc -l)
if [ $PENDING -gt 0 ]; then
  echo "   Found $PENDING pending uploads — flushing..."
  BUFFER_DIR=/home/local_admin/finalproject/buffer   MINIO_ENDPOINT=http://localhost:9000   MINIO_ACCESS_KEY=minioadmin   MINIO_SECRET_KEY=minioadmin123   /home/local_admin/finalproject/venv/bin/python3 /home/local_admin/finalproject/scripts/flush_buffer.py
else
  echo '   No backlog — buffer is empty.'
fi

echo ''
echo '✅ Stack is up. Cron will resume normal pipeline runs every 5 min.'
echo ''
echo 'URLs:'
echo '   Frontend:  http://linub-vm:5000/'
echo '   Kibana:    http://linub-vm:5601'
echo '   MinIO:     http://linub-vm:9001'
