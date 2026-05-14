#!/bin/bash
# Demo prep — run 30-60 minutes BEFORE presentation
echo '=================================='
echo '  Demo Prep Checklist'
echo '=================================='

echo ''
echo '[1/8] System load:'
uptime

echo ''
echo '[2/8] Docker services:'
docker ps --format 'table {{.Names}}\t{{.Status}}' 2>/dev/null | head -15

echo ''
echo '[3/8] Bot.py health:'
curl -sm 5 http://localhost:5000/health 2>&1 | head -3

echo ''
echo '[4/8] Elasticsearch indices:'
curl -sm 8 'http://localhost:9200/_cat/indices/transit-*?h=index,docs.count,store.size&s=index' 2>&1

echo ''
echo '[5/8] Latest data freshness:'
curl -sm 5 http://localhost:5000/status 2>&1 | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    df = d.get('data_freshness', {})
    for k, v in df.items():
        print(f'  {k}: {v.get(\"latest\",\"?\")} ({v.get(\"total\",0)} docs)')
except: print('  bot not responding')
"

echo ''
echo '[6/8] Active lines now:'
for op in '3:Dan' '5:Egged' '18:Superbus' '2:Trains'; do
  ID=${op%%:*}; NAME=${op##*:}
  COUNT=$(curl -sm 8 "http://localhost:5000/api/active_lines?operator_ref=$ID" 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("count",0))' 2>/dev/null || echo 'TIMEOUT')
  echo "  $NAME (op=$ID): $COUNT active lines"
done

echo ''
echo '[7/8] Kibana saved objects:'
curl -sm 8 'http://localhost:5601/api/saved_objects/_find?type=dashboard&per_page=5' -H 'kbn-xsrf: true' 2>&1 | python3 -c "
import json,sys
d=json.load(sys.stdin)
for o in d['saved_objects']:
    print(f'  📊 {o[\"attributes\"][\"title\"]}')" 2>&1

echo ''
echo '[8/8] Backup latest:'
ls -lh /home/local_admin/finalproject/backups/kibana_full_*.ndjson 2>/dev/null | tail -1

echo ''
echo '=================================='
echo '  URLs לפתיחה לפני ההצגה:'
echo '=================================='
echo '  Frontend (Moovit-style):    http://linub-vm:5000/'
echo '  Bot status:                 http://linub-vm:5000/status'
echo '  Kibana:                     http://linub-vm:5601'
echo '  Airflow:                    http://linub-vm:8081 (admin/admin)'
echo '  MinIO Console:              http://linub-vm:9001 (minioadmin/minioadmin123)'
echo '  Kafka UI:                   http://linub-vm:8085'
