#!/bin/bash
# ================================================================
#  📊  Israel Public Transit — בדיקת מצב המערכת
#  run: bash status.sh
# ================================================================

BOLD='\033[1m'; GREEN='\033[0;32m'; RED='\033[0;31m'
YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

check() {
  local name=$1 url=$2
  if curl -sf "$url" > /dev/null 2>&1; then
    echo -e "  ${GREEN}✅ Active${NC}   $name  →  $url"
  else
    echo -e "  ${RED}❌ Unavailable${NC}  $name  →  $url"
  fi
}

echo -e "${BOLD}${CYAN}"
echo "  ╔══════════════════════════════════════════╗"
echo "  ║  📊  Public Transit Monitoring Status   ║"
echo "  ╚══════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${BOLD}  🐳  Containers:${NC}"
docker compose ps 2>/dev/null || echo "  Docker not running"

echo ""
echo -e "${BOLD}  🌐  Management UIs:${NC}"
check "Airflow"        "http://localhost:8081/health"
check "Kafka UI"       "http://localhost:8080"
check "MinIO"          "http://localhost:9000/minio/health/live"
check "Kibana"         "http://localhost:5601"
check "Elasticsearch"  "http://localhost:9200"
check "Bot API"        "http://localhost:5000/health"

echo ""
echo -e "${BOLD}  📡  External APIs (GTFS-RT):${NC}"
check "VehiclePositions" "https://gtfs.mot.gov.il/gtfsfiles/VehiclePositions.pb"
check "TripUpdates"      "https://gtfs.mot.gov.il/gtfsfiles/TripUpdates.pb"
check "ServiceAlerts"    "https://gtfs.mot.gov.il/gtfsfiles/ServiceAlerts.pb"

echo ""
echo -e "${BOLD}  📨  Kafka Topics:${NC}"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null \
  | sed 's/^/    • /' || echo "  Kafka not available"

echo ""
echo -e "${BOLD}  🗄️  MinIO Bucket:${NC}"
python3 - 2>/dev/null <<'EOF'
import boto3
try:
    s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
                      aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin123',
                      region_name='us-east-1')
    objs = s3.list_objects_v2(Bucket='israel-transit-lake', MaxKeys=5)
    count = objs.get('KeyCount', 0)
    print(f'  ✅ israel-transit-lake — {count} objects')
except Exception as e:
    print(f'  ⚠️  MinIO: {e}')
EOF

echo ""
echo -e "${BOLD}  ⏰  Airflow DAGs:${NC}"
docker compose exec -T airflow-webserver airflow dags list 2>/dev/null \
  | grep -E "dag_realtime|dag_etl|dag_daily" \
  | sed 's/^/  /' || echo "  Airflow not available"

echo ""
echo -e "To open real-time logs: ${YELLOW}docker compose logs -f${NC}"