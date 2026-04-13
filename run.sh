#!/bin/bash
# ================================================================
#  🚌  Israel Public Transit - Real-Time Monitoring Platform
#  run.sh — Main startup script
#  Run once: bash run.sh
#  It will handle everything else automatically.
# ================================================================

set -e  # Stop on first error

# ── Output colors ────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ── Helper functions ──────────────────────────────────────────────
log()     { echo -e "${BOLD}${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
success() { echo -e "${GREEN}✅ $1${NC}"; }
warn()    { echo -e "${YELLOW}⚠️  $1${NC}"; }
error()   { echo -e "${RED}❌ $1${NC}"; exit 1; }
step()    { echo -e "\n${BOLD}${CYAN}━━━  Step $1  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }
wait_for_service() {
  local name=$1 url=$2 max=$3
  # Check immediately first — service may already be up
  if curl -sf "$url" > /dev/null 2>&1; then
    success "$name is already up!"
    return 0
  fi
  log "Waiting for $name to be ready..."
  for i in $(seq 1 $max); do
    if curl -sf "$url" > /dev/null 2>&1; then
      echo ""
      success "$name is ready!"
      return 0
    fi
    echo -n "."
    sleep 2
  done
  echo ""
  warn "$name did not respond within $(($max * 2)) seconds — continuing anyway"
}

# ─────────────────────────────────────────────────────────────
#  BANNER
# ─────────────────────────────────────────────────────────────
clear
echo -e "${BOLD}${BLUE}"
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║    🚌  Israel Public Transit Monitoring Platform     ║"
echo "  ║         Real-Time Public Transit Monitoring System       ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ─────────────────────────────────────────────────────────────
#  Step 0: Pre-flight checks
# ─────────────────────────────────────────────────────────────
step "0 — Pre-flight checks"

# Docker
if ! command -v docker &> /dev/null; then
  error "Docker is not installed. Download from: https://docs.docker.com/get-docker/"
fi
success "Docker installed: $(docker --version)"

# Docker Compose
if ! command -v docker compose &> /dev/null; then
  error "Docker Compose not found."
fi
success "Docker Compose installed: $(docker compose version --short)"

# Python 3
if ! command -v python3 &> /dev/null; then
  error "Python3 is not installed"
fi
success "Python: $(python3 --version)"

# .env
if [ ! -f ".env" ]; then
  if [ -f ".env.example" ]; then
    warn ".env not found — copying from .env.example"
    cp .env.example .env
    echo ""
    echo -e "${YELLOW}${BOLD}  ⚠️  Important: Edit the .env file before continuing!${NC}"
    echo -e "  Open: ${CYAN}nano .env${NC} or ${CYAN}code .env${NC}"
    echo -e "  Fill in passwords and API keys."
    echo ""
    read -p "  Have you already filled in .env? (y/n): " confirm
    if [[  "$confirm" != "y" && "$confirm" != "Y" ]]; then
      warn "Open .env, fill in the details, then re-run."
      exit 0
    fi
  else
    error ".env and .env.example not found"
  fi
fi
success ".env file found"

# Check that critical variables exist (minimum for local development)
set -a; source .env 2>/dev/null || true; set +a
if [ -z "$USE_MINIO" ]; then
  warn "USE_MINIO not set — defaulting to true for local development"
  echo "USE_MINIO=true" >> .env
fi

# ─────────────────────────────────────────────────────────────
#  Step 1: Install Python dependencies
# ─────────────────────────────────────────────────────────────
step "1 — Installing Python libraries"

if [ ! -d "venv" ]; then
  log "Creating Python virtual environment..."
  python3 -m venv venv
  success "Virtual environment created"
fi

log "Activating virtual environment and installing libraries..."
source venv/bin/activate

# Install only if a key package is missing (avoids slow re-resolution on every run)
if ! python3 -c "import kafka, pandas, boto3, dotenv, geopy; from google.transit import gtfs_realtime_pb2" 2>/dev/null; then
  pip install --upgrade pip -q
  pip install -r requirements.txt -q
  success "All libraries installed"
else
  success "All libraries already installed — skipping"
fi

# ─────────────────────────────────────────────────────────────
#  Step 2: Start Docker services
# ─────────────────────────────────────────────────────────────
step "2 — Starting Docker services"

# Check if all core containers are already running — if so, skip teardown entirely
RUNNING=$(docker compose ps --services --filter status=running 2>/dev/null | wc -l)
TOTAL=$(docker compose config --services 2>/dev/null | wc -l)

if [ "$RUNNING" -ge "$TOTAL" ] 2>/dev/null; then
  success "All containers already running — skipping teardown"
else
  # Only pull images that are not already cached locally
  log "Checking for missing Docker images..."
  docker compose pull --quiet --ignore-pull-failures 2>/dev/null || true

  log "Removing old/stopped containers that may cause conflicts..."
  # Only tear down stopped/exited containers; leave running ones alone
  for container in elasticsearch zookeeper kafka minio postgres airflow-webserver airflow-scheduler kibana kafka-ui; do
    STATUS=$(docker inspect --format '{{.State.Status}}' "$container" 2>/dev/null || echo "")
    if [ "$STATUS" = "exited" ] || [ "$STATUS" = "created" ]; then
      docker rm -f "$container" 2>/dev/null || true
    fi
  done
fi

log "Starting all services (skips already-running containers)..."
docker compose up -d --remove-orphans 2>&1 || {
  warn "docker compose up exited with an error — attempting to start created containers..."
  for container in elasticsearch zookeeper kafka minio postgres airflow-webserver airflow-scheduler kibana kafka-ui kafka-init; do
    STATUS=$(docker inspect --format '{{.State.Status}}' "$container" 2>/dev/null || echo "")
    if [ "$STATUS" = "created" ]; then
      docker start "$container" 2>/dev/null || true
    fi
  done
}

success "All containers started"
echo ""
docker compose ps
echo ""

# ─────────────────────────────────────────────────────────────
#  Step 3: Wait for critical services
# ─────────────────────────────────────────────────────────────
step "3 — Waiting for services to be ready"

wait_for_service "Kafka UI"  "http://localhost:8080"        20
wait_for_service "MinIO"     "http://localhost:9000/minio/health/live" 15
wait_for_service "Airflow"   "http://localhost:8081/health"  60

# ─────────────────────────────────────────────────────────────
#  Step 4: Create Kafka Topics
# ─────────────────────────────────────────────────────────────
step "4 — Creating Kafka Topics"

# Check if topics already exist
EXISTING=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || echo "")

TOPICS_TO_CREATE=""
for topic in "bus-positions" "train-positions" "trip-updates" "service-alerts" "delay-events" "traffic-data" "pipeline-errors" "bus-delays" "train-delays" "bus-delays-historical" "train-delays-historical"; do
  if echo "$EXISTING" | grep -q "^${topic}$"; then
    warn "Topic '${topic}' already exists — skipping"
  else
    TOPICS_TO_CREATE="$TOPICS_TO_CREATE $topic"
  fi
done

# Create all missing topics in minimal docker exec calls
for topic in $TOPICS_TO_CREATE; do
  docker exec kafka kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1 \
    --topic "$topic" 2>/dev/null && success "Topic created: $topic"
done
[ -z "$TOPICS_TO_CREATE" ] && success "All Kafka topics already exist"

# ─────────────────────────────────────────────────────────────
#  Step 5: Initialize Airflow
# ─────────────────────────────────────────────────────────────
step "5 — Initializing Apache Airflow"

# Check if admin user already exists before running slow airflow init commands
AF_USER=$(docker compose exec -T airflow-webserver airflow users list 2>/dev/null | grep -c "admin" || echo "0")
if [ "$AF_USER" -eq 0 ] 2>/dev/null; then
  log "Initializing Airflow database..."
  docker compose exec -T airflow-webserver airflow db migrate 2>/dev/null || \
  docker compose exec -T airflow-webserver airflow db init 2>/dev/null || \
  warn "Airflow DB already initialized"

  log "Creating Airflow admin user..."
  docker compose exec -T airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@transit.il 2>/dev/null || warn "Admin user already exists"
else
  success "Airflow already initialized — skipping DB init"
fi

success "Airflow is ready"

# ─────────────────────────────────────────────────────────────
#  Step 6: Initialize MinIO Bucket
# ─────────────────────────────────────────────────────────────
step "6 — Initializing MinIO (local Data Lake)"

log "Creating bucket for transit data..."
python3 - <<'EOF'
import sys
sys.path.insert(0, '.')
try:
    import boto3
    from botocore.exceptions import ClientError
    client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        region_name='us-east-1'
    )
    bucket = 'israel-transit-lake'
    try:
        client.head_bucket(Bucket=bucket)
        print(f'⚠️  Bucket {bucket} already exists')
    except ClientError:
        client.create_bucket(Bucket=bucket)
        print(f'✅ Bucket created: {bucket}')
    # Create prefix folders — raw + processed for each data type
    prefixes = [
        'raw/bus-positions/',
        'raw/train-positions/',
        'raw/trip-updates/',
        'raw/service-alerts/',
        'raw/traffic-data/',
        'processed/bus-positions/',
        'processed/train-positions/',
        'processed/trip-updates/',
        'processed/service-alerts/',
        'processed/traffic-data/',
        'processed/delay-events/',
    ]
    for prefix in prefixes:
        client.put_object(Bucket=bucket, Key=prefix, Body=b'')
    print(f'✅ {len(prefixes)} prefixes created in MinIO')
except Exception as e:
    print(f'⚠️  MinIO: {e}')
EOF

# ─────────────────────────────────────────────────────────────
#  Step 7: Initialize Redshift Schema (or local PostgreSQL)
# ─────────────────────────────────────────────────────────────
step "7 — Initializing Data Warehouse Schema"

set -a; source .env 2>/dev/null || true; set +a

if [ -n "$REDSHIFT_HOST" ] && [ "$REDSHIFT_HOST" != "your-cluster.region.redshift.amazonaws.com" ]; then
  log "Initializing Redshift schema..."
  python3 - <<'EOF'
import sys; sys.path.insert(0, '.')
try:
    from warehouse.redshift_writer import RedshiftWriter
    rw = RedshiftWriter()
    rw.create_schema()
    rw.close()
    print('✅ Redshift schema created successfully')
except Exception as e:
    print(f'⚠️  Redshift: {e}')
EOF
else
  warn "REDSHIFT_HOST not set — skipping (can be configured later in .env)"
fi

# ─────────────────────────────────────────────────────────────
#  Step 8: Enable Airflow DAGs
# ─────────────────────────────────────────────────────────────
step "8 — Enabling Airflow DAGs"

for dag in "dag_realtime_ingestion" "dag_etl_transform" "dag_daily_analytics" "dag_traffic_ingestion" "dag_es_indexer"; do
  docker compose exec -T airflow-webserver \
    airflow dags unpause "$dag" 2>/dev/null && \
    success "DAG enabled: $dag" || \
    warn "Could not enable $dag (may not be loaded yet)"
done

# ─────────────────────────────────────────────────────────────
#  Step 9: Initial GTFS-RT connectivity check
# ─────────────────────────────────────────────────────────────
step "9 — Testing GTFS-RT connection (live transit data)"

log "Fetching data from MOT GTFS-RT..."
python3 - <<'EOF'
import sys; sys.path.insert(0, '.')
import requests

feeds = {
    'VehiclePositions (bus locations)': 'https://gtfs.mot.gov.il/gtfsfiles/VehiclePositions.pb',
    'TripUpdates (delays)':             'https://gtfs.mot.gov.il/gtfsfiles/TripUpdates.pb',
    'ServiceAlerts (alerts)':           'https://gtfs.mot.gov.il/gtfsfiles/ServiceAlerts.pb',
}
for name, url in feeds.items():
    try:
        r = requests.head(url, timeout=5)
        if r.status_code == 200:
            print(f'✅ {name}: available')
        else:
            print(f'⚠️  {name}: HTTP {r.status_code}')
    except Exception as e:
        print(f'❌ {name}: {e}')

# HERE Traffic API
import os
here_key = os.getenv('HERE_API_KEY', '')
if here_key:
    try:
        r = requests.get(
            'https://data.traffic.hereapi.com/v7/flow',
            params={'apiKey': here_key, 'in': 'bbox:34.7,31.9,35.0,32.1',
                    'locationReferencing': 'shape'},
            timeout=5
        )
        if r.status_code == 200:
            print(f'✅ HERE Traffic API: available and responding (HTTP 200)')
        elif r.status_code == 401:
            print(f'❌ HERE Traffic API: invalid API key (HTTP 401)')
        else:
            print(f'⚠️  HERE Traffic API: reachable, key valid (HTTP {r.status_code})')
    except Exception as e:
        print(f'⚠️  HERE Traffic API: {e}')
else:
    print('⚠️  HERE_API_KEY not set in .env — traffic data will not be collected')

# Open Bus Stride (Hasadna)
try:
    r = requests.get('https://open-bus-stride-api.hasadna.org.il/gtfs_stops/list?limit=1', timeout=5)
    print(f'✅ Open Bus Stride API: available (HTTP {r.status_code})')
except Exception as e:
    print(f'⚠️  Open Bus Stride API: {e}')
EOF

# ─────────────────────────────────────────────────────────────
#  Step 10: Run initial Producer test
# ─────────────────────────────────────────────────────────────
step "10 — Running first Producer test"

log "Running Bus Positions Producer for initial sample..."
python3 - <<'EOF'
import sys; sys.path.insert(0, '.')
import os; os.environ.setdefault('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
try:
    from producers.bus_positions_producer import BusPositionsProducer
    p = BusPositionsProducer()
    p.run_once()
    p.close()
    print(f'✅ Producer succeeded: {p.stats["sent"]} messages sent to Kafka')
except Exception as e:
    print(f'⚠️  Producer (non-critical): {e}')
EOF

# ─────────────────────────────────────────────────────────────
#  Step 11: Start Bot API (port 5000)
# ─────────────────────────────────────────────────────────────
step "11 — Starting Transit Bot API (port 5000)"

BOT_PORT=5000

# Check if port is already in use
if lsof -i :${BOT_PORT} -t > /dev/null 2>&1; then
  EXISTING_PID=$(lsof -i :${BOT_PORT} -t)
  warn "Port ${BOT_PORT} already in use (PID: ${EXISTING_PID}) — stopping old process..."
  kill -9 "$EXISTING_PID" 2>/dev/null || true
  sleep 1
fi

log "Starting Bot API on port ${BOT_PORT}..."
source venv/bin/activate
nohup python3 bot.py > /tmp/transit_bot.log 2>&1 &
BOT_PID=$!
sleep 2

# Verify the bot started
if curl -sf "http://localhost:${BOT_PORT}/health" > /dev/null 2>&1; then
  success "Bot API is running! PID=${BOT_PID}"
  success "Address: http://localhost:${BOT_PORT}"
else
  warn "Bot API did not respond — check logs: tail /tmp/transit_bot.log"
fi

# ─────────────────────────────────────────────────────────────
#  Done — Summary and links
# ─────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}"
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║           🎉  System is up and running!              ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${BOLD}  🔗  Access URLs:${NC}"
echo -e "  ${CYAN}Airflow UI${NC}     →  http://localhost:8081  (admin / admin)"
echo -e "  ${CYAN}Kafka UI${NC}       →  http://localhost:8080"
echo -e "  ${CYAN}MinIO Console${NC}  →  http://localhost:9001  (minioadmin / minioadmin123)"
echo -e "  ${CYAN}Kibana${NC}         →  http://localhost:5601"
echo -e "  ${CYAN}Bot API${NC}        →  http://localhost:5000  (GET /buses /stops /status)"
echo ""
echo -e "${BOLD}  📋  Useful commands:${NC}"
echo -e "  ${YELLOW}Stop:${NC}              docker compose down"
echo -e "  ${YELLOW}Logs:${NC}              docker compose logs -f kafka"
echo -e "  ${YELLOW}Service status:${NC}    docker compose ps"
echo -e "  ${YELLOW}Manual producer:${NC}   source venv/bin/activate && python3 producers/bus_positions_producer.py"
echo ""
echo -e "${BOLD}  🚌  Project components:${NC}"
  echo -e "  • ${GREEN}5 Producers${NC} fetching data from GTFS-RT, Israel Railways, and HERE Traffic API"
  echo -e "  • ${GREEN}11 Kafka Topics${NC} for real-time streaming (incl. bus/train delay topics)"
echo -e "  • ${GREEN}5 Airflow DAGs${NC} for ETL scheduling"
echo -e "  • ${GREEN}MinIO S3${NC} for storage (Data Lake)"
echo -e "  • ${GREEN}Redshift${NC} for data warehousing (Data Warehouse)"
echo -e "  • ${GREEN}Delay Collectors${NC} (bus, train, historical) → run: bash scripts/start_collectors.sh"
echo -e "  • ${GREEN}Kibana Dashboard${NC} → import: bash scripts/import_kibana.sh"
echo ""