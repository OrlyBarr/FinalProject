#!/bin/bash
# ================================================================
#  Start all Israel Transit delay collectors in the background
#  Run from the project root: bash scripts/start_collectors.sh
# ================================================================

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

log()     { echo -e "${BOLD}${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
success() { echo -e "${GREEN}✅ $1${NC}"; }
warn()    { echo -e "${YELLOW}⚠️  $1${NC}"; }
error()   { echo -e "${RED}❌ $1${NC}"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COLLECTORS_DIR="$PROJECT_ROOT/collectors"
LOG_DIR="/tmp/transit_collectors"

mkdir -p "$LOG_DIR"

# ── Python environment ────────────────────────────────────────────
if [[ -f "$PROJECT_ROOT/venv/bin/python3" ]]; then
  PYTHON="$PROJECT_ROOT/venv/bin/python3"
elif command -v python3 &>/dev/null; then
  PYTHON="python3"
else
  error "Python 3 not found. Please install or activate a virtualenv."
fi

log "Using Python: $PYTHON ($($PYTHON --version))"

# ── Install collector dependencies ───────────────────────────────
log "Installing collector requirements..."
$PYTHON -m pip install -q -r "$COLLECTORS_DIR/requirements.txt" && \
  success "Collector dependencies installed" || \
  warn "pip install had warnings — continuing"

# ── Stop any existing collector processes ────────────────────────
for script in bus_delay_collector.py train_delay_collector.py delay_kafka_consumer.py; do
  PIDS=$(pgrep -f "$script" 2>/dev/null || true)
  if [[ -n "$PIDS" ]]; then
    warn "Stopping existing $script (PIDs: $PIDS)"
    kill $PIDS 2>/dev/null || true
    sleep 1
  fi
done

cd "$COLLECTORS_DIR"

# ── Start Bus Delay Collector ─────────────────────────────────────
log "Starting Bus Delay Collector..."
nohup $PYTHON bus_delay_collector.py > "$LOG_DIR/bus_delay_collector.log" 2>&1 &
BUS_PID=$!
success "Bus Delay Collector started (PID=$BUS_PID) → $LOG_DIR/bus_delay_collector.log"

# ── Start Train Delay Collector ───────────────────────────────────
log "Starting Train Delay Collector..."
nohup $PYTHON train_delay_collector.py > "$LOG_DIR/train_delay_collector.log" 2>&1 &
TRAIN_PID=$!
success "Train Delay Collector started (PID=$TRAIN_PID) → $LOG_DIR/train_delay_collector.log"

# ── Start Delay Kafka Consumer (→ ES + MinIO) ────────────────────
log "Starting Delay Kafka Consumer (→ Elasticsearch + MinIO)..."
nohup $PYTHON delay_kafka_consumer.py > "$LOG_DIR/delay_kafka_consumer.log" 2>&1 &
CONSUMER_PID=$!
success "Delay Kafka Consumer started (PID=$CONSUMER_PID) → $LOG_DIR/delay_kafka_consumer.log"

echo ""
echo -e "${BOLD}${GREEN}All collectors running!${NC}"
echo -e "  Bus collector:    PID $BUS_PID"
echo -e "  Train collector:  PID $TRAIN_PID"
echo -e "  Kafka consumer:   PID $CONSUMER_PID"
echo ""
echo -e "${BOLD}View logs:${NC}"
echo -e "  tail -f $LOG_DIR/bus_delay_collector.log"
echo -e "  tail -f $LOG_DIR/train_delay_collector.log"
echo -e "  tail -f $LOG_DIR/delay_kafka_consumer.log"
echo ""
echo -e "To run historical fetch (last 7 days):"
echo -e "  cd $COLLECTORS_DIR && $PYTHON historical_fetcher.py --days 7"
