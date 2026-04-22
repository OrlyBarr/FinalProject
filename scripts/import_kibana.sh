#!/bin/bash
# ================================================================
#  Import Kibana dashboards — Israel Transit Monitoring Platform
#
#  Dashboard 1: transit_dashboard.ndjson
#    → "🚌 Israel Transit — גוש דן ותל אביב"
#    → Real-time monitoring: buses, trains, alerts, traffic
#
#  Dashboard 2: transit_delays_dashboard.ndjson
#    → "Israel Transit Delays Dashboard"
#    → Delay analysis: bus/train delays, heatmap, operator breakdown
#
#  Run from project root: bash scripts/import_kibana.sh
# ================================================================

set -e

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log()     { echo -e "${BOLD}${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
success() { echo -e "${GREEN}✅ $1${NC}"; }
warn()    { echo -e "${YELLOW}⚠️  $1${NC}"; }
error()   { echo -e "${RED}❌ $1${NC}"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
KIBANA_DIR="$PROJECT_ROOT/kibana"
KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"

# ── Wait for Kibana ───────────────────────────────────────────────
log "Waiting for Kibana at $KIBANA_URL ..."
for i in $(seq 1 40); do
  if curl -sf "$KIBANA_URL/api/status" > /dev/null 2>&1; then
    success "Kibana is ready!"
    break
  fi
  if [[ $i -eq 40 ]]; then
    error "Kibana did not respond after 120 s. Is it running? (docker compose up -d)"
  fi
  echo -n "."
  sleep 3
done
echo ""

# ── Helper: import one NDJSON file ───────────────────────────────
import_dashboard() {
  local FILENAME="$1"
  local TITLE="$2"
  local NDJSON="$KIBANA_DIR/$FILENAME"

  if [[ ! -f "$NDJSON" ]]; then
    warn "File not found — skipping: $FILENAME"
    return
  fi

  log "Importing: $TITLE ..."
  HTTP_CODE=$(curl -s -o /tmp/kibana_import_resp.json -w "%{http_code}" \
    -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
    -H "kbn-xsrf: true" \
    -F "file=@$NDJSON")

  if [[ "$HTTP_CODE" == "200" ]]; then
    ERRS=$(python3 -c "
import json; d=json.load(open('/tmp/kibana_import_resp.json'))
ok=d.get('successCount',0); errs=d.get('errors',[])
print(f'ok={ok} errors={len(errs)}')
" 2>/dev/null || echo "?")
    if [[ "$ERRS" == *"errors=0"* ]]; then
      success "$TITLE ($ERRS)"
    else
      warn "$TITLE — imported with notes: $ERRS"
    fi
  else
    warn "$TITLE — HTTP $HTTP_CODE"
    cat /tmp/kibana_import_resp.json 2>/dev/null | python3 -m json.tool 2>/dev/null || true
  fi
}

# ── Import both dashboards (order matters — index patterns first) ─
import_dashboard "transit_dashboard.ndjson"        "🚌 Israel Transit — ניטור בזמן אמת"
import_dashboard "transit_delays_dashboard.ndjson" "📊 Israel Transit Delays — ניתוח עיכובים"

# ── Summary ───────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}━━━  Kibana Import Complete  ━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${CYAN}Open Kibana:${NC}  $KIBANA_URL/app/dashboards"
echo ""
echo -e "  ${BOLD}Dashboards available:${NC}"
echo -e "  🚌 ${GREEN}Israel Transit — גוש דן ותל אביב${NC}"
echo -e "     Buses, trains, alerts, traffic — real-time, auto-refresh 30s"
echo ""
echo -e "  📊 ${GREEN}Israel Transit Delays Dashboard${NC}"
echo -e "     עיכוד ממוצע אוטובוס/רכבת | heatmap שעה×יום | top 10 קווים מאוחרים"
echo -e "     השוואת מסלולי רכבת | התפלגות עיכודים | עיכובים לפי מפעיל"
echo ""
