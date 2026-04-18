#!/bin/bash
# ================================================================
#  Import Kibana dashboard (Israel Transit Delays Dashboard)
#  Run from project root: bash scripts/import_kibana.sh
# ================================================================

set -e

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

log()     { echo -e "${BOLD}${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
success() { echo -e "${GREEN}✅ $1${NC}"; }
warn()    { echo -e "${YELLOW}⚠️  $1${NC}"; }
error()   { echo -e "${RED}❌ $1${NC}"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
NDJSON="$PROJECT_ROOT/kibana/kibana_dashboard.ndjson"
KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"

# ── Verify the dashboard file exists ─────────────────────────────
[[ -f "$NDJSON" ]] || error "Dashboard file not found: $NDJSON"

# ── Wait for Kibana ───────────────────────────────────────────────
log "Waiting for Kibana at $KIBANA_URL ..."
for i in $(seq 1 40); do
  if curl -sf "$KIBANA_URL/api/status" > /dev/null 2>&1; then
    success "Kibana is ready!"
    break
  fi
  if [[ $i -eq 40 ]]; then
    error "Kibana did not respond after 120 s. Is it running?"
  fi
  echo -n "."
  sleep 3
done

# ── Import ────────────────────────────────────────────────────────
log "Importing dashboard from $NDJSON ..."
HTTP_CODE=$(curl -s -o /tmp/kibana_import_response.json -w "%{http_code}" \
  -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F "file=@$NDJSON")

if [[ "$HTTP_CODE" == "200" ]]; then
  ERRORS=$(python3 -c "import json,sys; d=json.load(open('/tmp/kibana_import_response.json')); print(d.get('errors', []))" 2>/dev/null || echo "[]")
  if [[ "$ERRORS" == "[]" ]]; then
    success "Dashboard imported successfully!"
  else
    warn "Import completed with errors: $ERRORS"
  fi
else
  warn "Kibana returned HTTP $HTTP_CODE"
  cat /tmp/kibana_import_response.json || true
fi

echo ""
echo -e "${BOLD}Open dashboard:${NC} $KIBANA_URL/app/dashboards"
echo -e "Search for: ${BOLD}Israel Transit Delays Dashboard${NC}"
