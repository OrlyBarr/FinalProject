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
KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"

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

# ── Helper: import one ndjson file ───────────────────────────────
import_file() {
  local FILE="$1"
  local LABEL="$2"
  local TMP="/tmp/kibana_import_$(basename "$FILE").json"

  [[ -f "$FILE" ]] || { warn "File not found, skipping: $FILE"; return; }

  # kibana_dashboard.ndjson is stored as a JSON array — convert to ndjson
  if python3 -c "import json,sys; d=json.load(open('$FILE')); assert isinstance(d,list)" 2>/dev/null; then
    log "Converting JSON array → ndjson: $FILE"
    python3 -c "
import json, sys
objects = json.load(open('$FILE'))
for obj in objects:
    print(json.dumps(obj))
" > /tmp/converted_$(basename "$FILE")
    FILE="/tmp/converted_$(basename "$FILE")"
  fi

  log "Importing $LABEL ..."
  HTTP_CODE=$(curl -s -o "$TMP" -w "%{http_code}" \
    -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
    -H "kbn-xsrf: true" \
    -F "file=@$FILE")

  if [[ "$HTTP_CODE" == "200" ]]; then
    ERRORS=$(python3 -c "
import json
d = json.load(open('$TMP'))
errs = d.get('errors', [])
print(len(errs), 'error(s):' if errs else 'errors', errs if errs else '')
" 2>/dev/null || echo "unknown")
    if [[ "$ERRORS" == *"0 errors"* || "$ERRORS" == *"0 error"* ]]; then
      success "$LABEL imported successfully!"
    else
      warn "$LABEL imported with issues: $ERRORS"
    fi
  else
    warn "$LABEL — Kibana returned HTTP $HTTP_CODE"
    cat "$TMP" || true
  fi
}

# ── Import both dashboards ────────────────────────────────────────
import_file "$PROJECT_ROOT/kibana/kibana_dashboard.ndjson" "Israel Transit Delays Dashboard"
import_file "$PROJECT_ROOT/kibana/transit_dashboard.ndjson"  "Israel Transit Main Dashboard"

echo ""
echo -e "${BOLD}Open dashboards:${NC} $KIBANA_URL/app/dashboards"
echo -e "  • ${BOLD}Israel Transit Delays Dashboard${NC}"
echo -e "  • ${BOLD}Israel Transit — גוש דן ותל אביב${NC}"
