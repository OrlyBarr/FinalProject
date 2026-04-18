#!/bin/bash
# ================================================================
#  🛑  Israel Public Transit — עצירת המערכת
#  run: bash stop.sh
# ================================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

echo -e "${BOLD}${BLUE}"
echo "  ╔══════════════════════════════════════╗"
echo "  ║  🛑  Stopping Transit Monitoring System ║"
echo "  ╚══════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${YELLOW}Choose an action:${NC}"
echo "  1) Stop services (keep data)"
echo "  2) Stop and delete everything (including volumes / data)"
echo "  3) Cancel"
read -p "Choice (1/2/3): " choice

case $choice in
  1)
    echo -e "${BLUE}Stopping services...${NC}"
    docker compose stop
    echo -e "${GREEN}✅ All services stopped. Data is preserved.${NC}"
    echo -e "To restart: ${YELLOW}docker compose start${NC}"
    ;;
  2)
    read -p "⚠️  This will delete all data! Sure? (yes): " confirm
    if [ "$confirm" = "yes" ]; then
      echo -e "${RED}Deleting everything...${NC}"
      docker compose down -v --remove-orphans
      echo -e "${GREEN}✅ Everything deleted. To restart: bash run.sh${NC}"
    else
      echo "Cancelled."
    fi
    ;;
  *)
    echo "Cancelled."
    ;;
esac