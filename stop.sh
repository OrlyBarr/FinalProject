#!/bin/bash
# ================================================================
#  🛑  Israel Public Transit — עצירת המערכת
#  run: bash stop.sh
# ================================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

echo -e "${BOLD}${BLUE}"
echo "  ╔══════════════════════════════════════╗"
echo "  ║  🛑  עוצר את מערכת ניטור התחבורה   ║"
echo "  ╚══════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${YELLOW}בחר פעולה:${NC}"
echo "  1) עצור שירותים (שמור נתונים)"
echo "  2) עצור ומחק הכל (כולל volumes / נתונים)"
echo "  3) ביטול"
read -p "בחירה (1/2/3): " choice

case $choice in
  1)
    echo -e "${BLUE}עוצר שירותים...${NC}"
    docker compose stop
    echo -e "${GREEN}✅ כל השירותים נעצרו. הנתונים שמורים.${NC}"
    echo -e "להפעלה מחדש: ${YELLOW}docker compose start${NC}"
    ;;
  2)
    read -p "⚠️  זה ימחק את כל הנתונים! בטוח? (yes): " confirm
    if [ "$confirm" = "yes" ]; then
      echo -e "${RED}מוחק הכל...${NC}"
      docker compose down -v --remove-orphans
      echo -e "${GREEN}✅ הכל נמחק. להרצה מחדש: bash run.sh${NC}"
    else
      echo "בוטל."
    fi
    ;;
  *)
    echo "בוטל."
    ;;
esac