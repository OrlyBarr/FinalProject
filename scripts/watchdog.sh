#!/bin/bash
# Watchdog: keep bot.py + cron-pipeline alive
# Run every 1 minute via cron:
#   * * * * * /home/local_admin/finalproject/scripts/watchdog.sh

LOG=/home/local_admin/finalproject/watchdog.log
PROJECT=/home/local_admin/finalproject

# 1. Check bot.py
if ! pgrep -f 'python3 -u bot.py\|python3 bot.py' > /dev/null; then
  echo "$(date) [WATCHDOG] bot.py not running — restarting" >> $LOG
  cd $PROJECT && nohup $PROJECT/venv/bin/python3 -u bot.py > bot.log 2>&1 &
fi

# 2. Check bot is responding (HTTP health check)
if ! curl -sm 5 http://localhost:5000/health > /dev/null 2>&1; then
  echo "$(date) [WATCHDOG] bot.py not responding on :5000 — killing + restarting" >> $LOG
  pkill -9 -f 'bot.py' 2>/dev/null
  sleep 2
  cd $PROJECT && nohup $PROJECT/venv/bin/python3 -u bot.py > bot.log 2>&1 &
fi

# 3. Check Elasticsearch responsive
if ! curl -sm 5 http://localhost:9200 > /dev/null 2>&1; then
  echo "$(date) [WATCHDOG] Elasticsearch not responding!" >> $LOG
fi

# 4. Trim watchdog log (keep last 500 lines)
if [ -f $LOG ] && [ $(wc -l < $LOG) -gt 500 ]; then
  tail -200 $LOG > $LOG.tmp && mv $LOG.tmp $LOG
fi
