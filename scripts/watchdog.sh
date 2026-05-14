#!/bin/bash
# Watchdog: keep bot.py + Airflow scheduler alive
# Run every 1 minute via cron:
#   * * * * * /home/local_admin/finalproject/scripts/watchdog.sh

LOG=/home/local_admin/finalproject/watchdog.log
PROJECT=/home/local_admin/finalproject

# ── 1. Check bot.py ────────────────────────────────────────────────────────────
if ! pgrep -f 'python3 -u bot.py\|python3 bot.py' > /dev/null; then
  echo "$(date) [WATCHDOG] bot.py not running — restarting" >> $LOG
  cd $PROJECT && nohup $PROJECT/venv/bin/python3 -u bot.py > bot.log 2>&1 &
fi

# ── 2. Check bot HTTP health ────────────────────────────────────────────────────
if ! curl -sm 5 http://localhost:5000/health > /dev/null 2>&1; then
  echo "$(date) [WATCHDOG] bot.py not responding — killing + restarting" >> $LOG
  pkill -9 -f 'bot.py' 2>/dev/null
  sleep 2
  cd $PROJECT && nohup $PROJECT/venv/bin/python3 -u bot.py > bot.log 2>&1 &
fi

# ── 3. Check Airflow scheduler heartbeat ───────────────────────────────────────
# Query DB: if latest SchedulerJob heartbeat is >3 minutes old → restart container
HEARTBEAT_AGE=$(docker exec postgres psql -U airflow -t -c \
  "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(latest_heartbeat)))::int
   FROM job WHERE job_type='SchedulerJob' AND state='running';" 2>/dev/null | tr -d ' ')

if [ -z "$HEARTBEAT_AGE" ] || [ "$HEARTBEAT_AGE" -gt 180 ] 2>/dev/null; then
  echo "$(date) [WATCHDOG] Airflow scheduler heartbeat stale (${HEARTBEAT_AGE}s) — restarting container" >> $LOG
  # Kill zombie running task_instances before restart to prevent blocking on restart
  docker exec postgres psql -U airflow -c \
    "UPDATE task_instance SET state='failed', end_date=NOW()
     WHERE state IN ('running','queued','scheduled')
       AND start_date < NOW() - INTERVAL '10 minutes';" 2>/dev/null
  docker exec postgres psql -U airflow -c \
    "UPDATE dag_run SET state='failed', end_date=NOW()
     WHERE state IN ('running','queued')
       AND execution_date < NOW() - INTERVAL '10 minutes';" 2>/dev/null
  # Restart the scheduler container
  docker exec airflow-scheduler kill 1 2>/dev/null || true
  echo "$(date) [WATCHDOG] Scheduler container restarted" >> $LOG
fi

# ── 4. Check MinIO alive ───────────────────────────────────────────────────────
if ! curl -sm 5 http://localhost:9000/minio/health/live > /dev/null 2>&1; then
  echo "$(date) [WATCHDOG] MinIO not responding!" >> $LOG
fi

# ── 5. Trim watchdog log (keep last 1000 lines) ────────────────────────────────
if [ -f $LOG ] && [ $(wc -l < $LOG) -gt 1000 ]; then
  tail -500 $LOG > $LOG.tmp && mv $LOG.tmp $LOG
fi
