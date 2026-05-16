#!/usr/bin/env bash
# ============================================================
# start_sequenced.sh — staged startup for the Israel Transit stack
# ------------------------------------------------------------
# Why this exists:
#   When all services in docker-compose.yml have restart: unless-stopped
#   or restart: always, a VM reboot causes every container to start at
#   the same moment. On the 8 GB linub-vm the combined startup demand
#   (~28 GB) exhausts RAM and swap, the OOM killer fires, and sshd
#   itself can be killed before you can intervene.
#
# Fix:
#   1) All services in docker-compose.yml are set to restart: "no"
#      so nothing auto-starts on boot.
#   2) This script brings the stack up in deliberate stages with
#      sleeps in between, so each tier of services can stabilise
#      before the next tier loads.
#
# Usage:
#   bash /home/local_admin/finalproject/scripts/start_sequenced.sh
# ============================================================

set -u  # treat unset vars as errors, but do NOT use -e — we want to
        # keep going even if one stage reports a warning

COMPOSE_FILE="/home/local_admin/finalproject/docker-compose.yml"
DC="sudo docker compose -f ${COMPOSE_FILE}"

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
log() {
    echo ""
    echo "============================================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "============================================================"
}

show_health() {
    echo ""
    echo "--- docker compose ps ---"
    ${DC} ps
    echo "--- free -h ---"
    free -h 2>/dev/null || true
    echo ""
}

# ------------------------------------------------------------
# Stage 1: foundation services (storage + coordination)
#   These have no inter-dependencies and are required by everything else.
# ------------------------------------------------------------
log "Stage 1/4: starting foundation services (postgres, zookeeper, minio)"
${DC} up -d postgres zookeeper minio
echo "Waiting 20s for foundation services to come up..."
sleep 20
show_health

# ------------------------------------------------------------
# Stage 2: Kafka broker
#   Needs zookeeper healthy. We wait longer because Kafka's
#   cub zk-ready preflight can take a while on a cold VM.
# ------------------------------------------------------------
log "Stage 2/4: starting Kafka broker"
${DC} up -d kafka
echo "Waiting 30s for Kafka to register with ZooKeeper..."
sleep 30
show_health

# ------------------------------------------------------------
# Stage 3: Kafka-side tooling (UI + topic initialiser)
#   kafka-init is a one-shot that creates the topics; it exits 0
#   when finished. kafka-ui is a long-running web UI on :8085.
# ------------------------------------------------------------
log "Stage 3/4: starting Kafka tools (kafka-ui, kafka-init)"
${DC} up -d kafka-ui kafka-init
echo "Waiting 10s for kafka-init to create topics..."
sleep 10
show_health

# ------------------------------------------------------------
# Stage 4: Airflow (webserver + scheduler)
#   These are the heaviest services in the stack. Bring them up
#   only after Kafka and Postgres are settled.
# ------------------------------------------------------------
log "Stage 4/4: starting Airflow (webserver, scheduler)"
${DC} up -d airflow-webserver airflow-scheduler
echo "Waiting 30s for Airflow webserver+scheduler to initialise..."
sleep 30
show_health

# ------------------------------------------------------------
# Stage 5 (OPTIONAL): Elasticsearch + Kibana
#   Disabled by default — the user has Elasticsearch turned off
#   to free ~1.5 GB of RAM. Uncomment the block below only after
#   confirming that free RAM is comfortably above 1.5 GB.
# ------------------------------------------------------------
# log "Stage 5/5 (optional): starting Elasticsearch + Kibana"
# ${DC} up -d elasticsearch kibana
# echo "Waiting 60s for Elasticsearch cluster to go yellow/green..."
# sleep 60
# show_health

log "All requested stages started. Final status below."
show_health

echo ""
echo "Done. URLs (from the VM host):"
echo "  Airflow UI    http://localhost:8081"
echo "  Kafka UI      http://localhost:8085"
echo "  MinIO Console http://localhost:9001"
echo "  bot.py        http://localhost:5000   (run separately: python bot.py)"
