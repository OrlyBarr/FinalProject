"""
airflow/dags/dag_es_indexer.py
DAG 4: Kafka → Elasticsearch real-time indexer
Schedule: every 1 minute

FIXES:
  - execution_timeout raised from 25s to 60s
    (was impossible: 6 parallel tasks each with 5s consumer timeout + ES write time)
  - schedule_interval changed from 30s to 1 minute to give runs room to breathe
  - Each index task now wraps in try/except and pushes -1 on error so
    log_indexing_summary can report failures instead of silently showing 0
  - index_minio_backfill documents the duplicate-indexing risk and adds a guard
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow")
from resilient_pipeline import RESILIENT_DEFAULT_ARGS

import os
os.environ.setdefault("ELASTICSEARCH_HOST",     "http://elasticsearch:9200")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

default_args = {
    **RESILIENT_DEFAULT_ARGS,
    "email_on_failure":  False,
    "execution_timeout": timedelta(seconds=60),
}


def _safe_index(context, xcom_key: str, topic: str, max_msgs: int, consumer_timeout_ms: int):
    """
    FIX: Shared wrapper that catches exceptions and pushes -1 on failure,
    so log_indexing_summary can distinguish 'no data' (0) from 'task failed' (-1).
    Without this, a crash silently showed 0 in the summary.
    """
    from storage.es_indexer import index_topic
    try:
        n = index_topic(topic, max_msgs=max_msgs, consumer_timeout_ms=consumer_timeout_ms)
        context["ti"].xcom_push(key=xcom_key, value=n)
        print(f"ES {topic} indexed: {n}")
        return n
    except Exception as e:
        print(f"ERROR indexing {topic}: {e}")
        context["ti"].xcom_push(key=xcom_key, value=-1)
        raise  # re-raise so Airflow marks the task failed


def index_bus_positions(**context):
    _safe_index(context, "bus_n", "bus_positions", max_msgs=1000, consumer_timeout_ms=5000)


def index_trip_updates(**context):
    _safe_index(context, "trips_n", "trip_updates", max_msgs=1000, consumer_timeout_ms=5000)


def index_train_positions(**context):
    _safe_index(context, "trains_n", "train_positions", max_msgs=500, consumer_timeout_ms=5000)


def index_service_alerts(**context):
    _safe_index(context, "alerts_n", "service_alerts", max_msgs=200, consumer_timeout_ms=3000)


def index_delay_events(**context):
    _safe_index(context, "delays_n", "delay_events", max_msgs=200, consumer_timeout_ms=3000)


def index_minio_backfill(**context):
    """
    Read latest files from MinIO and index into ES (catches data missed by Kafka consumer).

    FIX: Added already-indexed guard using an ES doc to track the last indexed file key.
    Without this, the same max_files=3 MinIO files were re-indexed every 30 seconds,
    causing duplicate documents in Elasticsearch.
    """
    from storage.es_indexer import index_from_minio, get_last_indexed_key, set_last_indexed_key
    from datetime import datetime

    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("Asia/Jerusalem")).strftime("year=%Y/month=%m/day=%d")

    try:
        last_key = get_last_indexed_key("bus_positions", today)
        n = index_from_minio("bus_positions", date_prefix=today, max_files=3, after_key=last_key)
        if n > 0:
            set_last_indexed_key("bus_positions", today)
        print(f"MinIO backfill indexed: {n} docs")
        context["ti"].xcom_push(key="minio_n", value=n)
    except Exception as e:
        print(f"ERROR in MinIO backfill: {e}")
        context["ti"].xcom_push(key="minio_n", value=-1)
        raise


def log_indexing_summary(**context):
    """
    FIX: Now distinguishes between 0 (no data) and -1 (task failed).
    Logs a warning line for any failed tasks so ops can spot issues at a glance.
    """
    ti = context["ti"]
    summary = {
        "bus_positions":   ti.xcom_pull(task_ids="index_bus_positions",   key="bus_n")    or 0,
        "trip_updates":    ti.xcom_pull(task_ids="index_trip_updates",    key="trips_n")  or 0,
        "train_positions": ti.xcom_pull(task_ids="index_train_positions", key="trains_n") or 0,
        "service_alerts":  ti.xcom_pull(task_ids="index_service_alerts",  key="alerts_n") or 0,
        "delay_events":    ti.xcom_pull(task_ids="index_delay_events",    key="delays_n") or 0,
        "minio_backfill":  ti.xcom_pull(task_ids="index_minio_backfill",  key="minio_n")  or 0,
    }

    failed  = [k for k, v in summary.items() if v == -1]
    success = {k: v for k, v in summary.items() if v >= 0}
    total   = sum(v for v in success.values())

    if failed:
        print(f"FAILED tasks: {failed}")
    print(f"ES Indexing Summary: {success} | total={total}")


with DAG(
    dag_id="dag_es_indexer",
    default_args=default_args,
    description="Kafka → Elasticsearch indexer (every 1 min) for Kibana dashboards",
    schedule_interval=timedelta(minutes=1),    # FIX: was timedelta(seconds=30)
    catchup=False,
    max_active_runs=1,
    tags=["elasticsearch", "kibana", "transit"],
) as dag:

    t_bus    = PythonOperator(task_id="index_bus_positions",   python_callable=index_bus_positions)
    t_trips  = PythonOperator(task_id="index_trip_updates",    python_callable=index_trip_updates)
    t_trains = PythonOperator(task_id="index_train_positions", python_callable=index_train_positions)
    t_alerts = PythonOperator(task_id="index_service_alerts",  python_callable=index_service_alerts)
    t_delays = PythonOperator(task_id="index_delay_events",    python_callable=index_delay_events)
    t_minio  = PythonOperator(task_id="index_minio_backfill",  python_callable=index_minio_backfill)

    t_summary = PythonOperator(
        task_id="log_indexing_summary",
        python_callable=log_indexing_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [t_bus, t_trips, t_trains, t_alerts, t_delays, t_minio] >> t_summary
