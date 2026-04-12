"""
airflow/dags/dag_realtime_ingestion.py
DAG 1: Fetches all GTFS-RT feeds + Railways API → Kafka topics
Schedule: every minute (producers internally fetch every 30-60s)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
sys.path.append("/opt/airflow")

default_args = {
    "owner":            "transit-team",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 4, 13),
    "email_on_failure": True,
    "retries":          3,
    "retry_delay":      timedelta(seconds=30),
    "execution_timeout": timedelta(seconds=55),
}


def fetch_bus_positions(**context):
    from producers.bus_positions_producer import BusPositionsProducer
    producer = BusPositionsProducer()
    try:
        producer.run_once()
        context["ti"].xcom_push(key="bus_count", value=producer.stats["sent"])
    finally:
        producer.close()


def fetch_trip_updates(**context):
    from producers.trip_updates_producer import TripUpdatesProducer
    producer = TripUpdatesProducer()
    try:
        producer.run_once()
        context["ti"].xcom_push(key="trip_count", value=producer.stats["sent"])
    finally:
        producer.close()


def fetch_train_positions(**context):
    from producers.train_positions_producer import TrainPositionsProducer
    producer = TrainPositionsProducer()
    try:
        producer.run_once()
        context["ti"].xcom_push(key="train_count", value=producer.stats["sent"])
    finally:
        producer.close()


def fetch_service_alerts(**context):
    from producers.service_alert_producer import ServiceAlertsProducer
    producer = ServiceAlertsProducer()
    try:
        producer.run_once()
        context["ti"].xcom_push(key="alert_count", value=producer.stats["sent"])
    finally:
        producer.close()


def check_kafka_health(**context):
    """
    Advisory Kafka health check — logs warnings but never raises.
    Producers run regardless so a Kafka hiccup does not block all ingestion.
    """
    try:
        from kafka import KafkaAdminClient
        import os
        admin = KafkaAdminClient(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            request_timeout_ms=5000,
        )
        existing = set(admin.list_topics())
        required = {"bus-positions", "train-positions", "trip-updates", "service-alerts", "delay-events"}
        missing  = required - existing
        admin.close()
        if missing:
            print(f"WARNING: Missing Kafka topics: {missing} — producers will still run")
        else:
            print(f"Kafka healthy — topics: {existing}")
    except Exception as e:
        # Do NOT raise — let downstream tasks run and fail individually if needed
        print(f"WARNING: Kafka health check failed ({e}) — proceeding anyway")


def validate_ingestion(**context):
    """Validate that all producers sent data and log summary."""
    ti = context["ti"]
    counts = {
        "bus_positions": ti.xcom_pull(task_ids="fetch_bus_positions",  key="bus_count")   or 0,
        "trip_updates":  ti.xcom_pull(task_ids="fetch_trip_updates",   key="trip_count")  or 0,
        "train_positions": ti.xcom_pull(task_ids="fetch_train_positions", key="train_count") or 0,
        "service_alerts":  ti.xcom_pull(task_ids="fetch_service_alerts",  key="alert_count") or 0,
    }
    total = sum(counts.values())
    print(f"Ingestion summary: {counts} | total={total}")
    return total


def upload_to_minio(**context):
    """Upload latest transit data to MinIO data lake."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from storage.minio_uploader import airflow_upload_task
    return airflow_upload_task(**context)


# ─────────────────────────────────────────
# DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="dag_realtime_ingestion",
    default_args=default_args,
    description="Fetch GTFS-RT + Railways → Kafka (every 2 min)",
    schedule_interval=timedelta(minutes=2),    # was 30s — too aggressive, caused scheduler backlog
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "realtime", "transit", "israel"],
) as dag:

    # Health check is advisory only — runs in parallel with producers, never blocks them
    health = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Parallel fetching — run regardless of health check result
    t_bus    = PythonOperator(task_id="fetch_bus_positions",   python_callable=fetch_bus_positions)
    t_trips  = PythonOperator(task_id="fetch_trip_updates",    python_callable=fetch_trip_updates)
    t_trains = PythonOperator(task_id="fetch_train_positions", python_callable=fetch_train_positions)
    t_alerts = PythonOperator(task_id="fetch_service_alerts",  python_callable=fetch_service_alerts)

    t_validate = PythonOperator(
        task_id="validate_ingestion",
        python_callable=validate_ingestion,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Producers run independently; health check and validate/minio run after all are done
    [t_bus, t_trips, t_trains, t_alerts] >> t_validate >> t_minio
    [t_bus, t_trips, t_trains, t_alerts] >> health