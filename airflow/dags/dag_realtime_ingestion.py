"""
airflow/dags/dag_realtime_ingestion.py
DAG 1: Fetches all GTFS-RT feeds + Railways API → Kafka topics
Schedule: every 2 minutes

תיקונים:
  - start_date נוסף ל-default_args (חובה ב-Airflow, חסר מ-RESILIENT_DEFAULT_ARGS)
  - max_retry_delay מוגבל ל-5 דקות (real-time — לא 30)
  - validate_ingestion מזהיר כשאין נתונים
  - resilient_pipeline נטען בבטחה עם fallback אם לא נגיש
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
sys.path.insert(0, "/opt/airflow")

# ── טעינת RESILIENT_DEFAULT_ARGS עם fallback בטוח ───────────────────────────
# resilient_pipeline.py צריך להיות מועלה ל-/opt/airflow (ראה docker-compose.yml)
try:
    from resilient_pipeline import RESILIENT_DEFAULT_ARGS
except ImportError:
    # Fallback אם הקובץ לא נמצא — ערכי ברירת מחדל סבירים
    RESILIENT_DEFAULT_ARGS = {
        "owner":                     "transit-team",
        "depends_on_past":           False,
        "retries":                   5,
        "retry_delay":               timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay":           timedelta(minutes=5),
        "execution_timeout":         timedelta(seconds=55),
        "on_failure_callback":       None,
    }

default_args = {
    **RESILIENT_DEFAULT_ARGS,
    "start_date":        datetime(2026, 4, 13),  # חובה — Airflow לא יטען DAG בלי זה
    "email_on_failure":  True,
    "execution_timeout": timedelta(seconds=55),  # חייב להיכנס בחלון של 2 דקות
    "max_retry_delay":   timedelta(minutes=5),   # real-time — מקסימום 5 דקות, לא 30
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
        "bus_positions":   ti.xcom_pull(task_ids="fetch_bus_positions",   key="bus_count")   or 0,
        "trip_updates":    ti.xcom_pull(task_ids="fetch_trip_updates",    key="trip_count")  or 0,
        "train_positions": ti.xcom_pull(task_ids="fetch_train_positions", key="train_count") or 0,
        "service_alerts":  ti.xcom_pull(task_ids="fetch_service_alerts",  key="alert_count") or 0,
    }
    total = sum(counts.values())
    print(f"Ingestion summary: {counts} | total={total}")
    if total == 0:
        print("WARNING: No records ingested this cycle — check API/Kafka availability")
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
    schedule_interval=timedelta(minutes=2),    # every 2 min — balanced between freshness and load
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