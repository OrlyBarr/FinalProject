"""
airflow/dags/dag_traffic_ingestion.py
DAG 5: Fetch HERE Traffic Flow data for all of Israel → Kafka
Schedule: every 5 minutes

FIXES:
  - records = producer.fetch_data() or [] guards against None return,
    which would cause len(records) to raise TypeError
  - xcom_push moved outside the if-block so segments_n is always set
    (log_summary was pulling a key that wasn't pushed on empty results)
  - Added note: traffic-data topic must exist in kafka-init (see docker-compose)

NOTE FOR docker-compose kafka-init:
  Add this line to the kafka-init command to create the missing topic:
    kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 \
      --partitions 2 --replication-factor 1 --topic traffic-data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.insert(0, "/opt/airflow")
try:
    from resilient_pipeline import RESILIENT_DEFAULT_ARGS
except ImportError:
    from datetime import timedelta
    RESILIENT_DEFAULT_ARGS = {
        "owner": "transit-team",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": None,
    }

default_args = {
    **RESILIENT_DEFAULT_ARGS,
    "start_date":        datetime(2026, 4, 13),
    "email_on_failure":  False,  # OPT: no SMTP configured — errors would cascade
    "execution_timeout": timedelta(minutes=4),
}


def run_traffic_producer(**context):
    """
    Fetch HERE traffic data for all Israel and publish to Kafka.
    Gracefully handles Kafka connectivity failures — task succeeds with n=0
    instead of crashing so the scheduler backlog does not grow.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")
    from producers.traffic_producer import TrafficProducer

    producer = TrafficProducer()
    n = 0
    try:
        records = producer.fetch_data() or []
        if records:
            try:
                producer.send_batch(records)
                n = len(records)
                print(f"Traffic: {n} segments published to Kafka")
            except Exception as kafka_err:
                # Kafka publish failed — log but do not fail the task
                # Data is still being written to MinIO via dag_direct_traffic
                print(f"WARNING: Kafka publish failed ({kafka_err}) — data available via dag_direct_traffic")
                n = 0
        else:
            print("Traffic: no records returned from HERE API this cycle")
    except Exception as e:
        print(f"WARNING: Traffic producer error ({e})")
    finally:
        try:
            producer.close()
        except Exception:
            pass

    context["ti"].xcom_push(key="segments_n", value=n)


def log_traffic_summary(**context):
    ti = context["ti"]
    n  = ti.xcom_pull(task_ids="fetch_traffic", key="segments_n") or 0
    print(f"Traffic ingestion summary: {n} segments fetched from HERE API")


with DAG(
    dag_id="dag_traffic_ingestion",
    default_args=default_args,
    description="Fetch HERE Traffic Flow → Kafka (every 5 min)",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["traffic", "here", "ingestion"],
) as dag:

    t_fetch   = PythonOperator(task_id="fetch_traffic", python_callable=run_traffic_producer)
    t_summary = PythonOperator(task_id="log_summary",   python_callable=log_traffic_summary)

    t_fetch >> t_summary