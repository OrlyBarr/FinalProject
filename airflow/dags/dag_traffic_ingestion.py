"""
airflow/dags/dag_traffic_ingestion.py
DAG: Fetch HERE Traffic Flow data for all of Israel → Kafka
Schedule: every 5 minutes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":             "transit-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "email_on_failure":  True,
    "retries":           1,
    "retry_delay":       timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=4),
}


def run_traffic_producer(**context):
    """Fetch HERE traffic data for all Israel and publish to Kafka."""
    import sys
    sys.path.append("/opt/airflow")
    from producers.traffic_producer import TrafficProducer

    producer = TrafficProducer()
    try:
        records = producer.fetch_data()
        if records:
            producer.send_batch(records)
        print(f"✅ Traffic: {len(records)} segments published to Kafka")
        context["ti"].xcom_push(key="segments_n", value=len(records))
    finally:
        producer.close()


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

    t_fetch   = PythonOperator(task_id="fetch_traffic",   python_callable=run_traffic_producer)
    t_summary = PythonOperator(task_id="log_summary",     python_callable=log_traffic_summary)

    t_fetch >> t_summary