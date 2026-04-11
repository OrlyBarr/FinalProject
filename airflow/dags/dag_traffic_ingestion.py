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
    """
    Fetch HERE traffic data for all Israel and publish to Kafka.

    FIX: records = producer.fetch_data() or []
         Prevents TypeError when fetch_data() returns None (e.g. on API error
         that doesn't raise, or when HERE returns an empty response object).

    FIX: xcom_push is now always called (not only inside `if records`)
         so log_traffic_summary never pulls a missing key.

    NOTE: HERE Traffic API returns the same road segments every 5 minutes.
         Consider hashing (segment_id + timestamp) before send_batch to
         deduplicate segments that haven't changed since the last pull.
    """
    import sys
    sys.path.append("/opt/airflow")
    from producers.traffic_producer import TrafficProducer

    producer = TrafficProducer()
    n = 0
    try:
        records = producer.fetch_data() or []   # FIX: guard against None
        if records:
            producer.send_batch(records)
            n = len(records)
            print(f"Traffic: {n} segments published to Kafka")
        else:
            print("Traffic: no records returned from HERE API this cycle")
    finally:
        producer.close()

    context["ti"].xcom_push(key="segments_n", value=n)  # FIX: always push


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