"""
airflow/dags/dag_traffic_to_minio.py
DAG: Fetch HERE Traffic → MinIO TRAFIC-DATA bucket (direct, no Kafka)
Schedule: every 5 minutes

This DAG bypasses Kafka and writes traffic data directly to MinIO.
It runs independently of dag_traffic_ingestion and dag_etl_transform.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":            "transit-team",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),
}


def run_traffic_to_minio(**context):
    import sys
    sys.path.insert(0, "/opt/airflow")
    from storage.traffic_to_minio import traffic_to_minio_task
    return traffic_to_minio_task(**context)


with DAG(
    dag_id="dag_traffic_to_minio",
    default_args=default_args,
    description="HERE Traffic Flow → MinIO TRAFIC-DATA (direct, no Kafka)",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["traffic", "minio", "here"],
) as dag:

    PythonOperator(
        task_id="traffic_to_minio",
        python_callable=run_traffic_to_minio,
    )
