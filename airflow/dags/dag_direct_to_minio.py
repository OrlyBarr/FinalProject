"""
airflow/dags/dag_direct_to_minio.py
DAG: Fetch buses + trains + traffic directly → MinIO israel-transit-lake
Schedule: every minute (no Kafka dependency)

This DAG runs independently of the Kafka-based pipeline and guarantees
that data reaches MinIO even when Kafka or any producer is unhealthy.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner":             "transit-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=4),
}


def run_buses(**context):
    import sys; sys.path.insert(0, "/opt/airflow")
    from storage.direct_to_minio import run
    r = run(only="buses")
    context["ti"].xcom_push(key="buses", value=r.get("buses", 0))


def run_trains(**context):
    import sys; sys.path.insert(0, "/opt/airflow")
    from storage.direct_to_minio import run
    r = run(only="trains")
    context["ti"].xcom_push(key="trains", value=r.get("trains", 0))


def run_traffic(**context):
    import sys; sys.path.insert(0, "/opt/airflow")
    from storage.direct_to_minio import run
    r = run(only="traffic")
    context["ti"].xcom_push(key="traffic", value=r.get("traffic", 0))


def log_summary(**context):
    ti = context["ti"]
    buses   = ti.xcom_pull(task_ids="upload_buses",   key="buses")   or 0
    trains  = ti.xcom_pull(task_ids="upload_trains",  key="trains")  or 0
    traffic = ti.xcom_pull(task_ids="upload_traffic", key="traffic") or 0
    print(f"direct_to_minio summary: buses={buses} trains={trains} traffic={traffic} "
          f"total={buses+trains+traffic}")


with DAG(
    dag_id="dag_direct_to_minio",
    default_args=default_args,
    description="Buses + Trains + Traffic → MinIO israel-transit-lake (no Kafka)",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    tags=["minio", "transit", "direct"],
) as dag:

    t_buses   = PythonOperator(task_id="upload_buses",   python_callable=run_buses)
    t_trains  = PythonOperator(task_id="upload_trains",  python_callable=run_trains)
    t_traffic = PythonOperator(task_id="upload_traffic", python_callable=run_traffic)
    t_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [t_buses, t_trains, t_traffic] >> t_summary
