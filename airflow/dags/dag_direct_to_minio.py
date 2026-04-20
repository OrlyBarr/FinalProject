"""
airflow/dags/dag_direct_to_minio.py

Three DAGs — no Kafka dependency, write directly to israel-transit-lake:

  dag_direct_transit  — buses + trains every 1 minute
  dag_direct_traffic  — HERE traffic every 5 minutes (rate-limit friendly)
  dag_direct_alerts   — stop delays + service alerts every 5 minutes

Note: MOT GTFS-RT feeds are WAF-blocked. Delays/alerts use Open Bus Stride.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
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

_default = {
    **RESILIENT_DEFAULT_ARGS,
    "start_date":        datetime(2026, 4, 13),
    "execution_timeout": timedelta(minutes=4),
}


def _run(only, **context):
    import sys; sys.path.insert(0, "/opt/airflow")
    from storage.direct_to_minio import run
    r = run(only=only)
    if context.get("ti"):
        context["ti"].xcom_push(key=only, value=r.get(only, 0))


# ── DAG 1: Buses + Trains — every 1 minute ────────────────────────────────────
with DAG(
    dag_id="dag_direct_transit",
    default_args=_default,
    description="Buses + Trains → MinIO israel-transit-lake (every 1 min, no Kafka)",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    tags=["minio", "transit", "direct"],
) as dag_transit:

    t_buses  = PythonOperator(task_id="upload_buses",  python_callable=lambda **kw: _run("buses",  **kw))
    t_trains = PythonOperator(task_id="upload_trains", python_callable=lambda **kw: _run("trains", **kw))

    def _summary_transit(**context):
        ti = context["ti"]
        buses  = ti.xcom_pull(task_ids="upload_buses",  key="buses")  or 0
        trains = ti.xcom_pull(task_ids="upload_trains", key="trains") or 0
        print(f"Transit summary: buses={buses} trains={trains}")

    t_summary = PythonOperator(
        task_id="log_summary",
        python_callable=_summary_transit,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    [t_buses, t_trains] >> t_summary


# ── DAG 2: Traffic — every 5 minutes ─────────────────────────────────────────
with DAG(
    dag_id="dag_direct_traffic",
    default_args=_default,
    description="HERE Traffic → MinIO israel-transit-lake (every 5 min, no Kafka)",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["minio", "traffic", "here", "direct"],
) as dag_traffic:

    PythonOperator(
        task_id="upload_traffic",
        python_callable=lambda **kw: _run("traffic", **kw),
    )


# ── DAG 3: Delays + Service Alerts — every 5 minutes ─────────────────────────
with DAG(
    dag_id="dag_direct_alerts",
    default_args=_default,
    description="Stop delays + service alerts → MinIO (every 5 min, Open Bus Stride)",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["minio", "delays", "alerts", "direct"],
) as dag_alerts:

    t_delays = PythonOperator(
        task_id="upload_delays",
        python_callable=lambda **kw: _run("delays", **kw),
    )
    t_alerts = PythonOperator(
        task_id="upload_alerts",
        python_callable=lambda **kw: _run("alerts", **kw),
    )

    def _summary_alerts(**context):
        ti = context["ti"]
        delays = ti.xcom_pull(task_ids="upload_delays", key="delays") or 0
        alerts = ti.xcom_pull(task_ids="upload_alerts", key="alerts") or 0
        print(f"Alerts summary: delays={delays} alerts={alerts}")

    t_summary_alerts = PythonOperator(
        task_id="log_summary",
        python_callable=_summary_alerts,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    [t_delays, t_alerts] >> t_summary_alerts
