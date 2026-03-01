"""
airflow/dags/dag_es_indexer.py
DAG 4: Kafka → Elasticsearch real-time indexer
Schedule: every 30 seconds
Reads from all Kafka topics and bulk-upserts into Elasticsearch
so Kibana can visualize live transit data.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.append("/opt/airflow")

import os
os.environ.setdefault("ELASTICSEARCH_HOST",    "http://elasticsearch:9200")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS","kafka:29092")

default_args = {
    "owner":             "transit-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "email_on_failure":  False,
    "retries":           1,
    "retry_delay":       timedelta(seconds=10),
    "execution_timeout": timedelta(seconds=25),
}


def index_bus_positions(**context):
    from storage.es_indexer import index_topic
    n = index_topic("bus_positions", max_msgs=1000, consumer_timeout_ms=5000)
    print(f"ES bus positions indexed: {n}")
    context["ti"].xcom_push(key="bus_n", value=n)


def index_trip_updates(**context):
    from storage.es_indexer import index_topic
    n = index_topic("trip_updates", max_msgs=1000, consumer_timeout_ms=5000)
    print(f"ES trip updates indexed: {n}")
    context["ti"].xcom_push(key="trips_n", value=n)


def index_train_positions(**context):
    from storage.es_indexer import index_topic
    n = index_topic("train_positions", max_msgs=500, consumer_timeout_ms=5000)
    print(f"ES train positions indexed: {n}")
    context["ti"].xcom_push(key="trains_n", value=n)


def index_service_alerts(**context):
    from storage.es_indexer import index_topic
    n = index_topic("service_alerts", max_msgs=200, consumer_timeout_ms=3000)
    print(f"ES service alerts indexed: {n}")
    context["ti"].xcom_push(key="alerts_n", value=n)


def index_delay_events(**context):
    from storage.es_indexer import index_topic
    n = index_topic("delay_events", max_msgs=200, consumer_timeout_ms=3000)
    print(f"ES delay events indexed: {n}")
    context["ti"].xcom_push(key="delays_n", value=n)


def log_indexing_summary(**context):
    ti = context["ti"]
    summary = {
        "bus_positions":   ti.xcom_pull(task_ids="index_bus_positions",  key="bus_n")    or 0,
        "trip_updates":    ti.xcom_pull(task_ids="index_trip_updates",   key="trips_n")  or 0,
        "train_positions": ti.xcom_pull(task_ids="index_train_positions",key="trains_n") or 0,
        "service_alerts":  ti.xcom_pull(task_ids="index_service_alerts", key="alerts_n") or 0,
        "delay_events":    ti.xcom_pull(task_ids="index_delay_events",   key="delays_n") or 0,
    }
    total = sum(summary.values())
    print(f"ES Indexing Summary: {summary} | total={total}")


with DAG(
    dag_id="dag_es_indexer",
    default_args=default_args,
    description="Kafka → Elasticsearch indexer (every 30s) for Kibana dashboards",
    schedule_interval=timedelta(seconds=30),
    catchup=False,
    max_active_runs=1,
    tags=["elasticsearch", "kibana", "transit"],
) as dag:

    t_bus    = PythonOperator(task_id="index_bus_positions",  python_callable=index_bus_positions)
    t_trips  = PythonOperator(task_id="index_trip_updates",   python_callable=index_trip_updates)
    t_trains = PythonOperator(task_id="index_train_positions",python_callable=index_train_positions)
    t_alerts = PythonOperator(task_id="index_service_alerts", python_callable=index_service_alerts)
    t_delays = PythonOperator(task_id="index_delay_events",   python_callable=index_delay_events)

    t_summary = PythonOperator(
        task_id="log_indexing_summary",
        python_callable=log_indexing_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [t_bus, t_trips, t_trains, t_alerts, t_delays] >> t_summary
