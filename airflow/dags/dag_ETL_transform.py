"""
airflow/dags/dag_etl_transform.py
DAG 2: Consume all Kafka topics → Transform → S3 + Redshift
Schedule: every 10 minutes
"""

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
sys.path.append("/opt/airflow")

default_args = {
    "owner":            "transit-team",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 1, 1),
    "email_on_failure": True,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=9),
}


def _consume_topic(topic_key: str, group_id: str, transformer_cls, s3_prefix: str, redshift_table: str, max_msgs: int = 1000):
    """Generic consume → transform → load function."""
    from kafka import KafkaConsumer
    from config.settings import KAFKA_TOPICS
    from storage.s3_writer import S3Writer
    import json, os

    consumer = KafkaConsumer(
        KAFKA_TOPICS[topic_key],
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,    # 5s max wait — keeps tasks within 30s schedule
        max_poll_records=500,
    )

    transformer     = transformer_cls()
    s3_writer       = S3Writer(prefix=s3_prefix)

    records  = []
    count    = 0

    for msg in consumer:
        if count >= max_msgs:
            break
        raw = msg.value.get("data", {})
        if not raw:
            continue
        try:
            transformed = transformer.transform(raw)
            transformed["ingested_at"] = msg.value.get("fetched_at")
            records.append(transformed)
            count += 1
        except Exception as e:
            print(f"Transform error: {e}")

    consumer.commit()
    consumer.close()

    if records:
        s3_writer.write_batch(records)
        if os.getenv("REDSHIFT_HOST"):   # skip Redshift when not configured
            from warehouse.redshift_writer import RedshiftWriter
            redshift_writer = RedshiftWriter()
            redshift_writer.bulk_insert(redshift_table, records)
            redshift_writer.close()

    return len(records)


def consume_bus_positions(**context):
    from etl.transformers import BusPositionTransformer
    n = _consume_topic(
        "bus_positions", "bus-etl-group",
        BusPositionTransformer,
        "processed/bus-positions",
        "transit.fact_bus_positions",
        max_msgs=2000,
    )
    print(f"Bus positions processed: {n}")
    context["ti"].xcom_push(key="bus_n", value=n)


def consume_trip_updates(**context):
    from etl.transformers import TripUpdateTransformer
    n = _consume_topic(
        "trip_updates", "trips-etl-group",
        TripUpdateTransformer,
        "processed/trip-updates",
        "transit.fact_trip_updates",
        max_msgs=3000,
    )
    print(f"Trip updates processed: {n}")
    context["ti"].xcom_push(key="trips_n", value=n)


def consume_train_positions(**context):
    from etl.transformers import TrainPositionTransformer
    n = _consume_topic(
        "train_positions", "trains-etl-group",
        TrainPositionTransformer,
        "processed/train-positions",
        "transit.fact_train_positions",
    )
    print(f"Train positions processed: {n}")
    context["ti"].xcom_push(key="trains_n", value=n)


def consume_service_alerts(**context):
    from etl.transformers import ServiceAlertTransformer
    from kafka import KafkaConsumer
    from config.settings import KAFKA_TOPICS
    from storage.s3_writer import S3Writer
    import json, os

    consumer = KafkaConsumer(
        KAFKA_TOPICS["service_alerts"],
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        group_id="alerts-etl-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,    # 5s max wait
    )
    transformer     = ServiceAlertTransformer()
    s3_writer       = S3Writer(prefix="processed/service-alerts")

    records = []
    for msg in consumer:
        raw = msg.value.get("data", {})
        if not raw.get("alert_id"):
            continue
        transformed = transformer.transform(raw)
        transformed["ingested_at"] = msg.value.get("fetched_at")
        records.append(transformed)

    consumer.commit()
    consumer.close()

    if records:
        s3_writer.write_batch(records)
        if os.getenv("REDSHIFT_HOST"):   # skip Redshift when not configured
            from warehouse.redshift_writer import RedshiftWriter
            redshift_writer = RedshiftWriter()
            for r in records:
                redshift_writer.upsert("transit.fact_service_alerts", r, "alert_id")
            redshift_writer.close()

    print(f"Service alerts processed: {len(records)}")
    context["ti"].xcom_push(key="alerts_n", value=len(records))


def detect_and_publish_delay_events(**context):
    """
    Read from fact_trip_updates, find new severe delays,
    publish to delay-events Kafka topic for real-time alerting.
    Skipped when REDSHIFT_HOST is not configured.
    """
    from kafka import KafkaProducer
    import json, os
    from datetime import timezone

    if not os.getenv("REDSHIFT_HOST"):
        print("REDSHIFT_HOST not set — skipping delay detection (requires Redshift)")
        return

    from warehouse.redshift_writer import RedshiftWriter
    rw = RedshiftWriter()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    severe_delays = rw.execute_query(f"""
        SELECT
            trip_id,
            route_short_name,
            stop_id,
            delay_minutes,
            delay_category,
            time_period,
            operator_name,
            processed_at
        FROM transit.fact_trip_updates
        WHERE delay_category IN ('severe', 'critical')
          AND processed_at >= DATEADD(minute, -10, GETDATE())
        ORDER BY delay_minutes DESC
        LIMIT 100;
    """)
    rw.close()

    if not severe_delays:
        print("No severe delays detected")
        return

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    for delay in severe_delays:
        producer.send("delay-events", value={
            "event_type":     "severe_delay",
            "trip_id":        delay["trip_id"],
            "route":          delay["route_short_name"],
            "stop_id":        delay["stop_id"],
            "delay_minutes":  delay["delay_minutes"],
            "severity":       delay["delay_category"],
            "time_period":    delay["time_period"],
            "detected_at":    now_str,
        })

    producer.flush()
    producer.close()
    print(f"Published {len(severe_delays)} delay events to Kafka")
    context["ti"].xcom_push(key="delay_events_n", value=len(severe_delays))


def log_etl_summary(**context):
    ti = context["ti"]
    summary = {
        "bus_positions":   ti.xcom_pull(task_ids="consume_bus_positions",  key="bus_n")    or 0,
        "trip_updates":    ti.xcom_pull(task_ids="consume_trip_updates",   key="trips_n")  or 0,
        "train_positions": ti.xcom_pull(task_ids="consume_train_positions",key="trains_n") or 0,
        "service_alerts":  ti.xcom_pull(task_ids="consume_service_alerts", key="alerts_n") or 0,
        "delay_events":    ti.xcom_pull(task_ids="detect_delay_events",    key="delay_events_n") or 0,
    }
    total = sum(summary.values())
    print(f"ETL Summary: {summary} | total={total}")


# ─────────────────────────────────────────
# DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="dag_etl_transform",
    default_args=default_args,
    description="Consume Kafka → Transform → S3 + Redshift (every 10 min)",
    schedule_interval=timedelta(seconds=30),   # every 30 seconds
    catchup=False,
    max_active_runs=1,
    tags=["etl", "transform", "transit"],
) as dag:

    t_bus    = PythonOperator(task_id="consume_bus_positions",   python_callable=consume_bus_positions)
    t_trips  = PythonOperator(task_id="consume_trip_updates",    python_callable=consume_trip_updates)
    t_trains = PythonOperator(task_id="consume_train_positions", python_callable=consume_train_positions)
    t_alerts = PythonOperator(task_id="consume_service_alerts",  python_callable=consume_service_alerts)

    t_delays = PythonOperator(
        task_id="detect_delay_events",
        python_callable=detect_and_publish_delay_events,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_summary = PythonOperator(
        task_id="log_etl_summary",
        python_callable=log_etl_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [t_bus, t_trips, t_trains, t_alerts] >> t_delays >> t_summary