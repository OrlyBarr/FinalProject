"""
airflow/dags/dag_etl_transform.py
DAG 2: Consume all Kafka topics → Transform → S3 + Redshift
Schedule: every 10 minutes

FIXES:
  - schedule_interval changed from timedelta(seconds=30) to timedelta(minutes=10)
    (docstring said 10 min, code had 30s — this was hammering Redshift 120x/hour)
  - traffic topic key changed from "traffic_data" to "traffic-data" to match
    the Kafka topic name created in kafka-init (hyphen, not underscore)
  - consume_service_alerts refactored to use _consume_topic (removed copy-paste logic)
  - Redshift upsert for service_alerts now uses batch_upsert instead of per-record loop
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


def _consume_topic(topic_key: str, group_id: str, transformer_cls, s3_prefix: str,
                   redshift_table: str, max_msgs: int = 1000,
                   filter_fn=None, upsert_key: str = None):
    """
    Generic consume → raw S3 → transform → processed S3 + Redshift.

    Args:
        topic_key:       Key in KAFKA_TOPICS config (e.g. "bus-positions")
        group_id:        Kafka consumer group ID (must be unique per DAG/topic)
        transformer_cls: Transformer class with a .transform(raw) method
        s3_prefix:       S3 prefix for processed output (raw/ derived automatically)
        redshift_table:  Target Redshift table (schema.table)
        max_msgs:        Max messages to consume per run
        filter_fn:       Optional callable(raw_record) → bool to filter records
        upsert_key:      If set, use batch_upsert with this key instead of bulk_insert

    Flow:
      1. Read from Kafka
      2. Save raw records to S3 raw/
      3. Transform (with optional filter)
      4. Save processed records to S3 processed/
      5. Load to Redshift via bulk_insert or batch_upsert (if configured)
    """
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
        consumer_timeout_ms=5000,
        max_poll_records=500,
    )

    transformer = transformer_cls()

    raw_prefix       = s3_prefix.replace("processed/", "raw/")
    processed_prefix = s3_prefix if s3_prefix.startswith("processed/") else s3_prefix.replace("raw/", "processed/")
    s3_raw           = S3Writer(prefix=raw_prefix)
    s3_processed     = S3Writer(prefix=processed_prefix)

    raw_records       = []
    processed_records = []
    count             = 0

    for msg in consumer:
        if count >= max_msgs:
            break
        raw = msg.value.get("data", {})
        if not raw:
            continue

        # Apply optional record filter (e.g. require alert_id)
        if filter_fn and not filter_fn(raw):
            continue

        raw_record = dict(raw)
        raw_record["_fetched_at"]   = msg.value.get("fetched_at")
        raw_record["_source"]       = msg.value.get("source")
        raw_record["_kafka_offset"] = msg.offset
        raw_records.append(raw_record)

        try:
            transformed = transformer.transform(raw)
            transformed["ingested_at"] = msg.value.get("fetched_at")
            processed_records.append(transformed)
        except Exception as e:
            print(f"Transform error on record {count}: {e}")

        count += 1

    consumer.commit()
    consumer.close()

    if raw_records:
        s3_raw.write_batch(raw_records)
        print(f"Raw: {len(raw_records)} records → s3://{raw_prefix}")

    if processed_records:
        s3_processed.write_batch(processed_records)
        print(f"Processed: {len(processed_records)} records → s3://{processed_prefix}")

        if os.getenv("REDSHIFT_HOST"):
            from warehouse.redshift_writer import RedshiftWriter
            rw = RedshiftWriter()
            if upsert_key:
                # FIX: batch upsert instead of per-record loop
                rw.batch_upsert(redshift_table, processed_records, upsert_key)
            else:
                rw.bulk_insert(redshift_table, processed_records)
            rw.close()

    return len(processed_records)


def consume_bus_positions(**context):
    from etl.transformers import BusPositionTransformer
    n = _consume_topic(
        "bus-positions", "bus-etl-group",
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
        "trip-updates", "trips-etl-group",
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
        "train-positions", "trains-etl-group",
        TrainPositionTransformer,
        "processed/train-positions",
        "transit.fact_train_positions",
    )
    print(f"Train positions processed: {n}")
    context["ti"].xcom_push(key="trains_n", value=n)


def consume_service_alerts(**context):
    """
    FIX: Refactored to use _consume_topic with filter_fn and upsert_key
    instead of duplicating the consumer loop logic.
    """
    from etl.transformers import ServiceAlertTransformer
    n = _consume_topic(
        "service-alerts", "alerts-etl-group",
        ServiceAlertTransformer,
        "processed/service-alerts",
        "transit.fact_service_alerts",
        filter_fn=lambda raw: bool(raw.get("alert_id")),  # skip records without alert_id
        upsert_key="alert_id",                             # FIX: batch upsert, not per-record loop
    )
    print(f"Service alerts processed: {n}")
    context["ti"].xcom_push(key="alerts_n", value=n)


def detect_and_publish_delay_events(**context):
    """
    Read from fact_trip_updates, find new severe delays,
    publish to delay-events Kafka topic for real-time alerting.
    Skipped when REDSHIFT_HOST is not configured.
    """
    from kafka import KafkaProducer
    import json, os

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
            "event_type":    "severe_delay",
            "trip_id":       delay["trip_id"],
            "route":         delay["route_short_name"],
            "stop_id":       delay["stop_id"],
            "delay_minutes": delay["delay_minutes"],
            "severity":      delay["delay_category"],
            "time_period":   delay["time_period"],
            "detected_at":   now_str,
        })

    producer.flush()
    producer.close()
    print(f"Published {len(severe_delays)} delay events to Kafka")
    context["ti"].xcom_push(key="delay_events_n", value=len(severe_delays))


def consume_traffic(**context):
    """
    FIX: topic_key changed from "traffic_data" (underscore) to "traffic-data" (hyphen)
    to match the Kafka topic name defined in kafka-init and config/settings.py.
    """
    from etl.traffic_transformer import TrafficTransformer
    n = _consume_topic(
        "traffic-data", "traffic-etl-group",   # FIX: was "traffic_data"
        TrafficTransformer,
        "processed/traffic-data",
        "transit.fact_traffic_flow",
        max_msgs=5000,
    )
    print(f"Traffic segments processed: {n}")
    context["ti"].xcom_push(key="traffic_n", value=n)


def log_etl_summary(**context):
    ti = context["ti"]
    summary = {
        "bus_positions":   ti.xcom_pull(task_ids="consume_bus_positions",  key="bus_n")          or 0,
        "trip_updates":    ti.xcom_pull(task_ids="consume_trip_updates",   key="trips_n")        or 0,
        "train_positions": ti.xcom_pull(task_ids="consume_train_positions",key="trains_n")       or 0,
        "service_alerts":  ti.xcom_pull(task_ids="consume_service_alerts", key="alerts_n")       or 0,
        "traffic_flow":    ti.xcom_pull(task_ids="consume_traffic",        key="traffic_n")      or 0,
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
    schedule_interval=timedelta(minutes=10),   # FIX: was timedelta(seconds=30)
    catchup=False,
    max_active_runs=1,
    tags=["etl", "transform", "transit"],
) as dag:

    t_traffic = PythonOperator(task_id="consume_traffic",          python_callable=consume_traffic)
    t_bus     = PythonOperator(task_id="consume_bus_positions",    python_callable=consume_bus_positions)
    t_trips   = PythonOperator(task_id="consume_trip_updates",     python_callable=consume_trip_updates)
    t_trains  = PythonOperator(task_id="consume_train_positions",  python_callable=consume_train_positions)
    t_alerts  = PythonOperator(task_id="consume_service_alerts",   python_callable=consume_service_alerts)

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

    [t_bus, t_trips, t_trains, t_alerts, t_traffic] >> t_delays >> t_summary