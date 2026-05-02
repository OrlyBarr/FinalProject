"""
airflow/dags/dag_ETL_transform.py
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
    "execution_timeout": timedelta(minutes=9),
}


def _consume_topic(topic_key: str, group_id: str, transformer_cls, s3_prefix: str,
                   redshift_table: str, max_msgs: int = 1000,
                   filter_fn=None, upsert_key: str = None):
    """
    Generic consume → raw S3 → transform → processed S3 + Redshift.

    Args:
        topic_key:       Kafka topic name directly (e.g. "bus-positions", "traffic-data")
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

    FIX: RedshiftWriter is now created once per _consume_topic call (outside _flush_chunk)
         instead of once per 500-record chunk. Each instantiation opens a TCP+SSL+auth
         connection to Redshift — creating it inside the flush loop was opening/closing
         up to 6 connections per topic per ETL run (2000 records / 500 chunk = 4 flushes).
    """
    from kafka import KafkaConsumer
    from storage.s3_writer import S3Writer
    import json, os

    CHUNK_SIZE = 500   # flush to S3 every N records to avoid OOM on large topics

    consumer = KafkaConsumer(
        topic_key,   # topic_key IS the Kafka topic name (e.g. "bus-positions", "traffic-data")
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

    # FIX #1: Create RedshiftWriter ONCE here, outside _flush_chunk.
    # Previously it was instantiated inside _flush_chunk, opening a new Redshift
    # TCP+SSL connection every 500 records — expensive and unnecessary.
    rw = None
    if os.getenv("REDSHIFT_HOST"):
        from warehouse.redshift_writer import RedshiftWriter
        rw = RedshiftWriter()

    raw_records       = []
    processed_records = []
    count             = 0
    total_processed   = 0

    def _flush_chunk():
        """Write current buffers to S3 and clear them."""
        nonlocal total_processed
        if raw_records:
            s3_raw.write_batch(raw_records)
            raw_records.clear()
        if processed_records:
            s3_processed.write_batch(processed_records)
            total_processed += len(processed_records)
            if rw is not None:
                if upsert_key:
                    rw.batch_upsert(redshift_table, processed_records, upsert_key)
                else:
                    rw.bulk_insert(redshift_table, processed_records)
            processed_records.clear()

    try:
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

            # Flush every CHUNK_SIZE records to prevent OOM
            if count % CHUNK_SIZE == 0:
                _flush_chunk()

        consumer.commit()
        consumer.close()

        # Final flush for remaining records
        _flush_chunk()
    finally:
        # FIX #1 (cont): Always close the single shared Redshift connection
        if rw is not None:
            rw.close()

    print(f"Raw: flushed via chunks → s3://{raw_prefix}")
    print(f"Processed: {total_processed} records → s3://{processed_prefix}")

    return total_processed


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
    Detect severe delay events from the trip-updates Kafka topic and:
      1. Write them to MinIO raw/delay-events/ and processed/delay-events/
      2. Publish to the delay-events Kafka topic for real-time alerting + ES indexing

    Uses a dedicated consumer group (delay-detect-group) so it reads trip-updates
    independently of the ETL consumer (trips-etl-group) without interfering with it.

    DOES NOT require REDSHIFT — works with Kafka + MinIO only.
    Threshold: delay_minutes >= 5 (minor delays excluded from delay-events).
    """
    from kafka import KafkaConsumer, KafkaProducer
    from etl.transformers import TripUpdateTransformer
    from storage.s3_writer import S3Writer
    import json, os
    from datetime import datetime, timezone
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    DELAY_THRESHOLD_MIN = 5  # only flag delays >= 5 minutes as events

    consumer = KafkaConsumer(
        "trip-updates",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        group_id="delay-detect-group",   # dedicated group — independent of trips-etl-group
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        max_poll_records=500,
    )

    transformer  = TripUpdateTransformer()
    s3_raw       = S3Writer(prefix="raw/delay-events")
    s3_processed = S3Writer(prefix="processed/delay-events")

    # Create KafkaProducer up-front so _flush_delay_chunk can use it
    kp = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    CHUNK_SIZE       = 500   # flush every N events to avoid OOM
    raw_chunk        = []
    proc_chunk       = []
    total_raw        = 0
    total_proc       = 0

    def _flush_delay_chunk():
        nonlocal total_raw, total_proc
        if raw_chunk:
            s3_raw.write_batch(raw_chunk)
            total_raw += len(raw_chunk)
            raw_chunk.clear()
        if proc_chunk:
            s3_processed.write_batch(proc_chunk)
            # Publish this batch to Kafka delay-events topic
            for event in proc_chunk:
                kp.send("delay-events", value={
                    "source":     "delay-detector",
                    "fetched_at": event["detected_at"],
                    "data":       event,
                })
            kp.flush()
            total_proc += len(proc_chunk)
            proc_chunk.clear()

    for msg in consumer:
        if total_raw + len(raw_chunk) >= 5000:
            break
        raw = msg.value.get("data", {})
        if not raw:
            continue
        try:
            transformed = transformer.transform(raw)
            if transformed.get("delay_minutes", 0) >= DELAY_THRESHOLD_MIN:
                raw_rec = dict(raw)
                raw_rec["_fetched_at"]   = msg.value.get("fetched_at")
                raw_rec["_source"]       = msg.value.get("source")
                raw_rec["_kafka_offset"] = msg.offset
                raw_chunk.append(raw_rec)

                transformed["detected_at"] = datetime.now(ZoneInfo("Asia/Jerusalem")).isoformat()
                proc_chunk.append(transformed)

                if len(raw_chunk) >= CHUNK_SIZE:
                    _flush_delay_chunk()
        except Exception as e:
            print(f"Delay transform error: {e}")

    consumer.commit()
    consumer.close()

    # Final flush
    _flush_delay_chunk()
    kp.close()

    print(f"Detected {total_proc} delay events (threshold: {DELAY_THRESHOLD_MIN} min) — flushed in chunks")
    context["ti"].xcom_push(key="delay_events_n", value=total_proc)


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
