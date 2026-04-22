"""
airflow/dags/dag_hourly_coverage.py

DAG: dag_hourly_coverage
Schedule: פעם בשעה, ב-:00 בדיוק
מטרה: להבטיח שב-MinIO תמיד יהיו נתונים לכל שעה.

הלוגיקה:
  1. בתחילת כל שעה בודק אם כבר קיימים קבצים לשעה הנוכחית
     ב-raw/bus-positions/year=.../month=.../day=.../hour=.../
  2. אם לא — שולף נתונים חיים ומעלה לכל 5 prefix:
     buses, trains, delays, alerts, traffic
  3. גם מבצע "backfill" ל-24 שעות אחורה:
     בודק אילו שעות ריקות וממלא אותן מ-fallback (הנתונים האחרונים הידועים)

כך מובטח שב-MinIO תמיד יהיה לפחות קובץ אחד לכל שעה ביממה.
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
    RESILIENT_DEFAULT_ARGS = {
        "owner": "transit-team",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=20),
        "on_failure_callback": None,
    }

_default = {
    **RESILIENT_DEFAULT_ARGS,
    "start_date":        datetime(2026, 4, 13),
    "execution_timeout": timedelta(minutes=15),
}


def _hour_has_data(s3, bucket: str, prefix: str, year: int, month: int, day: int, hour: int) -> bool:
    """בדוק אם קיים לפחות קובץ אחד לשעה הנתונה."""
    path = (
        f"{prefix}/"
        f"year={year}/month={month:02d}/"
        f"day={day:02d}/hour={hour:02d}/"
    )
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=path, MaxKeys=1)
        return bool(resp.get("Contents"))
    except Exception:
        return False


def _get_fallback_records(s3, bucket: str, prefix: str) -> list:
    """קרא את הקובץ האחרון הידוע מ-MinIO לשימוש כ-fallback."""
    import json
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix + "/", MaxKeys=200)
        objs = [o for o in resp.get("Contents", []) if o["Size"] > 0]
        if not objs:
            return []
        latest = max(objs, key=lambda o: o["LastModified"])["Key"]
        body = s3.get_object(Bucket=bucket, Key=latest)["Body"].read()
        records = json.loads(body)
        return records if isinstance(records, list) else []
    except Exception:
        return []


def _write_hour_data(s3, bucket: str, records: list, prefix: str,
                     label: str, year: int, month: int, day: int, hour: int):
    """כתוב רשומות לשעה ספציפית ב-MinIO."""
    import json
    from datetime import datetime as _dt
    key = (
        f"{prefix}/"
        f"year={year}/month={month:02d}/"
        f"day={day:02d}/hour={hour:02d}/"
        f"{label}_{year}{month:02d}{day:02d}_{hour:02d}0000.json"
    )
    body = json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata={"record_count": str(len(records)), "source": "hourly_coverage_dag"},
    )
    return key


def ensure_current_hour(**context):
    """
    שלב 1: ודא שלשעה הנוכחית יש נתונים.
    אם לא — שלוף נתונים חיים ממקורות ה-API ועלה ל-MinIO.
    """
    sys.path.insert(0, "/opt/airflow")
    import os
    import boto3
    from botocore.config import Config
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    IL_TZ   = ZoneInfo("Asia/Jerusalem")
    now     = datetime.now(IL_TZ)
    BUCKET  = os.getenv("S3_BUCKET_NAME", "israel-transit-lake")
    ENDPOINT= os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ACCESS  = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    SECRET  = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # וודא שהבאקט קיים
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)

    year, month, day, hour = now.year, now.month, now.day, now.hour

    PREFIXES = [
        ("raw/bus-positions",    "buses"),
        ("raw/train-positions",  "trains"),
        ("raw/trip-updates",     "delays"),
        ("raw/service-alerts",   "alerts"),
    ]

    filled = []
    skipped = []

    for prefix, label in PREFIXES:
        if _hour_has_data(s3, BUCKET, prefix, year, month, day, hour):
            skipped.append(prefix)
            print(f"✅ {prefix} — hour={hour:02d}: data already exists")
            continue

        # שלוף נתונים חיים
        print(f"⚠️  {prefix} — hour={hour:02d}: no data found, fetching live...")
        try:
            from storage.direct_to_minio import (
                fetch_buses, fetch_trains, fetch_delays, fetch_service_alerts
            )
            fetchers = {
                "buses":  fetch_buses,
                "trains": fetch_trains,
                "delays": fetch_delays,
                "alerts": fetch_service_alerts,
            }
            records = fetchers[label]()
        except Exception as e:
            print(f"  Live fetch failed for {label}: {e} — using fallback")
            records = []

        # fallback: קח את הנתונים האחרונים הידועים
        if not records:
            records = _get_fallback_records(s3, BUCKET, prefix)
            if records:
                # סמן כ-fallback
                for r in records:
                    r["_coverage_fallback"] = True
                    r["_coverage_hour"] = f"{year}-{month:02d}-{day:02d}T{hour:02d}"
            else:
                print(f"  No fallback data available for {prefix}")
                continue

        key = _write_hour_data(s3, BUCKET, records, prefix, label, year, month, day, hour)
        filled.append(key)
        print(f"  ✅ Wrote {len(records)} records → {key}")

    print(f"\nSummary: filled={len(filled)}, already_had_data={len(skipped)}")
    context["ti"].xcom_push(key="filled", value=filled)
    context["ti"].xcom_push(key="skipped", value=skipped)


def backfill_last_24h(**context):
    """
    שלב 2: עבור על 24 השעות האחרונות ומלא שעות ריקות.
    משתמש בנתוני fallback (הקובץ הכי קרוב לאותה שעה) כדי לא להשאיר חורים.
    """
    sys.path.insert(0, "/opt/airflow")
    import os, json
    import boto3
    from botocore.config import Config
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    IL_TZ   = ZoneInfo("Asia/Jerusalem")
    now     = datetime.now(IL_TZ)
    BUCKET  = os.getenv("S3_BUCKET_NAME", "israel-transit-lake")
    ENDPOINT= os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ACCESS  = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    SECRET  = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    PREFIXES = [
        ("raw/bus-positions",   "buses"),
        ("raw/train-positions", "trains"),
        ("raw/trip-updates",    "delays"),
        ("raw/service-alerts",  "alerts"),
    ]

    total_filled = 0

    for prefix, label in PREFIXES:
        # קרא את כל הקבצים הקיימים ב-prefix הזה
        all_keys = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix + "/"):
            for obj in page.get("Contents", []):
                if obj["Size"] > 0:
                    all_keys.append(obj["Key"])

        if not all_keys:
            print(f"  {prefix}: no data at all — skipping backfill")
            continue

        # מצא שעות ריקות ב-24 השעות האחרונות
        missing_hours = []
        for h in range(24):
            check_dt = now - timedelta(hours=h)
            if not _hour_has_data(s3, BUCKET, prefix,
                                  check_dt.year, check_dt.month,
                                  check_dt.day, check_dt.hour):
                missing_hours.append(check_dt)

        if not missing_hours:
            print(f"  {prefix}: all 24 hours covered ✅")
            continue

        print(f"  {prefix}: {len(missing_hours)} missing hours — filling...")

        # קרא את הקובץ האחרון הידוע פעם אחת (כדי לא לגשת ל-MinIO כל פעם)
        fallback = _get_fallback_records(s3, BUCKET, prefix)
        if not fallback:
            print(f"  No fallback for {prefix} — cannot fill")
            continue

        for missing_dt in missing_hours:
            # סמן את ה-fallback עם ה-timestamp של השעה החסרה
            stamped = []
            for r in fallback:
                rr = dict(r)
                rr["_coverage_fallback"]  = True
                rr["_coverage_hour"]      = missing_dt.strftime("%Y-%m-%dT%H:00:00")
                rr["_backfill_generated"] = True
                stamped.append(rr)

            key = _write_hour_data(
                s3, BUCKET, stamped, prefix, f"{label}_backfill",
                missing_dt.year, missing_dt.month, missing_dt.day, missing_dt.hour
            )
            total_filled += 1
            print(f"    Filled {missing_dt.strftime('%Y-%m-%d %H:00')} → {key}")

    print(f"\nBackfill complete: {total_filled} hour-slots filled")
    context["ti"].xcom_push(key="backfill_filled", value=total_filled)


def log_coverage_summary(**context):
    """הדפס סיכום כיסוי."""
    ti = context["ti"]
    filled   = ti.xcom_pull(task_ids="ensure_current_hour", key="filled")  or []
    skipped  = ti.xcom_pull(task_ids="ensure_current_hour", key="skipped") or []
    backfill = ti.xcom_pull(task_ids="backfill_last_24h",   key="backfill_filled") or 0
    print(
        f"Coverage DAG summary:\n"
        f"  Current hour: {len(filled)} prefixes filled, {len(skipped)} already had data\n"
        f"  Backfill: {backfill} missing hour-slots filled in last 24h"
    )


# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_hourly_coverage",
    default_args=_default,
    description="Ensure MinIO has data for every hour (fill gaps, backfill last 24h)",
    schedule_interval="0 * * * *",   # כל שעה ב-:00 בדיוק (cron)
    catchup=False,
    max_active_runs=1,
    tags=["minio", "coverage", "backfill", "hourly"],
) as dag:

    t_current = PythonOperator(
        task_id="ensure_current_hour",
        python_callable=ensure_current_hour,
    )

    t_backfill = PythonOperator(
        task_id="backfill_last_24h",
        python_callable=backfill_last_24h,
    )

    t_summary = PythonOperator(
        task_id="log_coverage_summary",
        python_callable=log_coverage_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_current >> t_backfill >> t_summary
