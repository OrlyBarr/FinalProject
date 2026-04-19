"""
airflow/dags/dag_gtfs_load.py
==============================
DAG לטעינת GTFS Static מ-MOT ל-PostgreSQL.
רץ פעם ביום בשעה 03:00 (אחרי עדכון MOT).

Tasks:
  1. check_current    — בדיקה אם הנתונים עדכניים (פחות מ-24 שעות)
  2. download_gtfs    — הורדת קובץ ה-ZIP
  3. load_to_postgres — פירוק וטעינה ל-DB
  4. verify_load      — אימות שהנתונים נטענו
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
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
    }

default_args = {
    **RESILIENT_DEFAULT_ARGS,
    "start_date":        datetime(2026, 4, 20),
    "email_on_failure":  True,
    "execution_timeout": timedelta(minutes=30),  # הורדה + פירוק עד 30 דקות
    "max_retry_delay":   timedelta(minutes=30),
}


def check_data_freshness(**context):
    """
    בדוק אם הנתונים עדכניים (פחות מ-23 שעות).
    אם כן — דלג על הטעינה.
    """
    import os
    sys.path.insert(0, "/opt/airflow")
    try:
        from gtfs_query import get_gtfs_summary
        summary = get_gtfs_summary()
        last_load = summary.get("last_load")
        if last_load:
            age = (datetime.now() - last_load).total_seconds() / 3600
            print(f"GTFS last loaded {age:.1f}h ago")
            if age < 23:
                print("Data is fresh — skipping reload")
                return False  # ShortCircuit: דלג
    except Exception as e:
        print(f"Could not check freshness: {e}")
    return True  # המשך לטעינה


def download_and_load(**context):
    """הורד את GTFS וטען ל-PostgreSQL."""
    import io, zipfile, os
    sys.path.insert(0, "/opt/airflow")

    # Import from load_gtfs
    from load_gtfs import (
        get_conn, ensure_schema, download_gtfs,
        load_agency, load_routes, load_stops,
        load_trips, load_stop_times, load_calendar,
        GTFS_URL
    )

    conn = get_conn()
    ensure_schema(conn)

    # נקה נתונים ישנים
    with conn.cursor() as cur:
        for t in ["gtfs.stop_times","gtfs.trips","gtfs.stops",
                  "gtfs.routes","gtfs.agency","gtfs.calendar","gtfs.calendar_dates"]:
            cur.execute(f"TRUNCATE {t} CASCADE")
    conn.commit()

    # הורד
    data = download_gtfs()
    context["ti"].xcom_push(key="zip_size_mb", value=round(len(data)/1024/1024, 1))

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        load_agency(zf, conn)
        routes_n = load_routes(zf, conn)

        with conn.cursor() as cur:
            cur.execute("SELECT route_id FROM gtfs.routes")
            route_ids = {r[0] for r in cur.fetchall()}

        stops_n  = load_stops(zf, conn, bbox_filter=True)

        with conn.cursor() as cur:
            cur.execute("SELECT stop_id FROM gtfs.stops")
            stop_ids = {r[0] for r in cur.fetchall()}

        trips_n = load_trips(zf, conn, route_ids)

        with conn.cursor() as cur:
            cur.execute("SELECT trip_id FROM gtfs.trips")
            trip_ids = {r[0] for r in cur.fetchall()}

        st_n = load_stop_times(zf, conn, trip_ids, stop_ids)
        load_calendar(zf, conn)

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO gtfs.load_status (source_url,routes_cnt,stops_cnt,trips_cnt) VALUES (%s,%s,%s,%s)",
                (GTFS_URL, routes_n, stops_n, trips_n)
            )
        conn.commit()

    context["ti"].xcom_push(key="routes_n", value=routes_n)
    context["ti"].xcom_push(key="stops_n",  value=stops_n)
    context["ti"].xcom_push(key="trips_n",  value=trips_n)
    context["ti"].xcom_push(key="st_n",     value=st_n)

    print(f"✅ Loaded: {routes_n} routes, {stops_n} stops, {trips_n} trips, {st_n} stop_times")
    conn.close()


def verify_load(**context):
    """אמת שהנתונים נטענו."""
    sys.path.insert(0, "/opt/airflow")
    from gtfs_query import get_gtfs_summary, is_available

    if not is_available():
        raise ValueError("GTFS data not available after load!")

    summary = get_gtfs_summary()
    print(f"✅ Verified: {summary}")

    ti = context["ti"]
    routes_n = ti.xcom_pull(task_ids="download_and_load", key="routes_n") or 0
    if routes_n < 100:
        raise ValueError(f"Too few routes loaded: {routes_n}")


with DAG(
    dag_id="dag_gtfs_load",
    default_args=default_args,
    description="Load GTFS Static (MOT) → PostgreSQL — daily at 03:00",
    schedule_interval="0 3 * * *",   # 03:00 כל יום
    catchup=False,
    max_active_runs=1,
    tags=["gtfs", "static", "postgres", "transit"],
) as dag:

    t_check = ShortCircuitOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    t_load = PythonOperator(
        task_id="download_and_load",
        python_callable=download_and_load,
    )

    t_verify = PythonOperator(
        task_id="verify_load",
        python_callable=verify_load,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    t_check >> t_load >> t_verify