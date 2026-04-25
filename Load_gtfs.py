#!/usr/bin/env python3
"""
load_gtfs.py
============
מוריד GTFS Static של משרד התחבורה ומטעין ל-PostgreSQL.

שימוש:
  python3 load_gtfs.py              # טעינה מלאה
  python3 load_gtfs.py --check      # בדיקת מצב
  python3 load_gtfs.py --routes     # טעינת routes בלבד (מהיר)

מקור: https://gtfs.mot.gov.il/gtfsfiles/gtfs.zip
טבלאות: gtfs_routes, gtfs_stops, gtfs_trips, gtfs_stop_times, gtfs_agency

הערה: GTFS Static מתעדכן פעם ביום ע"י MOT.
      הקובץ כ-150MB, פירוק ~1-2 דקות.
"""

import sys
import os
import io
import csv
import zipfile
import logging
import argparse
import time
import urllib.request
from datetime import datetime

log = logging.getLogger("load_gtfs")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s"
)

# ── Config ────────────────────────────────────────────────────────────────────
GTFS_URL = "https://gtfs.mot.gov.il/gtfsfiles/gtfs.zip"
GTFS_URL_ALT = "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip"

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB",  "airflow")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")

# גוש דן bounding box — לסינון תחנות
BBOX = {
    "lat_min": 31.9, "lat_max": 32.25,
    "lon_min": 34.7, "lon_max": 35.0,
}

# ── DDL ───────────────────────────────────────────────────────────────────────
DDL = """
CREATE SCHEMA IF NOT EXISTS gtfs;

CREATE TABLE IF NOT EXISTS gtfs.agency (
    agency_id   TEXT PRIMARY KEY,
    agency_name TEXT,
    agency_url  TEXT,
    agency_timezone TEXT,
    agency_lang TEXT,
    agency_phone TEXT
);

CREATE TABLE IF NOT EXISTS gtfs.routes (
    route_id         TEXT PRIMARY KEY,
    agency_id        TEXT,
    route_short_name TEXT,
    route_long_name  TEXT,
    route_desc       TEXT,
    route_type       INTEGER,
    route_color      TEXT,
    route_text_color TEXT
);

CREATE TABLE IF NOT EXISTS gtfs.stops (
    stop_id    TEXT PRIMARY KEY,
    stop_code  TEXT,
    stop_name  TEXT,
    stop_lat   FLOAT,
    stop_lon   FLOAT,
    zone_id    TEXT,
    stop_url   TEXT,
    location_type INTEGER,
    parent_station TEXT
);

CREATE TABLE IF NOT EXISTS gtfs.trips (
    route_id   TEXT,
    service_id TEXT,
    trip_id    TEXT PRIMARY KEY,
    trip_headsign TEXT,
    direction_id INTEGER,
    shape_id   TEXT,
    wheelchair_accessible INTEGER
);

CREATE TABLE IF NOT EXISTS gtfs.stop_times (
    trip_id        TEXT,
    arrival_time   TEXT,
    departure_time TEXT,
    stop_id        TEXT,
    stop_sequence  INTEGER,
    pickup_type    INTEGER,
    drop_off_type  INTEGER,
    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE TABLE IF NOT EXISTS gtfs.calendar (
    service_id TEXT PRIMARY KEY,
    monday INTEGER, tuesday INTEGER, wednesday INTEGER,
    thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER,
    start_date TEXT, end_date TEXT
);

CREATE TABLE IF NOT EXISTS gtfs.calendar_dates (
    service_id     TEXT,
    date           TEXT,
    exception_type INTEGER,
    PRIMARY KEY (service_id, date)
);

-- metadata
CREATE TABLE IF NOT EXISTS gtfs.load_status (
    loaded_at   TIMESTAMP DEFAULT NOW(),
    source_url  TEXT,
    routes_cnt  INTEGER,
    stops_cnt   INTEGER,
    trips_cnt   INTEGER
);

-- indexes
CREATE INDEX IF NOT EXISTS idx_routes_agency   ON gtfs.routes(agency_id);
CREATE INDEX IF NOT EXISTS idx_routes_short    ON gtfs.routes(route_short_name);
CREATE INDEX IF NOT EXISTS idx_stops_name      ON gtfs.stops(stop_name);
CREATE INDEX IF NOT EXISTS idx_stops_geo       ON gtfs.stops(stop_lat, stop_lon);
CREATE INDEX IF NOT EXISTS idx_trips_route     ON gtfs.trips(route_id);
CREATE INDEX IF NOT EXISTS idx_st_stop         ON gtfs.stop_times(stop_id);
CREATE INDEX IF NOT EXISTS idx_st_trip         ON gtfs.stop_times(trip_id);
"""


def get_conn():
    import psycopg2
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        connect_timeout=10,
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    log.info("Schema ready")


def download_gtfs() -> str:
    """
    מוריד את קובץ ה-ZIP של GTFS לקובץ זמני בדיסק (ולא לזיכרון).
    מחזיר נתיב לקובץ. הקורא אחראי למחוק אחרי שימוש.
    """
    import tempfile
    CHUNK = 1024 * 1024  # 1 MB chunks — large enough for fast I/O, small enough to stay safe

    for url in [GTFS_URL, GTFS_URL_ALT]:
        log.info(f"Downloading GTFS from {url} (streaming to disk)...")
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "IsraelTransitBot/1.0",
                    "Accept": "application/zip,*/*",
                },
            )
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip", prefix="gtfs_")
            total = 0
            with urllib.request.urlopen(req, timeout=120) as resp:
                while True:
                    chunk = resp.read(CHUNK)
                    if not chunk:
                        break
                    tmp.write(chunk)
                    total += len(chunk)
            tmp.flush()
            tmp.close()
            log.info(f"Downloaded {total/1024/1024:.1f} MB → {tmp.name}")
            return tmp.name
        except Exception as e:
            log.warning(f"Failed to download from {url}: {e}")
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
    raise RuntimeError("Could not download GTFS from any source")


def load_csv(zf: zipfile.ZipFile, filename: str) -> list[dict]:
    """קורא CSV קטן מתוך ה-ZIP, מחזיר list of dicts. לקבצים גדולים (stop_times) השתמש ב-_stream_csv."""
    if filename not in zf.namelist():
        log.warning(f"{filename} not found in ZIP")
        return []
    with zf.open(filename) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        return list(reader)


def _stream_csv(zf: zipfile.ZipFile, filename: str):
    """גנרטור — מחזיר שורה בכל פעם ללא טעינה לזיכרון (לקבצים גדולים כגון stop_times)."""
    if filename not in zf.namelist():
        log.warning(f"{filename} not found in ZIP")
        return
    with zf.open(filename) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            yield row


def bulk_insert(conn, table: str, rows: list[dict], columns: list[str], batch=5000):
    """הכנסת נתונים בבאצ'ים."""
    if not rows:
        return 0
    from psycopg2.extras import execute_values
    cols_str = ", ".join(columns)
    placeholders = f"INSERT INTO {table} ({cols_str}) VALUES %s ON CONFLICT DO NOTHING"
    count = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch):
            batch_rows = rows[i:i+batch]
            values = []
            for r in batch_rows:
                row = tuple(r.get(c, None) or None for c in columns)
                values.append(row)
            execute_values(cur, placeholders, values)
            count += len(batch_rows)
    conn.commit()
    return count


def load_agency(zf, conn):
    rows = load_csv(zf, "agency.txt")
    cols = ["agency_id","agency_name","agency_url","agency_timezone","agency_lang","agency_phone"]
    n = bulk_insert(conn, "gtfs.agency", rows, cols)
    log.info(f"agency: {n} rows")
    return n


def load_routes(zf, conn, agency_filter=None):
    """טוען routes — עם אופציה לסינון לפי agency."""
    rows = load_csv(zf, "routes.txt")
    if agency_filter:
        rows = [r for r in rows if r.get("agency_id") in agency_filter]
    cols = ["route_id","agency_id","route_short_name","route_long_name",
            "route_desc","route_type","route_color","route_text_color"]
    # המרת route_type לint
    for r in rows:
        try: r["route_type"] = int(r.get("route_type","3"))
        except: r["route_type"] = 3
    n = bulk_insert(conn, "gtfs.routes", rows, cols)
    log.info(f"routes: {n} rows")
    return n


def load_stops(zf, conn, bbox_filter=True):
    """טוען תחנות — עם סינון גיאוגרפי לגוש דן."""
    rows = load_csv(zf, "stops.txt")
    if bbox_filter:
        filtered = []
        for r in rows:
            try:
                lat = float(r.get("stop_lat") or 0)
                lon = float(r.get("stop_lon") or 0)
                if (BBOX["lat_min"] <= lat <= BBOX["lat_max"] and
                    BBOX["lon_min"] <= lon <= BBOX["lon_max"]):
                    filtered.append(r)
            except:
                pass
        log.info(f"Stops: {len(rows)} total → {len(filtered)} in Gush Dan")
        rows = filtered
    cols = ["stop_id","stop_code","stop_name","stop_lat","stop_lon",
            "zone_id","stop_url","location_type","parent_station"]
    for r in rows:
        try: r["stop_lat"] = float(r.get("stop_lat") or 0)
        except: r["stop_lat"] = None
        try: r["stop_lon"] = float(r.get("stop_lon") or 0)
        except: r["stop_lon"] = None
        try: r["location_type"] = int(r.get("location_type") or 0)
        except: r["location_type"] = 0
    n = bulk_insert(conn, "gtfs.stops", rows, cols)
    log.info(f"stops: {n} rows")
    return n


def load_trips(zf, conn, route_ids: set = None):
    """טוען נסיעות — מסונן לפי routes שנטענו."""
    rows = load_csv(zf, "trips.txt")
    if route_ids:
        rows = [r for r in rows if r.get("route_id") in route_ids]
    cols = ["route_id","service_id","trip_id","trip_headsign",
            "direction_id","shape_id","wheelchair_accessible"]
    for r in rows:
        try: r["direction_id"] = int(r.get("direction_id") or 0)
        except: r["direction_id"] = None
        try: r["wheelchair_accessible"] = int(r.get("wheelchair_accessible") or 0)
        except: r["wheelchair_accessible"] = None
    n = bulk_insert(conn, "gtfs.trips", rows, cols)
    log.info(f"trips: {n} rows")
    return n


def load_stop_times(zf, conn, trip_ids: set, stop_ids: set, batch_size: int = 5000):
    """
    טוען stop_times בסטרימינג — שורה בכל פעם, לא טוען 5M שורות לזיכרון.
    ללא תיקון זה, load_csv טוענת ~4GB RAM ומקרסת את ה-VM.
    """
    from psycopg2.extras import execute_values
    log.info("Loading stop_times via streaming (prevents OOM)...")
    cols = ["trip_id","arrival_time","departure_time","stop_id",
            "stop_sequence","pickup_type","drop_off_type"]
    cols_str = ", ".join(cols)
    sql = f"INSERT INTO gtfs.stop_times ({cols_str}) VALUES %s ON CONFLICT DO NOTHING"

    batch   = []
    total   = 0
    skipped = 0

    def _flush(cur, b):
        if not b:
            return
        execute_values(cur, sql, b)

    with conn.cursor() as cur:
        for row in _stream_csv(zf, "stop_times.txt"):
            total += 1
            if total % 100000 == 0:
                log.info(f"  stop_times progress: {total:,} scanned, {total-skipped:,} loaded")
            if row.get("trip_id") not in trip_ids or row.get("stop_id") not in stop_ids:
                skipped += 1
                continue
            try:
                batch.append((
                    row.get("trip_id"),
                    row.get("arrival_time"),
                    row.get("departure_time"),
                    row.get("stop_id"),
                    int(row.get("stop_sequence") or 0),
                    int(row.get("pickup_type")   or 0),
                    int(row.get("drop_off_type") or 0),
                ))
            except (ValueError, TypeError):
                skipped += 1
                continue

            if len(batch) >= batch_size:
                _flush(cur, batch)
                batch.clear()
                conn.commit()

        _flush(cur, batch)
        conn.commit()

    loaded = total - skipped
    log.info(f"stop_times: {total:,} scanned → {loaded:,} loaded ({skipped:,} skipped)")
    return loaded


def load_calendar(zf, conn):
    for fname, table, cols in [
        ("calendar.txt", "gtfs.calendar",
         ["service_id","monday","tuesday","wednesday","thursday",
          "friday","saturday","sunday","start_date","end_date"]),
        ("calendar_dates.txt", "gtfs.calendar_dates",
         ["service_id","date","exception_type"]),
    ]:
        rows = load_csv(zf, fname)
        if rows:
            n = bulk_insert(conn, table, rows, cols)
            log.info(f"{fname}: {n} rows")


def check_status(conn):
    """מציג מצב הנתונים."""
    with conn.cursor() as cur:
        for table in ["gtfs.agency","gtfs.routes","gtfs.stops","gtfs.trips","gtfs.stop_times"]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"  {table}: {count:,} rows")
            except Exception as e:
                print(f"  {table}: ERROR — {e}")

        try:
            cur.execute("SELECT loaded_at, routes_cnt, stops_cnt FROM gtfs.load_status ORDER BY loaded_at DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                print(f"\n  Last load: {row[0]} | routes={row[1]} stops={row[2]}")
        except:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check",  action="store_true", help="בדיקת מצב בלבד")
    parser.add_argument("--routes", action="store_true", help="טעינת routes בלבד")
    parser.add_argument("--no-stop-times", action="store_true", help="דלג על stop_times")
    args = parser.parse_args()

    conn = get_conn()
    log.info(f"Connected to PostgreSQL {DB_HOST}:{DB_PORT}/{DB_NAME}")

    ensure_schema(conn)

    if args.check:
        print("\n📊 GTFS Data Status:")
        check_status(conn)
        conn.close()
        return

    t0 = time.time()

    # נקה טבלאות קיימות
    log.info("Clearing existing data...")
    with conn.cursor() as cur:
        for t in ["gtfs.stop_times","gtfs.trips","gtfs.stops",
                  "gtfs.routes","gtfs.agency","gtfs.calendar","gtfs.calendar_dates"]:
            cur.execute(f"TRUNCATE {t} CASCADE")
    conn.commit()

    # הורד GTFS לדיסק (לא לזיכרון — הקובץ 150MB+)
    gtfs_path = download_gtfs()
    try:
        with zipfile.ZipFile(gtfs_path) as zf:
            log.info(f"ZIP contents: {zf.namelist()}")

            # טען agency
            load_agency(zf, conn)

            # טען routes
            routes_count = load_routes(zf, conn)

            if args.routes:
                log.info("--routes flag: skipping stops, trips, stop_times")
            else:
                # קבל route_ids שנטענו
                with conn.cursor() as cur:
                    cur.execute("SELECT route_id FROM gtfs.routes")
                    route_ids = {r[0] for r in cur.fetchall()}

                # טען stops (סינון גיאוגרפי לגוש דן)
                stops_count = load_stops(zf, conn, bbox_filter=True)

                # קבל stop_ids שנטענו
                with conn.cursor() as cur:
                    cur.execute("SELECT stop_id FROM gtfs.stops")
                    stop_ids = {r[0] for r in cur.fetchall()}

                # טען trips
                trips_count = load_trips(zf, conn, route_ids)

                # קבל trip_ids
                with conn.cursor() as cur:
                    cur.execute("SELECT trip_id FROM gtfs.trips")
                    trip_ids = {r[0] for r in cur.fetchall()}

                # טען stop_times (הכבד ביותר)
                if not args.no_stop_times:
                    load_stop_times(zf, conn, trip_ids, stop_ids)

                # טען calendar
                load_calendar(zf, conn)

                # שמור סטטוס
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO gtfs.load_status (source_url, routes_cnt, stops_cnt, trips_cnt) VALUES (%s,%s,%s,%s)",
                        (GTFS_URL, routes_count, stops_count, trips_count)
                    )
                conn.commit()
    finally:
        try: os.unlink(gtfs_path)
        except Exception: pass

    # מחק קובץ זמני
    try:
        os.unlink(gtfs_path)
    except Exception:
        pass

    elapsed = time.time() - t0
    log.info(f"✅ GTFS load complete in {elapsed:.1f}s")
    print(f"\n✅ GTFS loaded in {elapsed:.1f}s")
    check_status(conn)
    conn.close()


if __name__ == "__main__":
    main()