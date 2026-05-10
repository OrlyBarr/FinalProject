#!/usr/bin/env python3
"""
scripts/load_gtfs_from_stride.py
טוען נתוני GTFS (routes + stops) ישירות מה-Stride API של Hasadna,
ללא צורך בהורדת קובץ ZIP מאתר משרד התחבורה (שחסום לגישה ישירה מהשרת).

מטעין:
  - gtfs.agency   — מפעילים
  - gtfs.routes   — קווים (route_short_name, route_long_name, operator)
  - gtfs.stops    — תחנות (stop_id, stop_name, lat, lon, city)

מריץ מהמסוף:
  python3 scripts/load_gtfs_from_stride.py
  python3 scripts/load_gtfs_from_stride.py --check

דרישות:
  POSTGRES_HOST=localhost POSTGRES_PORT=15432 (ברירת מחדל)
"""

import argparse
import logging
import os
import sys
import time

import psycopg2
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [load_gtfs_stride] %(levelname)s %(message)s",
)
log = logging.getLogger("load_gtfs_stride")

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("POSTGRES_HOST",     "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "15432"))
DB_NAME = os.getenv("POSTGRES_DB",       "airflow")
DB_USER = os.getenv("POSTGRES_USER",     "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")

# ── Stride API ─────────────────────────────────────────────────────────────────
STRIDE_BASE = "https://open-bus-stride-api.hasadna.org.il"
PAGE_SIZE   = 1000   # records per API page
MAX_PAGES   = 300    # safety cap (300k routes / 300k stops max)

# ── גוש דן + קצת מסביב ─────────────────────────────────────────────────────────
# לא מסנן תחנות לאזור בלבד — טוענים הכל כדי שה-/gtfs/nearby יעבוד
STOP_LAT_MIN = 31.5
STOP_LAT_MAX = 32.5
STOP_LON_MIN = 34.5
STOP_LON_MAX = 35.2

# מפעילים ישראלים (operator_ref → שם)
OPERATORS = {
    2: "רכבת ישראל", 3: "דן", 5: "אגד", 6: "NTA מטרופולין",
    14: "מטרופולין", 15: "אגד תעבורה", 16: "תנופה", 18: "סופרבוס",
    21: "קווים", 25: "נתיב אקספרס", 32: "V-Line", 42: "אפיקים",
}


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS gtfs")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gtfs.agency (
                agency_id   TEXT PRIMARY KEY,
                agency_name TEXT NOT NULL
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gtfs.routes (
                route_id         TEXT PRIMARY KEY,
                agency_id        TEXT,
                route_short_name TEXT,
                route_long_name  TEXT,
                route_type       INTEGER DEFAULT 3
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gtfs.stops (
                stop_id   TEXT PRIMARY KEY,
                stop_name TEXT,
                stop_lat  DOUBLE PRECISION,
                stop_lon  DOUBLE PRECISION,
                city      TEXT
            )""")
        # stops — spatial index for nearby queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stops_lat_lon
            ON gtfs.stops (stop_lat, stop_lon)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gtfs.trips (
                trip_id    TEXT PRIMARY KEY,
                route_id   TEXT,
                service_id TEXT
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gtfs.stop_times (
                trip_id        TEXT,
                arrival_time   TEXT,
                departure_time TEXT,
                stop_id        TEXT,
                stop_sequence  INTEGER
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gtfs.calendar (
                service_id TEXT PRIMARY KEY,
                monday INTEGER, tuesday INTEGER, wednesday INTEGER,
                thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER,
                start_date TEXT, end_date TEXT
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gtfs.calendar_dates (
                service_id     TEXT,
                date           TEXT,
                exception_type INTEGER
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gtfs.load_status (
                loaded_at  TIMESTAMPTZ DEFAULT NOW(),
                source_url TEXT,
                routes_cnt INTEGER,
                stops_cnt  INTEGER,
                trips_cnt  INTEGER
            )""")
    conn.commit()
    log.info("Schema ready")


def stride_pages(endpoint: str, params: dict):
    """Generator — yields records page by page from Stride."""
    session = requests.Session()
    session.headers["User-Agent"] = "IsraelTransitBot/2.0"
    offset = 0
    total_yielded = 0
    for _ in range(MAX_PAGES):
        p = {**params, "limit": PAGE_SIZE, "offset": offset}
        for attempt in range(3):
            try:
                r = session.get(f"{STRIDE_BASE}{endpoint}", params=p, timeout=30)
                r.raise_for_status()
                break
            except Exception as e:
                if attempt == 2:
                    log.error(f"Stride {endpoint} failed after 3 attempts: {e}")
                    return
                time.sleep(3 * (attempt + 1))
        data = r.json()
        if not data:
            break
        for rec in data:
            yield rec
            total_yielded += 1
        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.1)   # polite rate limit
    log.info(f"  {endpoint}: {total_yielded} records fetched")


def load_agencies(conn) -> int:
    log.info("Loading agencies...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE gtfs.agency CASCADE")
        for op_id, op_name in OPERATORS.items():
            cur.execute(
                "INSERT INTO gtfs.agency (agency_id, agency_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (str(op_id), op_name),
            )
    conn.commit()
    log.info(f"  Loaded {len(OPERATORS)} agencies")
    return len(OPERATORS)


def load_routes(conn) -> int:
    log.info("Loading routes from Stride gtfs_routes...")
    seen_routes: set = set()

    with conn.cursor() as cur:
        cur.execute("TRUNCATE gtfs.routes CASCADE")

    inserted = 0
    batch = []

    for rec in stride_pages("/gtfs_routes/list", {"date": ""}):
        route_id = str(rec.get("id", ""))
        if not route_id or route_id in seen_routes:
            continue
        seen_routes.add(route_id)

        op_ref  = rec.get("operator_ref")
        rsn     = rec.get("route_short_name") or str(rec.get("line_ref", ""))
        rln     = rec.get("route_long_name") or ""
        rtype   = int(rec.get("route_type") or 3)

        batch.append((route_id, str(op_ref) if op_ref else None, rsn, rln, rtype))

        if len(batch) >= 500:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO gtfs.routes
                         (route_id, agency_id, route_short_name, route_long_name, route_type)
                       VALUES %s ON CONFLICT DO NOTHING""",
                    batch,
                )
            conn.commit()
            inserted += len(batch)
            batch = []
            log.info(f"  routes: {inserted} so far...")

    if batch:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO gtfs.routes
                     (route_id, agency_id, route_short_name, route_long_name, route_type)
                   VALUES %s ON CONFLICT DO NOTHING""",
                batch,
            )
        conn.commit()
        inserted += len(batch)

    log.info(f"  Loaded {inserted} routes")
    return inserted


def load_stops(conn) -> int:
    log.info("Loading stops from Stride gtfs_stops (Israel bbox)...")
    seen: set = set()

    with conn.cursor() as cur:
        cur.execute("TRUNCATE gtfs.stops CASCADE")

    inserted = 0
    batch = []

    params = {
        "lat__greater_or_equal": STOP_LAT_MIN,
        "lat__lower_or_equal":   STOP_LAT_MAX,
        "lon__greater_or_equal": STOP_LON_MIN,
        "lon__lower_or_equal":   STOP_LON_MAX,
        "date": "",
    }

    for rec in stride_pages("/gtfs_stops/list", params):
        stop_id = str(rec.get("code") or rec.get("id", ""))
        if not stop_id or stop_id in seen:
            continue
        seen.add(stop_id)

        lat  = rec.get("lat")
        lon  = rec.get("lon")
        name = rec.get("name") or f"תחנה {stop_id}"
        city = rec.get("city") or ""

        if lat is None or lon is None:
            continue

        batch.append((stop_id, name, float(lat), float(lon), city))

        if len(batch) >= 500:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO gtfs.stops (stop_id, stop_name, stop_lat, stop_lon, city)
                       VALUES %s ON CONFLICT DO NOTHING""",
                    batch,
                )
            conn.commit()
            inserted += len(batch)
            batch = []
            log.info(f"  stops: {inserted} so far...")

    if batch:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO gtfs.stops (stop_id, stop_name, stop_lat, stop_lon, city)
                   VALUES %s ON CONFLICT DO NOTHING""",
                batch,
            )
        conn.commit()
        inserted += len(batch)

    log.info(f"  Loaded {inserted} stops")
    return inserted


def check_status(conn):
    with conn.cursor() as cur:
        for tbl in ["agency", "routes", "stops", "trips", "stop_times"]:
            cur.execute(f"SELECT COUNT(*) FROM gtfs.{tbl}")
            cnt = cur.fetchone()[0]
            print(f"  gtfs.{tbl}: {cnt:,} rows")


def main():
    import psycopg2.extras

    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="בדיקת מצב בלבד")
    args = parser.parse_args()

    conn = get_conn()
    log.info(f"Connected to PostgreSQL {DB_HOST}:{DB_PORT}/{DB_NAME}")
    ensure_schema(conn)

    if args.check:
        print("\n📊 GTFS Data Status (loaded from Stride):")
        check_status(conn)
        conn.close()
        return

    t0 = time.time()

    ag_cnt    = load_agencies(conn)
    routes_cnt = load_routes(conn)
    stops_cnt  = load_stops(conn)

    # שמור סטטוס
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO gtfs.load_status (source_url, routes_cnt, stops_cnt, trips_cnt) VALUES (%s,%s,%s,%s)",
            ("stride-api", routes_cnt, stops_cnt, 0),
        )
    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    log.info(
        f"Done in {elapsed:.0f}s — "
        f"agencies={ag_cnt}, routes={routes_cnt}, stops={stops_cnt}"
    )
    print(f"\n✅ GTFS loaded from Stride API:")
    print(f"   קווים:   {routes_cnt:,}")
    print(f"   תחנות:   {stops_cnt:,}")
    print(f"   זמן:     {elapsed:.0f} שניות")


if __name__ == "__main__":
    # psycopg2.extras needs to be imported inside main() for execute_values
    import psycopg2.extras
    main()
