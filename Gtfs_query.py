"""
gtfs_query.py
=============
מודול שאילתות GTFS מ-PostgreSQL לשימוש ב-bot.py ובסוכן ה-AI.

פונקציות:
  search_routes(q, operator_id)  — חיפוש קווים
  get_route_stops(route_id)       — תחנות של קו
  get_nearby_stops(lat, lon, m)   — תחנות ליד כתובת
  get_stop_times(stop_id, time)   — לוח זמנים לתחנה
  search_stops(name)              — חיפוש תחנות לפי שם
  get_operators()                 — רשימת מפעילים
"""

import os
import logging

log = logging.getLogger("gtfs_query")

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB",  "airflow")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")


def _conn():
    import psycopg2
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        connect_timeout=5,
    )


def _q(sql: str, params=()) -> list[dict]:
    """ריצת query, מחזיר list of dicts."""
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        log.error(f"Query error: {e}")
        return []
    finally:
        try: conn.close()
        except: pass


def is_available() -> bool:
    """בדיקה אם נתוני GTFS זמינים."""
    try:
        rows = _q("SELECT COUNT(*) as cnt FROM gtfs.routes")
        return bool(rows and rows[0]["cnt"] > 0)
    except:
        return False


def get_operators() -> list[dict]:
    """מחזיר רשימת מפעילים עם מספר הקווים שלהם."""
    return _q("""
        SELECT
            a.agency_id,
            a.agency_name,
            COUNT(DISTINCT r.route_id) as routes_count
        FROM gtfs.agency a
        LEFT JOIN gtfs.routes r ON r.agency_id = a.agency_id
        GROUP BY a.agency_id, a.agency_name
        ORDER BY routes_count DESC
    """)


def search_routes(query: str = "", operator_id: str = "", limit: int = 50) -> list[dict]:
    """
    מחפש קווים לפי שם קצר / שם ארוך / מפעיל.
    query — מספר קו או מילת חיפוש
    operator_id — agency_id לסינון לפי מפעיל
    """
    params = []
    where  = []

    if operator_id:
        where.append("r.agency_id = %s")
        params.append(operator_id)

    if query:
        where.append("""(
            r.route_short_name ILIKE %s OR
            r.route_long_name  ILIKE %s OR
            r.route_id         ILIKE %s
        )""")
        q = f"%{query}%"
        params += [q, q, q]

    where_str = "WHERE " + " AND ".join(where) if where else ""

    return _q(f"""
        SELECT
            r.route_id,
            r.route_short_name,
            r.route_long_name,
            r.route_type,
            r.agency_id,
            a.agency_name
        FROM gtfs.routes r
        LEFT JOIN gtfs.agency a ON a.agency_id = r.agency_id
        {where_str}
        ORDER BY
            CASE WHEN r.route_short_name ~ '^[0-9]+$'
                 THEN r.route_short_name::integer ELSE NULL END NULLS LAST,
            r.route_short_name
        LIMIT %s
    """, params + [limit])


def get_route_stops(route_id: str, direction: int = 0) -> list[dict]:
    """
    מחזיר תחנות של קו לפי route_id.
    מנסה direction=0 תחילה; אם ריק, מנסה direction=1 ואחר-כך כל direction.
    """
    for dir_filter in [f"AND t.direction_id = {direction}", "AND t.direction_id = 1", ""]:
        rows = _q(f"""
            SELECT DISTINCT ON (st.stop_sequence)
                s.stop_id,
                s.stop_name,
                s.stop_code,
                s.stop_lat,
                s.stop_lon,
                st.stop_sequence,
                st.arrival_time,
                st.departure_time
            FROM gtfs.trips t
            JOIN gtfs.stop_times st ON st.trip_id = t.trip_id
            JOIN gtfs.stops s       ON s.stop_id  = st.stop_id
            WHERE t.route_id = %s
              {dir_filter}
            ORDER BY st.stop_sequence
            LIMIT 80
        """, (route_id,))
        if rows:
            return rows
    return []


def get_route_stops_by_short_name(short_name: str) -> list[dict]:
    """מחזיר תחנות לפי מספר קו (route_short_name)."""
    rows = _q("""
        SELECT route_id, agency_id, route_long_name
        FROM gtfs.routes
        WHERE route_short_name = %s
        LIMIT 1
    """, (short_name,))
    if not rows:
        return []
    return get_route_stops(rows[0]["route_id"])


def get_nearby_stops(lat: float, lon: float, radius_m: int = 500) -> list[dict]:
    """
    מחזיר תחנות בטווח radius_m מטרים מהנקודה.
    משתמש בחישוב מרחק Euclidean מקורב (מדויק מספיק לישראל).
    """
    # 1 degree lat ≈ 111,320m; 1 degree lon ≈ 88,000m בישראל
    d_lat = radius_m / 111320.0
    d_lon = radius_m / 88000.0

    return _q("""
        SELECT
            s.stop_id,
            s.stop_name,
            s.stop_code,
            s.stop_lat,
            s.stop_lon,
            ROUND(
                SQRT(
                    POWER((s.stop_lat - %s) * 111320, 2) +
                    POWER((s.stop_lon - %s) * 88000,  2)
                )::numeric, 0
            ) AS distance_m
        FROM gtfs.stops s
        WHERE s.stop_lat BETWEEN %s AND %s
          AND s.stop_lon BETWEEN %s AND %s
        ORDER BY distance_m
        LIMIT 20
    """, (lat, lon,
          lat - d_lat, lat + d_lat,
          lon - d_lon, lon + d_lon))


def get_routes_at_stop(stop_id: str) -> list[dict]:
    """מחזיר כל הקווים שעוברים בתחנה מסוימת."""
    return _q("""
        SELECT DISTINCT
            r.route_id,
            r.route_short_name,
            r.route_long_name,
            a.agency_name
        FROM gtfs.stop_times st
        JOIN gtfs.trips  t ON t.trip_id  = st.trip_id
        JOIN gtfs.routes r ON r.route_id = t.route_id
        LEFT JOIN gtfs.agency a ON a.agency_id = r.agency_id
        WHERE st.stop_id = %s
        ORDER BY
            CASE WHEN r.route_short_name ~ '^[0-9]+$'
                 THEN r.route_short_name::integer ELSE NULL END NULLS LAST,
            r.route_short_name
        LIMIT 50
    """, (stop_id,))


def get_line_ref_map(limit: int = 8000) -> dict[str, str]:
    """
    מחזיר mapping מ-route_id (=siri line_ref) → route_short_name.
    משמש לתרגום מספרים פנימיים של SIRI לשמות קווים קריאים (כגון 16542 → '63').
    """
    rows = _q(
        "SELECT route_id, route_short_name FROM gtfs.routes "
        "WHERE route_short_name IS NOT NULL AND route_short_name <> '' "
        "LIMIT %s",
        (limit,),
    )
    return {str(r["route_id"]): str(r["route_short_name"]) for r in rows}


def get_stop_schedule(stop_id: str, from_time: str = "00:00:00",
                      day_type: str = "weekday", limit: int = 20) -> list[dict]:
    """
    מחזיר לוח זמנים לתחנה מזמן נתון.
    day_type: weekday / saturday / sunday
    """
    day_col = {
        "weekday":  "monday",
        "saturday": "saturday",
        "sunday":   "sunday",
    }.get(day_type, "monday")

    return _q(f"""
        SELECT
            st.arrival_time,
            st.departure_time,
            r.route_short_name,
            r.route_long_name,
            t.trip_headsign,
            a.agency_name
        FROM gtfs.stop_times st
        JOIN gtfs.trips  t ON t.trip_id  = st.trip_id
        JOIN gtfs.routes r ON r.route_id = t.route_id
        LEFT JOIN gtfs.agency   a ON a.agency_id  = r.agency_id
        LEFT JOIN gtfs.calendar c ON c.service_id = t.service_id
        WHERE st.stop_id = %s
          AND st.arrival_time >= %s
          AND (c.{day_col} = 1 OR c.service_id IS NULL)
        ORDER BY st.arrival_time
        LIMIT %s
    """, (stop_id, from_time, limit))


def search_stops(name: str, limit: int = 10) -> list[dict]:
    """חיפוש תחנות לפי שם."""
    return _q("""
        SELECT stop_id, stop_name, stop_code, stop_lat, stop_lon
        FROM gtfs.stops
        WHERE stop_name ILIKE %s
        ORDER BY stop_name
        LIMIT %s
    """, (f"%{name}%", limit))


def get_gtfs_summary() -> dict:
    """סיכום כמות הנתונים."""
    rows = _q("""
        SELECT
            (SELECT COUNT(*) FROM gtfs.routes)     as routes,
            (SELECT COUNT(*) FROM gtfs.stops)      as stops,
            (SELECT COUNT(*) FROM gtfs.trips)      as trips,
            (SELECT COUNT(*) FROM gtfs.stop_times) as stop_times,
            (SELECT MAX(loaded_at) FROM gtfs.load_status) as last_load
    """)
    return rows[0] if rows else {}