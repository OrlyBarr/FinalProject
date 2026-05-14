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
    מסנן לגוש דן: מציג רק קווים שהמסלול שלהם מזכיר עיר בגוש דן.
    מחזיר קו אחד (הגרסה האחרונה) לכל (route_short_name, agency_id).
    """
    # ערי גוש דן — קו שמזכיר לפחות אחת מהן נחשב גוש דן
    GUSH_DAN_CITIES = (
        r'(תל.?אביב|ת\.מרכזית תל|גבעתיים|חולון|בת.?ים|רמת.?גן|'
        r'בני.?ברק|פתח.?תקווה|הרצליה|רמת.?השרון|אזור|יהוד|'
        r'קרית.?אונו|אור.?יהודה|גבעת.?שמואל|ראשון.?לציון|'
        r'נס.?ציונה|כפר.?סבא|הוד.?השרון|סבידור|רדינג)'
    )

    params: list = []
    where:  list = []

    # אל תחזיר קווים עם route_short_name חסר/NaN
    where.append("r.route_short_name IS NOT NULL")
    where.append("r.route_short_name != 'NaN'")
    where.append("r.route_short_name != ''")

    if operator_id:
        # מפעיל ספציפי נבחר — לא מסננים לפי עיר
        where.append("r.agency_id = %s")
        params.append(operator_id)
    else:
        # סינון גוש דן: route_long_name חייב להזכיר עיר בגוש דן
        where.append("r.route_long_name ~ %s")
        params.append(GUSH_DAN_CITIES)

    if query:
        where.append("""(
            r.route_short_name ILIKE %s OR
            r.route_long_name  ILIKE %s
        )""")
        q = f"%{query}%"
        params += [q, q]

    where_str = "WHERE " + " AND ".join(where)

    # DISTINCT ON מחזיר גרסה אחת (האחרונה לפי route_id) לכל שילוב קו+מפעיל
    return _q(f"""
        SELECT DISTINCT ON (
            CASE WHEN r.route_short_name ~ '^[0-9]+$'
                 THEN r.route_short_name::integer ELSE NULL END,
            r.route_short_name,
            r.agency_id
        )
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
            r.route_short_name,
            r.agency_id,
            r.route_id DESC
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


def get_route_stops_by_short_name(short_name: str, operator_id: str = "") -> list[dict]:
    """
    מחזיר תחנות לפי מספר קו, עם סינון אופציונלי לפי מפעיל.
    אם לא נמצא ב-GTFS ומספר הקו הוא line_ref גדול (>1000, כמו קווי רכבת),
    מחזיר תחנות מ-Stride API ישירות.
    """
    where = ["route_short_name = %s"]
    params: list = [short_name]
    if operator_id:
        where.append("agency_id = %s")
        params.append(operator_id)
    rows = _q(
        f"SELECT route_id, agency_id, route_long_name FROM gtfs.routes "
        f"WHERE {' AND '.join(where)} ORDER BY route_id LIMIT 1",
        params,
    )
    if rows:
        return get_route_stops(rows[0]["route_id"])

    # ── Fallback: line_ref גדול (רכבת / קו פנימי של Stride) ──────────────────
    # מנסה למצוא מיקום נוכחי דרך Stride ולהציג תחנות מ-gtfs.stops
    if short_name.lstrip("-").isdigit() and abs(int(short_name)) > 999:
        try:
            import urllib.request, json as _json
            STRIDE = "https://open-bus-stride-api.hasadna.org.il"

            # שלב 1: מצא מיקום נוכחי של הקו
            url2 = (f"{STRIDE}/siri_vehicle_locations/list"
                    f"?siri_routes__line_ref={short_name}&limit=1&order_by=id+desc")
            with urllib.request.urlopen(url2, timeout=10) as r2:
                vehicles = _json.loads(r2.read())
            if not vehicles:
                return []

            v = vehicles[0]
            op_ref = v.get("siri_route__operator_ref")
            v_lat  = v.get("lat")
            v_lon  = v.get("lon")

            # ── רכבת ישראל (operator_ref=2): SIRI line_ref לא מסונכרן עם GTFS ──
            # מחזירים תחנות רכבת קרובות למיקום הנוכחי
            if op_ref == 2 and v_lat and v_lon:
                d_lat = 0.25   # ~28km
                d_lon = 0.35   # ~31km
                nearby_train_stops = _q("""
                    SELECT stop_id, stop_name, stop_lat, stop_lon, city,
                           ROUND(SQRT(
                               POWER((stop_lat - %s) * 111320, 2) +
                               POWER((stop_lon - %s) * 88000,  2)
                           )::integer, 0) AS distance_m
                    FROM gtfs.stops
                    WHERE stop_lat BETWEEN %s AND %s
                      AND stop_lon BETWEEN %s AND %s
                      AND stop_name LIKE '%%ת. רכבת%%'
                    ORDER BY distance_m
                    LIMIT 15
                """, (v_lat, v_lon,
                      v_lat - d_lat, v_lat + d_lat,
                      v_lon - d_lon, v_lon + d_lon))
                return [{
                    "stop_id":       s["stop_id"],
                    "stop_name":     s["stop_name"],
                    "stop_lat":      s["stop_lat"],
                    "stop_lon":      s["stop_lon"],
                    "stop_sequence": i + 1,
                    "city":          s.get("city") or "",
                    "source":        "stride-nearby",
                } for i, s in enumerate(nearby_train_stops)]

            # ── אוטובוס / שאר המפעילים: שלוף עצירות מ-siri_ride_stops ──────────
            ride_id = v.get("siri_ride__id") or v.get("id")
            url3 = f"{STRIDE}/siri_ride_stops/list?siri_ride__id={ride_id}&limit=80"
            with urllib.request.urlopen(url3, timeout=10) as r3:
                ride_stops = _json.loads(r3.read())
            if not ride_stops:
                return []

            # lookup כל ה-siri_stop codes ב-gtfs.stops בבת אחת
            stop_codes = [str(s.get("siri_stop__code")) for s in ride_stops
                          if s.get("siri_stop__code")]
            stop_map: dict = {}
            if stop_codes:
                placeholders = ",".join(["%s"] * len(stop_codes))
                db_stops = _q(
                    f"SELECT stop_id, stop_name, stop_lat, stop_lon, city "
                    f"FROM gtfs.stops WHERE stop_id IN ({placeholders})",
                    stop_codes,
                )
                stop_map = {s["stop_id"]: s for s in db_stops}

            ride_stops.sort(key=lambda x: x.get("order", 0))
            result = []
            for s in ride_stops:
                code = str(s.get("siri_stop__code") or "")
                info = stop_map.get(code, {})
                lat  = info.get("stop_lat") or s.get("gtfs_stop__lat")
                lon  = info.get("stop_lon") or s.get("gtfs_stop__lon")
                if lat is None or lon is None:
                    continue
                result.append({
                    "stop_id":       code or str(s.get("id")),
                    "stop_name":     info.get("stop_name") or s.get("gtfs_stop__name") or f"תחנה {s.get('order','')}",
                    "stop_lat":      lat,
                    "stop_lon":      lon,
                    "stop_sequence": s.get("order", 0),
                    "city":          info.get("city") or s.get("gtfs_stop__city") or "",
                    "source":        "stride",
                })
            return result

        except Exception as e:
            log.debug(f"Stride fallback for line_ref={short_name}: {e}")
            return []
    return []


def get_route_schedule(short_name: str, operator_id: str = "",
                       direction: int = 0) -> dict:
    """
    מחזיר פרטי קו מ-GTFS עם תחנות + זמן עזיבה מתוכנן הקרוב ביותר לכל תחנה
    (לפי לוח זמנים סטטי של היום ומן הרגע הנוכחי).

    Returns dict:
        route: {route_id, route_short_name, route_long_name, agency_id, agency_name}
        stops: [{stop_id, stop_name, stop_lat, stop_lon,
                 stop_sequence, scheduled_dep}]
        direction: int used
    """
    from datetime import datetime as _dt

    now_str  = _dt.now().strftime("%H:%M:%S")
    day_col  = ["monday","tuesday","wednesday","thursday",
                "friday","saturday","sunday"][_dt.now().weekday()]

    # Locate route
    r_where  = ["r.route_short_name = %s"]
    r_params: list = [short_name]
    if operator_id:
        r_where.append("r.agency_id = %s")
        r_params.append(operator_id)

    route_rows = _q(
        f"SELECT r.route_id, r.route_short_name, r.route_long_name, "
        f"       r.agency_id, a.agency_name "
        f"FROM gtfs.routes r "
        f"LEFT JOIN gtfs.agency a ON a.agency_id = r.agency_id "
        f"WHERE {' AND '.join(r_where)} ORDER BY r.route_id LIMIT 1",
        r_params,
    )
    if not route_rows:
        return {}

    route    = route_rows[0]
    route_id = route["route_id"]

    # Try requested direction; fall back to opposite then any
    for dir_filter in [f"AND t.direction_id = {direction}",
                       f"AND t.direction_id = {1 - direction}", ""]:
        stops = _q(f"""
            SELECT DISTINCT ON (st.stop_sequence)
                s.stop_id,
                s.stop_name,
                s.stop_lat::float  AS stop_lat,
                s.stop_lon::float  AS stop_lon,
                st.stop_sequence,
                st.departure_time  AS scheduled_dep
            FROM gtfs.stop_times  st
            JOIN gtfs.trips       t  ON t.trip_id   = st.trip_id
            JOIN gtfs.stops       s  ON s.stop_id   = st.stop_id
            LEFT JOIN gtfs.calendar c ON c.service_id = t.service_id
            WHERE t.route_id = %s
              {dir_filter}
              AND st.departure_time >= %s
              AND (c.{day_col} = 1 OR c.service_id IS NULL)
            ORDER BY st.stop_sequence, st.departure_time
            LIMIT 60
        """, (route_id, now_str))
        if stops:
            return {"route": route, "stops": stops, "direction": direction}

    # Last resort: no time filter (shows full stop list even if all trips passed)
    stops = _q("""
        SELECT DISTINCT ON (st.stop_sequence)
            s.stop_id, s.stop_name,
            s.stop_lat::float AS stop_lat, s.stop_lon::float AS stop_lon,
            st.stop_sequence,  st.departure_time AS scheduled_dep
        FROM gtfs.stop_times st
        JOIN gtfs.trips  t ON t.trip_id = st.trip_id
        JOIN gtfs.stops  s ON s.stop_id = st.stop_id
        WHERE t.route_id = %s
        ORDER BY st.stop_sequence
        LIMIT 60
    """, (route_id,))
    return {"route": route, "stops": stops, "direction": -1} if stops else {}


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
            s.stop_lat,
            s.stop_lon,
            ROUND(
                SQRT(
                    POWER((s.stop_lat - %s) * 111320, 2) +
                    POWER((s.stop_lon - %s) * 88000,  2)
                )::integer
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
        SELECT stop_id, stop_name, stop_lat, stop_lon
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