"""
airflow/dags/dag_daily_analysis.py
DAG 3: Daily KPI aggregations, performance reports, HTML summary
Schedule: 04:00 IL time (01:00 UTC) - after end of service day

Data flow (no Kafka dependency):
  1. aggregate_delay_stats    — MOT GTFS-RT TripUpdates → transit-trip-updates (ES)
  2. aggregate_route_perf     — Stride SIRI bus positions → transit-bus-positions (ES)
  3. calculate_kpis           — aggregate from ES, push KPIs to XCom
  4. generate_daily_report    — build HTML report from KPIs

ES indices written to here match the Kibana index patterns in /kibana/transit_dashboard.ndjson:
  transit-bus-positions*    (idx-transit-bus-positions)
  transit-trip-updates*     (idx-transit-trip-updates)
airflow/dags/dag_daily_analysis.py
DAG 3: Daily KPI aggregations, performance reports, HTML summary
Schedule: 04:00 IL time (01:00 UTC) - after end of service day
"""

import os
from datetime import datetime, timedelta, date, timezone
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
        "retries": 5,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": None,
    }

ES_HOST     = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
STRIDE_URL  = "https://open-bus-stride-api.hasadna.org.il/siri_vehicle_locations/list"
GTFS_RT_URL = "https://gtfs.mot.gov.il/gtfsfiles/TripUpdates.pb"
DELAY_THRESHOLD_SEC = 180   # 3 minutes

OPERATORS = {
    "2": "רכבת ישראל", "3": "דן", "5": "אגד", "6": "NTA מטרופולין",
    "14": "מטרופולין", "15": "אגד תעבורה", "16": "תנופה", "18": "סופרבוס",
    "21": "קווים", "25": "נתיב אקספרס", "32": "V-Line", "42": "אפיקים",
}

default_args = {
    **RESILIENT_DEFAULT_ARGS,
    "start_date":        datetime(2026, 4, 13),
    "email_on_failure":  False,  # OPT: no SMTP configured — errors would cascade
    "execution_timeout": timedelta(minutes=45),  # daily aggregation needs extra time
}


def _ensure_index(es, index_name: str, mappings: dict) -> None:
    """
    Create an ES index with the given mappings.
    If the index already exists with incompatible string mappings (pure 'keyword'
    instead of 'text+keyword'), delete and recreate it so Kibana .keyword
    sub-field queries work correctly.
    """
    if es.indices.exists(index=index_name):
        # Check if any string field is pure keyword (missing .keyword sub-field).
        # If so, the index was created with the old mapping — drop and recreate.
        try:
            current = es.indices.get_mapping(index=index_name)
            props = (
                current.get(index_name, {})
                .get("mappings", {})
                .get("properties", {})
            )
            needs_recreate = any(
                v.get("type") == "keyword"
                for k, v in props.items()
                if k in ("operator_name", "route_id", "vehicle_id", "route_short_name")
            )
            if needs_recreate:
                es.indices.delete(index=index_name)
                print(f"Deleted index {index_name} (had pure-keyword mapping; recreating with text+keyword)")
            else:
                return  # mapping already correct
        except Exception:
            return  # leave the index as-is on any error

    es.indices.create(index=index_name, body={
        "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "5s"},
        "mappings": mappings,
    })


def _hour_to_period(hour: int) -> str:
    if 6  <= hour < 9:  return "morning_rush"
    if 9  <= hour < 12: return "mid_morning"
    if 12 <= hour < 15: return "midday"
    if 15 <= hour < 19: return "evening_rush"
    if 19 <= hour < 23: return "evening"
    return "off_peak"


def aggregate_delay_stats(**context):
    """
    Fetch MOT GTFS-RT TripUpdates directly, index raw records into
    transit-trip-updates (the index Kibana's idx-transit-trip-updates watches),
    then push daily stats to XCom for the report.
    """
    import requests
    from google.transit import gtfs_realtime_pb2
    from elasticsearch import Elasticsearch, helpers

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    es = Elasticsearch(ES_HOST)

    _KWORD = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
    _ensure_index(es, "transit-trip-updates", {"properties": {
        "timestamp":       {"type": "date"},
        "route_id":        _KWORD,
        "trip_id":         _KWORD,
        "stop_id":         _KWORD,
        "start_date":      _KWORD,
        "delay_seconds":   {"type": "integer"},
        "arrival_delay":   {"type": "integer"},
        "departure_delay": {"type": "integer"},
        "is_delayed":      {"type": "boolean"},
        "is_cancelled":    {"type": "boolean"},
        "_indexed_at":     {"type": "date"},
    }})

    docs = []
    try:
        resp = requests.get(GTFS_RT_URL, timeout=12)
        resp.raise_for_status()
        if resp.headers.get("Content-Type", "").startswith("text/html"):
            print("WARNING: MOT GTFS-RT returned HTML — API may be blocked, skipping index write")
        else:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(resp.content)
            now_utc = datetime.now(timezone.utc)
            feed_ts = (
                datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)
                if feed.header.timestamp else now_utc
            )
            for entity in feed.entity:
                if not entity.HasField("trip_update"):
                    continue
                tu   = entity.trip_update
                trip = tu.trip
                is_cancelled = (trip.schedule_relationship == 3)
                for stu in tu.stop_time_update:
                    arr_delay = stu.arrival.delay   if stu.HasField("arrival")   else None
                    dep_delay = stu.departure.delay if stu.HasField("departure") else None
                    delay_sec = dep_delay if dep_delay is not None else arr_delay
                    # Use feed ingestion time as timestamp so Kibana's "last 24h"
                    # filter always includes these docs (scheduled departure times
                    # can be in the future and would fall outside the window).
                    ts = feed_ts.isoformat()
                    docs.append({
                        "_index": "transit-trip-updates",
                        "_source": {
                            "timestamp":    ts,
                            "trip_id":      trip.trip_id,
                            "route_id":     trip.route_id,
                            "direction_id": trip.direction_id,
                            "start_date":   trip.start_date,
                            "stop_id":      stu.stop_id,
                            "stop_sequence": stu.stop_sequence,
                            "delay_seconds": delay_sec,
                            "arrival_delay":  arr_delay,
                            "departure_delay": dep_delay,
                            "is_delayed":   bool(delay_sec is not None and delay_sec >= DELAY_THRESHOLD_SEC),
                            "is_cancelled": is_cancelled,
                            "_indexed_at":  now_utc.isoformat(),
                        },
                    })
    except Exception as e:
        print(f"GTFS-RT fetch/parse error: {e} — continuing with empty set")

    if docs:
        success, _ = helpers.bulk(es, docs, raise_on_error=False)
        print(f"Indexed {success}/{len(docs)} trip-update docs into transit-trip-updates")

    total     = len(docs)
    delayed   = sum(1 for d in docs if d["_source"].get("is_delayed"))
    cancelled = sum(1 for d in docs if d["_source"].get("is_cancelled"))
    delays    = [d["_source"]["delay_seconds"] for d in docs
                 if d["_source"].get("delay_seconds") is not None]
    avg_delay = round(sum(delays) / len(delays), 1) if delays else 0

    # ── PostgreSQL: aggregate daily delay stats from in-memory docs (no extra ES query) ──
    # Groups the docs list already built above — zero additional API or ES calls.
    # FIX: Split DELETE + INSERT into separate operations.
    # FIX: p90 computed in Python via sorted list (no PERCENTILE_CONT needed).
    try:
        import psycopg2
        from collections import defaultdict
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "airflow"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS transit")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transit.agg_delay_stats (
                stat_date DATE, stat_hour INTEGER, time_period TEXT,
                route_id TEXT, route_short_name TEXT,
                total_trips INTEGER, delayed_trips INTEGER, cancelled_trips INTEGER,
                avg_delay_seconds FLOAT, max_delay_seconds INTEGER,
                p90_delay_seconds FLOAT, delay_rate_pct FLOAT, cancellation_rate FLOAT
            )""")
        cur.execute("DELETE FROM transit.agg_delay_stats WHERE stat_date = %s", (yesterday,))
        # Group by (hour, time_period, route_id) from in-memory docs
        groups = defaultdict(list)
        for d in docs:
            src = d["_source"]
            try:
                hour = int(str(src.get("timestamp", ""))[11:13])
            except (ValueError, TypeError):
                hour = 0
            groups[(hour, _hour_to_period(hour), src.get("route_id", ""))].append(src)
        for (hour, period, route_id), grp in groups.items():
            dvals = sorted(g["delay_seconds"] for g in grp if g.get("delay_seconds") is not None)
            tt = len(grp)
            n_delayed   = sum(1 for g in grp if g.get("is_delayed"))
            n_cancelled = sum(1 for g in grp if g.get("is_cancelled"))
            avg_d = round(sum(dvals) / len(dvals), 2) if dvals else 0
            max_d = int(dvals[-1]) if dvals else 0
            p90_d = dvals[int(len(dvals) * 0.9)] if dvals else 0
            cur.execute(
                "INSERT INTO transit.agg_delay_stats VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (yesterday, hour, period, route_id, route_id,
                 tt, n_delayed, n_cancelled, avg_d, max_d, p90_d,
                 round(100.0 * n_delayed / tt, 2) if tt else 0,
                 round(100.0 * n_cancelled / tt, 2) if tt else 0),
            )
        conn.commit()
        conn.close()
        print(f"PostgreSQL transit.agg_delay_stats: {len(groups)} route-hour rows for {yesterday}")
    except Exception as e:
        print(f"PostgreSQL agg_delay_stats skipped: {e}")

    context["ti"].xcom_push(key="report_date", value=yesterday)
    context["ti"].xcom_push(key="trip_stats", value={
        "total": total, "delayed": delayed,
        "cancelled": cancelled, "avg_delay_seconds": avg_delay,
    })
    print(f"Trip updates: total={total} delayed={delayed} cancelled={cancelled} avg_delay={avg_delay}s")


def aggregate_route_performance(**context):
    """
    Fetch live bus positions from Stride SIRI API, index into
    transit-bus-positions (the index Kibana's idx-transit-bus-positions watches).
    """
    import requests
    from elasticsearch import Elasticsearch, helpers

    es = Elasticsearch(ES_HOST)

    _KWORD = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
    _ensure_index(es, "transit-bus-positions", {"properties": {
        "timestamp":        {"type": "date"},
        "vehicle_id":       _KWORD,
        "route_id":         _KWORD,
        "route_short_name": _KWORD,
        "operator_id":      _KWORD,
        "operator_name":    _KWORD,
        "latitude":         {"type": "float"},
        "longitude":        {"type": "float"},
        "location":         {"type": "geo_point"},
        "speed_kmh":        {"type": "float"},
        "is_moving":        {"type": "boolean"},
        "_indexed_at":      {"type": "date"},
    }})

    docs = []
    now_utc = datetime.now(timezone.utc)
    now_ts  = now_utc.isoformat()

    params = {
        "limit":    500,
        "order_by": "id desc",
        "lat__greater_or_equal": 31.97,
        "lat__lower_or_equal":   32.19,
        "lon__greater_or_equal": 34.73,
        "lon__lower_or_equal":   34.93,
    }
    try:
        resp = requests.get(STRIDE_URL, params=params, timeout=12)
        resp.raise_for_status()
        for item in resp.json():
            lat = item.get("lat")
            lon = item.get("lon")
            if not lat or not lon:
                continue
            try:
                lat, lon = float(lat), float(lon)
            except (TypeError, ValueError):
                continue

            operator_ref = (
                str(item.get("siri_route__operator_ref") or "").strip() or
                str(item.get("operator_ref") or "").strip()
            )
            line_ref = (
                str(item.get("siri_route__line_ref") or "").strip() or
                str(item.get("line_ref") or "").strip()
            )
            route_short_name = (
                str(item.get("siri_route__gtfs_route__route_short_name") or "").strip()
                or line_ref
            )
            velocity = item.get("velocity")
            try:
                speed_kmh = float(velocity) if velocity is not None else None
            except (TypeError, ValueError):
                speed_kmh = None

            docs.append({
                "_index": "transit-bus-positions",
                "_source": {
                    "timestamp":        item.get("recorded_at_time", now_ts),
                    "vehicle_id":       str(item.get("siri_ride__vehicle_ref") or item.get("id") or ""),
                    "route_id":         line_ref,
                    "route_short_name": route_short_name,
                    "operator_id":      operator_ref,
                    "operator_name":    OPERATORS.get(operator_ref, "Unknown"),
                    "latitude":         round(lat, 6),
                    "longitude":        round(lon, 6),
                    "location":         {"lat": lat, "lon": lon},
                    "speed_kmh":        speed_kmh,
                    "is_moving":        bool(speed_kmh and speed_kmh > 0),
                    "area":             "gush_dan",
                    "_indexed_at":      now_ts,
                },
            })
    except Exception as e:
        print(f"Stride API error: {e} — skipping bus position index write")

    if docs:
        success, _ = helpers.bulk(es, docs, raise_on_error=False)
        print(f"Indexed {success}/{len(docs)} bus-position docs into transit-bus-positions")
    else:
        print("No bus positions fetched")

    # ── PostgreSQL: aggregate route performance from in-memory docs (no extra ES query) ──
    # Groups the docs list already built above — zero additional API or ES calls.
    # FIX: Split DELETE + INSERT into separate operations.
    try:
        import psycopg2
        from collections import defaultdict
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "airflow"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS transit")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transit.agg_route_performance (
                perf_date DATE, route_id TEXT, route_short_name TEXT, operator_name TEXT,
                total_vehicles INTEGER, avg_speed_kmh FLOAT,
                total_delays INTEGER, avg_delay_min FLOAT,
                worst_delay_min FLOAT, on_time_rate_pct FLOAT
            )""")
        cur.execute("DELETE FROM transit.agg_route_performance WHERE perf_date = %s", (yesterday,))
        # Group by (route_id, route_short_name, operator_name) from in-memory docs
        groups = defaultdict(list)
        for d in docs:
            src = d["_source"]
            groups[(
                src.get("route_id", ""),
                src.get("route_short_name", ""),
                src.get("operator_name", ""),
            )].append(src)
        for (route_id, rsn, op_name), grp in groups.items():
            speeds = [g["speed_kmh"] for g in grp if g.get("speed_kmh") is not None]
            vehicles = len(set(g.get("vehicle_id", "") for g in grp))
            avg_spd = round(sum(speeds) / len(speeds), 2) if speeds else 0
            cur.execute(
                "INSERT INTO transit.agg_route_performance VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (yesterday, route_id, rsn, op_name,
                 vehicles, avg_spd, 0, 0.0, 0.0, 100.0),
            )
        conn.commit()
        conn.close()
        print(f"PostgreSQL transit.agg_route_performance: {len(groups)} route rows for {yesterday}")
    except Exception as e:
        print(f"PostgreSQL agg_route_performance skipped: {e}")


def calculate_kpis(**context):
    """
    Aggregate KPIs directly from Elasticsearch indices.
    Reads transit-trip-updates, transit-bus-positions, transit-train-positions.
    """
    from elasticsearch import Elasticsearch

    ti        = context["ti"]
    yesterday = ti.xcom_pull(task_ids="aggregate_delay_stats", key="report_date")
    trip_stats = ti.xcom_pull(task_ids="aggregate_delay_stats", key="trip_stats") or {}
    today     = (date.fromisoformat(yesterday) + timedelta(days=1)).isoformat()
    time_range = {"range": {"timestamp": {"gte": f"{yesterday}T00:00:00Z", "lt": f"{today}T00:00:00Z"}}}

    es = Elasticsearch(ES_HOST)

    # ── Delay KPIs from trip_stats XCom (computed in task 1 from live data) ──
    total     = trip_stats.get("total", 0)
    delayed   = trip_stats.get("delayed", 0)
    cancelled = trip_stats.get("cancelled", 0)
    avg_delay_sec = trip_stats.get("avg_delay_seconds", 0)
    delay_rate    = round(100.0 * delayed / total, 2) if total else 0

    # ── Route-level breakdown from transit-trip-updates ──
    route_resp = es.search(
        index="transit-trip-updates",
        ignore_unavailable=True,
        body={
            "size": 0,
            "query": time_range,
            "aggs": {
                "by_route": {
                    "terms": {"field": "route_id.keyword", "size": 200},
                    "aggs": {
                        "delayed_count":   {"filter": {"term": {"is_delayed":   True}}},
                        "cancelled_count": {"filter": {"term": {"is_cancelled": True}}},
                        "avg_delay":       {"avg":    {"field": "delay_seconds"}},
                    },
                }
            },
        },
    )

    route_buckets = route_resp.get("aggregations", {}).get("by_route", {}).get("buckets", [])
    worst_routes = sorted(
        [
            {
                "route_short_name": b["key"],
                "operator_name":    "",
                "avg_delay_rate":   round(100.0 * b["delayed_count"]["doc_count"] / b["doc_count"], 2)
                                    if b["doc_count"] else 0,
                "avg_delay_min":    round((b["avg_delay"]["value"] or 0) / 60.0, 1),
            }
            for b in route_buckets
        ],
        key=lambda x: x["avg_delay_rate"],
        reverse=True,
    )[:10]

    best_routes = sorted(
        [
            {
                "route_short_name": b["key"],
                "operator_name":    "",
                "on_time_pct":      round(100.0 - (100.0 * b["delayed_count"]["doc_count"] / b["doc_count"]), 2)
                                    if b["doc_count"] else 100.0,
            }
            for b in route_buckets
            if b["doc_count"] > 0
        ],
        key=lambda x: x["on_time_pct"],
        reverse=True,
    )[:10]

    # ── Peak hour analysis from transit-trip-updates ──
    peak_resp = es.search(
        index="transit-trip-updates",
        ignore_unavailable=True,
        body={
            "size": 0,
            "query": {"bool": {"must": [time_range, {"term": {"is_delayed": True}}]}},
            "aggs": {
                "by_hour": {
                    "date_histogram": {"field": "timestamp", "calendar_interval": "1h"},
                    "aggs": {"avg_delay_rate": {"avg": {"field": "delay_seconds"}}},
                }
            },
        },
    )
    peak_hours = []
    for b in peak_resp.get("aggregations", {}).get("by_hour", {}).get("buckets", []):
        hour_num = int(b["key_as_string"][11:13])
        peak_hours.append({
            "stat_hour":     hour_num,
            "time_period":   _hour_to_period(hour_num),
            "delayed_count": b["doc_count"],
            "delay_rate":    round(b["avg_delay_rate"]["value"] or 0, 1),
        })

    # ── Train KPIs from transit-train-positions ──
    train_resp = es.search(
        index="transit-train-positions",
        ignore_unavailable=True,
        body={
            "size": 0,
            "query": time_range,
            "aggs": {
                "trains_monitored": {"cardinality": {"field": "vehicle_id.keyword"}},
                "delayed_trains":   {"filter":      {"term": {"is_delayed":   True}}},
                "cancelled_trains": {"filter":      {"term": {"is_cancelled": True}}},
                "avg_delay":        {"avg":         {"field": "delay_minutes"}},
                "max_delay":        {"max":         {"field": "delay_minutes"}},
            },
        },
    )
    t = train_resp.get("aggregations", {})
    train_kpis = {
        "trains_monitored": t.get("trains_monitored", {}).get("value", 0),
        "delayed_trains":   t.get("delayed_trains",   {}).get("doc_count", 0),
        "cancelled_trains": t.get("cancelled_trains", {}).get("doc_count", 0),
        "avg_delay_min":    round(t.get("avg_delay", {}).get("value") or 0, 1),
        "max_delay_min":    int(t.get("max_delay",   {}).get("value") or 0),
    }

    # ── Active routes count from transit-bus-positions ──
    routes_resp = es.search(
        index="transit-bus-positions",
        ignore_unavailable=True,
        body={"size": 0, "query": time_range,
              "aggs": {"total_routes": {"cardinality": {"field": "route_id.keyword"}}}},
    )
    total_routes = routes_resp.get("aggregations", {}).get("total_routes", {}).get("value", 0)

    kpis = {
        "total_routes":         total_routes or len(route_buckets),
        "total_trips":          total,
        "total_delayed":        delayed,
        "total_cancelled":      cancelled,
        "avg_delay_min":        round(avg_delay_sec / 60.0, 1),
        "network_delay_rate":   delay_rate,
        "network_on_time_rate": round(100 - delay_rate, 2),
    }

    ti.xcom_push(key="kpis",         value=kpis)
    ti.xcom_push(key="worst_routes",  value=worst_routes)
    ti.xcom_push(key="best_routes",   value=best_routes)
    ti.xcom_push(key="peak_hours",    value=peak_hours)
    ti.xcom_push(key="train_kpis",    value=train_kpis)

    print(f"KPIs: {kpis}")


def generate_daily_report(**context):
    """Build HTML daily report and save to file."""
    ti        = context["ti"]
    yesterday = ti.xcom_pull(task_ids="aggregate_delay_stats",  key="report_date")
    kpis      = ti.xcom_pull(task_ids="calculate_kpis",         key="kpis")         or {}
    worst     = ti.xcom_pull(task_ids="calculate_kpis",         key="worst_routes") or []
    best      = ti.xcom_pull(task_ids="calculate_kpis",         key="best_routes")  or []
    peak      = ti.xcom_pull(task_ids="calculate_kpis",         key="peak_hours")   or []
    train_kpis = ti.xcom_pull(task_ids="calculate_kpis",        key="train_kpis")   or {}

    # Peak hour bar chart (ASCII-style for email)
    peak_chart = ""
    for ph in peak:
        bar_len = int(float(ph.get("delay_rate", 0)) * 2)
        bar = "█" * bar_len
        peak_chart += f"<tr><td>{ph.get('stat_hour', '')}:00</td><td>{ph.get('time_period','')}</td><td style='font-family:monospace;color:#e74c3c'>{bar}</td><td>{ph.get('delay_rate', 0)}%</td></tr>"

    worst_rows = "".join(
        f"<tr><td>🚌 {r.get('route_short_name')}</td><td>{r.get('operator_name','')}</td>"
        f"<td style='color:#e74c3c'>{r.get('avg_delay_rate', 0)}%</td>"
        f"<td>{round(float(r.get('avg_delay_min', 0)), 1)} min</td></tr>"
        for r in worst
    )
    best_rows = "".join(
        f"<tr><td>🚌 {r.get('route_short_name')}</td><td>{r.get('operator_name','')}</td>"
        f"<td style='color:#27ae60'>{r.get('on_time_pct', 0)}%</td></tr>"
        for r in best
    )

    on_time_rate = kpis.get("network_on_time_rate", 0)
    on_time_color = "#27ae60" if float(on_time_rate) >= 80 else "#e74c3c"

    html = f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="he">
    <head><meta charset="UTF-8">
    <style>
      body {{ font-family: Arial, sans-serif; background: #f5f5f5; color: #333; direction: rtl; }}
      .header {{ background: #1a3a5c; color: white; padding: 20px; text-align: center; }}
      .kpi-grid {{ display: flex; gap: 15px; padding: 20px; flex-wrap: wrap; }}
      .kpi-card {{ background: white; border-radius: 8px; padding: 15px; min-width: 160px;
                   text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1); flex: 1; }}
      .kpi-value {{ font-size: 32px; font-weight: bold; margin: 8px 0; }}
      .kpi-label {{ font-size: 12px; color: #666; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th {{ background: #1a3a5c; color: white; padding: 8px 12px; }}
      td {{ padding: 7px 12px; border-bottom: 1px solid #eee; }}
      .section {{ background: white; margin: 10px 20px; border-radius: 8px;
                  padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
      h3 {{ color: #1a3a5c; border-right: 4px solid #e74c3c; padding-right: 10px; }}
    </style>
    </head>
    <body>
    <div class="header">
      <h1>🚌🚆 דוח יומי - ניטור תחבורה ציבורית ישראל</h1>
      <p>תאריך: {yesterday} | נוצר אוטומטית על ידי מערכת ניטור בזמן אמת</p>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-value" style="color:{on_time_color}">{on_time_rate}%</div>
        <div class="kpi-label">אחוז דיוק ברשת</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#e67e22">{kpis.get('avg_delay_min', 0)}</div>
        <div class="kpi-label">ממוצע איחור (דקות)</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#2980b9">{kpis.get('total_trips', 0):,}</div>
        <div class="kpi-label">סה"כ נסיעות</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#e74c3c">{kpis.get('total_delayed', 0):,}</div>
        <div class="kpi-label">נסיעות מאוחרות</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#8e44ad">{kpis.get('total_routes', 0)}</div>
        <div class="kpi-label">קווים פעילים</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#c0392b">{kpis.get('total_cancelled', 0)}</div>
        <div class="kpi-label">נסיעות שבוטלו</div>
      </div>
    </div>

    <div class="section">
      <h3>🚆 ביצועי רכבת ישראל</h3>
      <table>
        <tr><th>רכבות שנוטרו</th><th>מאוחרות</th><th>בוטלו</th><th>ממוצע איחור</th><th>מקסימום איחור</th></tr>
        <tr>
          <td>{train_kpis.get('trains_monitored', 0)}</td>
          <td style="color:#e74c3c">{train_kpis.get('delayed_trains', 0)}</td>
          <td style="color:#c0392b">{train_kpis.get('cancelled_trains', 0)}</td>
          <td>{train_kpis.get('avg_delay_min', 0)} דק'</td>
          <td>{train_kpis.get('max_delay_min', 0)} דק'</td>
        </tr>
      </table>
    </div>

    <div class="section">
      <h3>⏱️ ניתוח לפי שעות - שיעור איחורים</h3>
      <table><tr><th>שעה</th><th>תקופה</th><th>גרף</th><th>אחוז</th></tr>{peak_chart}</table>
    </div>

    <div style="display:flex; gap:20px; margin:0 20px;">
      <div class="section" style="flex:1">
        <h3>🔴 הקווים הבעייתיים ביותר</h3>
        <table><tr><th>קו</th><th>מפעיל</th><th>% איחורים</th><th>ממוצע</th></tr>{worst_rows}</table>
      </div>
      <div class="section" style="flex:1">
        <h3>🟢 הקווים הדייקנים ביותר</h3>
        <table><tr><th>קו</th><th>מפעיל</th><th>% בזמן</th></tr>{best_rows}</table>
      </div>
    </div>

    <div style="text-align:center;color:#999;font-size:11px;padding:20px">
      נוצר על ידי Israel Transit Intelligence Platform | Naya College Final Project 2025
    </div>
    </body></html>
    """

    output_path = f"/opt/airflow/logs/transit_report_{yesterday}.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Keep only last 7 daily reports — prevent disk fill in the airflow_logs volume
    import glob, os as _os
    old_reports = sorted(glob.glob("/opt/airflow/logs/transit_report_*.html"))
    for old in old_reports[:-7]:
        try:
            _os.unlink(old)
        except Exception:
            pass

    print(f"Daily report saved → {output_path}")
    print(f"Network on-time rate: {on_time_rate}% | Avg delay: {kpis.get('avg_delay_min')} min")


def index_bus_delays(**context):
    """
    Fetch bus delay data from Stride SIRI vehicle monitoring and index into
    the 'bus-delays' ES index that kibana_dashboard.ndjson watches.
    Fields required by dashboard: collected_at, delay_minutes, line_ref,
    operator_ref, is_delayed.
    """
    import requests
    from elasticsearch import Elasticsearch, helpers
    from datetime import datetime, timezone

    es = Elasticsearch(ES_HOST)
    _KWORD = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
    _ensure_index(es, "bus-delays", {"properties": {
        "collected_at":   {"type": "date"},
        "delay_minutes":  {"type": "float"},
        "delay_seconds":  {"type": "integer"},
        "line_ref":       _KWORD,
        "operator_ref":   _KWORD,
        "operator_name":  _KWORD,
        "vehicle_ref":    _KWORD,
        "is_delayed":     {"type": "boolean"},
        "lat":            {"type": "float"},
        "lon":            {"type": "float"},
    }})

    OPERATORS = {
        "3": "דן", "5": "אגד", "6": "NTA מטרופולין", "14": "מטרופולין",
        "15": "אגד תעבורה", "16": "תנופה", "18": "סופרבוס",
        "21": "קווים", "25": "נתיב אקספרס", "32": "V-Line", "42": "אפיקים",
    }

    now_utc = datetime.now(timezone.utc)
    docs = []
    try:
        resp = requests.get(
            "https://open-bus-stride-api.hasadna.org.il/siri_vehicle_monitoring/list",
            params={
                "limit": 5000,
                "lat__greater_or_equal": 31.97,
                "lat__lower_or_equal":   32.19,
                "lon__greater_or_equal": 34.73,
                "lon__lower_or_equal":   34.93,
            },
            timeout=15,
        )
        resp.raise_for_status()
        items = resp.json() if isinstance(resp.json(), list) else resp.json().get("results", [])
        for v in items:
            delay_sec = v.get("delay") or 0
            op_ref    = str(v.get("operator_ref") or v.get("siri_route__operator_ref") or "")
            docs.append({
                "_index": "bus-delays",
                "_source": {
                    "collected_at":  now_utc.isoformat(),
                    "delay_seconds": int(delay_sec),
                    "delay_minutes": round(delay_sec / 60, 2),
                    "line_ref":      str(v.get("line_ref") or v.get("siri_route__line_ref") or ""),
                    "operator_ref":  op_ref,
                    "operator_name": OPERATORS.get(op_ref, op_ref),
                    "vehicle_ref":   str(v.get("vehicle_ref") or ""),
                    "is_delayed":    bool(delay_sec >= 300),
                    "lat":           v.get("lat"),
                    "lon":           v.get("lon"),
                },
            })
    except Exception as e:
        print(f"bus-delays fetch error: {e}")

    if docs:
        success, _ = helpers.bulk(es, docs, raise_on_error=False)
        print(f"Indexed {success}/{len(docs)} docs into bus-delays")
    else:
        print("No bus-delay docs to index")


def index_train_delays(**context):
    """
    Fetch train positions from Stride (operator_ref=2) and index into
    the 'train-delays' ES index that kibana_dashboard.ndjson watches.
    Fields required by dashboard: collected_at, delay_minutes, station_id,
    route_origin, day_of_week, hour_of_day.
    """
    import requests
    from elasticsearch import Elasticsearch, helpers
    from datetime import datetime, timezone

    es = Elasticsearch(ES_HOST)
    _KWORD = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
    _ensure_index(es, "train-delays", {"properties": {
        "collected_at":  {"type": "date"},
        "delay_minutes": {"type": "float"},
        "delay_seconds": {"type": "integer"},
        "station_id":    _KWORD,
        "route_origin":  _KWORD,
        "vehicle_ref":   _KWORD,
        "is_delayed":    {"type": "boolean"},
        "day_of_week":   {"type": "integer"},
        "hour_of_day":   {"type": "integer"},
        "lat":           {"type": "float"},
        "lon":           {"type": "float"},
    }})

    now_utc = datetime.now(timezone.utc)
    docs = []
    try:
        resp = requests.get(
            STRIDE_URL,
            params={
                "siri_route__operator_ref": 2,
                "limit": 2000,
                "lat__greater_or_equal": 31.0,
                "lat__lower_or_equal":   33.5,
                "lon__greater_or_equal": 34.3,
                "lon__lower_or_equal":   35.9,
            },
            timeout=15,
        )
        resp.raise_for_status()
        items = resp.json() if isinstance(resp.json(), list) else resp.json().get("results", [])
        for v in items:
            delay_sec = v.get("delay") or 0
            ts = now_utc
            docs.append({
                "_index": "train-delays",
                "_source": {
                    "collected_at":  ts.isoformat(),
                    "delay_seconds": int(delay_sec),
                    "delay_minutes": round(delay_sec / 60, 2),
                    "station_id":    str(v.get("stop_point_ref") or v.get("station_id") or ""),
                    "route_origin":  str(v.get("origin_aimed_departure_time") or
                                        v.get("siri_route__line_ref") or ""),
                    "vehicle_ref":   str(v.get("vehicle_ref") or ""),
                    "is_delayed":    bool(delay_sec >= 300),
                    "day_of_week":   ts.weekday(),
                    "hour_of_day":   ts.hour,
                    "lat":           v.get("lat"),
                    "lon":           v.get("lon"),
                },
            })
    except Exception as e:
        print(f"train-delays fetch error: {e}")

    if docs:
        success, _ = helpers.bulk(es, docs, raise_on_error=False)
        print(f"Indexed {success}/{len(docs)} docs into train-delays")
    else:
        print("No train-delay docs to index")


# ─────────────────────────────────────────
# DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="dag_daily_analysis",
    default_args=default_args,
    description="Daily KPIs, aggregations, route performance report",
    schedule_interval="0 1 * * *",             # FIX: 01:00 UTC = 04:00 IL winter / 04:00 IL summer (DST-aware cron). timedelta(hours=24) drifts relative to start_date and does not honour a fixed clock time.
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    tags=["analytics", "daily", "transit", "israel"],
) as dag:

    t_agg     = PythonOperator(task_id="aggregate_delay_stats",   python_callable=aggregate_delay_stats)
    t_routes  = PythonOperator(task_id="aggregate_route_perf",    python_callable=aggregate_route_performance)
    t_kpis    = PythonOperator(task_id="calculate_kpis",          python_callable=calculate_kpis)
    t_bus_del = PythonOperator(task_id="index_bus_delays",        python_callable=index_bus_delays)
    t_trn_del = PythonOperator(task_id="index_train_delays",      python_callable=index_train_delays)
    t_report  = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_agg >> [t_routes, t_kpis] >> t_report
