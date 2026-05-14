#!/usr/bin/env python3
"""
Create Kibana index patterns, visualizations, and dashboards for Israel Transit.
Runs against localhost:5601 (inside the VM or with port-forward).

Usage:
    python3 scripts/create_kibana_dashboards.py
"""

import json
import urllib.request
import urllib.error
import sys
import time

KIBANA_URL = "http://localhost:5601"
HEADERS = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json",
}


def kib_post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{KIBANA_URL}{path}", data=data,
        headers=HEADERS, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        content = e.read().decode()
        # 409 = already exists, that's fine
        if e.code == 409:
            return {"_id": "exists"}
        print(f"  HTTP {e.code} on {path}: {content[:200]}")
        return None
    except Exception as ex:
        print(f"  Error on {path}: {ex}")
        return None


def kib_put_saved_object(obj_type, obj_id, attrs, overwrite=True):
    path = f"/api/saved_objects/{obj_type}/{obj_id}?overwrite={str(overwrite).lower()}"
    data = json.dumps({"attributes": attrs}).encode()
    req = urllib.request.Request(
        f"{KIBANA_URL}{path}", data=data,
        headers=HEADERS, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        content = e.read().decode()
        print(f"  HTTP {e.code} on {path}: {content[:200]}")
        return None
    except Exception as ex:
        print(f"  Error on {path}: {ex}")
        return None


def wait_for_kibana(retries=12, delay=10):
    print("Waiting for Kibana to be ready...", end="", flush=True)
    for i in range(retries):
        try:
            req = urllib.request.Request(
                f"{KIBANA_URL}/api/status",
                headers={"kbn-xsrf": "true"},
            )
            with urllib.request.urlopen(req, timeout=8) as r:
                st = json.loads(r.read())
                # Kibana 8.x uses "level" field with value "available"; older versions use "state"=="green"
                overall = st.get("status", {}).get("overall", {})
                level = overall.get("level", overall.get("state", "?"))
                if level in ("available", "green"):
                    print(f" ready (level={level})")
                    return True
                print(f" {level}", end="", flush=True)
        except Exception:
            print(".", end="", flush=True)
        time.sleep(delay)
    print(" timeout!")
    return False


def create_index_pattern(index_id, title, time_field):
    print(f"  Index pattern: {title}")
    return kib_put_saved_object("index-pattern", index_id, {
        "title": title,
        "timeFieldName": time_field,
    })


def main():
    if not wait_for_kibana():
        print("Kibana not ready — aborting.")
        sys.exit(1)

    print("\n=== Creating Index Patterns ===")
    create_index_pattern("ip-bus-positions",   "transit-bus-positions",   "fetched_at")
    create_index_pattern("ip-train-positions", "transit-train-positions", "recorded_at")
    create_index_pattern("ip-traffic",         "transit-traffic",         "recorded_at")
    create_index_pattern("ip-delay-events",    "transit-delay-events",    "fetched_at")

    # ── Visualizations ────────────────────────────────────────────────────────
    print("\n=== Creating Visualizations ===")

    # 1. Vehicles by operator (pie)
    print("  [1] Vehicles by operator (pie)")
    kib_put_saved_object("visualization", "vis-bus-by-operator", {
        "title": "אוטובוסים פעילים לפי מפעיל",
        "visState": json.dumps({
            "title": "אוטובוסים פעילים לפי מפעיל",
            "type": "pie",
            "params": {
                "addLegend": True, "addTooltip": True,
                "isDonut": True, "legendPosition": "right",
                "labels": {"show": True, "values": True, "last_level": True, "truncate": 100},
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
                 "params": {"field": "operator_name.keyword", "size": 15,
                            "order": "desc", "orderBy": "1"}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "פיצול כלי רכב לפי מפעיל",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-bus-positions",
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # 2. Active buses count (metric)
    print("  [2] Active buses count (metric)")
    kib_put_saved_object("visualization", "vis-bus-count", {
        "title": "סה\"כ כלי רכב פעילים",
        "visState": json.dumps({
            "title": "סה\"כ כלי רכב פעילים",
            "type": "metric",
            "params": {
                "addLegend": False, "addTooltip": True,
                "metric": {
                    "percentageMode": False,
                    "useRanges": False,
                    "colorSchema": "Green to Red",
                    "metricColorMode": "None",
                    "colorsRange": [{"from": 0, "to": 10000}],
                    "labels": {"show": True},
                    "invertColors": False,
                    "style": {"bgFill": "#000", "bgColor": False, "labelColor": False,
                              "subText": "", "fontSize": 60},
                },
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "מספר אוטובוסים פעילים כרגע",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-bus-positions",
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # 3. Bus positions over time (line chart)
    print("  [3] Ingestion rate over time (bus)")
    kib_put_saved_object("visualization", "vis-bus-over-time", {
        "title": "קצב קליטת נתוני אוטובוסים",
        "visState": json.dumps({
            "title": "קצב קליטת נתוני אוטובוסים",
            "type": "line",
            "params": {
                "grid": {"categoryLines": False},
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "bottom", "show": True,
                                  "style": {}, "scale": {"type": "linear"},
                                  "labels": {"show": True, "truncate": 100},
                                  "title": {}}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1",
                               "type": "value", "position": "left", "show": True,
                               "style": {}, "scale": {"type": "linear", "mode": "normal"},
                               "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                               "title": {"text": "כלי רכב"}}],
                "seriesParams": [{"show": True, "type": "line", "mode": "normal",
                                  "data": {"label": "כלי רכב", "id": "1"},
                                  "valueAxis": "ValueAxis-1",
                                  "drawLinesBetweenPoints": True,
                                  "lineWidth": 2, "interpolate": "linear",
                                  "showCircles": True}],
                "addTooltip": True, "addLegend": True, "legendPosition": "right",
                "times": [], "addTimeMarker": False,
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
                 "params": {"field": "fetched_at", "interval": "auto",
                            "min_doc_count": 1, "extended_bounds": {}}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "קצב קליטת נתוני אוטובוסים לאורך זמן",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-bus-positions",
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # 4. Train count metric
    print("  [4] Train count (metric)")
    kib_put_saved_object("visualization", "vis-train-count", {
        "title": "סה\"כ רכבות",
        "visState": json.dumps({
            "title": "סה\"כ רכבות",
            "type": "metric",
            "params": {
                "metric": {
                    "percentageMode": False,
                    "useRanges": False,
                    "colorSchema": "Green to Red",
                    "metricColorMode": "None",
                    "colorsRange": [{"from": 0, "to": 10000}],
                    "labels": {"show": True},
                    "style": {"fontSize": 60},
                },
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "מספר רכבות",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-train-positions",
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # 5. Train ingestion rate over time
    print("  [5] Train ingestion rate over time")
    kib_put_saved_object("visualization", "vis-train-over-time", {
        "title": "קצב קליטת נתוני רכבות",
        "visState": json.dumps({
            "title": "קצב קליטת נתוני רכבות",
            "type": "line",
            "params": {
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "bottom", "show": True,
                                  "labels": {"show": True, "truncate": 100}, "title": {}}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1",
                               "type": "value", "position": "left", "show": True,
                               "labels": {"show": True},
                               "title": {"text": "רכבות"}}],
                "seriesParams": [{"show": True, "type": "line", "mode": "normal",
                                  "data": {"label": "רכבות", "id": "1"},
                                  "valueAxis": "ValueAxis-1", "lineWidth": 2,
                                  "drawLinesBetweenPoints": True, "showCircles": True}],
                "addTooltip": True, "addLegend": True,
                "legendPosition": "right", "times": [], "addTimeMarker": False,
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
                 "params": {"field": "recorded_at", "interval": "auto",
                            "min_doc_count": 1, "extended_bounds": {}}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "קצב קליטת נתוני רכבות לאורך זמן",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-train-positions",
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # 6. Traffic — jam_factor by road
    print("  [6] Traffic jam factor by road")
    kib_put_saved_object("visualization", "vis-traffic-jam-roads", {
        "title": "עומס תנועה לפי כביש",
        "visState": json.dumps({
            "title": "עומס תנועה לפי כביש",
            "type": "horizontal_bar",
            "params": {
                "type": "histogram", "grid": {"categoryLines": False},
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "left", "show": True,
                                  "labels": {"show": True, "truncate": 200}, "title": {}}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1",
                               "type": "value", "position": "bottom", "show": True,
                               "labels": {"show": True},
                               "title": {"text": "מקדם עומס"}}],
                "seriesParams": [{"show": True, "type": "histogram", "mode": "normal",
                                  "data": {"label": "מקדם עומס", "id": "1"},
                                  "valueAxis": "ValueAxis-1"}],
                "addTooltip": True, "addLegend": True, "legendPosition": "right",
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "avg", "schema": "metric",
                 "params": {"field": "jam_factor"}},
                {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
                 "params": {"field": "road_name.keyword", "size": 15,
                            "order": "desc", "orderBy": "1"}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "מקדם עומס ממוצע לפי כביש",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-traffic",
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # 7. Traffic speed over time
    print("  [7] Traffic speed over time")
    kib_put_saved_object("visualization", "vis-traffic-speed-time", {
        "title": "מהירות תנועה לאורך זמן",
        "visState": json.dumps({
            "title": "מהירות תנועה לאורך זמן",
            "type": "line",
            "params": {
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "bottom", "show": True,
                                  "labels": {"show": True, "truncate": 100}, "title": {}}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1",
                               "type": "value", "position": "left", "show": True,
                               "labels": {"show": True},
                               "title": {"text": "מהירות (קמ\"ש)"}}],
                "seriesParams": [{"show": True, "type": "line", "mode": "normal",
                                  "data": {"label": "מהירות ממוצעת", "id": "1"},
                                  "valueAxis": "ValueAxis-1", "lineWidth": 2,
                                  "drawLinesBetweenPoints": True, "showCircles": False}],
                "addTooltip": True, "addLegend": True, "legendPosition": "right",
                "times": [], "addTimeMarker": False,
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "avg", "schema": "metric",
                 "params": {"field": "speed_kmh"}},
                {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
                 "params": {"field": "recorded_at", "interval": "auto",
                            "min_doc_count": 1, "extended_bounds": {}}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "מהירות תנועה ממוצעת לאורך זמן",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-traffic",
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # 8. Congested segments count (metric)
    print("  [8] Congested segments metric")
    kib_put_saved_object("visualization", "vis-traffic-congested", {
        "title": "קטעים עמוסים",
        "visState": json.dumps({
            "title": "קטעים עמוסים",
            "type": "metric",
            "params": {
                "metric": {
                    "percentageMode": False,
                    "useRanges": True,
                    "colorSchema": "Green to Red",
                    "metricColorMode": "Background",
                    "colorsRange": [
                        {"from": 0, "to": 10},
                        {"from": 10, "to": 50},
                        {"from": 50, "to": 10000},
                    ],
                    "labels": {"show": True},
                    "style": {"fontSize": 45},
                },
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric",
                 "params": {}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "מספר קטעי כביש עמוסים",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-traffic",
                "query": {"query": "is_congested: true", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # 9. Top active bus lines (bar)
    print("  [9] Top active bus lines")
    kib_put_saved_object("visualization", "vis-top-bus-lines", {
        "title": "קווי אוטובוס פעילים ביותר",
        "visState": json.dumps({
            "title": "קווי אוטובוס פעילים ביותר",
            "type": "horizontal_bar",
            "params": {
                "type": "histogram", "grid": {"categoryLines": False},
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "left", "show": True,
                                  "labels": {"show": True, "truncate": 100}, "title": {}}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1",
                               "type": "value", "position": "bottom", "show": True,
                               "labels": {"show": True},
                               "title": {"text": "מספר כלי רכב"}}],
                "seriesParams": [{"show": True, "type": "histogram", "mode": "normal",
                                  "data": {"label": "כלי רכב", "id": "1"},
                                  "valueAxis": "ValueAxis-1"}],
                "addTooltip": True, "addLegend": False,
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
                 "params": {"field": "route_short_name.keyword", "size": 20,
                            "order": "desc", "orderBy": "1"}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "קווי אוטובוס עם הכי הרבה כלי רכב",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-bus-positions",
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # 10. Delayed trains metric
    print("  [10] Delayed trains metric")
    kib_put_saved_object("visualization", "vis-delayed-trains", {
        "title": "רכבות מאוחרות",
        "visState": json.dumps({
            "title": "רכבות מאוחרות",
            "type": "metric",
            "params": {
                "metric": {
                    "percentageMode": False,
                    "useRanges": True,
                    "colorSchema": "Green to Red",
                    "metricColorMode": "Background",
                    "colorsRange": [
                        {"from": 0, "to": 5},
                        {"from": 5, "to": 20},
                        {"from": 20, "to": 10000},
                    ],
                    "labels": {"show": True},
                    "style": {"fontSize": 45},
                },
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            ],
        }),
        "uiStateJSON": "{}",
        "description": "מספר רכבות מאוחרות",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "index": "ip-train-positions",
                "query": {"query": "is_delayed: true", "language": "kuery"},
                "filter": [],
            })
        },
    })

    # ── Dashboards ────────────────────────────────────────────────────────────
    print("\n=== Creating Dashboards ===")

    # Dashboard 1: ניטור תחבורה ציבורית בזמן אמת
    print("  [Dashboard 1] ניטור תחבורה ציבורית בזמן אמת")
    panels_1 = [
        {
            "panelIndex": "1", "gridData": {"x": 0, "y": 0, "w": 12, "h": 6, "i": "1"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-bus-count",
            "embeddableConfig": {"title": "אוטובוסים פעילים"},
        },
        {
            "panelIndex": "2", "gridData": {"x": 12, "y": 0, "w": 12, "h": 6, "i": "2"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-train-count",
            "embeddableConfig": {"title": "רכבות"},
        },
        {
            "panelIndex": "3", "gridData": {"x": 24, "y": 0, "w": 12, "h": 6, "i": "3"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-traffic-congested",
            "embeddableConfig": {"title": "קטעים עמוסים"},
        },
        {
            "panelIndex": "4", "gridData": {"x": 0, "y": 6, "w": 18, "h": 14, "i": "4"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-bus-by-operator",
            "embeddableConfig": {"title": "אוטובוסים לפי מפעיל"},
        },
        {
            "panelIndex": "5", "gridData": {"x": 18, "y": 6, "w": 30, "h": 14, "i": "5"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-top-bus-lines",
            "embeddableConfig": {"title": "קווים פעילים"},
        },
        {
            "panelIndex": "6", "gridData": {"x": 0, "y": 20, "w": 48, "h": 12, "i": "6"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-bus-over-time",
            "embeddableConfig": {"title": "קצב קליטת נתונים"},
        },
        {
            "panelIndex": "7", "gridData": {"x": 0, "y": 32, "w": 48, "h": 12, "i": "7"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-train-over-time",
            "embeddableConfig": {"title": "קצב קליטת נתוני רכבות"},
        },
    ]

    kib_put_saved_object("dashboard", "dashboard-transit-realtime", {
        "title": "ניטור תחבורה ציבורית — זמן אמת",
        "hits": 0,
        "description": "מעקב בזמן אמת אחר כלי רכב, עיכובים ועומס תנועה בגוש דן",
        "panelsJSON": json.dumps(panels_1),
        "optionsJSON": json.dumps({
            "useMargins": True,
            "hidePanelTitles": False,
            "syncColors": False,
        }),
        "timeRestore": True,
        "timeTo": "now",
        "timeFrom": "now-6h",
        "refreshInterval": {"pause": False, "value": 60000},
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []}),
        },
    })

    # Dashboard 2: ניתוח תנועה ועיכובים
    print("  [Dashboard 2] ניתוח תנועה ועיכובים")
    panels_2 = [
        {
            "panelIndex": "1", "gridData": {"x": 0, "y": 0, "w": 16, "h": 6, "i": "1"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-traffic-congested",
            "embeddableConfig": {"title": "קטעים עמוסים"},
        },
        {
            "panelIndex": "2", "gridData": {"x": 16, "y": 0, "w": 16, "h": 6, "i": "2"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-delayed-trains",
            "embeddableConfig": {"title": "רכבות מאוחרות"},
        },
        {
            "panelIndex": "3", "gridData": {"x": 32, "y": 0, "w": 16, "h": 6, "i": "3"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-train-count",
            "embeddableConfig": {"title": "סה\"כ רכבות"},
        },
        {
            "panelIndex": "4", "gridData": {"x": 0, "y": 6, "w": 48, "h": 14, "i": "4"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-traffic-jam-roads",
            "embeddableConfig": {"title": "עומס תנועה לפי כביש"},
        },
        {
            "panelIndex": "5", "gridData": {"x": 0, "y": 20, "w": 48, "h": 12, "i": "5"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-traffic-speed-time",
            "embeddableConfig": {"title": "מהירות תנועה לאורך זמן"},
        },
        {
            "panelIndex": "6", "gridData": {"x": 0, "y": 32, "w": 48, "h": 12, "i": "6"},
            "version": "8.8.0",
            "type": "visualization", "id": "vis-train-over-time",
            "embeddableConfig": {"title": "נסיעות רכבות לאורך זמן"},
        },
    ]

    kib_put_saved_object("dashboard", "dashboard-traffic-delays", {
        "title": "ניתוח תנועה ועיכובים — גוש דן",
        "hits": 0,
        "description": "ניתוח עומסי תנועה, עיכובי רכבות ומהירות ממוצעת באזור גוש דן",
        "panelsJSON": json.dumps(panels_2),
        "optionsJSON": json.dumps({
            "useMargins": True,
            "hidePanelTitles": False,
            "syncColors": False,
        }),
        "timeRestore": True,
        "timeTo": "now",
        "timeFrom": "now-24h",
        "refreshInterval": {"pause": False, "value": 120000},
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []}),
        },
    })

    print("\n✅ Done!")
    print(f"  Dashboard 1: {KIBANA_URL}/app/kibana#/dashboard/dashboard-transit-realtime")
    print(f"  Dashboard 2: {KIBANA_URL}/app/kibana#/dashboard/dashboard-traffic-delays")
    print(f"  Index patterns: {KIBANA_URL}/app/kibana#/management/kibana/index_patterns")


if __name__ == "__main__":
    main()
