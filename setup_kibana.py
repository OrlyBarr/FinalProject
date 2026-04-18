#!/usr/bin/env python3
"""
setup_kibana.py — Israel Transit Kibana Dashboard (v3 — fixed)
===============================================================
תיקונים:
  - שגיאת 'Cannot read mode' נפתרה: אין שימוש ב-seriesParams
  - גרפי זמן: מוגדר timeFrom=now-15d כדי לכסות 13-16 אפריל
  - כל הגרפים עם visState תואם Kibana 8

נתונים אמיתיים:
  transit-bus-positions   (11.9M) — _indexed_at, vehicle_id, speed_kmh,
    operator_name, route_id, current_status, bearing, stop_id, location
  transit-train-positions (449K)  — _indexed_at, vehicle_id, velocity,
    trip_id, train_number

Run: python3 setup_kibana.py
"""

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import requests, json, time

KIBANA  = "http://localhost:5601"
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}
BUS     = "transit-bus-positions"
TRAIN   = "transit-train-positions"

INDEX_PATTERNS = [
    {"id": BUS,   "title": BUS,   "timeFieldName": "_indexed_at"},
    {"id": TRAIN, "title": TRAIN, "timeFieldName": "_indexed_at"},
]

ALL_VIZ_IDS = [
    "kpi-bus-count", "kpi-bus-vehicles", "kpi-bus-speed",
    "kpi-bus-active", "kpi-train-count", "kpi-train-vehicles",
    "tl-bus", "tl-train",
    "spd-hist-bus", "spd-hist-train",
    "spd-time-bus", "spd-time-train",
    "pie-operator", "pie-status",
    "top-routes", "top-operators",
    "top-bus-veh", "top-train-veh",
    "bearing-hist",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def wait_for_kibana():
    print("⏳ Waiting for Kibana...")
    for i in range(30):
        try:
            r = requests.get(f"{KIBANA}/api/status", timeout=5)
            if r.status_code == 200:
                lvl = (r.json().get("status") or {}).get("overall", {}).get("level", "")
                if lvl in ("available", "green"):
                    print("✅ Kibana ready"); return True
        except Exception:
            pass
        print(f"  {i+1}/30..."); time.sleep(3)
    return False


def upsert_pattern(p):
    url = f"{KIBANA}/api/saved_objects/index-pattern/{p['id']}"
    requests.delete(url, headers=HEADERS)
    r = requests.post(url, headers=HEADERS,
                      json={"attributes": {"title": p["title"],
                                           "timeFieldName": p["timeFieldName"]}})
    print(f"{'✅' if r.status_code in (200,201) else '❌'} pattern: {p['title']}")


def set_default():
    requests.post(f"{KIBANA}/api/kibana/settings/defaultIndex",
                  headers=HEADERS, json={"value": BUS})


def ss(index, q=""):
    return json.dumps({"index": index,
                       "query": {"query": q, "language": "kuery"},
                       "filter": []})


def viz(vid, title, state, index, q=""):
    body = {"attributes": {
        "title": title,
        "visState": json.dumps(state),
        "uiStateJSON": "{}",
        "description": "",
        "kibanaSavedObjectMeta": {"searchSourceJSON": ss(index, q)},
    }}
    r = requests.post(f"{KIBANA}/api/saved_objects/visualization/{vid}",
                      headers=HEADERS, json=body)
    ok = r.status_code in (200, 201)
    print(f"{'✅' if ok else '❌'} {title}" +
          ("" if ok else f"  [{r.status_code}] {r.text[:120]}"))
    return ok


# ── agg factories ─────────────────────────────────────────────────────────────

def a_count(lbl="רשומות"):
    return {"id":"1","enabled":True,"type":"count","schema":"metric",
            "params":{"customLabel": lbl}}

def a_avg(field, lbl=""):
    return {"id":"1","enabled":True,"type":"avg","schema":"metric",
            "params":{"field": field, "customLabel": lbl or field}}

def a_card(field, lbl=""):
    return {"id":"1","enabled":True,"type":"cardinality","schema":"metric",
            "params":{"field": field, "customLabel": lbl or field}}

def a_date(field="_indexed_at"):
    return {"id":"2","enabled":True,"type":"date_histogram","schema":"segment",
            "params":{"field": field, "interval":"auto",
                      "min_doc_count":1, "useNormalizedEsInterval":True,
                      "drop_partials": False, "extended_bounds":{}}}

def a_terms(field, size=10, lbl=""):
    return {"id":"2","enabled":True,"type":"terms","schema":"segment",
            "params":{"field": field, "size": size,
                      "order":"desc", "orderBy":"1",
                      "otherBucket": True, "otherBucketLabel":"אחרים",
                      "missingBucket": False,
                      "customLabel": lbl or field}}

def a_hist(field, interval, lo, hi, lbl=""):
    return {"id":"2","enabled":True,"type":"histogram","schema":"segment",
            "params":{"field": field, "interval": interval,
                      "min_doc_count": True,
                      "has_extended_bounds": True,
                      "extended_bounds":{"min": lo, "max": hi},
                      "customLabel": lbl or field}}


# ── visState builders (Kibana 8 compatible) ───────────────────────────────────

def metric_vs(title, agg, sub="", schema="Blues",
              use_ranges=False, ranges=None, invert=False):
    return {
        "title": title, "type": "metric",
        "params": {
            "addTooltip": True, "addLegend": False, "type": "metric",
            "metric": {
                "percentageMode": False,
                "useRanges": use_ranges,
                "colorSchema": schema,
                "metricColorMode": "Background" if use_ranges else "None",
                "colorsRange": ranges or [{"from":0,"to":99999}],
                "invertColors": invert,
                "labels": {"show": True},
                "style": {"bgFill":"#000","bgColor": use_ranges,
                          "labelColor": False, "subText": sub, "fontSize": 40},
            },
        },
        "aggs": [agg],
    }


def line_vs(title, m_agg, y_lbl="", threshold=None):
    """
    Kibana 8 line chart — visState без seriesParams ב-top level.
    seriesParams מוגדר בתוך params בלבד.
    """
    series = [{
        "show": True, "type": "line", "mode": "normal",
        "data": {"label": y_lbl or "ערך", "id": "1"},
        "valueAxis": "ValueAxis-1",
        "drawLinesBetweenPoints": True,
        "lineWidth": 2,
        "interpolate": "linear",
        "showCircles": True,
    }]
    return {
        "title": title, "type": "line",
        "params": {
            "type": "line",
            "grid": {"categoryLines": False},
            "categoryAxes": [{
                "id": "CategoryAxis-1", "type": "category",
                "position": "bottom", "show": True,
                "style": {}, "scale": {"type": "linear"},
                "labels": {"show": True, "filter": True, "truncate": 100},
                "title": {},
            }],
            "valueAxes": [{
                "id": "ValueAxis-1", "name": "LeftAxis-1",
                "type": "value", "position": "left", "show": True,
                "style": {}, "scale": {"type": "linear", "mode": "normal"},
                "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                "title": {"text": y_lbl},
            }],
            "seriesParams": series,
            "addTooltip": True, "addLegend": False,
            "legendPosition": "right",
            "times": [], "addTimeMarker": False,
            "thresholdLine": {
                "show": bool(threshold),
                "value": threshold or 10,
                "width": 2, "style": "full", "color": "#E7664C",
            },
        },
        "aggs": [m_agg, a_date()],
    }


def hbar_vs(title, m_agg, s_agg, y_lbl=""):
    """Kibana 8 horizontal bar — seriesParams inside params."""
    series = [{
        "show": True, "type": "histogram", "mode": "stacked",
        "data": {"label": y_lbl or "ערך", "id": "1"},
        "valueAxis": "ValueAxis-1",
        "drawLinesBetweenPoints": True,
        "lineWidth": 2, "showCircles": True,
    }]
    return {
        "title": title, "type": "horizontal_bar",
        "params": {
            "type": "histogram",
            "grid": {"categoryLines": False},
            "categoryAxes": [{
                "id": "CategoryAxis-1", "type": "category",
                "position": "left", "show": True,
                "style": {}, "scale": {"type": "linear"},
                "labels": {"show": True, "rotate": 0,
                           "filter": True, "truncate": 200},
                "title": {},
            }],
            "valueAxes": [{
                "id": "ValueAxis-1", "name": "BottomAxis-1",
                "type": "value", "position": "bottom", "show": True,
                "style": {}, "scale": {"type": "linear", "mode": "normal"},
                "labels": {"show": True, "rotate": 0,
                           "filter": True, "truncate": 100},
                "title": {"text": y_lbl},
            }],
            "seriesParams": series,
            "addTooltip": True, "addLegend": False,
            "legendPosition": "right",
            "times": [], "addTimeMarker": False,
            "thresholdLine": {"show": False, "value": 10,
                              "width": 1, "style": "full", "color": "#E7664C"},
        },
        "aggs": [m_agg, s_agg],
    }


def hist_vs(title, m_agg, s_agg, y_lbl=""):
    """Kibana 8 vertical histogram."""
    series = [{
        "show": True, "type": "histogram", "mode": "stacked",
        "data": {"label": y_lbl or "ערך", "id": "1"},
        "valueAxis": "ValueAxis-1",
        "drawLinesBetweenPoints": True,
        "lineWidth": 2, "showCircles": True,
    }]
    return {
        "title": title, "type": "histogram",
        "params": {
            "type": "histogram",
            "grid": {"categoryLines": False},
            "categoryAxes": [{
                "id": "CategoryAxis-1", "type": "category",
                "position": "bottom", "show": True,
                "style": {}, "scale": {"type": "linear"},
                "labels": {"show": True, "filter": True, "truncate": 100},
                "title": {},
            }],
            "valueAxes": [{
                "id": "ValueAxis-1", "name": "LeftAxis-1",
                "type": "value", "position": "left", "show": True,
                "style": {}, "scale": {"type": "linear", "mode": "normal"},
                "labels": {"show": True, "rotate": 0,
                           "filter": False, "truncate": 100},
                "title": {"text": y_lbl},
            }],
            "seriesParams": series,
            "addTooltip": True, "addLegend": False,
            "legendPosition": "right",
            "times": [], "addTimeMarker": False,
            "thresholdLine": {"show": False, "value": 10,
                              "width": 1, "style": "full", "color": "#E7664C"},
        },
        "aggs": [m_agg, s_agg],
    }


def pie_vs(title, s_agg):
    return {
        "title": title, "type": "pie",
        "params": {
            "type": "pie", "addTooltip": True, "addLegend": True,
            "legendPosition": "right", "isDonut": True,
            "labels": {"show": True, "values": True,
                       "last_level": True, "truncate": 100},
        },
        "aggs": [a_count(), s_agg],
    }


# ── build all visualizations ──────────────────────────────────────────────────

def create_visualizations():
    done = []

    def add(vid, ok, x, y, w, h):
        if ok: done.append((vid, x, y, w, h))

    # ══ ROW 1 — KPI metrics (y=0, h=5) ══════════════════════════════════════

    add("kpi-bus-count", viz(
        "kpi-bus-count", "🚌 רשומות אוטובוסים",
        metric_vs("רשומות אוטובוסים", a_count("רשומות"), "רשומות ב-ES"),
        BUS,
    ), 0, 0, 8, 5)

    add("kpi-bus-vehicles", viz(
        "kpi-bus-vehicles", "🚌 אוטובוסים ייחודיים",
        metric_vs("אוטובוסים ייחודיים",
                  a_card("vehicle_id", "אוטובוסים"), "vehicle IDs"),
        BUS,
    ), 8, 0, 8, 5)

    add("kpi-bus-speed", viz(
        "kpi-bus-speed", "💨 מהירות ממוצעת אוטובוסים",
        metric_vs("מהירות ממוצעת",
                  a_avg("speed_kmh", "מהירות (קמ\"ש)"),
                  "קמ\"ש ממוצע",
                  schema="Green to Red",
                  use_ranges=True,
                  ranges=[{"from":0,"to":15},
                          {"from":15,"to":40},
                          {"from":40,"to":999}]),
        BUS, "speed_kmh > 0",
    ), 16, 0, 8, 5)

    add("kpi-bus-active", viz(
        "kpi-bus-active", "🟢 אוטובוסים In-Transit",
        metric_vs("In Transit",
                  a_count("בנסיעה"),
                  "current_status: in_transit",
                  schema="Green to Red",
                  use_ranges=True,
                  ranges=[{"from":0,"to":50},
                          {"from":50,"to":200},
                          {"from":200,"to":99999}],
                  invert=True),
        BUS, "current_status: in_transit",
    ), 24, 0, 8, 5)

    add("kpi-train-count", viz(
        "kpi-train-count", "🚆 רשומות רכבות",
        metric_vs("רשומות רכבות", a_count("רשומות"), "רשומות ב-ES",
                  schema="Blues"),
        TRAIN,
    ), 32, 0, 8, 5)

    add("kpi-train-vehicles", viz(
        "kpi-train-vehicles", "🚆 רכבות ייחודיות",
        metric_vs("רכבות ייחודיות",
                  a_card("vehicle_id", "רכבות"), "vehicle IDs",
                  schema="Blues"),
        TRAIN,
    ), 40, 0, 8, 5)

    # ══ ROW 2 — Timelines (y=5, h=10) ════════════════════════════════════════

    add("tl-bus", viz(
        "tl-bus", "📈 פעילות אוטובוסים לאורך זמן",
        line_vs("פעילות אוטובוסים לאורך זמן",
                a_count("רשומות"), "רשומות"),
        BUS,
    ), 0, 5, 24, 10)

    add("tl-train", viz(
        "tl-train", "📈 פעילות רכבות לאורך זמן",
        line_vs("פעילות רכבות לאורך זמן",
                a_count("רשומות"), "רשומות"),
        TRAIN,
    ), 24, 5, 24, 10)

    # ══ ROW 3 — Speed histograms (y=15, h=10) ════════════════════════════════

    add("spd-hist-bus", viz(
        "spd-hist-bus", "💨 התפלגות מהירות אוטובוסים",
        hist_vs("התפלגות מהירות אוטובוסים",
                a_count("כלי רכב"),
                a_hist("speed_kmh", 5, 0, 120, "מהירות (קמ\"ש)"),
                "מספר כלי רכב"),
        BUS, "speed_kmh > 0",
    ), 0, 15, 24, 10)

    add("spd-hist-train", viz(
        "spd-hist-train", "💨 התפלגות מהירות רכבות",
        hist_vs("התפלגות מהירות רכבות",
                a_count("רכבות"),
                a_hist("velocity", 10, 0, 160, "מהירות (קמ\"ש)"),
                "מספר רכבות"),
        TRAIN, "velocity > 0",
    ), 24, 15, 24, 10)

    # ══ ROW 4 — Speed over time (y=25, h=10) ════════════════════════════════

    add("spd-time-bus", viz(
        "spd-time-bus", "📉 מהירות ממוצעת אוטובוסים לאורך זמן",
        line_vs("מהירות ממוצעת אוטובוסים לאורך זמן",
                a_avg("speed_kmh", "מהירות (קמ\"ש)"),
                "קמ\"ש", threshold=20),
        BUS, "speed_kmh > 0",
    ), 0, 25, 24, 10)

    add("spd-time-train", viz(
        "spd-time-train", "📉 מהירות ממוצעת רכבות לאורך זמן",
        line_vs("מהירות ממוצעת רכבות לאורך זמן",
                a_avg("velocity", "מהירות (קמ\"ש)"),
                "קמ\"ש"),
        TRAIN, "velocity > 0",
    ), 24, 25, 24, 10)

    # ══ ROW 5 — Pie charts (y=35, h=10) ══════════════════════════════════════

    add("pie-operator", viz(
        "pie-operator", "🏢 פילוח לפי מפעיל",
        pie_vs("פילוח לפי מפעיל",
               a_terms("operator_name.keyword", 12, "מפעיל")),
        BUS,
    ), 0, 35, 24, 10)

    add("pie-status", viz(
        "pie-status", "🟢 פילוח לפי סטטוס",
        pie_vs("פילוח לפי סטטוס",
               a_terms("current_status.keyword", 5, "סטטוס")),
        BUS,
    ), 24, 35, 24, 10)

    # ══ ROW 6 — Top routes + operators (y=45, h=12) ══════════════════════════

    add("top-routes", viz(
        "top-routes", "🛣️ Top 15 קווים פעילים",
        hbar_vs("Top קווים פעילים",
                a_count("רשומות"),
                a_terms("route_id.keyword", 15, "קו (route_id)"),
                "מספר רשומות"),
        BUS,
    ), 0, 45, 24, 12)

    add("top-operators", viz(
        "top-operators", "🏢 Top מפעילים לפי פעילות",
        hbar_vs("Top מפעילים",
                a_count("רשומות"),
                a_terms("operator_name.keyword", 15, "מפעיל"),
                "מספר רשומות"),
        BUS,
    ), 24, 45, 24, 12)

    # ══ ROW 7 — Top vehicles (y=57, h=12) ════════════════════════════════════

    add("top-bus-veh", viz(
        "top-bus-veh", "🚌 Top 15 אוטובוסים פעילים",
        hbar_vs("Top אוטובוסים פעילים",
                a_count("רשומות"),
                a_terms("vehicle_id.keyword", 15, "vehicle_id"),
                "מספר רשומות"),
        BUS,
    ), 0, 57, 24, 12)

    add("top-train-veh", viz(
        "top-train-veh", "🚆 Top 15 רכבות פעילות",
        hbar_vs("Top רכבות פעילות",
                a_count("רשומות"),
                a_terms("vehicle_id.keyword", 15, "vehicle_id"),
                "מספר רשומות"),
        TRAIN,
    ), 24, 57, 24, 12)

    # ══ ROW 8 — Bearing (y=69, h=10) ═════════════════════════════════════════

    add("bearing-hist", viz(
        "bearing-hist", "🧭 התפלגות כיוון נסיעה (Bearing 0°–360°)",
        hist_vs("התפלגות כיוון נסיעה",
                a_count("רשומות"),
                a_hist("bearing", 10, 0, 360, "כיוון (מעלות)"),
                "מספר רשומות"),
        BUS,
    ), 0, 69, 48, 10)

    return done


# ── dashboard ─────────────────────────────────────────────────────────────────

def create_dashboard(viz_list):
    did = "israel-transit-dashboard"
    requests.delete(f"{KIBANA}/api/saved_objects/dashboard/{did}", headers=HEADERS)

    panels = []
    refs   = []
    for i, (vid, gx, gy, gw, gh) in enumerate(viz_list):
        panels.append({
            "version": "8.8.0",
            "gridData": {"x": gx, "y": gy, "w": gw, "h": gh, "i": str(i)},
            "panelIndex": str(i),
            "embeddableConfig": {"enhancements": {}},
            "panelRefName": f"p{i}",
        })
        refs.append({"name": f"p{i}", "type": "visualization", "id": vid})

    body = {
        "attributes": {
            "title":       "🚌 Israel Transit — Real-Time Dashboard",
            "description": "ניטור אוטובוסים ורכבות בישראל | bus-positions + train-positions",
            "hits": 0,
            "timeRestore": True,
            "timeFrom":    "now-15d",   # מכסה 13–16 אפריל
            "timeTo":      "now",
            "refreshInterval": {"pause": False, "value": 60000},
            "panelsJSON":  json.dumps(panels),
            "optionsJSON": json.dumps({"useMargins": True,
                                       "syncColors": False,
                                       "hidePanelTitles": False}),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        },
        "references": refs,
    }

    r = requests.post(f"{KIBANA}/api/saved_objects/dashboard/{did}",
                      headers=HEADERS, json=body)
    if r.status_code in (200, 201):
        print(f"\n✅ Dashboard created!")
        print(f"   👉 http://localhost:5601/app/dashboards#/view/{did}")
    else:
        print(f"❌ {r.status_code}: {r.text[:300]}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n🚌 Israel Transit — Kibana Setup v3\n")

    print("🧹 Cleaning...")
    for t, i in ([("dashboard", "israel-transit-dashboard")] +
                 [("visualization", v) for v in ALL_VIZ_IDS]):
        requests.delete(f"{KIBANA}/api/saved_objects/{t}/{i}", headers=HEADERS)

    if not wait_for_kibana():
        print("❌ Kibana not available"); return

    print("\n📋 Index patterns (timeField=_indexed_at)...")
    for p in INDEX_PATTERNS:
        upsert_pattern(p)
    set_default()

    print("\n📊 Visualizations...")
    vl = create_visualizations()
    print(f"\n   {len(vl)}/{len(ALL_VIZ_IDS)} created")

    print("\n🖥️  Dashboard...")
    create_dashboard(vl)

    print("\n✅ Done!")
    print("   http://localhost:5601/app/dashboards")
    print("\n💡 אם גרפי הזמן עדיין ריקים — לחצי 'Refresh' ב-Kibana")
    print("   ווודאי שטווח הזמן הוא לפחות 'Last 15 days'")


if __name__ == "__main__":
    main()