#!/usr/bin/env python3
"""
setup_kibana.py — Israel Transit Kibana Dashboard (v4)
=======================================================
עדכונים:
  - סינון לגוש דן ותל אביב (area:gush_dan)
  - תמיכה ב-is_moving flag (מהירות אופציונלית ב-SIRI)
  - speed_kmh יכול להיות null — גרפים מסננים כראוי
  - גרפי is_moving vs parked
  - operator_name תקין (ללא "operator_")
  - route_short_name — מספר קו קריא

נתונים:
  transit-bus-positions   — _indexed_at, vehicle_id, speed_kmh (nullable),
    operator_name, route_short_name, line_ref, is_moving, area, location
  transit-train-positions — _indexed_at, vehicle_id, velocity (nullable),
    train_number, line_ref, is_moving, area

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

# פילטר גלובלי — גוש דן ותל אביב בלבד
# נתונים ישנים לפני השינוי לא יסוננו (אין להם שדה area)
GUSH_DAN_FILTER = 'area:gush_dan OR NOT area:*'

INDEX_PATTERNS = [
    {"id": BUS,   "title": BUS,   "timeFieldName": "_indexed_at"},
    {"id": TRAIN, "title": TRAIN, "timeFieldName": "_indexed_at"},
]

ALL_VIZ_IDS = [
    # ROW 1 — KPIs
    "kpi-bus-count", "kpi-bus-moving", "kpi-bus-speed",
    "kpi-bus-operators", "kpi-train-count", "kpi-train-moving",
    # ROW 2 — Timelines
    "tl-bus", "tl-train",
    # ROW 3 — Moving vs Parked
    "moving-pie-bus", "moving-pie-train",
    # ROW 4 — Speed
    "spd-hist-bus", "spd-time-bus",
    # ROW 5 — Operators & Lines
    "bar-operators", "bar-lines",
    # ROW 6 — Hourly activity
    "hourly-bus",
    # ROW 7 — Top vehicles & stops
    "top-bus-veh", "top-train-veh",
    "top-stops", "bearing-hist",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def wait_for_kibana():
    print("⏳ Waiting for Kibana...")
    for i in range(30):
        try:
            r = requests.get(f"{KIBANA}/api/status", timeout=5)
            if r.status_code == 200:
                d   = r.json()
                lvl = d.get("overall", {}).get("level") or d.get("status", {}).get("overall", {}).get("state") or "available"
                if lvl in ("available", "green", "all_green", "some_degraded"):
                    print(f"✅ Kibana ready ({lvl})")
                    return True
                print(f"  Kibana status: {lvl} — waiting...")
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
          ("" if ok else f"  [{r.status_code}] {r.text[:100]}"))
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


# ── visState builders ─────────────────────────────────────────────────────────

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
    series = [{
        "show": True, "type": "line", "mode": "normal",
        "data": {"label": y_lbl or "ערך", "id": "1"},
        "valueAxis": "ValueAxis-1",
        "drawLinesBetweenPoints": True,
        "lineWidth": 2, "interpolate": "linear", "showCircles": True,
    }]
    return {
        "title": title, "type": "line",
        "params": {
            "type": "line", "grid": {"categoryLines": False},
            "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                              "position": "bottom", "show": True, "style": {},
                              "scale": {"type": "linear"},
                              "labels": {"show": True, "filter": True, "truncate": 100},
                              "title": {}}],
            "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1",
                           "type": "value", "position": "left", "show": True,
                           "style": {}, "scale": {"type": "linear", "mode": "normal"},
                           "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                           "title": {"text": y_lbl}}],
            "seriesParams": series,
            "addTooltip": True, "addLegend": False, "legendPosition": "right",
            "times": [], "addTimeMarker": False,
            "thresholdLine": {"show": bool(threshold), "value": threshold or 10,
                              "width": 2, "style": "full", "color": "#E7664C"},
        },
        "aggs": [m_agg, a_date()],
    }


def hbar_vs(title, m_agg, s_agg, y_lbl=""):
    series = [{"show": True, "type": "histogram", "mode": "stacked",
               "data": {"label": y_lbl or "ערך", "id": "1"},
               "valueAxis": "ValueAxis-1",
               "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True}]
    return {
        "title": title, "type": "horizontal_bar",
        "params": {
            "type": "histogram", "grid": {"categoryLines": False},
            "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                              "position": "left", "show": True, "style": {},
                              "scale": {"type": "linear"},
                              "labels": {"show": True, "rotate": 0,
                                         "filter": True, "truncate": 200},
                              "title": {}}],
            "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1",
                           "type": "value", "position": "bottom", "show": True,
                           "style": {}, "scale": {"type": "linear", "mode": "normal"},
                           "labels": {"show": True, "rotate": 0,
                                      "filter": True, "truncate": 100},
                           "title": {"text": y_lbl}}],
            "seriesParams": series,
            "addTooltip": True, "addLegend": False, "legendPosition": "right",
            "times": [], "addTimeMarker": False,
            "thresholdLine": {"show": False, "value": 10,
                              "width": 1, "style": "full", "color": "#E7664C"},
        },
        "aggs": [m_agg, s_agg],
    }


def hist_vs(title, m_agg, s_agg, y_lbl=""):
    series = [{"show": True, "type": "histogram", "mode": "stacked",
               "data": {"label": y_lbl or "ערך", "id": "1"},
               "valueAxis": "ValueAxis-1",
               "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True}]
    return {
        "title": title, "type": "histogram",
        "params": {
            "type": "histogram", "grid": {"categoryLines": False},
            "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                              "position": "bottom", "show": True, "style": {},
                              "scale": {"type": "linear"},
                              "labels": {"show": True, "filter": True, "truncate": 100},
                              "title": {}}],
            "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1",
                           "type": "value", "position": "left", "show": True,
                           "style": {}, "scale": {"type": "linear", "mode": "normal"},
                           "labels": {"show": True, "rotate": 0,
                                      "filter": False, "truncate": 100},
                           "title": {"text": y_lbl}}],
            "seriesParams": series,
            "addTooltip": True, "addLegend": False, "legendPosition": "right",
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


# ── create visualizations ─────────────────────────────────────────────────────

def create_visualizations():
    done = []

    def add(vid, ok, x, y, w, h):
        if ok: done.append((vid, x, y, w, h))

    # ══ ROW 1 — KPI (y=0, h=5) ═══════════════════════════════════════════════

    # סה"כ רשומות אוטובוסים בגוש דן
    add("kpi-bus-count", viz(
        "kpi-bus-count", "🚌 רשומות — גוש דן",
        metric_vs("רשומות אוטובוסים", a_count("רשומות"), "גוש דן + ת\"א"),
        BUS,
    ), 0, 0, 8, 5)

    # אוטובוסים נוסעים (is_moving:true)
    add("kpi-bus-moving", viz(
        "kpi-bus-moving", "🟢 אוטובוסים נוסעים",
        metric_vs("נוסעים כרגע", a_count("נוסעים"),
                  "is_moving = true",
                  schema="Green to Red", use_ranges=True,
                  ranges=[{"from":0,"to":50},{"from":50,"to":200},{"from":200,"to":99999}],
                  invert=True),
        BUS, "is_moving: true",
    ), 8, 0, 8, 5)

    # מהירות ממוצעת — רק כלי רכב שמדווחים מהירות
    add("kpi-bus-speed", viz(
        "kpi-bus-speed", "💨 מהירות ממוצעת",
        metric_vs("מהירות ממוצעת",
                  a_avg("speed_kmh", "מהירות (קמ\"ש)"),
                  "קמ\"ש | רק מדווחי מהירות",
                  schema="Green to Red", use_ranges=True,
                  ranges=[{"from":0,"to":15},{"from":15,"to":40},{"from":40,"to":999}]),
        BUS, "is_moving: true AND speed_kmh > 0",
    ), 16, 0, 8, 5)

    # מפעילים ייחודיים
    add("kpi-bus-operators", viz(
        "kpi-bus-operators", "🏢 מפעילים פעילים",
        metric_vs("מפעילים", a_card("operator_name", "מפעילים"),
                  "חברות מפעילות"),
        BUS, 'NOT operator_name:"Unknown"',
    ), 24, 0, 8, 5)

    # רכבות — רשומות
    add("kpi-train-count", viz(
        "kpi-train-count", "🚆 רשומות רכבות",
        metric_vs("רשומות רכבות", a_count("רשומות"),
                  "גוש דן + ת\"א", schema="Blues"),
        TRAIN,
    ), 32, 0, 8, 5)

    # רכבות נוסעות
    add("kpi-train-moving", viz(
        "kpi-train-moving", "🚆 רכבות נוסעות",
        metric_vs("רכבות נוסעות", a_count("נוסעות"),
                  "is_moving = true", schema="Blues"),
        TRAIN, "is_moving: true",
    ), 40, 0, 8, 5)

    # ══ ROW 2 — Timelines (y=5, h=10) ════════════════════════════════════════

    add("tl-bus", viz(
        "tl-bus", "📈 פעילות אוטובוסים לאורך זמן — גוש דן",
        line_vs("פעילות אוטובוסים לאורך זמן",
                a_count("רשומות"), "רשומות"),
        BUS,
    ), 0, 5, 24, 10)

    add("tl-train", viz(
        "tl-train", "📈 פעילות רכבות לאורך זמן — גוש דן",
        line_vs("פעילות רכבות לאורך זמן",
                a_count("רשומות"), "רשומות"),
        TRAIN,
    ), 24, 5, 24, 10)

    # ══ ROW 3 — Moving vs Parked pies (y=15, h=10) ═══════════════════════════

    # פילוח נוסע/חונה/לא ידוע — אוטובוסים
    add("moving-pie-bus", viz(
        "moving-pie-bus", "🚌 נוסע vs חונה — אוטובוסים",
        pie_vs("נוסע vs חונה",
               a_terms("is_moving", 3, "סטטוס")),
        BUS,
    ), 0, 15, 24, 10)

    # פילוח נוסע/חונה — רכבות
    add("moving-pie-train", viz(
        "moving-pie-train", "🚆 נוסע vs חונה — רכבות",
        pie_vs("נוסע vs חונה — רכבות",
               a_terms("is_moving", 3, "סטטוס")),
        TRAIN,
    ), 24, 15, 24, 10)

    # ══ ROW 4 — Speed (y=25, h=10) ══════════════════════════════════════════

    # התפלגות מהירות — רק כאלה שמדווחים
    add("spd-hist-bus", viz(
        "spd-hist-bus", "💨 התפלגות מהירות אוטובוסים",
        hist_vs("התפלגות מהירות (כלי רכב מדווחים בלבד)",
                a_count("כלי רכב"),
                a_hist("speed_kmh", 5, 0, 120, "מהירות (קמ\"ש)"),
                "מספר כלי רכב"),
        BUS, "speed_kmh > 0",
    ), 0, 25, 24, 10)

    # מהירות לאורך זמן
    add("spd-time-bus", viz(
        "spd-time-bus", "📉 מהירות ממוצעת לאורך זמן",
        line_vs("מהירות ממוצעת לאורך זמן",
                a_avg("speed_kmh", "מהירות (קמ\"ש)"),
                "קמ\"ש", threshold=20),
        BUS, "speed_kmh > 0",
    ), 24, 25, 24, 10)

    # ══ ROW 5 — Operators & Lines (y=35, h=12) ════════════════════════════

    # מפעילים — ללא Unknown
    add("bar-operators", viz(
        "bar-operators", "🏢 פעילות לפי מפעיל — גוש דן",
        hbar_vs("מפעילים",
                a_count("רשומות"),
                a_terms("operator_name", 12, "מפעיל"),
                "מספר רשומות"),
        BUS, 'NOT operator_name:"Unknown"',
    ), 0, 35, 24, 12)

    # קווים — route_short_name (מספר קו קריא)
    add("bar-lines", viz(
        "bar-lines", "🛣️ Top 15 קווים פעילים — גוש דן",
        hbar_vs("Top קווים",
                a_count("רשומות"),
                a_terms("route_short_name", 15, "מספר קו"),
                "מספר רשומות"),
        BUS, 'route_short_name:* AND NOT route_short_name:""',
    ), 24, 35, 24, 12)

    # ══ ROW 6 — Hourly activity (y=47, h=12) ═════════════════════════════

    add("hourly-bus", viz(
        "hourly-bus", "🕐 פעילות אוטובוסים לפי שעה — גוש דן",
        {
            "title": "פעילות לפי שעה ביום",
            "type": "line",
            "params": {
                "type": "line", "grid": {"categoryLines": False},
                "addLegend": False, "addTooltip": True, "addTimeMarker": False,
                "categoryAxes": [{"id": "CA-1", "type": "category",
                                  "position": "bottom", "show": True, "style": {},
                                  "scale": {"type": "linear"},
                                  "labels": {"show": True, "filter": True, "truncate": 100},
                                  "title": {}}],
                "valueAxes": [{"id": "VA-1", "name": "LeftAxis-1",
                               "type": "value", "position": "left", "show": True,
                               "style": {}, "scale": {"type": "linear", "mode": "normal"},
                               "labels": {"show": True, "rotate": 0,
                                          "filter": False, "truncate": 100},
                               "title": {"text": "מספר רשומות"}}],
                "seriesParams": [{"show": True, "type": "histogram", "mode": "stacked",
                                  "data": {"label": "רשומות", "id": "1"},
                                  "valueAxis": "VA-1",
                                  "drawLinesBetweenPoints": True,
                                  "lineWidth": 2, "showCircles": True}],
                "times": [], "legendPosition": "right",
                "thresholdLine": {"show": False, "value": 10,
                                  "width": 1, "style": "full", "color": "#E7664C"},
            },
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric",
                 "params": {"customLabel": "רשומות"}},
                {"id": "2", "enabled": True, "type": "date_histogram",
                 "schema": "segment",
                 "params": {"field": "_indexed_at", "interval": "1h",
                            "min_doc_count": 1, "useNormalizedEsInterval": True,
                            "drop_partials": False, "extended_bounds": {},
                            "customLabel": "שעה"}},
            ],
        },
        BUS,
    ), 0, 47, 48, 12)

    # ══ ROW 7 — Top vehicles & stops (y=59, h=12) ═════════════════════════

    add("top-bus-veh", viz(
        "top-bus-veh", "🚌 Top 15 אוטובוסים פעילים",
        hbar_vs("Top אוטובוסים",
                a_count("רשומות"),
                a_terms("vehicle_id", 15, "vehicle_id"),
                "מספר רשומות"),
        BUS,
    ), 0, 59, 24, 12)

    add("top-train-veh", viz(
        "top-train-veh", "🚆 Top 15 נסיעות רכבת",
        hbar_vs("Top רכבות",
                a_count("רשומות"),
                a_terms("trip_id", 15, "trip_id"),
                "מספר רשומות"),
        TRAIN,
    ), 24, 59, 24, 12)

    # ══ ROW 8 — Stops & Bearing (y=71, h=12) ═════════════════════════════

    add("top-stops", viz(
        "top-stops", "🚏 Top 15 תחנות עמוסות — גוש דן",
        hbar_vs("Top תחנות",
                a_count("רשומות"),
                a_terms("stop_id", 15, "תחנה (stop_id)"),
                "מספר רשומות"),
        BUS,
    ), 0, 71, 24, 12)

    add("bearing-hist", viz(
        "bearing-hist", "🧭 התפלגות כיוון נסיעה",
        hist_vs("כיוון נסיעה (0°–360°)",
                a_count("רשומות"),
                a_hist("bearing", 10, 0, 360, "כיוון (מעלות)"),
                "מספר רשומות"),
        BUS,
    ), 24, 71, 24, 12)

    return done


# ── dashboard ─────────────────────────────────────────────────────────────────

def create_dashboard(viz_list):
    did = "israel-transit-dashboard"
    requests.delete(f"{KIBANA}/api/saved_objects/dashboard/{did}", headers=HEADERS)

    panels, refs = [], []
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
            "title":       "🚌 Israel Transit — גוש דן ותל אביב",
            "description": "ניטור אוטובוסים ורכבות בגוש דן ותל אביב",
            "hits": 0,
            "timeRestore": True,
            "timeFrom":    "now-60d",
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
    print("\n🚌 Israel Transit — Kibana Setup v4 (גוש דן)\n")

    print("🧹 Cleaning...")
    for t, i in ([("dashboard", "israel-transit-dashboard")] +
                 [("visualization", v) for v in ALL_VIZ_IDS]):
        requests.delete(f"{KIBANA}/api/saved_objects/{t}/{i}", headers=HEADERS)

    if not wait_for_kibana():
        print("❌ Kibana not available"); return

    print("\n📋 Index patterns...")
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


if __name__ == "__main__":
    main()