#!/usr/bin/env python3
"""
setup_kibana.py
Automatically creates index patterns and dashboard in Kibana.
Run: python3 setup_kibana.py
"""

import requests
import json
import time

KIBANA = "http://localhost:5601"
HEADERS = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json",
}

# ── Index Patterns ────────────────────────────────────────────────────────────
INDEX_PATTERNS = [
    {
        "id":    "transit-bus-positions",
        "title": "transit-bus-positions",
        "timeFieldName": "_indexed_at",
    },
    {
        "id":    "transit-trip-updates",
        "title": "transit-trip-updates",
        "timeFieldName": "_indexed_at",
    },
    {
        "id":    "transit-train-positions",
        "title": "transit-train-positions",
        "timeFieldName": "_indexed_at",
    },
    {
        "id":    "transit-service-alerts",
        "title": "transit-service-alerts",
        "timeFieldName": "_indexed_at",
    },
    {
        "id":    "transit-delay-events",
        "title": "transit-delay-events",
        "timeFieldName": "_indexed_at",
    },
    {
        "id":    "transit-traffic-data",
        "title": "transit-traffic-data",
        "timeFieldName": "_indexed_at",
    },
]


def wait_for_kibana():
    print("⏳ Waiting for Kibana...")
    for i in range(30):
        try:
            r = requests.get(f"{KIBANA}/api/status", timeout=5)
            if r.status_code == 200:
                status = r.json().get("status", {}).get("overall", {}).get("level", "")
                if status in ("available", "green"):
                    print("✅ Kibana is ready")
                    return True
        except Exception:
            pass
        print(f"  waiting... ({i+1}/30)")
        time.sleep(3)
    return False


def create_index_pattern(pattern):
    url = f"{KIBANA}/api/saved_objects/index-pattern/{pattern['id']}"

    # Check if already exists
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        print(f"⚠️  Index pattern already exists: {pattern['title']}")
        return True

    body = {
        "attributes": {
            "title":         pattern["title"],
            "timeFieldName": pattern.get("timeFieldName", ""),
        }
    }
    r = requests.post(url, headers=HEADERS, json=body)
    if r.status_code in (200, 201):
        print(f"✅ Index pattern created: {pattern['title']}")
        return True
    else:
        print(f"❌ Failed: {pattern['title']} — {r.status_code}: {r.text[:100]}")
        return False


def set_default_index_pattern():
    """Set bus positions as default index pattern."""
    body = {"value": "transit-bus-positions"}
    r = requests.post(
        f"{KIBANA}/api/kibana/settings/defaultIndex",
        headers=HEADERS,
        json=body,
    )
    if r.status_code == 200:
        print("✅ Default index pattern set: transit-bus-positions")
    else:
        print(f"⚠️  Could not set default: {r.status_code}")


def create_dashboard():
    """Create a basic Israel Transit dashboard."""
    dashboard_id = "israel-transit-dashboard"

    # Check if exists
    r = requests.get(
        f"{KIBANA}/api/saved_objects/dashboard/{dashboard_id}",
        headers=HEADERS
    )
    if r.status_code == 200:
        print("⚠️  Dashboard already exists")
        return

    # Create visualizations first
    viz_ids = []

    # Viz 1: Bus count over time
    viz1_id = "bus-count-over-time"
    viz1 = {
        "attributes": {
            "title": "🚌 Bus Positions Over Time",
            "visState": json.dumps({
                "title": "Bus Positions Over Time",
                "type": "line",
                "params": {
                    "addLegend": True,
                    "addTimeMarker": False,
                    "addTooltip": True,
                    "defaultYExtents": False,
                    "times": [],
                    "type": "line",
                },
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                    {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
                     "params": {"field": "_indexed_at", "interval": "auto", "min_doc_count": 1}},
                ],
            }),
            "uiStateJSON": "{}",
            "description": "",
            "savedSearchId": None,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "transit-bus-positions",
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            }
        }
    }
    r = requests.post(
        f"{KIBANA}/api/saved_objects/visualization/{viz1_id}",
        headers=HEADERS, json=viz1
    )
    if r.status_code in (200, 201):
        viz_ids.append(viz1_id)
        print(f"✅ Visualization created: {viz1['attributes']['title']}")

    # Viz 2: Operator distribution
    viz2_id = "operator-distribution"
    viz2 = {
        "attributes": {
            "title": "🏢 Operator Distribution",
            "visState": json.dumps({
                "title": "Operator Distribution",
                "type": "pie",
                "params": {"addLegend": True, "addTooltip": True, "isDonut": True, "type": "pie"},
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                    {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
                     "params": {"field": "operator_name.keyword", "size": 10, "order": "desc", "orderBy": "1"}},
                ],
            }),
            "uiStateJSON": "{}",
            "description": "",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "transit-bus-positions",
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            }
        }
    }
    r = requests.post(
        f"{KIBANA}/api/saved_objects/visualization/{viz2_id}",
        headers=HEADERS, json=viz2
    )
    if r.status_code in (200, 201):
        viz_ids.append(viz2_id)
        print(f"✅ Visualization created: {viz2['attributes']['title']}")

    # Viz 3: Delay distribution
    viz3_id = "delay-distribution"
    viz3 = {
        "attributes": {
            "title": "⏱️ Delay Distribution",
            "visState": json.dumps({
                "title": "Delay Distribution",
                "type": "histogram",
                "params": {"addLegend": False, "addTooltip": True, "type": "histogram"},
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                    {"id": "2", "enabled": True, "type": "histogram", "schema": "segment",
                     "params": {"field": "delay_minutes", "interval": 2, "min_doc_count": 1}},
                ],
            }),
            "uiStateJSON": "{}",
            "description": "",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "transit-trip-updates",
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            }
        }
    }
    r = requests.post(
        f"{KIBANA}/api/saved_objects/visualization/{viz3_id}",
        headers=HEADERS, json=viz3
    )
    if r.status_code in (200, 201):
        viz_ids.append(viz3_id)
        print(f"✅ Visualization created: {viz3['attributes']['title']}")

    # Viz 4: Traffic congestion by region
    viz4_id = "traffic-congestion-region"
    viz4 = {
        "attributes": {
            "title": "🚗 Traffic Congestion by Region",
            "visState": json.dumps({
                "title": "Traffic by Region",
                "type": "histogram",
                "params": {"addLegend": True, "addTooltip": True, "type": "histogram", "mode": "stacked"},
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg", "schema": "metric",
                     "params": {"field": "jam_factor"}},
                    {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
                     "params": {"field": "region_he.keyword", "size": 10, "order": "desc", "orderBy": "1"}},
                ],
            }),
            "uiStateJSON": "{}",
            "description": "",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "transit-traffic-data",
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            }
        }
    }
    r = requests.post(
        f"{KIBANA}/api/saved_objects/visualization/{viz4_id}",
        headers=HEADERS, json=viz4
    )
    if r.status_code in (200, 201):
        viz_ids.append(viz4_id)
        print(f"✅ Visualization created: {viz4['attributes']['title']}")

    # Viz 5: Active buses metric
    viz5_id = "active-buses-metric"
    viz5 = {
        "attributes": {
            "title": "🚌 Total Active Buses",
            "visState": json.dumps({
                "title": "Total Active Buses",
                "type": "metric",
                "params": {"addTooltip": True, "addLegend": False, "type": "metric",
                           "metric": {"percentageMode": False, "useRanges": False,
                                      "colorSchema": "Green to Red", "metricColorMode": "None",
                                      "colorsRange": [{"from": 0, "to": 10000}],
                                      "labels": {"show": True}, "invertColors": False,
                                      "style": {"bgFill": "#000", "bgColor": False,
                                                "labelColor": False, "subText": "",
                                                "fontSize": 60}}},
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                ],
            }),
            "uiStateJSON": "{}",
            "description": "",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "transit-bus-positions",
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            }
        }
    }
    r = requests.post(
        f"{KIBANA}/api/saved_objects/visualization/{viz5_id}",
        headers=HEADERS, json=viz5
    )
    if r.status_code in (200, 201):
        viz_ids.append(viz5_id)
        print(f"✅ Visualization created: {viz5['attributes']['title']}")

    # Create dashboard with all visualizations
    panels = []
    positions = [
        (0, 0, 24, 8),   # bus count over time — full width
        (0, 8, 12, 8),   # operator pie
        (12, 8, 12, 8),  # active buses metric
        (0, 16, 12, 8),  # delay distribution
        (12, 16, 12, 8), # traffic by region
    ]

    for i, vid in enumerate(viz_ids):
        if i < len(positions):
            x, y, w, h = positions[i]
            panels.append({
                "version": "8.8.0",
                "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(i)},
                "panelIndex": str(i),
                "embeddableConfig": {},
                "panelRefName": f"panel_{i}",
            })

    references = [
        {"name": f"panel_{i}", "type": "visualization", "id": vid}
        for i, vid in enumerate(viz_ids)
    ]

    dashboard_body = {
        "attributes": {
            "title": "🚌 Israel Transit — Real-Time Dashboard",
            "hits": 0,
            "description": "Real-time monitoring of Israel public transit — buses, trains, delays and traffic",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "hidePanelTitles": False}),
            "version": 1,
            "timeRestore": False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            }
        },
        "references": references,
    }

    r = requests.post(
        f"{KIBANA}/api/saved_objects/dashboard/{dashboard_id}",
        headers=HEADERS,
        json=dashboard_body,
    )
    if r.status_code in (200, 201):
        print(f"\n✅ Dashboard created!")
        print(f"   👉 http://localhost:5601/app/dashboards#/view/{dashboard_id}")
    else:
        print(f"❌ Dashboard failed: {r.status_code}: {r.text[:200]}")


def main():
    print("\n🚌 Israel Transit — Kibana Setup\n")

    # Delete old index patterns and dashboard before recreating
    for obj_type, obj_id in [
        ("dashboard", "israel-transit-dashboard"),
        ("visualization", "bus-count-over-time"),
        ("visualization", "operator-distribution"),
        ("visualization", "delay-distribution"),
        ("visualization", "traffic-congestion-region"),
        ("visualization", "active-buses-metric"),
    ]:
        requests.delete(f"{KIBANA}/api/saved_objects/{obj_type}/{obj_id}", headers=HEADERS)

    if not wait_for_kibana():
        print("❌ Kibana not available")
        return

    print("\n📋 Creating index patterns...")
    for pattern in INDEX_PATTERNS:
        create_index_pattern(pattern)

    set_default_index_pattern()

    print("\n📊 Creating dashboard...")
    create_dashboard()

    print("\n✅ Done! Open Kibana:")
    print("   http://localhost:5601")
    print("   Dashboard: http://localhost:5601/app/dashboards")


if __name__ == "__main__":
    main()