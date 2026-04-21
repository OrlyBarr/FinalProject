#!/usr/bin/env python3
"""
setup_kibana_maps.py — Israel Transit Map Dashboards
====================================================
יוצר שני דשבורדי מפה:
  1. Live Map  — כל האוטובוסים הפעילים (heat map + נקודות)
  2. Route Map — מסלול נסיעה של רכב ספציפי (Documents layer)

דרישות: transit-bus-positions עם שדה location (geo_point)

Run: python3 setup_kibana_maps.py
"""

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import requests, json, time

KIBANA  = "http://localhost:5601"
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}
BUS     = "transit-bus-positions"

# IDs לניקוי
ALL_MAP_IDS = [
    "map-live-buses",
    "map-vehicle-route",
    "dash-live-map",
    "dash-route-map",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def wait_for_kibana():
    print("⏳ Waiting for Kibana...")
    for i in range(30):
        try:
            r = requests.get(f"{KIBANA}/api/status", timeout=5)
            if r.status_code == 200:
                d   = r.json()
                lvl = d.get("overall", {}).get("level", "available")
                print(f"✅ Kibana ready ({lvl})")
                return True
        except Exception:
            pass
        print(f"  {i+1}/30..."); time.sleep(3)
    return False


def upsert_pattern():
    url = f"{KIBANA}/api/saved_objects/index-pattern/{BUS}"
    requests.delete(url, headers=HEADERS)
    r = requests.post(url, headers=HEADERS, json={
        "attributes": {
            "title": BUS,
            "timeFieldName": "_indexed_at",
        }
    })
    print(f"{'✅' if r.status_code in (200,201) else '❌'} index-pattern: {BUS}")


def post_map(map_id, title, description, layers, time_from="now-60d"):
    """יוצר Kibana Map saved object."""
    body = {
        "attributes": {
            "title":       title,
            "description": description,
            "mapStateJSON": json.dumps({
                "zoom":   8,
                "center": {"lat": 32.1, "lon": 34.85},  # מרכז ישראל — גוש דן
                "timeFilters": {
                    "from": time_from,
                    "to":   "now",
                },
                "query":   {"query": "", "language": "kuery"},
                "filters": [],
                "settings": {
                    "initialLocation":   "FIXED_LOCATION",
                    "fixedLocation":     {"lat": 32.1, "lon": 34.85, "zoom": 8},
                    "browserLocation":   {"zoom": 3},
                    "autoFitToDataBounds": True,
                    "backgroundColor":    "#ffffff",
                    "disableInteractive": False,
                    "disableTooltipControl":  False,
                    "hideToolbarOverlay":     False,
                    "hideLayerControl":       False,
                    "hideViewControl":        False,
                    "initialZoomLevel":       None,
                    "maxZoom": 24, "minZoom": 0,
                },
            }),
            "layerListJSON": json.dumps(layers),
            "uiStateJSON":   json.dumps({"isLayerTOCOpen": True, "openTOCDetails": []}),
        }
    }
    r = requests.post(
        f"{KIBANA}/api/saved_objects/map/{map_id}",
        headers=HEADERS, json=body,
    )
    ok = r.status_code in (200, 201)
    print(f"{'✅' if ok else '❌'} Map: {title}" +
          ("" if ok else f" [{r.status_code}] {r.text[:150]}"))
    return ok


def post_dashboard(dash_id, title, description, map_id, time_from="now-60d"):
    """יוצר dashboard עם מפה אחת."""
    panels = [{
        "version":    "8.8.0",
        "type":       "map",
        "gridData":   {"x": 0, "y": 0, "w": 48, "h": 30, "i": "0"},
        "panelIndex": "0",
        "embeddableConfig": {
            "isLayerTOCOpen":  True,
            "openTOCDetails":  [],
            "mapCenter":       {"lat": 31.8, "lon": 34.9, "zoom": 7},
            "enhancements":    {},
        },
        "panelRefName": "panel_0",
    }]

    body = {
        "attributes": {
            "title":       title,
            "description": description,
            "hits":        0,
            "timeRestore": True,
            "timeFrom":    time_from,
            "timeTo":      "now",
            "refreshInterval": {"pause": False, "value": 30000},
            "panelsJSON":  json.dumps(panels),
            "optionsJSON": json.dumps({
                "useMargins":      True,
                "syncColors":      False,
                "hidePanelTitles": False,
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query":  {"query": "", "language": "kuery"},
                    "filter": [],
                })
            },
        },
        "references": [
            {"name": "panel_0", "type": "map", "id": map_id}
        ],
    }

    r = requests.post(
        f"{KIBANA}/api/saved_objects/dashboard/{dash_id}",
        headers=HEADERS, json=body,
    )
    ok = r.status_code in (200, 201)
    print(f"{'✅' if ok else '❌'} Dashboard: {title}" +
          ("" if ok else f" [{r.status_code}] {r.text[:150]}"))
    if ok:
        print(f"   👉 http://localhost:5601/app/dashboards#/view/{dash_id}")
    return ok


# ── Layer definitions ─────────────────────────────────────────────────────────

def tile_layer():
    """שכבת מפה בסיסית — Kibana 8.8 משתמש ב-EMS_VECTOR_TILE."""
    return {
        "id":      "tile-base",
        "type":    "EMS_VECTOR_TILE",
        "name":    "Road map",
        "visible": True,
        "style":   {"type": "EMS_VECTOR_TILE"},
        "sourceDescriptor": {
            "type":         "EMS_TMS",
            "isAutoSelect": True,
            "lightModeDefault": "road_map_desaturated",
        },
    }


def heatmap_layer(index_pattern_id):
    """שכבת heat map — צפיפות אוטובוסים."""
    return {
        "id":      "heatmap-buses",
        "type":    "HEATMAP",
        "name":    "🔥 צפיפות אוטובוסים",
        "visible": True,
        "style": {
            "type":        "HEATMAP",
            "colorRampName": "Yellow to Red",
        },
        "query": {"query": "", "language": "kuery"},
        "filters": [],
        "sourceDescriptor": {
            "type":             "ES_GEO_GRID",
            "id":               "heatmap-src",
            "indexPatternId":   index_pattern_id,
            "geoField":         "location",
            "requestType":      "heatmap",
            "resolution":       "COARSE",
            "metrics": [{"type": "count", "label": "רשומות"}],
            "applyGlobalQuery":  True,
            "applyGlobalTime":   True,
            "applyForceRefresh": True,
        },
    }


def cluster_layer(index_pattern_id):
    """שכבת clusters — מספר אוטובוסים לאזור."""
    return {
        "id":      "cluster-buses",
        "type":    "BLENDED_VECTOR",
        "name":    "🚌 כמות אוטובוסים לאזור",
        "visible": True,
        "style": {
            "type": "VECTOR",
            "properties": {
                "fillColor": {
                    "type":  "DYNAMIC",
                    "options": {
                        "field":       {"name": "doc_count", "origin": "source"},
                        "color":       "Blues",
                        "fieldMetaOptions": {"isEnabled": True, "sigma": 3},
                    },
                },
                "lineColor":   {"type": "STATIC", "options": {"color": "#41414d"}},
                "lineWidth":   {"type": "STATIC", "options": {"size": 1}},
                "iconSize":    {"type": "STATIC", "options": {"size": 6}},
                "labelText":   {"type": "STATIC", "options": {"value": ""}},
            },
        },
        "query": {"query": "", "language": "kuery"},
        "filters": [],
        "sourceDescriptor": {
            "type":           "ES_GEO_GRID",
            "id":             "cluster-src",
            "indexPatternId": index_pattern_id,
            "geoField":       "location",
            "requestType":    "point",
            "resolution":     "COARSE",
            "metrics": [{"type": "count", "label": "אוטובוסים"}],
            "applyGlobalQuery":  True,
            "applyGlobalTime":   True,
            "applyForceRefresh": True,
        },
    }


def all_buses_layer(index_pattern_id):
    """שכבה אחת — כל האוטובוסים, צבע דינמי לפי מפעיל."""
    return {
        "id":      "all-buses",
        "type":    "GEOJSON_VECTOR",
        "name":    "🚌 כל האוטובוסים (לפי מפעיל)",
        "visible": True,
        "style": {
            "type": "VECTOR",
            "properties": {
                "fillColor": {
                    "type": "DYNAMIC",
                    "options": {
                        "field": {
                            "name":   "operator_name",
                            "origin": "source",
                        },
                        "color":  "Elastic Classic",
                        "fieldMetaOptions": {
                            "isEnabled": True,
                            "sigma":     3,
                        },
                    },
                },
                "lineColor": {"type": "STATIC", "options": {"color": "#ffffff"}},
                "lineWidth": {"type": "STATIC", "options": {"size": 1}},
                "iconSize":  {"type": "STATIC", "options": {"size": 8}},
                "symbolizeAs": {"options": {"value": "circle"}},
                "labelText": {"type": "STATIC", "options": {"value": ""}},
            },
        },
        "query":   {"query": 'NOT operator_name:"operator_"', "language": "kuery"},
        "filters": [],
        "sourceDescriptor": {
            "type":              "ES_SEARCH",
            "id":                "src-all-buses",
            "indexPatternId":    index_pattern_id,
            "geoField":          "location",
            "limit":             10000,
            "filterByMapBounds": True,
            "tooltipProperties": [
                "vehicle_id", "operator_name", "trip_id",
                "speed_kmh", "bearing", "_indexed_at",
            ],
            "sortField":   "_indexed_at",
            "sortOrder":   "desc",
            "scalingType": "LIMIT",
            "applyGlobalQuery":  True,
            "applyGlobalTime":   True,
            "applyForceRefresh": True,
        },
    }


# ── Get index pattern ID ──────────────────────────────────────────────────────

def get_index_pattern_id(title):
    """מחזיר את ה-ID של index pattern לפי שם."""
    r = requests.get(
        f"{KIBANA}/api/saved_objects/_find",
        headers=HEADERS,
        params={"type": "index-pattern", "search": title,
                "search_fields": "title", "per_page": 5},
    )
    if r.status_code == 200:
        items = r.json().get("saved_objects", [])
        for item in items:
            if item["attributes"]["title"] == title:
                return item["id"]
    return title  # fallback: use title as ID


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n🗺️  Israel Transit — Map Dashboards Setup\n")

    print("🧹 Cleaning old maps...")
    for t, oid in [
        ("map",       "map-live-buses"),
        ("map",       "map-vehicle-route"),
        ("dashboard", "dash-live-map"),
        ("dashboard", "dash-route-map"),
    ]:
        requests.delete(
            f"{KIBANA}/api/saved_objects/{t}/{oid}",
            headers=HEADERS,
        )

    if not wait_for_kibana():
        print("❌ Kibana not available"); return

    # ודא שה-index pattern קיים
    upsert_pattern()
    ip_id = get_index_pattern_id(BUS)
    print(f"   Index pattern ID: {ip_id}")

    # ════════════════════════════════════════════════════════
    #  Map 1: Live Bus Map — כל האוטובוסים הפעילים
    # ════════════════════════════════════════════════════════
    print("\n📍 Creating Live Bus Map...")

    live_layers = [
        tile_layer(),
        heatmap_layer(ip_id),
        all_buses_layer(ip_id),
    ]

    map_ok = post_map(
        "map-live-buses",
        "🚌 Live Bus Map — אוטובוסים פעילים",
        "מפה חיה של כל האוטובוסים הפעילים בישראל",
        live_layers,
        time_from="now-60d",
    )

    if map_ok:
        post_dashboard(
            "dash-live-map",
            "🚌 Live Bus Map — אוטובוסים פעילים",
            "מפה חיה עם heat map וקלאסטרים",
            "map-live-buses",
            time_from="now-60d",
        )

    # ════════════════════════════════════════════════════════
    #  Map 2: Vehicle Route Map — מסלול רכב ספציפי
    # ════════════════════════════════════════════════════════
    print("\n🛣️  Creating Vehicle Route Map...")

    # שלב 1: מצא vehicle_id עם הכי הרבה רשומות
    best_vehicle = ""
    try:
        r = requests.post(
            f"http://localhost:9200/{BUS}/_search",
            headers={"Content-Type": "application/json"},
            json={
                "size": 0,
                "aggs": {
                    "top_v": {
                        "terms": {"field": "vehicle_id", "size": 1}
                    }
                }
            }
        )
        if r.status_code == 200:
            buckets = r.json()["aggregations"]["top_v"]["buckets"]
            if buckets:
                best_vehicle = buckets[0]["key"]
                print(f"   Auto-selected vehicle: {best_vehicle} "
                      f"({buckets[0]['doc_count']} records)")
    except Exception as e:
        print(f"   Could not auto-select vehicle: {e}")

    route_query = f'vehicle_id:"{best_vehicle}"' if best_vehicle else ""

    route_layers = [
        tile_layer(),
        {
            "id":      "route-vehicle",
            "type":    "GEOJSON_VECTOR",
            "name":    f"🚌 מסלול {best_vehicle or 'רכב'}",
            "visible": True,
            "style": {
                "type": "VECTOR",
                "properties": {
                    "fillColor":   {"type": "STATIC", "options": {"color": "#E91E63"}},
                    "lineColor":   {"type": "STATIC", "options": {"color": "#ffffff"}},
                    "lineWidth":   {"type": "STATIC", "options": {"size": 1}},
                    "iconSize":    {"type": "STATIC", "options": {"size": 7}},
                    "symbolizeAs": {"options": {"value": "circle"}},
                    "labelText":   {"type": "STATIC", "options": {"value": ""}},
                },
            },
            "query":   {"query": route_query, "language": "kuery"},
            "filters": [],
            "sourceDescriptor": {
                "type":              "ES_SEARCH",
                "id":                "src-route",
                "indexPatternId":    ip_id,
                "geoField":          "location",
                "limit":             5000,
                "filterByMapBounds": False,
                "tooltipProperties": [
                    "vehicle_id", "operator_name", "trip_id",
                    "speed_kmh", "bearing", "_indexed_at",
                ],
                "sortField":   "_indexed_at",
                "sortOrder":   "asc",
                "scalingType": "LIMIT",
                "applyGlobalQuery":  False,
                "applyGlobalTime":   True,
                "applyForceRefresh": True,
            },
        },
    ]

    map_ok2 = post_map(
        "map-vehicle-route",
        f"🛣️ מסלול רכב — {best_vehicle or 'Vehicle Route'}",
        "מסלול נסיעה של רכב ספציפי. ניתן לשנות את ה-vehicle_id בסינון.",
        route_layers,
        time_from="now-60d",
    )

    if map_ok2:
        post_dashboard(
            "dash-route-map",
            f"🛣️ מסלול רכב — {best_vehicle or 'Vehicle Route'}",
            "מסלול נסיעה של רכב. שני את vehicle_id בסרגל הסינון למעלה.",
            "map-vehicle-route",
            time_from="now-60d",
        )

    print("\n✅ Done!")
    print("\n🗺️  קישורים לדשבורדים:")
    print("   Live Map:    http://localhost:5601/app/dashboards#/view/dash-live-map")
    print("   Route Map:   http://localhost:5601/app/dashboards#/view/dash-route-map")
    print("\n💡 לשינוי רכב בדשבורד המסלול:")
    print('   הוסיפי סינון: vehicle_id : "YOUR_VEHICLE_ID"')
    print("   ואז לחצי Refresh")


if __name__ == "__main__":
    main()