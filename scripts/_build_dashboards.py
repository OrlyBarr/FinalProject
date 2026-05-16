#!/usr/bin/env python3
"""Build 3 Kibana dashboards (Buses / Trains / Delays+Alerts) on Elastic Cloud.

- Recreates data views with CORRECT time fields + a geo_point runtime field.
- Creates by-value Lens panels (no fragile saved-object refs) for time series,
  top-N bars, and metrics.
- Adds a Maps saved object for bus & train locations.
"""
import warnings, json, sys, uuid
warnings.filterwarnings("ignore")
import urllib3; urllib3.disable_warnings()
import requests

d = open(".env").read()
def g(k): return d.split(k + "=")[1].split("\n")[0].strip()
KB  = g("KIBANA_URL").rstrip("/")
KEY = g("ELASTIC_API_KEY")
H = {"kbn-xsrf": "true", "Content-Type": "application/json",
     "Authorization": f"ApiKey {KEY}"}

def api(method, path, body=None):
    r = requests.request(method, f"{KB}{path}", headers=H,
                         json=body, verify=False, timeout=60)
    return r

# geo_point runtime field from separate lat/lon
def geo_runtime(lat_f, lon_f):
    return {"location": {"type": "geo_point",
        "script": {"source":
            f"if(doc['{lat_f}'].size()!=0 && doc['{lon_f}'].size()!=0)"
            f"{{emit(doc['{lat_f}'].value, doc['{lon_f}'].value);}}"}}}

# ---- 1. (Re)create data views with correct time field + geo runtime ----
DV = [
  ("transit-bus-positions",  "Bus Positions",  "timestamp",
      geo_runtime("latitude","longitude")),
  ("transit-train-positions","Train Positions","recorded_at",
      geo_runtime("lat","lon")),
  ("transit-service-alerts", "Service Alerts", "fetched_at", {}),
  ("transit-traffic",        "Traffic",        "timestamp", {}),
]
ids = {}
# delete old (wrong-time-field) views first
ex = api("GET", "/api/data_views").json().get("data_view", [])
byt = {v["title"]: v["id"] for v in ex}
for title, name, tf, rt in DV:
    if title in byt:
        api("DELETE", f"/api/data_views/data_view/{byt[title]}")
    body = {"data_view": {"title": title, "name": name, "timeFieldName": tf}}
    if rt:
        body["data_view"]["runtimeFieldMap"] = rt
    r = api("POST", "/api/data_views/data_view", body)
    if r.status_code in (200, 201):
        ids[title] = r.json()["data_view"]["id"]
        print(f"DV  {title:26s} time={tf:12s} id={ids[title]}")
    else:
        print(f"DV FAIL {title}: {r.status_code} {r.text[:140]}")

BUS = ids.get("transit-bus-positions")
TRN = ids.get("transit-train-positions")
ALT = ids.get("transit-service-alerts")

# ---- Lens by-value panel builders ----
def col(cid): return cid

def lens_metric(title, name, dv, agg, field=None):
    # NOTE: signature is (title, name, dv, agg, field). The callers pass an
    # English label as 2nd arg; previously the function omitted it which
    # shifted dv→agg and put the data-view id in the wrong slot, so every
    # metric panel referenced a bogus index and rendered blank.
    cId = "m1"
    if agg == "count":
        col = {"label": name, "dataType": "number",
               "operationType": "count", "sourceField": "___records___",
               "isBucketed": False}
    else:
        col = {"label": name, "dataType": "number",
               "operationType": agg, "sourceField": field,
               "isBucketed": False}
    cols = {cId: col}
    # lnsLegacyMetric has a stable, well-supported state (uses "accessor")
    return _lens(title, dv, "lnsLegacyMetric",
        {"layerId": "l1", "accessor": cId, "layerType": "data"},
        {"l1": {"columns": cols, "columnOrder": [cId],
                "incompleteColumns": {}}})

def lens_xy(title, dv, time_field, split=None):
    cols = {
      "x": {"label": time_field, "dataType":"date","operationType":"date_histogram",
            "sourceField": time_field, "isBucketed": True,
            "params":{"interval":"auto"}},
      "y": {"label":"Count","dataType":"number","operationType":"count",
            "sourceField":"___records___","isBucketed":False},
    }
    order=["x","y"]
    layer={"layerId":"l1","layerType":"data","accessors":["y"],
           "xAccessor":"x","seriesType":"line",
           "position":"top","showGridlines":False}
    if split:
        cols["s"]={"label":f"Top {split}","dataType":"string",
            "operationType":"terms","sourceField":split,"isBucketed":True,
            "params":{"size":8,"orderBy":{"type":"column","columnId":"y"},
                      "orderDirection":"desc"}}
        order=["x","s","y"]
        layer["splitAccessor"]="s"
    return _lens(title, dv, "lnsXY",
        {"legend":{"isVisible":True,"position":"right"},
         "valueLabels":"hide","preferredSeriesType":"line",
         "layers":[layer]},
        {"l1":{"columns":cols,"columnOrder":order,"incompleteColumns":{}}})

def lens_bar(title, dv, term_field, size=15, metric="count", metric_field=None):
    y = {"label":"Count","dataType":"number","operationType":"count",
         "sourceField":"___records___","isBucketed":False}
    if metric=="average":
        y={"label":f"Avg {metric_field}","dataType":"number",
           "operationType":"average","sourceField":metric_field,"isBucketed":False}
    cols={
      "b":{"label":f"Top {term_field}","dataType":"string",
           "operationType":"terms","sourceField":term_field,"isBucketed":True,
           "params":{"size":size,"orderBy":{"type":"column","columnId":"y"},
                     "orderDirection":"desc"}},
      "y":y,
    }
    return _lens(title, dv, "lnsXY",
        {"legend":{"isVisible":False,"position":"right"},
         "valueLabels":"hide","preferredSeriesType":"bar_horizontal",
         "layers":[{"layerId":"l1","layerType":"data","accessors":["y"],
                    "xAccessor":"b","seriesType":"bar_horizontal"}]},
        {"l1":{"columns":cols,"columnOrder":["b","y"],"incompleteColumns":{}}})

def _lens(title, dv, vis_type, vis_state, datasource_layers):
    return {
      "title": title,
      "visualizationType": vis_type,
      "type": "lens",
      "references": [
        {"type":"index-pattern","id":dv,
         "name":"indexpattern-datasource-layer-l1"}],
      "state": {
        "visualization": vis_state,
        "query": {"query":"","language":"kuery"},
        "filters": [],
        "datasourceStates": {"formBased": {"layers": datasource_layers}},
      },
    }

def panel(lens_attrs, x, y, w, h, pid):
    return {
      "version":"9.0.0",
      "type":"lens",
      "gridData":{"x":x,"y":y,"w":w,"h":h,"i":pid},
      "panelIndex":pid,
      "embeddableConfig":{"attributes":lens_attrs,"enhancements":{}},
      "title":lens_attrs["title"],
    }

def make_dashboard(did, title, desc, panels):
    body={"attributes":{
        "title":title,"description":desc,
        "panelsJSON":json.dumps(panels),
        "timeRestore":True,
        "timeFrom":"2026-04-01T00:00:00.000Z",
        "timeTo":"2026-06-01T00:00:00.000Z",
        "refreshInterval":{"pause":True,"value":0},
        "kibanaSavedObjectMeta":{"searchSourceJSON":
            json.dumps({"query":{"query":"","language":"kuery"},"filter":[]})},
    }}
    r=api("POST", f"/api/saved_objects/dashboard/{did}?overwrite=true", body)
    print(f"DASH {title:34s} -> {r.status_code}")
    return r.status_code in (200,201)

# ---------- Dashboard 1: BUSES ----------
p=[]
p.append(panel(lens_metric("סה\"כ רשומות אוטובוסים","Total Buses",BUS,"count"),0,0,12,7,"b1"))
p.append(panel(lens_xy("אוטובוסים לאורך זמן",BUS,"timestamp"),12,0,36,7,"b2"))
p.append(panel(lens_bar("Top 15 קווי אוטובוס (line_ref)",BUS,"line_ref",15),0,7,24,15,"b3"))
p.append(panel(lens_bar("אוטובוסים לפי מפעיל",BUS,"operator_name",12),24,7,24,15,"b4"))
p.append(panel(lens_xy("אוטובוסים לאורך זמן לפי מפעיל",BUS,"timestamp","operator_name"),0,22,48,12,"b5"))
make_dashboard("transit-buses","🚌 אוטובוסים — Buses",
   "פעילות אוטובוסים: לאורך זמן, top קווים, מפעילים (אפר׳–מאי 2026)",p)

# ---------- Dashboard 2: TRAINS ----------
p=[]
p.append(panel(lens_metric("סה\"כ רשומות רכבות","Total Trains",TRN,"count"),0,0,12,7,"t1"))
p.append(panel(lens_metric("מהירות ממוצעת (velocity)","Avg Velocity",TRN,"average","velocity"),12,0,12,7,"t2"))
p.append(panel(lens_xy("רכבות לאורך זמן",TRN,"recorded_at"),24,0,24,7,"t3"))
p.append(panel(lens_bar("Top 15 קווי רכבת",TRN,"line_ref",15),0,7,24,15,"t4"))
p.append(panel(lens_bar("מהירות ממוצעת לפי קו",TRN,"line_ref",15,"average","velocity"),24,7,24,15,"t5"))
make_dashboard("transit-trains","🚆 רכבות — Trains",
   "פעילות רכבות: התפלגות לאורך זמן, מהירות ממוצעת, כמות כוללת",p)

# ---------- Dashboard 3: DELAYS + SERVICE ALERTS ----------
p=[]
p.append(panel(lens_metric("סה\"כ התראות/נסיעות","Total Alerts",ALT,"count"),0,0,12,7,"a1"))
p.append(panel(lens_metric("עיכוב ממוצע (דק')","Avg Delay min",ALT,"average","extra_delay_min"),12,0,12,7,"a2"))
p.append(panel(lens_xy("התראות לאורך זמן",ALT,"fetched_at"),24,0,24,7,"a3"))
p.append(panel(lens_bar("לפי חומרה (severity)",ALT,"severity",10),0,7,24,15,"a4"))
p.append(panel(lens_bar("לפי סוג התראה (alert_type)",ALT,"alert_type.keyword",10),24,7,24,15,"a5"))
p.append(panel(lens_xy("התראות לאורך זמן לפי חומרה",ALT,"fetched_at","severity"),0,22,48,12,"a6"))
make_dashboard("transit-delays","⚠️ עיכובים והתראות — Delays & Alerts",
   "Service alerts: חומרה, סוג, עיכוב ממוצע לאורך זמן",p)

# ---------- Maps for bus & train locations ----------
def make_map(mid, title, dv_id, dv_title):
    layer={
      "sourceDescriptor":{"type":"ES_SEARCH","id":mid+"-src",
        "indexPatternId":dv_id,"geoField":"location",
        "scalingType":"MVT","tooltipProperties":[],
        "filterByMapBounds":True,"applyGlobalQuery":True,
        "applyGlobalTime":True,"sortOrder":"desc"},
      "id":mid+"-lyr","label":dv_title,"minZoom":0,"maxZoom":24,
      "alpha":0.75,"visible":True,"type":"MVT_VECTOR",
      "style":{"type":"VECTOR","properties":{
        "fillColor":{"type":"STATIC","options":{"color":"#2A78C7"}},
        "lineColor":{"type":"STATIC","options":{"color":"#1A4D80"}},
        "iconSize":{"type":"STATIC","options":{"size":4}}}},
    }
    attrs={
      "title":title,
      "description":"מיקומי כלי רכב (geo_point runtime field)",
      "mapStateJSON":json.dumps({"zoom":9.5,
         "center":{"lon":34.82,"lat":32.07},
         "timeFilters":{"from":"2026-04-01T00:00:00Z","to":"2026-06-01T00:00:00Z"},
         "refreshConfig":{"isPaused":True,"interval":0},
         "query":{"query":"","language":"kuery"},"filters":[]}),
      "layerListJSON":json.dumps([
        {"sourceDescriptor":{"type":"EMS_TMS","isAutoSelect":True},
         "id":"basemap","label":None,"minZoom":0,"maxZoom":24,
         "alpha":1,"visible":True,"type":"EMS_VECTOR_TILE","style":{}},
        layer]),
      "uiStateJSON":"{}",
    }
    body={"attributes":attrs,
      "references":[{"type":"index-pattern","id":dv_id,
        "name":"layer_1_source_index_pattern"}]}
    r=api("POST", f"/api/saved_objects/map/{mid}?overwrite=true", body)
    print(f"MAP  {title:30s} -> {r.status_code} {('' if r.status_code in (200,201) else r.text[:120])}")
    return r.status_code in (200,201)

make_map("map-buses","🗺️ מפת אוטובוסים", BUS, "Bus Positions")
make_map("map-trains","🗺️ מפת רכבות", TRN, "Train Positions")

print("\n===== DASHBOARDS =====")
print(f"Buses : {KB}/app/dashboards#/view/transit-buses")
print(f"Trains: {KB}/app/dashboards#/view/transit-trains")
print(f"Delays: {KB}/app/dashboards#/view/transit-delays")
print(f"Map(bus):   {KB}/app/maps/map/map-buses")
print(f"Map(train): {KB}/app/maps/map/map-trains")
