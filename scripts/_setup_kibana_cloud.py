#!/usr/bin/env python3
"""Create Kibana data views + a dashboard on Elastic Cloud."""
import warnings, json, sys
warnings.filterwarnings("ignore")
import urllib3
urllib3.disable_warnings()
import requests

d = open(".env").read()
def g(k): return d.split(k + "=")[1].split("\n")[0].strip()
KB  = g("KIBANA_URL").rstrip("/")
KEY = g("ELASTIC_API_KEY")

H = {"kbn-xsrf": "true", "Content-Type": "application/json",
     "Authorization": f"ApiKey {KEY}"}

VIEWS = [
    ("transit-bus-positions",   "Bus Positions",     "timestamp"),
    ("transit-train-positions", "Train Positions",   "timestamp"),
    ("transit-traffic",         "Traffic",           "timestamp"),
    ("transit-service-alerts",  "Service Alerts",    "timestamp"),
    ("transit-trip-updates",    "Trip Updates",      "timestamp"),
    ("transit-delay-events",    "Delay Events",      "timestamp"),
]

created = {}
for title, name, tf in VIEWS:
    body = {"data_view": {"title": title, "name": name}}
    # try with time field, fall back to recorded_at, then none
    for tfield in (tf, "recorded_at", None):
        b = json.loads(json.dumps(body))
        if tfield:
            b["data_view"]["timeFieldName"] = tfield
        r = requests.post(f"{KB}/api/data_views/data_view",
                           headers=H, json=b, verify=False, timeout=40)
        if r.status_code in (200, 201):
            created[title] = r.json()["data_view"]["id"]
            print(f"CREATED  {title:28s} -> id={created[title]} (time={tfield})")
            break
        if "Duplicate" in r.text or r.status_code == 409:
            # fetch existing id
            rr = requests.get(f"{KB}/api/data_views", headers=H,
                              verify=False, timeout=40)
            for dv in rr.json().get("data_view", []):
                if dv["title"] == title:
                    created[title] = dv["id"]
            print(f"EXISTS   {title:28s} -> id={created.get(title)}")
            break
    else:
        print(f"FAIL     {title}: {r.status_code} {r.text[:160]}")

# Minimal dashboard so it exists & is openable
dash = {
    "attributes": {
        "title": "Israel Transit — Overview",
        "description": "Bus/Train/Traffic live + historical (Apr–May 2026)",
        "panelsJSON": "[]",
        "timeRestore": False,
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"},
                                            "filter": []})
        },
    }
}
r = requests.post(f"{KB}/api/saved_objects/dashboard/israel-transit-overview?overwrite=true",
                  headers=H, json=dash, verify=False, timeout=40)
print(f"\nDASHBOARD status={r.status_code}")
dash_ok = r.status_code in (200, 201)

print("\n===== OPEN THESE IN KIBANA =====")
print(f"Login:     {KB}/login   (user: elastic — password from Cloud console)")
for title, name, _ in VIEWS:
    vid = created.get(title, "")
    if vid:
        print(f"Discover [{name}]: {KB}/app/discover#/?_a=(index:'{vid}')")
if dash_ok:
    print(f"Dashboard: {KB}/app/dashboards#/view/israel-transit-overview")
print(f"\nData views created/found: {len(created)}/6")
