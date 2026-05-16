#!/usr/bin/env python3
"""Relabel operator_name 'Unknown' -> 'ללא מפעיל (backfill)' on bus docs.

These ~1.8M docs come from old backfill runs where the Stride API
returned no operator id at all (genuinely unattributable). Relabeling
makes the dashboard self-explanatory instead of looking like a bug.
Idempotent.
"""
import warnings, time
warnings.filterwarnings("ignore")
from elasticsearch import Elasticsearch

d = open(".env").read()
def g(k): return d.split(k + "=")[1].split("\n")[0].strip()
es = Elasticsearch(cloud_id=g("ELASTIC_CLOUD_ID"), api_key=g("ELASTIC_API_KEY"),
                   request_timeout=120, verify_certs=False, ssl_show_warn=False)

IDX = "transit-bus-positions"
NEW = "ללא מפעיל (backfill)"
q = {"term": {"operator_name": "Unknown"}}

before = es.count(index=IDX, query=q)["count"]
print(f"'Unknown' docs before: {before:,}")

resp = es.update_by_query(
    index=IDX, query=q,
    script={"source": "ctx._source.operator_name = params.n;",
            "lang": "painless", "params": {"n": NEW}},
    conflicts="proceed", slices="auto", wait_for_completion=False,
)
task = resp["task"]
print(f"task={task}")
while True:
    t = es.tasks.get(task_id=task)
    s = t["task"]["status"]
    print(f"  updated={s.get('updated',0):,}/{s.get('total',0):,}"
          f"{' DONE' if t.get('completed') else ''}")
    if t.get("completed"):
        break
    time.sleep(15)

after = es.count(index=IDX, query=q)["count"]
print(f"'Unknown' docs after: {after:,}")
a = es.search(index=IDX, size=0, aggs={
    "x": {"terms": {"field": "operator_name", "size": 6}}})
for b in a["aggregations"]["x"]["buckets"]:
    print(f"  {b['key']:26s} {b['doc_count']:>9,}")
