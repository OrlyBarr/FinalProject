#!/usr/bin/env python3
"""
Fix bad operator_name on existing cloud bus-positions docs.

~78% of historical bus docs were written by the backfill scripts
(full_backfill / wake_backfill / fix_backfill_processed) which stored
only `operator_ref` and never mapped it to operator_name/operator_id.
Result: operator_name is "", "operator_", or missing.

This runs ONE update_by_query:
  * if operator_ref is known  -> operator_name = OPERATORS[ref], operator_id = ref
  * else (Stride gave no ref) -> operator_name = "Unknown"
Idempotent; safe to re-run.
"""
import warnings, time
warnings.filterwarnings("ignore")
from elasticsearch import Elasticsearch

d = open(".env").read()
def g(k): return d.split(k + "=")[1].split("\n")[0].strip()
es = Elasticsearch(cloud_id=g("ELASTIC_CLOUD_ID"), api_key=g("ELASTIC_API_KEY"),
                   request_timeout=120, verify_certs=False, ssl_show_warn=False)

OPERATORS = {
    "3": "Dan", "4": "Egged", "5": "Egged", "6": "Egged Taavura",
    "7": "Metropoline", "14": "Nateev Express", "15": "Nateev Express",
    "16": "Kavim", "18": "Galim", "23": "Superbus", "25": "Egged",
    "31": "Nadan", "32": "KBS", "34": "Afikim", "37": "Electra Afikim",
    "42": "Malam", "44": "GB Tours", "52": "Gush Dan Bus",
    "40": "Kavim Hatichon", "135": "Tnufa",
}

IDX = "transit-bus-positions"

bad_query = {
    "bool": {
        "should": [
            {"term": {"operator_name": ""}},
            {"term": {"operator_name": "operator_"}},
            {"bool": {"must_not": {"exists": {"field": "operator_name"}}}},
        ],
        "minimum_should_match": 1,
    }
}

before = es.count(index=IDX, query=bad_query)["count"]
print(f"bad operator_name docs before: {before:,}")

script = {
    "source": """
        String ref = null;
        if (ctx._source.operator_ref != null) {
            ref = String.valueOf(ctx._source.operator_ref).trim();
        }
        if (ref != null && ref.length() > 0 && params.m.containsKey(ref)) {
            ctx._source.operator_name = params.m.get(ref);
            ctx._source.operator_id = ref;
        } else {
            ctx._source.operator_name = 'Unknown';
            if (ref != null && ref.length() > 0) { ctx._source.operator_id = ref; }
        }
    """,
    "lang": "painless",
    "params": {"m": OPERATORS},
}

resp = es.update_by_query(
    index=IDX, query=bad_query, script=script,
    conflicts="proceed", slices="auto", wait_for_completion=False,
    requests_per_second=-1,
)
task = resp["task"]
print(f"update_by_query started, task={task}")

# poll the task
while True:
    t = es.tasks.get(task_id=task)
    st = t["task"]["status"]
    done = t.get("completed", False)
    print(f"  updated={st.get('updated',0):,} "
          f"total={st.get('total',0):,} "
          f"conflicts={st.get('version_conflicts',0):,} "
          f"{'DONE' if done else ''}")
    if done:
        break
    time.sleep(15)

after = es.count(index=IDX, query=bad_query)["count"]
print(f"bad operator_name docs after: {after:,}  (fixed {before-after:,})")

# show new distribution
a = es.search(index=IDX, size=0, aggs={
    "byname": {"terms": {"field": "operator_name", "size": 15,
                         "missing": "(MISSING)"}}})
print("--- operator_name now ---")
for b in a["aggregations"]["byname"]["buckets"]:
    print(f"  {str(b['key'])[:28]:28s} {b['doc_count']:>9,}")
