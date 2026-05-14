"""
ml/explore.py - Schema + value distribution exploration for the transit indices.

Connects to Elasticsearch (default: VM via SSH tunnel, fallback: localhost:9200)
and inspects:
  - transit-trip-updates  (intended delay source — many fields are NULL in practice)
  - transit-bus-positions (fallback delay source via velocity)
  - transit-traffic       (cross-source enrichment for jam_factor)

Saves a 1000-row CSV snapshot from bus-positions to ml/data_sample.csv so
the rest of the pipeline can iterate offline.

Run:
    python ml/explore.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
SAMPLE_CSV = ROOT / "data_sample.csv"
ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
SSH_HOST = os.environ.get("ES_SSH_HOST", "local_admin@192.168.1.110")


def _es_get(index: str, body: dict, size: int = 1000) -> dict:
    """Fetch from ES via direct HTTP if reachable, else SSH+curl."""
    try:
        from elasticsearch import Elasticsearch  # type: ignore

        es = Elasticsearch(ES_URL, request_timeout=60)
        if es.ping():
            res = es.search(index=index, body=body, size=size)
            return res.body if hasattr(res, "body") else dict(res)
    except Exception:
        pass

    # Fallback: SSH + curl on the VM
    payload = json.dumps(body).replace("'", "'\\''")
    cmd = (
        f"curl -sm 60 -H 'Content-Type: application/json' "
        f"'http://localhost:9200/{index}/_search?size={size}' -d '{payload}'"
    )
    out = subprocess.check_output(["ssh", SSH_HOST, cmd], timeout=120)
    return json.loads(out)


def describe(records: list, name: str) -> None:
    print(f"\n=== {name}  (n={len(records)}) ===")
    if not records:
        print("  (no records)")
        return
    df = pd.DataFrame(records)
    print("  Columns:")
    for c in df.columns:
        n_null = df[c].isna().sum()
        dtype = df[c].dtype
        sample_val = df[c].dropna().iloc[0] if df[c].notna().any() else "ALL_NULL"
        print(f"    - {c:<22s} {str(dtype):<10s}  nulls={n_null:>4d}/{len(df):<4d}  e.g. {str(sample_val)[:40]}")


def main() -> int:
    print(f"[explore] ES_URL={ES_URL}  ssh_fallback={SSH_HOST}")

    # --- transit-trip-updates ---
    body = {"query": {"match_all": {}}}
    res = _es_get("transit-trip-updates", body, size=1000)
    hits = [h["_source"] for h in res.get("hits", {}).get("hits", [])]
    describe(hits, "transit-trip-updates (sample 1000)")

    delay_present = sum(1 for r in hits if r.get("delay_minutes") is not None)
    print(f"  -> records with delay_minutes set: {delay_present} / {len(hits)}")
    status_counts = Counter(r.get("status") for r in hits)
    print(f"  -> status distribution: {dict(status_counts)}")

    # --- transit-bus-positions (this is where useful signal lives) ---
    body = {
        "_source": [
            "velocity", "route_short_name", "operator_id", "operator_name",
            "recorded_at", "lat", "lon", "line_ref", "bearing",
        ],
        "query": {"bool": {"must": [{"exists": {"field": "route_short_name"}}]}},
    }
    res = _es_get("transit-bus-positions", body, size=1000)
    bus_hits = [h["_source"] for h in res.get("hits", {}).get("hits", [])]
    describe(bus_hits, "transit-bus-positions (sample 1000)")

    if bus_hits:
        df = pd.DataFrame(bus_hits)
        print(f"  -> velocity stats: min={df['velocity'].min()} mean={df['velocity'].mean():.1f} "
              f"p50={df['velocity'].median()} p90={df['velocity'].quantile(0.9)} max={df['velocity'].max()}")
        print(f"  -> top operators: {df['operator_name'].value_counts().head(5).to_dict()}")
        print(f"  -> unique routes: {df['route_short_name'].nunique()}")
        df.to_csv(SAMPLE_CSV, index=False, encoding="utf-8-sig")
        print(f"  -> wrote {SAMPLE_CSV} ({SAMPLE_CSV.stat().st_size:,} bytes)")

    # --- transit-traffic (jam_factor enrichment) ---
    body = {
        "_source": ["jam_factor", "speed_kmh", "free_flow_kmh", "is_congested",
                    "lat", "lon", "recorded_at", "hour_of_day", "day_of_week"],
        "query": {"match_all": {}},
    }
    res = _es_get("transit-traffic", body, size=1000)
    traffic_hits = [h["_source"] for h in res.get("hits", {}).get("hits", [])]
    describe(traffic_hits, "transit-traffic (sample 1000)")

    if traffic_hits:
        df = pd.DataFrame(traffic_hits)
        print(f"  -> jam_factor stats: mean={df['jam_factor'].mean():.2f} "
              f"p50={df['jam_factor'].median()} p90={df['jam_factor'].quantile(0.9)} max={df['jam_factor'].max()}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
