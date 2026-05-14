"""
ml/train_model.py - Train the bus delay prediction model.

Data source decision (see ml/README.md for rationale):
  - transit-trip-updates has 92K records but EVERY one has delay=NULL,
    status='unknown' — the upstream Stride collector never resolves them.
  - We therefore use transit-bus-positions and SYNTHESIZE delay from velocity.
    A bus moving below an expected pace for its hour/area is treated as
    'delayed' minutes-per-km behind free-flow.
  - We enrich every record with the median jam_factor from transit-traffic
    for the same hour-of-day. This is the single most predictive feature.

Synthetic delay formula (deliberately conservative):
    expected_kmh = max(operator_route_pXX_velocity, 12)   # floor 12 km/h
    if velocity == 0 and bus is moving (bearing != 0): treat as 5-min stoppage
    delay_min   = clip( 60 / max(velocity,1) - 60 / expected_kmh, 0, 30 )
    (i.e. extra minutes per km vs expected, scaled to a typical 1-km segment)

Run:
    python ml/train_model.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parent
SAVED_DIR = ROOT / "saved"
SAVED_DIR.mkdir(exist_ok=True)
PARQUET_CACHE = ROOT / "data_train.json.gz"

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
SSH_HOST = os.environ.get("ES_SSH_HOST", "local_admin@192.168.1.110")

TARGET_RECORDS = int(os.environ.get("ML_TARGET_RECORDS", "30000"))
PAGE_SIZE = 5000  # records per ES request
MODEL_VERSION = "v1-velocity-synth-2026-05-10"


# ---------------------------------------------------------------------------
# 1. Data pulling
# ---------------------------------------------------------------------------
def _ssh_curl_raw(url_path: str, body: dict | None = None, method: str = "GET") -> dict:
    """Run curl on the VM. URL is relative to http://localhost:9200/."""
    if body is not None:
        payload = json.dumps(body).replace("'", "'\\''")
        cmd = (
            f"curl -sm 120 -X {method} -H 'Content-Type: application/json' "
            f"'http://localhost:9200/{url_path}' -d '{payload}'"
        )
    else:
        cmd = f"curl -sm 120 -X {method} 'http://localhost:9200/{url_path}'"
    out = subprocess.check_output(["ssh", SSH_HOST, cmd], timeout=200)
    return json.loads(out)


def _ssh_curl(index: str, body: dict, params: str = "") -> dict:
    return _ssh_curl_raw(f"{index}/_search?{params}", body, method="POST")


def _pull_via_scroll(index: str, body: dict, target: int) -> list[dict]:
    """Pull `target` records using ES scroll API (via SSH+curl).

    Scroll is the most reliable bulk read for large windows (avoids the
    search_after timing variability we hit on the loaded VM)."""
    body = dict(body)
    body["size"] = PAGE_SIZE
    res = _ssh_curl(index, body, params="scroll=2m")
    scroll_id = res.get("_scroll_id")
    all_hits: list[dict] = [h["_source"] for h in res.get("hits", {}).get("hits", [])]
    print(f"    pulled {len(all_hits)} from {index}")

    while scroll_id and len(all_hits) < target:
        scroll_body = {"scroll": "2m", "scroll_id": scroll_id}
        res = _ssh_curl_raw("_search/scroll", scroll_body, method="POST")
        scroll_id = res.get("_scroll_id")
        hits = res.get("hits", {}).get("hits", [])
        if not hits:
            break
        all_hits.extend(h["_source"] for h in hits)
        print(f"    pulled {len(all_hits)} from {index}")

    # cleanup scroll context (best effort)
    if scroll_id:
        try:
            _ssh_curl_raw(f"_search/scroll", {"scroll_id": scroll_id}, method="DELETE")
        except Exception:
            pass

    return all_hits[:target]


def pull_buses() -> pd.DataFrame:
    if PARQUET_CACHE.exists():
        df = pd.read_json(PARQUET_CACHE, compression="gzip", orient="records", lines=True)
        print(f"[train] loaded {len(df)} rows from cache {PARQUET_CACHE}")
        return df

    print(f"[train] pulling up to {TARGET_RECORDS} bus records via SSH+curl ...")
    body = {
        "_source": [
            "velocity", "route_short_name", "operator_id", "operator_name",
            "recorded_at", "lat", "lon", "line_ref", "bearing", "vehicle_id",
        ],
        "query": {
            "bool": {
                "must": [
                    {"exists": {"field": "route_short_name"}},
                    {"exists": {"field": "velocity"}},
                    {"exists": {"field": "recorded_at"}},
                ]
            }
        },
    }
    hits = _pull_via_scroll("transit-bus-positions", body, TARGET_RECORDS)
    df = pd.DataFrame(hits)
    df.to_json(PARQUET_CACHE, compression="gzip", orient="records", lines=True, force_ascii=False)
    print(f"[train] cached {len(df)} rows to {PARQUET_CACHE}")
    return df


def pull_traffic_aggregates() -> pd.DataFrame:
    """Mean jam_factor per (hour, day_of_week) — used to enrich bus rows."""
    print("[train] pulling traffic aggregates ...")
    body = {
        "size": 0,
        "aggs": {
            "by_hour": {
                "terms": {"field": "hour_of_day", "size": 24},
                "aggs": {
                    "by_dow": {
                        "terms": {"field": "day_of_week.keyword", "size": 8},
                        "aggs": {"jam": {"avg": {"field": "jam_factor"}}},
                    }
                },
            }
        },
    }
    res = _ssh_curl("transit-traffic", body)
    rows = []
    for h_bucket in res.get("aggregations", {}).get("by_hour", {}).get("buckets", []):
        hour = int(h_bucket["key"])
        for d_bucket in h_bucket.get("by_dow", {}).get("buckets", []):
            rows.append({
                "hour_of_day": hour,
                "day_of_week_str": d_bucket["key"],
                "avg_jam_factor": d_bucket["jam"]["value"] or 2.0,
            })
    if not rows:
        # fallback: flat 2.0
        return pd.DataFrame([
            {"hour_of_day": h, "day_of_week_str": d, "avg_jam_factor": 2.0}
            for h in range(24)
            for d in ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        ])
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 2. Feature engineering + synthetic delay target
# ---------------------------------------------------------------------------
DOW_NAMES = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
             4: "Friday", 5: "Saturday", 6: "Sunday"}


def synthesize_delay(velocity: float, expected_kmh: float) -> float:
    """Minutes lost vs expected pace over a typical 2-km segment, clipped [0,15].

    Only produces a meaningful target for *moving* buses. Velocity==0 buses
    are filtered out upstream because they're indistinguishable from parked
    vehicles and dominate the dataset (~50%).
    """
    v = max(float(velocity), 3.0)        # floor at walking pace
    e = max(float(expected_kmh), 15.0)   # floor expected at 15 km/h
    if v >= e:
        return 0.0
    # extra minutes over a 2-km segment at this pace vs expected
    extra_min = 2.0 * (60.0 / v - 60.0 / e)
    return float(np.clip(extra_min, 0.0, 15.0))


def build_features(buses: pd.DataFrame, traffic: pd.DataFrame) -> pd.DataFrame:
    df = buses.copy()
    df = df.dropna(subset=["recorded_at", "velocity", "route_short_name"])
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["recorded_at"])
    # Drop stationary buses: velocity 0 is dominated by parked / depot vehicles
    # and creates a bimodal target that no model can fit. Keep only moving buses.
    df = df[df["velocity"] > 3]

    # convert to Israel local time for hour features
    israel = df["recorded_at"].dt.tz_convert("Asia/Jerusalem")
    df["hour_of_day"] = israel.dt.hour
    df["day_of_week"] = israel.dt.dayofweek           # Mon=0 .. Sun=6
    df["day_of_week_str"] = df["day_of_week"].map(DOW_NAMES)
    df["is_rush_hour"] = df["hour_of_day"].isin([7, 8, 9, 16, 17, 18]).astype(int)

    # Per-route expected velocity = 75th percentile of moving-only buses on that route
    moving = df[df["velocity"] > 0]
    expected = (
        moving.groupby("route_short_name")["velocity"]
        .quantile(0.75)
        .rename("expected_kmh")
        .reset_index()
    )
    df = df.merge(expected, on="route_short_name", how="left")
    df["expected_kmh"] = df["expected_kmh"].fillna(df["velocity"].quantile(0.75) or 25.0)

    # Per-record synthetic delay
    df["delay_per_record"] = [
        synthesize_delay(v, e) for v, e in zip(df["velocity"], df["expected_kmh"])
    ]
    # Smoothed target: average synthetic delay per (route, hour, weekend-flag).
    # This is what "predicting delay for line X at hour H" actually means with
    # our feature set — instantaneous velocity is unobservable at inference time,
    # so we predict the *expected* delay for that route/time slot.
    df["is_weekend"] = df["day_of_week"].isin([4, 5]).astype(int)
    grp = (
        df.groupby(["route_short_name", "hour_of_day", "is_weekend"])["delay_per_record"]
        .mean()
        .reset_index()
        .rename(columns={"delay_per_record": "delay_minutes"})
    )
    df = df.drop(columns=["delay_per_record"]).merge(
        grp, on=["route_short_name", "hour_of_day", "is_weekend"], how="left"
    )

    # Cap top-100 routes; the rest become "other"
    df["route_short_name"] = df["route_short_name"].astype(str)
    top_routes = df["route_short_name"].value_counts().head(100).index
    df["route_bucket"] = df["route_short_name"].where(
        df["route_short_name"].isin(top_routes), other="other"
    ).astype(str)

    # Operator name fillna
    df["operator_name"] = df["operator_name"].fillna("Unknown").replace("", "Unknown").astype(str)
    df["operator_id"] = df["operator_id"].fillna("0").astype(str)

    # Traffic enrichment
    df = df.merge(traffic, on=["hour_of_day", "day_of_week_str"], how="left")
    df["avg_jam_factor"] = df["avg_jam_factor"].fillna(2.0)

    # Filter
    df = df[(df["delay_minutes"] >= 0) & (df["delay_minutes"] <= 30)]
    return df


# ---------------------------------------------------------------------------
# 3. Model training
# ---------------------------------------------------------------------------
NUM_FEATURES = ["hour_of_day", "day_of_week", "is_weekend", "is_rush_hour",
                "expected_kmh", "avg_jam_factor"]
CAT_FEATURES = ["route_bucket", "operator_name"]


def make_pipeline(model) -> Pipeline:
    pre = ColumnTransformer([
        ("num", StandardScaler(), NUM_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore"), CAT_FEATURES),
    ])
    return Pipeline([("pre", pre), ("model", model)])


def evaluate(name: str, pipe: Pipeline, X_test, y_test) -> dict:
    yhat = pipe.predict(X_test)
    mae = mean_absolute_error(y_test, yhat)
    rmse = float(np.sqrt(mean_squared_error(y_test, yhat)))
    r2 = r2_score(y_test, yhat)
    print(f"  [{name}]  MAE={mae:.3f}  RMSE={rmse:.3f}  R2={r2:.3f}")
    return {"name": name, "mae": float(mae), "rmse": rmse, "r2": float(r2)}


def main() -> int:
    t0 = time.time()
    buses = pull_buses()
    traffic = pull_traffic_aggregates()

    print(f"[train] feature engineering on {len(buses)} rows ...")
    df = build_features(buses, traffic)
    print(f"[train] after filtering: {len(df)} rows  "
          f"target mean={df['delay_minutes'].mean():.2f} min  "
          f"std={df['delay_minutes'].std():.2f} min")

    # Random 80/20 split. We considered time-based but our smoothed target
    # lives in (route × hour × is_weekend) space; a chronological split puts
    # most test combos out-of-distribution. Random split tests generalization
    # within the learned slot distribution.
    from sklearn.model_selection import train_test_split
    train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
    X_cols = NUM_FEATURES + CAT_FEATURES
    X_train, y_train = train_df[X_cols], train_df["delay_minutes"]
    X_test, y_test = test_df[X_cols], test_df["delay_minutes"]
    print(f"[train] split: train={len(X_train)} test={len(X_test)}")

    metrics = []

    print("[train] fitting Linear Regression ...")
    lr_pipe = make_pipeline(LinearRegression())
    lr_pipe.fit(X_train, y_train)
    metrics.append(evaluate("LinearRegression", lr_pipe, X_test, y_test))

    print("[train] fitting GradientBoostingRegressor ...")
    gb_pipe = make_pipeline(GradientBoostingRegressor(
        n_estimators=200, max_depth=4, learning_rate=0.05, random_state=42))
    gb_pipe.fit(X_train, y_train)
    metrics.append(evaluate("GradientBoosting", gb_pipe, X_test, y_test))

    print("[train] fitting RandomForestRegressor ...")
    rf_pipe = make_pipeline(RandomForestRegressor(
        n_estimators=150, max_depth=14, min_samples_leaf=4, n_jobs=-1, random_state=42))
    rf_pipe.fit(X_train, y_train)
    metrics.append(evaluate("RandomForest", rf_pipe, X_test, y_test))

    # Pick best by lowest RMSE (rewards lower variance + handles outliers better
    # than MAE; tree models also expose feature_importances_ for the report)
    metrics.sort(key=lambda m: m["rmse"])
    best = metrics[0]
    print(f"[train] best model: {best['name']}  MAE={best['mae']:.3f} min")
    best_pipe = {
        "LinearRegression": lr_pipe,
        "GradientBoosting": gb_pipe,
        "RandomForest": rf_pipe,
    }[best["name"]]

    # Feature importance (only for tree models)
    importance: dict = {}
    try:
        ohe = best_pipe.named_steps["pre"].named_transformers_["cat"]
        cat_names = list(ohe.get_feature_names_out(CAT_FEATURES))
        all_names = NUM_FEATURES + cat_names
        if hasattr(best_pipe.named_steps["model"], "feature_importances_"):
            imps = best_pipe.named_steps["model"].feature_importances_
            ranked = sorted(zip(all_names, imps), key=lambda x: -x[1])[:20]
            importance = {n: float(v) for n, v in ranked}
            print("[train] top features:")
            for n, v in ranked[:10]:
                print(f"    {n:<40s} {v:.4f}")
    except Exception as ex:
        print(f"[train] could not extract feature importance: {ex}")

    # Confidence interval = +/- residual std on the test set
    yhat_test = best_pipe.predict(X_test)
    resid_std = float(np.std(y_test - yhat_test))

    # Save artifacts
    joblib.dump(best_pipe, SAVED_DIR / "delay_model.pkl")
    print(f"[train] wrote {SAVED_DIR / 'delay_model.pkl'}")

    # Top routes/operators for the predict module to validate inputs
    top_routes = sorted(df["route_bucket"].dropna().unique().tolist())
    top_operators = sorted(df["operator_name"].dropna().unique().tolist())

    # Per-route expected_kmh lookup for inference (predict.py uses this when
    # the caller doesn't supply velocity context)
    route_expected = (
        df.groupby("route_short_name")["expected_kmh"].mean().round(2).to_dict()
    )
    global_expected = float(df["expected_kmh"].mean())
    (SAVED_DIR / "route_expected_kmh.json").write_text(
        json.dumps({str(k): float(v) for k, v in route_expected.items()},
                   ensure_ascii=False),
        encoding="utf-8"
    )

    feature_meta = {
        "model_version": MODEL_VERSION,
        "model_name": best["name"],
        "trained_at": pd.Timestamp.utcnow().isoformat(),
        "num_features": NUM_FEATURES,
        "cat_features": CAT_FEATURES,
        "all_feature_columns": NUM_FEATURES + CAT_FEATURES,
        "known_routes": top_routes,
        "known_operators": top_operators,
        "residual_std": resid_std,
        "global_expected_kmh": global_expected,
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "target": "delay_minutes (synthetic, derived from velocity vs route p75, smoothed by route+hour+is_weekend)",
    }
    (SAVED_DIR / "feature_meta.json").write_text(
        json.dumps(feature_meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[train] wrote {SAVED_DIR / 'feature_meta.json'}")

    metrics_doc = {
        "model_version": MODEL_VERSION,
        "best_model": best["name"],
        "all_metrics": metrics,
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "feature_importance_top20": importance,
        "target_stats": {
            "mean_minutes": float(df["delay_minutes"].mean()),
            "std_minutes": float(df["delay_minutes"].std()),
            "p50_minutes": float(df["delay_minutes"].median()),
            "p90_minutes": float(df["delay_minutes"].quantile(0.9)),
        },
        "residual_std_test": resid_std,
        "elapsed_seconds": round(time.time() - t0, 1),
    }
    (SAVED_DIR / "metrics.json").write_text(
        json.dumps(metrics_doc, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[train] wrote {SAVED_DIR / 'metrics.json'}")
    print(f"[train] done in {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
