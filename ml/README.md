# Bus Delay Prediction (`ml/`)

A small ML pipeline that predicts the **expected delay (in minutes)** for a
bus line at a given hour, day-of-week, and traffic level.  Trained on real
data from the project's Elasticsearch indices on `linub-vm`.

---

## What the model predicts

`predicted_delay_minutes` for a bus on `route_short_name` X, operator Y, at
hour H, day-of-week D, with current `jam_factor` J.

The output is the **expected (smoothed) delay for that route/time slot**,
not an instantaneous estimate of one specific bus.  Confidence interval is
±1 residual standard deviation on the held-out test set.

---

## Honest caveat: synthetic target

`transit-trip-updates` (the index that's *supposed* to carry GTFS-RT delays)
contains 92K records but **all 92K have `delay_minutes = null`**.  The
upstream Stride collector indexes the schema but never resolves actual
arrival/scheduled times into a delay value.

We therefore synthesize the target from `transit-bus-positions.velocity`:

```
expected_kmh   = 75th percentile of velocity for that route (moving buses only)
delay_per_record_minutes = clip( 2 × (60/v − 60/expected_kmh), 0, 15 )
delay_minutes  = mean(delay_per_record) per (route, hour_of_day, is_weekend)
```

Stationary buses (`velocity == 0`) are dropped because they are
indistinguishable from parked vehicles and dominate the raw dataset
(~55%).  The smoothed target lets the model learn typical-delay patterns
that are observable from the inference-time features (line, hour, day,
traffic) — instantaneous velocity is not available at prediction time.

If a future ETL job populates real `delay_minutes` in `transit-trip-updates`,
swap the data source in `train_model.py:pull_buses()` and the rest works
unchanged.

---

## Features

| Feature | Source | Type |
|---|---|---|
| `hour_of_day` (0–23, Israel local time) | bus `recorded_at` | num |
| `day_of_week` (0=Mon … 6=Sun) | bus `recorded_at` | num |
| `is_weekend` (Israel: Fri/Sat = 1) | derived | num |
| `is_rush_hour` (07–09 ∪ 16–19) | derived | num |
| `expected_kmh` (route p75 velocity) | bus-positions agg | num |
| `avg_jam_factor` (mean for hour/dow) | **transit-traffic** (cross-source) | num |
| `route_bucket` (top 100 routes + "other") | `route_short_name` | cat |
| `operator_name` | bus-positions | cat |

---

## Performance (test set, n=2 687)

Best model: **GradientBoostingRegressor** (200 trees, depth 4, lr 0.05).

| Model | MAE (min) | RMSE (min) | R² |
|---|---|---|---|
| LinearRegression | 0.59 | 1.10 | 0.51 |
| **GradientBoosting** | **0.60** | **0.89** | **0.68** |
| RandomForest | 0.62 | 0.96 | 0.62 |

Top features (gradient-boosting importance):

1. `expected_kmh` — 0.26
2. `operator_name=GB Tours` — 0.12
3. `avg_jam_factor` — 0.11
4. `hour_of_day` — 0.10
5. `route_bucket=17083` — 0.05

(Full ranking in `ml/saved/metrics.json`.)

---

## How to retrain

```bash
# from project root
python ml/train_model.py
```

The script:

1. Pulls up to 30 000 records from `transit-bus-positions` via SSH+curl
   (uses ES scroll API; cached to `ml/data_train.json.gz`).
2. Pulls per-(hour × day-of-week) average `jam_factor` from `transit-traffic`.
3. Synthesizes the smoothed delay target.
4. Fits LinearRegression, GradientBoostingRegressor, RandomForestRegressor.
5. Picks the lowest-RMSE model and saves to `ml/saved/`.

Override the data size with `ML_TARGET_RECORDS=50000 python ml/train_model.py`.

---

## Output files (`ml/saved/`)

| File | Purpose |
|---|---|
| `delay_model.pkl` | Joblib-pickled `Pipeline(ColumnTransformer → model)` |
| `feature_meta.json` | Feature schema, known routes/operators, residual std |
| `route_expected_kmh.json` | Per-route expected velocity (inference fallback) |
| `metrics.json` | All 3 models' metrics + feature importance + target stats |

---

## Inference

```python
from ml.predict import load_model, predict_delay

load_model()
print(predict_delay(line="171", operator="Dan", hour=8, day_of_week=1))
# {"predicted_delay_minutes": 1.4, "confidence_interval": [0.5, 2.3], ...}
```

Or via the bot.py HTTP endpoint:

```bash
curl 'http://localhost:5000/api/predict_delay?line=171&operator_ref=3'
```

If `hour`/`day_of_week`/`jam_factor` are omitted, the endpoint defaults to
the current Israel time and pulls a 1-hour mean `jam_factor` from
`transit-traffic`.

---

## Limitations

- Synthetic target (see above) — the model predicts *expected* delay
  patterns, not the actual delay any specific bus is currently experiencing.
- Trained on ~13 K moving-bus records pulled in May 2026.  Patterns may
  drift if Tel Aviv traffic conditions change substantially.
- `route_bucket` is top-100 by frequency.  Long-tail lines fall into "other"
  and get the average prediction for that hour/operator.
- Confidence interval is a flat ±1σ band — not properly calibrated quantiles.
- The target is bounded at 15 minutes; severe disruptions are not predicted.
