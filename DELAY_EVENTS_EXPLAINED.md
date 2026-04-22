# Delay Events & Service Alerts — How the System Works

## Question 1: Why is `detected_at` today but `fetched_at` is yesterday's evening?

### Short Answer
`fetched_at` and `detected_at` are **two different timestamps** that measure two different moments in the pipeline:

| Field | What it represents |
|---|---|
| `fetched_at` | When the **raw data was pulled** from the Open Bus Stride API |
| `detected_at` | When the system **decided the ride counts as a delay event** (after ETL processing) |

### Why the gap exists
The pipeline runs like this:

```
[Open Bus Stride API]
        ↓  (fetch every 5 min, DAG: dag_direct_alerts)
  raw/service-alerts/  ← fetched_at is stamped HERE (last night's run)
        ↓  (ETL job runs later: dag_ETL_transform)
  delay-events topic   ← detected_at is stamped HERE (this morning's run)
```

The `fetched_at` value is set at fetch time inside `fetch_service_alerts()`:
```python
fetched_at = now.isoformat()   # set when API is called
...
"fetched_at": fetched_at,      # stored in the raw record
```

Later, during ETL, `detected_at` is added at **detection time**:
```python
transformed["detected_at"] = datetime.now(ZoneInfo("Asia/Jerusalem")).isoformat()
```

So if the data was fetched the previous evening but the ETL ran this morning, `fetched_at` will be from last night and `detected_at` will be from today — which is exactly what you are seeing. **This is normal and expected.**

---

## Question 2: How does the system calculate delay events and service alerts? Why are most fields null?

### How service alerts are calculated

Service alerts are **not** fetched from a dedicated alerts API — the MOT GTFS-RT alerts endpoint (`gtfs.mot.gov.il`) is currently blocked/unavailable.

Instead, alerts are **derived** from ride data using the Open Bus Stride `siri_rides/list` endpoint.

A ride is flagged as an alert when:
- `updated_duration_minutes > duration_minutes + 10` → **Significant Delay**
- `updated_duration_minutes == 0` and `duration_minutes > 0` → **Cancellation**

```python
delay_added = updated_duration_minutes - duration_minutes
is_cancelled = (planned_dur > 0 and updated_dur == 0)
is_delayed   = (delay_added >= 10)
```

### How delay events are calculated

A delay event is created when:
1. **Primary path:** A ride from the service-alerts set has `extra_delay_min >= 5` or is a cancellation.
2. **Fallback path (ETL DAG):** Trip-updates from Kafka are consumed; any trip with `delay_minutes >= 5` is promoted to a delay event.
3. **Second fallback:** If no live data is available, the system reads the latest file from `raw/service-alerts/` in MinIO and flags rides that started `>= 10 minutes ago` based on elapsed time.

### Why most fields are null

The delay event document is built from the service-alerts record, which in turn comes from the `siri_rides/list` API. Most fields in that API response can be `null` or missing when:

- The ride has **not yet been tracked** by a real SIRI vehicle (no GPS ping).
- The ride `scheduled_start` is in the past but the vehicle has not yet reported.
- Fields like `stop_id`, `stop_name`, `line_ref`, etc. are only populated when a SIRI vehicle is **matched to the GTFS trip** — if the match fails, they stay `null`.
- The Open Bus Stride API itself sometimes returns sparse records when the ride data is incomplete.

**In short:** the delay event was detected (the scheduled ride is late), but the real-time vehicle data needed to fill in all the details was not available at the time of detection.

### Field population summary

| Field | When populated |
|---|---|
| `ride_id` | Always (from siri_ride_id) |
| `line_ref` | When SIRI route is matched |
| `stop_id` / `stop_name` | When stop-level SIRI data exists |
| `extra_delay_min` | When both planned and updated durations are available |
| `detected_at` | Always (set by the ETL at detection time) |
| `fetched_at` | Always (set at API fetch time) |
| `delay_seconds` | Derived from `extra_delay_min`; 0 if data is missing |

---

## Pipeline Overview

```
Open Bus Stride API
        │
        ▼ (every 5 min)
  MinIO: raw/service-alerts/          ← fetched_at stamped
        │
        ▼ (ETL DAG: dag_ETL_transform)
  Kafka: trip-updates
        │
        ▼
  Delay detector (threshold: 5 min)   ← detected_at stamped
        │
        ├──▶ MinIO: raw/delay-events/
        ├──▶ MinIO: processed/delay-events/
        └──▶ Kafka: delay-events
                    │
                    ▼
             Elasticsearch → Kibana
```
