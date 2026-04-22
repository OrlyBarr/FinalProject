import sys
sys.path.insert(0, '/opt/airflow')
from storage.direct_to_minio import run, fetch_delays, now_il
import json

print("=== Testing _effective_delay logic ===")
now = now_il()
print(f"now = {now.isoformat()}")

print("\n=== Fetching delays ===")
delays = fetch_delays()
print(f"Got {len(delays)} delay records")
if not delays:
    print("No delays returned!")
    sys.exit(1)

sample = delays[0]
print(f"Sample scheduled_start: {sample.get('scheduled_start')}")
print(f"Sample actual_time: {sample.get('actual_time')}")
print(f"Sample delay_seconds: {sample.get('delay_seconds')}")

# Test _effective_delay inline
def _effective_delay(d):
    ds = d.get("delay_seconds")
    if ds is not None:
        return ds
    if d.get("actual_time") is None:
        sched = d.get("scheduled_start")
        if sched:
            try:
                from datetime import datetime, timezone
                sched_dt = datetime.fromisoformat(sched)
                if sched_dt.tzinfo is None:
                    sched_dt = sched_dt.replace(tzinfo=timezone.utc)
                elapsed = (now - sched_dt).total_seconds()
                return max(0, elapsed)
            except Exception as e:
                print(f"  Error computing delay: {e}")
    return 0

sample_delay = _effective_delay(sample)
print(f"\nEffective delay for sample: {sample_delay:.0f} seconds ({sample_delay/60:.1f} minutes)")

threshold = 300
matching = [d for d in delays if _effective_delay(d) >= threshold]
print(f"\nRecords with effective delay >= {threshold}s: {len(matching)}/{len(delays)}")

if matching:
    print("=> Would write to raw/delay-events/ and processed/delay-events/")
else:
    print("=> NOTHING to write! All effective delays < 300s")
    sample_delays = [_effective_delay(d) for d in delays[:5]]
    print(f"Sample delays: {sample_delays}")
