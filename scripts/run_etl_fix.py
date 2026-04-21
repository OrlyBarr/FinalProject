"""One-shot ETL run to produce clean train-positions files with the fixed transformer."""
import sys, os, json
sys.path.insert(0, '/home/local_admin/finalproject')
os.environ.setdefault('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

from kafka import KafkaConsumer
from storage.s3_writer import S3Writer
from etl.transformers import TrainPositionTransformer

consumer = KafkaConsumer(
    'train-positions',
    bootstrap_servers='localhost:9092',
    group_id='trains-etl-fix1',
    value_deserializer=lambda v: json.loads(v.decode()),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    consumer_timeout_ms=8000,
    max_poll_records=500,
)

transformer = TrainPositionTransformer()
raw_w  = S3Writer(prefix='raw/train-positions')
proc_w = S3Writer(prefix='processed/train-positions')
raws, procs = [], []

for msg in consumer:
    raw = msg.value.get('data', {})
    if not raw:
        continue
    raws.append(raw)
    try:
        t = transformer.transform(raw)
        t['ingested_at'] = msg.value.get('fetched_at')
        procs.append(t)
    except Exception as e:
        print(f'Transform error: {e}')

consumer.commit()
consumer.close()

if raws:
    raw_w.write_batch(raws)
if procs:
    proc_w.write_batch(procs)

print(f'Consumed {len(raws)} raw messages, produced {len(procs)} processed records')
if procs:
    sample = procs[-1]  # newest record
    bad = [k for k, v in sample.items() if v is None or v == '']
    good_keys = [k for k in ['lat', 'lon', 'velocity', 'line_ref'] if k in sample]
    print(f'Sample (newest): {json.dumps(sample, ensure_ascii=False)[:500]}')
    print(f'Null/empty fields: {bad}')
    print(f'New fields present: {good_keys}')
    print(f'Station fields removed: {not any(k in sample for k in ["origin_station_id", "queried_station_name"])}')
