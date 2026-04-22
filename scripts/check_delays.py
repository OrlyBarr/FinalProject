import boto3, json
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin123')
r = s3.list_objects_v2(Bucket='israel-transit-lake', Prefix='raw/trip-updates/', MaxKeys=10)
keys = sorted([o['Key'] for o in r.get('Contents',[]) if o['Size']>0])
if not keys:
    print("No trip-updates files found")
    exit()
body = s3.get_object(Bucket='israel-transit-lake', Key=keys[-1])['Body'].read()
records = json.loads(body)
print(f'Total records: {len(records)}')
sample = records[0]
print('Keys:', list(sample.keys()))
print('Sample values:')
for k,v in sample.items():
    print(f'  {k}: {repr(v)}')
non_none = sum(1 for r in records if r.get('delay_seconds') is not None)
print(f'Records with delay_seconds not None: {non_none}/{len(records)}')
ds_vals = [r.get('delay_seconds') for r in records if r.get('delay_seconds') is not None]
if ds_vals:
    print(f'delay_seconds range: {min(ds_vals)} to {max(ds_vals)}')
late = sum(1 for r in records if str(r.get('status','')).lower() in ('late','very_late','delayed'))
print(f'Records with late/very_late/delayed status: {late}')
print('Unique status values:', set(r.get('status') for r in records[:100]))
