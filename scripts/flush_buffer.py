#!/usr/bin/env python3
"""
flush_buffer.py — store-and-forward flush worker.

Reads pending files from BUFFER_DIR/minio_pending/ and uploads them to MinIO.
On success, deletes the local copy. On failure, leaves them for next run.

Used by resilient_pipeline_run.sh after MinIO comes back online.
"""
import os
import sys
import json
import time
from pathlib import Path

import boto3
from botocore.client import Config

BUFFER_DIR  = Path(os.getenv('BUFFER_DIR', '/home/local_admin/finalproject/buffer'))
PENDING_DIR = BUFFER_DIR / 'minio_pending'

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
ACCESS         = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
SECRET         = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
BUCKET         = os.getenv('S3_BUCKET_NAME', 'israel-transit-lake')


def main():
    if not PENDING_DIR.exists():
        print(f'No pending dir at {PENDING_DIR}')
        return 0

    pending = sorted(PENDING_DIR.glob('*.meta.json'))
    if not pending:
        print('Nothing pending to flush.')
        return 0

    print(f'Flushing {len(pending)} pending files to MinIO...')

    s3 = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
    )

    # Verify connectivity once before starting
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception as e:
        print(f'MinIO unreachable: {e}. Will retry next run.')
        return 1

    flushed = 0
    failed  = 0
    for meta_f in pending:
        # meta file is foo.meta.json → data file is foo.data
        # Path.stem strips only one extension, so foo.meta.json → foo.meta
        # Strip the trailing .meta to get the base, then append .data
        base = meta_f.stem  # foo.meta
        if base.endswith('.meta'):
            base = base[:-5]
        data_f = PENDING_DIR / (base + '.data')
        if not data_f.exists():
            meta_f.unlink(missing_ok=True)
            continue
        try:
            with open(meta_f) as f:
                meta = json.load(f)
            data = data_f.read_bytes()
            s3.put_object(
                Bucket=meta.get('bucket', BUCKET),
                Key=meta['key'],
                Body=data,
                ContentType=meta.get('ctype', 'application/json'),
            )
            meta_f.unlink()
            data_f.unlink()
            flushed += 1
            if flushed % 10 == 0:
                print(f'  Flushed {flushed}...')
        except Exception as e:
            print(f'  FAIL {meta_f.name}: {e}')
            failed += 1
            if failed > 5:
                print('Too many failures — stopping')
                break
            time.sleep(2)

    print(f'Done. Flushed {flushed}, failed {failed}, remaining {len(list(PENDING_DIR.glob("*.meta.json")))}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
