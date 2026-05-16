#!/usr/bin/env python3
"""
Re-index historical data from MinIO into Elastic Cloud.

Reads processed/ JSON files from MinIO and bulk-indexes them into ES.
Designed for the one-time migration when moving ES from local Docker to Elastic Cloud.

Usage:
    # On the VM (after setting ELASTIC_CLOUD_ID + ELASTIC_API_KEY in .env):
    source venv/bin/activate
    python3 scripts/reindex_minio_to_cloud_es.py --days 14
    python3 scripts/reindex_minio_to_cloud_es.py --start 2026-05-01 --end 2026-05-15
    python3 scripts/reindex_minio_to_cloud_es.py --prefix processed/bus-positions/year=2026/month=05/

Performance: ~5K docs/sec on Elastic Cloud t3.small.search
"""
import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
from elasticsearch import helpers

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

# Make storage/ importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from storage.es_client import get_es_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("reindex")

# ───────────────────────────────────────────────────────────
# MinIO config (uses localhost when run on the VM host)
# ───────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET           = os.getenv("S3_BUCKET_NAME", "israel-transit-lake")

# MinIO prefix → ES index mapping
PREFIX_TO_INDEX = {
    "processed/bus-positions/":   "transit-bus-positions",
    "processed/train-positions/": "transit-train-positions",
    "processed/traffic-data/":    "transit-traffic",
    "raw/service-alerts/":        "transit-service-alerts",
}


def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4", connect_timeout=10, read_timeout=120),
    )


def iter_keys(s3, prefix: str):
    """Iterate all object keys under a prefix using pagination."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def fetch_records(s3, key: str):
    """Download one JSON object and yield records (handles both array + JSONL)."""
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if not body:
        return
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return
    # Try JSON array first
    if text.startswith("["):
        try:
            for rec in json.loads(text):
                yield rec
            return
        except json.JSONDecodeError:
            pass
    # JSONL fallback
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def reindex_prefix(es, s3, prefix: str, index: str, batch_size: int = 1000) -> int:
    """Bulk-index all records under one MinIO prefix into one ES index."""
    log.info("→ %s  →  %s", prefix, index)
    total = 0
    actions = []
    file_count = 0
    t0 = time.time()

    for key in iter_keys(s3, prefix):
        file_count += 1
        for rec in fetch_records(s3, key):
            # Deterministic _id → re-indexing the same record is idempotent
            # (no duplicates if Tue + Wed catch-up ranges overlap).
            _id = hashlib.md5(
                json.dumps(rec, sort_keys=True, default=str).encode()
            ).hexdigest()
            actions.append({"_index": index, "_id": _id, "_source": rec})
            if len(actions) >= batch_size:
                _flush(es, actions)
                total += len(actions)
                actions = []
        if file_count % 50 == 0:
            log.info("   files=%d  docs=%d  elapsed=%.0fs", file_count, total, time.time() - t0)

    if actions:
        _flush(es, actions)
        total += len(actions)

    log.info("✓ %s — %d files, %d docs in %.1fs", prefix, file_count, total, time.time() - t0)
    return total


def _flush(es, actions):
    """Bulk-index with retry on transient errors."""
    try:
        helpers.bulk(es, actions, request_timeout=120, raise_on_error=False)
    except Exception as e:
        log.warning("bulk error: %s", e)


def build_date_prefixes(base: str, start: datetime, end: datetime):
    """Generate year=Y/month=M/day=D prefixes between start and end."""
    cur = start
    while cur < end:
        yield f"{base}year={cur.year}/month={cur.month:02d}/day={cur.day:02d}/"
        cur += timedelta(days=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=None,
                    help="Re-index last N days (default: all data)")
    ap.add_argument("--start", type=str, default=None,
                    help="Start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end",   type=str, default=None,
                    help="End date   YYYY-MM-DD (exclusive). Default: today+1")
    ap.add_argument("--prefix", type=str, default=None,
                    help="Override: re-index this MinIO prefix only")
    ap.add_argument("--index",  type=str, default=None,
                    help="Override: target ES index when --prefix is set")
    ap.add_argument("--all", action="store_true",
                    help="Re-index ALL data: scan each base prefix fully, "
                         "no date filtering (catches every file in MinIO)")
    ap.add_argument("--since-last", action="store_true",
                    help="Catch-up mode: index from the last successful sync "
                         "(state file) to now, then record now. Idempotent "
                         "(deterministic _id) so overlapping runs are safe. "
                         "Use this Tue evening AND Wed — it just works.")
    ap.add_argument("--state-file", type=str,
                    default="/tmp/reindex_last_sync.txt",
                    help="Where --since-last stores the last sync timestamp")
    ap.add_argument("--batch-size", type=int, default=1000)
    args = ap.parse_args()

    es = get_es_client(request_timeout=60)
    if not es.ping():
        log.error("Cannot reach Elasticsearch. Check ELASTIC_CLOUD_ID / ELASTIC_API_KEY.")
        sys.exit(1)
    log.info("ES OK. Cluster: %s", es.info()["cluster_name"])

    s3 = make_s3()

    # Custom prefix mode
    if args.prefix:
        if not args.index:
            log.error("--index is required when --prefix is set")
            sys.exit(1)
        reindex_prefix(es, s3, args.prefix, args.index, args.batch_size)
        return

    # ALL mode — scan each base prefix completely, no date filtering.
    # This is the most thorough: it picks up EVERY file in MinIO under
    # each known prefix regardless of its partition date.
    if args.all:
        log.info("ALL mode: scanning every file under each base prefix")
        grand_total = 0
        for base, index in PREFIX_TO_INDEX.items():
            grand_total += reindex_prefix(es, s3, base, index, args.batch_size)
        log.info("=" * 60)
        log.info("DONE (ALL): %d total docs indexed", grand_total)
        return

    # Date range mode
    now = datetime.now(timezone.utc)
    if args.since_last:
        # Read last sync; default to 14 days back if no state yet.
        last = None
        try:
            with open(args.state_file) as f:
                last = datetime.fromisoformat(f.read().strip())
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
        except (FileNotFoundError, ValueError):
            pass
        if last is None:
            start = now - timedelta(days=14)
            log.info("since-last: no state file, defaulting to last 14 days")
        else:
            # 1-day overlap buffer; safe because _id is deterministic.
            start = last - timedelta(days=1)
            log.info("since-last: last sync was %s → indexing from %s",
                     last.date(), start.date())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    elif args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    elif args.days:
        start = now - timedelta(days=args.days)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start = datetime(2026, 3, 14, tzinfo=timezone.utc)  # project start
    end = (datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
           if args.end else now + timedelta(days=1))

    log.info("Range: %s → %s", start.date(), end.date())
    grand_total = 0
    for base, index in PREFIX_TO_INDEX.items():
        for prefix in build_date_prefixes(base, start, end):
            grand_total += reindex_prefix(es, s3, prefix, index, args.batch_size)

    if args.since_last:
        with open(args.state_file, "w") as f:
            f.write(now.isoformat())
        log.info("since-last: recorded sync time %s", now.isoformat())

    log.info("=" * 60)
    log.info("DONE: %d total docs indexed", grand_total)


if __name__ == "__main__":
    main()
