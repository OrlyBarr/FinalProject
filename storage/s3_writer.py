"""
storage/s3_writer.py
Writes transit records to S3/MinIO with date+hour partitioning.
Partition: s3://bucket/prefix/year=YYYY/month=MM/day=DD/hour=HH/
Hour-level partitioning is important for transit data (peak hours analysis).
"""

import json
import logging
import boto3
from datetime import datetime, timezone
import sys
sys.path.append("..")
from config.settings import (
    S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    USE_MINIO, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
)

logger = logging.getLogger("storage.s3")


class S3Writer:

    def __init__(self, prefix: str):
        self.prefix  = prefix
        self.bucket  = S3_BUCKET
        self.client  = self._create_client()
        self._buffer = []
        self._ensure_bucket()

    def _create_client(self):
        if USE_MINIO:
            logger.info("Using MinIO (local S3)")
            return boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name="us-east-1",
            )
        return boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )

    def _ensure_bucket(self):
        if not USE_MINIO:
            return
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except Exception:
            self.client.create_bucket(Bucket=self.bucket)
            logger.info(f"Created bucket: {self.bucket}")

    def _build_partition(self) -> str:
        """Hour-level partitioning for transit peak-hour analysis."""
        now = datetime.now(timezone.utc)
        return (f"year={now.year}/month={now.month:02d}/"
                f"day={now.day:02d}/hour={now.hour:02d}")

    def write(self, record: dict):
        self._buffer.append(record)

    def flush(self, partition: str = None):
        if not self._buffer:
            return
        if not partition:
            partition = self._build_partition()

        ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        key = f"{self.prefix}/{partition}/{ts}.json"

        content = "\n".join(json.dumps(r, ensure_ascii=False) for r in self._buffer)

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content.encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "record_count": str(len(self._buffer)),
                    "source":       self.prefix.split("/")[-1],
                }
            )
            logger.info(f"S3 write: {len(self._buffer)} records → s3://{self.bucket}/{key}")
            self._buffer.clear()
        except Exception as e:
            logger.error(f"S3 write failed: {e}")
            raise

    def write_batch(self, records: list, partition: str = None):
        for r in records:
            self._buffer.append(r)
        self.flush(partition)

    def write_and_flush(self, record: dict):
        self.write(record)
        self.flush()

    def list_partitions(self, prefix_filter: str = None) -> list:
        search = prefix_filter or self.prefix
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=search, Delimiter="/"):
                for cp in page.get("CommonPrefixes", []):
                    keys.append(cp["Prefix"])
            return keys
        except Exception as e:
            logger.error(f"S3 list error: {e}")
            return []