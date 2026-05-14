"""
resilient_pipeline.py
=====================
Three resilience layers for the Israel Transit pipeline:

  Layer 1 — Local Buffer   : Kafka down → save to file, send when back up
  Layer 2 — Airflow Retry  : retry with exponential backoff in DAGs
  Layer 3 — MinIO Direct   : MinIO down → save gzip locally, upload when back

Usage:
  from resilient_pipeline import ResilientProducer, ResilientMinioWriter
  from resilient_pipeline import RESILIENT_DEFAULT_ARGS   # in DAGs
"""

import json
import os
import time
import gzip
import logging
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BUFFER_DIR    = Path(os.getenv("BUFFER_DIR", "/tmp/transit_buffer"))
BUFFER_DIR.mkdir(parents=True, exist_ok=True)

MAX_BUFFER_MB  = int(os.getenv("MAX_BUFFER_MB",  "100"))   # 100 MB max — prevents disk fill on VM
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL",  "30"))


# ════════════════════════════════════════════════════════════════════════════
#  Layer 1 — Local Buffer + Kafka Resilient Producer
# ════════════════════════════════════════════════════════════════════════════

class ResilientProducer:
    """
    Kafka producer with local buffer fallback.
    If Kafka is down → saves messages to local files.
    When Kafka recovers → flushes the buffer automatically.
    """

    def __init__(self, kafka_bootstrap: str, buffer_dir: Path = BUFFER_DIR):
        self.bootstrap  = kafka_bootstrap
        self.buffer_dir = buffer_dir / "kafka"
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        self._producer  = None
        self._lock      = threading.Lock()
        self._kafka_ok  = False
        self._connect()

        t = threading.Thread(target=self._flush_loop, daemon=True)
        t.start()

    def _connect(self):
        try:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
                acks="all",
                retries=3,
                request_timeout_ms=10_000,
                api_version_auto_timeout_ms=5_000,
            )
            self._kafka_ok = True
            log.info("✅ Kafka connected")
        except Exception as e:
            self._kafka_ok = False
            log.warning(f"⚠️  Kafka unavailable, using local buffer: {e}")

    def send(self, topic: str, value: Any, key: str = None) -> bool:
        """
        Send a message to Kafka.
        If Kafka is down → buffer to a local file and return False.
        """
        with self._lock:
            if self._kafka_ok and self._producer:
                try:
                    self._producer.send(
                        topic,
                        value=value,
                        key=key.encode() if key else None,
                    )
                    return True
                except Exception as e:
                    log.warning(f"Kafka send failed, buffering: {e}")
                    self._kafka_ok = False
                    self._producer = None

            self._buffer(topic, value, key)
            return False

    def flush(self):
        if self._producer and self._kafka_ok:
            try:
                self._producer.flush(timeout=5)
            except Exception:
                pass

    def _buffer(self, topic: str, value: Any, key: str = None):
        # Guard: stop buffering if disk usage exceeds MAX_BUFFER_MB — prevents VM disk fill
        if self.buffer_size_mb() >= MAX_BUFFER_MB:
            log.error(f"Buffer limit ({MAX_BUFFER_MB} MB) reached — dropping message for topic {topic}")
            return
        ts    = int(time.time() * 1000)
        fname = self.buffer_dir / f"{topic}_{ts}.json"
        try:
            with open(fname, "w", encoding="utf-8") as f:
                json.dump({
                    "topic":       topic,
                    "key":         key,
                    "value":       value,
                    "buffered_at": datetime.now(timezone.utc).isoformat(),
                }, f, ensure_ascii=False)
        except Exception as e:
            log.error(f"Buffer write failed: {e}")

    def _flush_buffer(self) -> int:
        if not self._kafka_ok:
            self._connect()
        if not self._kafka_ok:
            return 0

        sent  = 0
        files = sorted(self.buffer_dir.glob("*.json"))
        for f in files:
            try:
                with open(f, encoding="utf-8") as fp:
                    item = json.load(fp)
                self._producer.send(
                    item["topic"],
                    value=item["value"],
                    key=item["key"].encode() if item.get("key") else None,
                )
                self._producer.flush(timeout=5)
                f.unlink()
                sent += 1
            except Exception as e:
                log.warning(f"Buffer flush failed at {f.name}: {e}")
                self._kafka_ok = False
                self._producer = None
                break

        if sent:
            log.info(f"✅ Flushed {sent} buffered messages to Kafka")
        return sent

    def _flush_loop(self):
        while True:
            time.sleep(FLUSH_INTERVAL)
            if list(self.buffer_dir.glob("*.json")):
                with self._lock:
                    self._flush_buffer()

    def buffer_size(self) -> int:
        return len(list(self.buffer_dir.glob("*.json")))

    def buffer_size_mb(self) -> float:
        total = sum(f.stat().st_size for f in self.buffer_dir.glob("*.json"))
        return total / (1024 * 1024)

    def status(self) -> dict:
        return {
            "kafka_ok":     self._kafka_ok,
            "buffer_files": self.buffer_size(),
            "buffer_mb":    round(self.buffer_size_mb(), 2),
        }


# ════════════════════════════════════════════════════════════════════════════
#  Layer 2 — Airflow Default Args with Exponential Backoff Retry
# ════════════════════════════════════════════════════════════════════════════

RESILIENT_DEFAULT_ARGS = {
    "owner":                     "transit-team",
    "depends_on_past":           False,
    "start_date":                datetime(2026, 4, 13),
    "email_on_failure":          False,   # OPT: no SMTP configured — errors would cascade
    "retries":                   5,
    "retry_delay":               timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=5),   # 30min caused retry storms → tasks blocked for hours
    "execution_timeout":         timedelta(minutes=10),
    "on_failure_callback":       None,   # plug in Slack/email alert here
}

"""
DAG usage example:

from resilient_pipeline import RESILIENT_DEFAULT_ARGS

with DAG(
    "dag_realtime_ingestion",
    default_args=RESILIENT_DEFAULT_ARGS,
    schedule_interval="* * * * *",
    catchup=False,
) as dag:
    ...
"""


# ════════════════════════════════════════════════════════════════════════════
#  Layer 3 — MinIO Resilient Writer (boto3-based) with Local Fallback
# ════════════════════════════════════════════════════════════════════════════

class ResilientMinioWriter:
    """
    Writes records to MinIO (via boto3 S3 API) with:
      - Exponential-backoff retry (up to max_retries attempts)
      - Local gzip fallback when MinIO is unreachable
      - Background thread that retries pending local files every 60 s
    """

    PENDING_DIR = BUFFER_DIR / "minio_pending"

    def __init__(
        self,
        endpoint:    str   = "http://minio:9000",
        access_key:  str   = "minioadmin",
        secret_key:  str   = "minioadmin123",
        bucket:      str   = "israel-transit-lake",
        max_retries: int   = 6,
    ):
        self.endpoint    = endpoint
        self.access_key  = access_key
        self.secret_key  = secret_key
        self.bucket      = bucket
        self.max_retries = max_retries
        self.PENDING_DIR.mkdir(parents=True, exist_ok=True)
        self._client     = None
        self._lock       = threading.Lock()
        self._connect()

        t = threading.Thread(target=self._upload_pending_loop, daemon=True)
        t.start()

    def _connect(self):
        try:
            import boto3
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name="us-east-1",
            )
            # Verify connectivity
            self._client.head_bucket(Bucket=self.bucket)
            log.info("✅ MinIO (ResilientMinioWriter) connected")
            return True
        except Exception as e:
            log.warning(f"⚠️  MinIO connect failed: {e}")
            self._client = None
            return False

    def upload(self, key: str, records: list, compress: bool = True) -> bool:
        """
        Serialize *records* to JSON, optionally gzip, and PUT to MinIO.
        Falls back to local disk if MinIO is unreachable after max_retries.
        Returns True on success, False if saved locally for later retry.
        """
        body  = json.dumps(records, ensure_ascii=False).encode()
        data  = gzip.compress(body) if compress else body
        ctype = "application/gzip" if compress else "application/json"

        for attempt in range(self.max_retries):
            with self._lock:
                if not self._client:
                    self._connect()
                client = self._client

            if client:
                try:
                    client.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=data,
                        ContentType=ctype,
                        Metadata={"record_count": str(len(records))},
                    )
                    log.info(f"✅ ResilientMinioWriter: {len(records)} records → s3://{self.bucket}/{key}")
                    return True
                except Exception as e:
                    wait = min(2 ** attempt, 60)
                    log.warning(f"MinIO upload failed (attempt {attempt+1}/{self.max_retries}): {e}. Retry in {wait}s")
                    with self._lock:
                        self._client = None
                    time.sleep(wait)
            else:
                time.sleep(2 ** attempt)

        # All retries exhausted — save locally
        log.error(f"MinIO unavailable after {self.max_retries} attempts — saving locally: {key}")
        self._save_pending(key, data, ctype)
        return False

    def _save_pending(self, key: str, data: bytes, ctype: str):
        safe = key.replace("/", "_")
        try:
            (self.PENDING_DIR / f"{safe}.data").write_bytes(data)
            with open(self.PENDING_DIR / f"{safe}.meta.json", "w") as f:
                json.dump({
                    "bucket":   self.bucket,
                    "key":      key,
                    "ctype":    ctype,
                    "saved_at": datetime.now(timezone.utc).isoformat(),
                }, f)
        except Exception as e:
            log.error(f"Failed to save pending upload: {e}")

    def _upload_pending(self) -> int:
        uploaded = 0
        for meta_f in self.PENDING_DIR.glob("*.meta.json"):
            data_f = self.PENDING_DIR / (meta_f.stem + ".data")
            if not data_f.exists():
                meta_f.unlink(missing_ok=True)
                continue
            try:
                with open(meta_f) as f:
                    meta = json.load(f)
                data = data_f.read_bytes()

                with self._lock:
                    if not self._client:
                        self._connect()
                    client = self._client

                if not client:
                    break

                client.put_object(
                    Bucket=meta["bucket"],
                    Key=meta["key"],
                    Body=data,
                    ContentType=meta["ctype"],
                )
                meta_f.unlink()
                data_f.unlink()
                uploaded += 1
                log.info(f"✅ Uploaded pending: {meta['key']}")
            except Exception as e:
                log.warning(f"Pending upload failed: {e}")
                with self._lock:
                    self._client = None
                break

        return uploaded

    def _upload_pending_loop(self):
        while True:
            time.sleep(60)
            if list(self.PENDING_DIR.glob("*.meta.json")):
                self._upload_pending()

    def pending_count(self) -> int:
        return len(list(self.PENDING_DIR.glob("*.meta.json")))

    def status(self) -> dict:
        return {
            "minio_ok":      self._client is not None,
            "pending_files": self.pending_count(),
            "endpoint":      self.endpoint,
        }


# ════════════════════════════════════════════════════════════════════════════
#  Health check — status of all resilience layers
# ════════════════════════════════════════════════════════════════════════════

def pipeline_health(producer: ResilientProducer = None,
                    minio: ResilientMinioWriter = None) -> dict:
    """Return the health status of all resilience layers."""
    result: dict = {"timestamp": datetime.now(timezone.utc).isoformat()}

    if producer:
        ks = producer.status()
        result["kafka"] = {
            "connected":    ks["kafka_ok"],
            "buffer_files": ks["buffer_files"],
            "buffer_mb":    ks["buffer_mb"],
            "healthy":      ks["kafka_ok"] and ks["buffer_files"] == 0,
        }

    if minio:
        ms = minio.status()
        result["minio"] = {
            "connected":     ms["minio_ok"],
            "pending_files": ms["pending_files"],
            "healthy":       ms["minio_ok"] and ms["pending_files"] == 0,
        }

    result["overall_healthy"] = all(
        v.get("healthy", True)
        for k, v in result.items()
        if isinstance(v, dict) and "healthy" in v
    )
    return result


# ════════════════════════════════════════════════════════════════════════════
#  Standalone test
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    endpoint   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    bootstrap  = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9092")

    producer = ResilientProducer(bootstrap)
    minio    = ResilientMinioWriter(endpoint, access_key, secret_key)

    print("Pipeline status:", json.dumps(pipeline_health(producer, minio), indent=2))

    test_record = {
        "vehicle_id":    "test-001",
        "operator_name": "Dan",
        "speed_kmh":     42,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }

    # Layer 1: Kafka with auto-buffer
    sent = producer.send("bus-positions", test_record)
    print(f"Kafka send: {'✅ delivered' if sent else '⚠️  buffered locally'}")

    # Layer 3: MinIO with retry + local fallback
    now = datetime.now(timezone.utc)
    ok = minio.upload(
        key=f"raw/bus-positions/year={now.year}/month={now.month:02d}/day={now.day:02d}/test_{int(time.time())}.json",
        records=[test_record],
    )
    print(f"MinIO upload: {'✅ uploaded' if ok else '⚠️  saved locally, will retry'}")

    print("\nFinal status:", json.dumps(pipeline_health(producer, minio), indent=2))
