"""
resilient_pipeline.py
=====================
שלוש שכבות חוסן לפייפליין:

  שכבה 1 — Local Buffer   : אם Kafka למטה → שמור לקובץ, שלח כשחוזר
  שכבה 2 — Airflow Retry  : retry עם exponential backoff ב-DAGs
  שכבה 3 — MinIO Direct   : אם Kafka למטה לאורך זמן → העלה ישירות ל-MinIO

שימוש:
  from resilient_pipeline import ResilientProducer, resilient_minio_upload
  from resilient_pipeline import RESILIENT_DEFAULT_ARGS  # ב-DAGs
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

# ── הגדרות ────────────────────────────────────────────────────────────────────
BUFFER_DIR      = Path(os.getenv("BUFFER_DIR", "./buffer"))
BUFFER_DIR.mkdir(parents=True, exist_ok=True)

MAX_BUFFER_MB   = int(os.getenv("MAX_BUFFER_MB", "500"))     # מקסימום גודל buffer
FLUSH_INTERVAL  = int(os.getenv("FLUSH_INTERVAL", "30"))     # שניות בין ניסיונות flush
MINIO_FALLBACK  = os.getenv("MINIO_FALLBACK", "true").lower() == "true"

# ════════════════════════════════════════════════════════════════════════════
#  שכבה 1 — Local Buffer + Kafka Resilient Producer
# ════════════════════════════════════════════════════════════════════════════

class ResilientProducer:
    """
    Kafka producer עם local buffer fallback.
    אם Kafka למטה — שומר הודעות לקבצים מקומיים.
    כשKafka חוזר — שולח את כל ה-buffer אוטומטית.
    """

    def __init__(self, kafka_bootstrap: str, buffer_dir: Path = BUFFER_DIR):
        self.bootstrap  = kafka_bootstrap
        self.buffer_dir = buffer_dir
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        self._producer  = None
        self._lock      = threading.Lock()
        self._kafka_ok  = False
        self._connect()

        # Thread שמנסה לשלוח buffer כל FLUSH_INTERVAL שניות
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
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=5000,
            )
            self._kafka_ok = True
            log.info("✅ Kafka connected")
        except Exception as e:
            self._kafka_ok = False
            log.warning(f"⚠️  Kafka unavailable, using local buffer: {e}")

    def send(self, topic: str, value: Any, key: str = None):
        """
        שולח הודעה ל-Kafka.
        אם Kafka למטה — שומר לקובץ מקומי.
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

            # Kafka למטה — שמור לקובץ
            self._buffer(topic, value, key)
            return False

    def flush(self):
        if self._producer and self._kafka_ok:
            try:
                self._producer.flush(timeout=5)
            except Exception:
                pass

    def _buffer(self, topic: str, value: Any, key: str = None):
        """שומר הודעה לקובץ JSON בתיקיית ה-buffer."""
        ts  = int(time.time() * 1000)
        fname = self.buffer_dir / f"{topic}_{ts}.json"
        try:
            with open(fname, "w", encoding="utf-8") as f:
                json.dump({
                    "topic":      topic,
                    "key":        key,
                    "value":      value,
                    "buffered_at": datetime.now(timezone.utc).isoformat(),
                }, f, ensure_ascii=False)
        except Exception as e:
            log.error(f"Buffer write failed: {e}")

    def _flush_buffer(self):
        """מנסה לשלוח קבצי buffer ל-Kafka."""
        if not self._kafka_ok:
            self._connect()
        if not self._kafka_ok:
            return 0

        sent = 0
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
                log.warning(f"Flush failed at {f.name}: {e}")
                self._kafka_ok = False
                self._producer = None
                break

        if sent:
            log.info(f"✅ Flushed {sent} buffered messages to Kafka")
        return sent

    def _flush_loop(self):
        """Thread שמנסה לשלוח buffer ברקע."""
        while True:
            time.sleep(FLUSH_INTERVAL)
            if list(self.buffer_dir.glob("*.json")):
                with self._lock:
                    self._flush_buffer()

    def buffer_size(self) -> int:
        """מספר הודעות ממתינות ב-buffer."""
        return len(list(self.buffer_dir.glob("*.json")))

    def buffer_size_mb(self) -> float:
        """גודל ה-buffer ב-MB."""
        total = sum(f.stat().st_size for f in self.buffer_dir.glob("*.json"))
        return total / (1024 * 1024)

    def status(self) -> dict:
        return {
            "kafka_ok":      self._kafka_ok,
            "buffer_files":  self.buffer_size(),
            "buffer_mb":     round(self.buffer_size_mb(), 2),
        }


# ════════════════════════════════════════════════════════════════════════════
#  שכבה 2 — Airflow Default Args עם Retry
# ════════════════════════════════════════════════════════════════════════════

RESILIENT_DEFAULT_ARGS = {
    "owner":                    "airflow",
    "depends_on_past":          False,
    "retries":                  5,
    "retry_delay":              timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=30),
    "execution_timeout":        timedelta(minutes=10),
    "on_failure_callback":      None,  # אפשר להוסיף Slack/email alert
}

"""
שימוש ב-DAG:

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
#  שכבה 3 — MinIO Direct Upload עם Retry + Local Fallback
# ════════════════════════════════════════════════════════════════════════════

class ResilientMinioWriter:
    """
    כותב ל-MinIO עם retry ו-local fallback.
    אם MinIO למטה → שומר gzip מקומי, מעלה כשחוזר.
    """

    PENDING_DIR = BUFFER_DIR / "minio_pending"

    def __init__(
        self,
        endpoint:   str,
        access_key: str,
        secret_key: str,
        secure:     bool = False,
        max_retries: int = 8,
    ):
        self.endpoint    = endpoint
        self.access_key  = access_key
        self.secret_key  = secret_key
        self.secure      = secure
        self.max_retries = max_retries
        self.PENDING_DIR.mkdir(parents=True, exist_ok=True)
        self._client     = None
        self._connect()

        # Thread לניסיון העלאת קבצים ממתינים
        t = threading.Thread(target=self._upload_pending_loop, daemon=True)
        t.start()

    def _connect(self):
        try:
            from minio import Minio
            self._client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
            )
            log.info("✅ MinIO connected")
            return True
        except Exception as e:
            log.warning(f"⚠️  MinIO connect failed: {e}")
            self._client = None
            return False

    def ensure_bucket(self, bucket: str):
        if not self._client:
            return
        try:
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
        except Exception as e:
            log.error(f"Bucket create failed: {e}")

    def upload(
        self,
        bucket:      str,
        object_name: str,
        records:     list,
        compress:    bool = True,
    ) -> bool:
        """
        מעלה רשומות ל-MinIO כ-JSONL (gzip אופציונלי).
        אם MinIO למטה → שומר לדיסק ומנסה שוב מאוחר יותר.
        """
        import io

        # בנה את ה-payload
        jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
        data  = gzip.compress(jsonl.encode()) if compress else jsonl.encode()
        ctype = "application/gzip" if compress else "application/x-ndjson"

        # נסה להעלות עם retry
        for attempt in range(self.max_retries):
            if not self._client:
                self._connect()
            if self._client:
                try:
                    self.ensure_bucket(bucket)
                    self._client.put_object(
                        bucket, object_name,
                        io.BytesIO(data), len(data),
                        content_type=ctype,
                    )
                    log.info(f"✅ MinIO upload: s3://{bucket}/{object_name} ({len(records)} records)")
                    return True
                except Exception as e:
                    wait = min(2 ** attempt, 60)
                    log.warning(f"MinIO upload failed (attempt {attempt+1}/{self.max_retries}): {e}. Retry in {wait}s")
                    time.sleep(wait)

        # כל הניסיונות נכשלו — שמור לדיסק
        log.error(f"MinIO unavailable — saving locally: {object_name}")
        self._save_pending(bucket, object_name, data, ctype)
        return False

    def _save_pending(self, bucket: str, object_name: str, data: bytes, ctype: str):
        """שומר קובץ ממתין להעלאה."""
        safe_name = object_name.replace("/", "_")
        meta_f = self.PENDING_DIR / f"{safe_name}.meta.json"
        data_f = self.PENDING_DIR / f"{safe_name}.data"
        try:
            data_f.write_bytes(data)
            with open(meta_f, "w") as f:
                json.dump({
                    "bucket":      bucket,
                    "object_name": object_name,
                    "ctype":       ctype,
                    "saved_at":    datetime.now(timezone.utc).isoformat(),
                }, f)
        except Exception as e:
            log.error(f"Failed to save pending: {e}")

    def _upload_pending(self) -> int:
        """מנסה להעלות קבצים ממתינים."""
        import io
        uploaded = 0
        for meta_f in self.PENDING_DIR.glob("*.meta.json"):
            data_f = meta_f.with_suffix("").with_suffix(".data")
            if not data_f.exists():
                meta_f.unlink(missing_ok=True)
                continue
            try:
                with open(meta_f) as f:
                    meta = json.load(f)
                data = data_f.read_bytes()

                if not self._client:
                    self._connect()
                if not self._client:
                    break

                self.ensure_bucket(meta["bucket"])
                self._client.put_object(
                    meta["bucket"], meta["object_name"],
                    io.BytesIO(data), len(data),
                    content_type=meta["ctype"],
                )
                meta_f.unlink()
                data_f.unlink()
                uploaded += 1
                log.info(f"✅ Uploaded pending: {meta['object_name']}")
            except Exception as e:
                log.warning(f"Pending upload failed: {e}")
                self._client = None
                break

        return uploaded

    def _upload_pending_loop(self):
        """Thread שמנסה להעלות קבצים ממתינים ברקע."""
        while True:
            time.sleep(60)
            pending = list(self.PENDING_DIR.glob("*.meta.json"))
            if pending:
                log.info(f"Attempting to upload {len(pending)} pending files...")
                self._upload_pending()

    def pending_count(self) -> int:
        return len(list(self.PENDING_DIR.glob("*.meta.json")))

    def status(self) -> dict:
        return {
            "minio_ok":      self._client is not None,
            "pending_files": self.pending_count(),
        }


# ════════════════════════════════════════════════════════════════════════════
#  Health check — מצב כל השכבות
# ════════════════════════════════════════════════════════════════════════════

def pipeline_health(producer: ResilientProducer, minio: ResilientMinioWriter) -> dict:
    """מחזיר מצב כל שכבות החוסן."""
    kafka_status = producer.status()
    minio_status = minio.status()

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "kafka": {
            "connected":    kafka_status["kafka_ok"],
            "buffer_files": kafka_status["buffer_files"],
            "buffer_mb":    kafka_status["buffer_mb"],
            "healthy":      kafka_status["kafka_ok"] and kafka_status["buffer_files"] == 0,
        },
        "minio": {
            "connected":     minio_status["minio_ok"],
            "pending_files": minio_status["pending_files"],
            "healthy":       minio_status["minio_ok"] and minio_status["pending_files"] == 0,
        },
        "overall_healthy": (
            kafka_status["kafka_ok"] and
            minio_status["minio_ok"] and
            kafka_status["buffer_files"] == 0 and
            minio_status["pending_files"] == 0
        ),
    }


# ════════════════════════════════════════════════════════════════════════════
#  דוגמת שימוש מלאה
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

    # אתחול
    producer = ResilientProducer("localhost:9092")
    minio    = ResilientMinioWriter(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )

    print("Pipeline status:", json.dumps(pipeline_health(producer, minio), indent=2))

    # דוגמת שליחה
    test_record = {
        "vehicle_id":    "test-123",
        "operator_name": "NTA",
        "speed_kmh":     45,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }

    # שכבה 1: שלח ל-Kafka (עם buffer אוטומטי אם Kafka למטה)
    sent = producer.send("transit-bus-positions", test_record)
    print(f"Kafka send: {'✅' if sent else '⚠️ buffered locally'}")

    # שכבה 3: העלה ל-MinIO (עם retry ו-local fallback)
    now = datetime.now(timezone.utc)
    ok = minio.upload(
        bucket="bus-positions-raw",
        object_name=f"{now.strftime('%Y/%m/%d/%H')}/test_{int(time.time())}.jsonl.gz",
        records=[test_record],
    )
    print(f"MinIO upload: {'✅' if ok else '⚠️ saved locally, will retry'}")

    print("\nFinal status:", json.dumps(pipeline_health(producer, minio), indent=2))