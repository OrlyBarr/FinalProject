"""
producers/base_producer.py
Base Kafka producer - shared logic for all transit data sources.
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
sys.path.append("..")
from config.settings import KAFKA_PRODUCER_CONFIG, KAFKA_TOPICS, LOG_FORMAT, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)


class BaseProducer(ABC):

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger      = logging.getLogger(f"producer.{source_name}")
        self.producer    = self._create_producer()
        self.stats       = {"sent": 0, "errors": 0}

    def _create_producer(self) -> KafkaProducer:
        config = KAFKA_PRODUCER_CONFIG.copy()
        config["value_serializer"] = lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
        config["key_serializer"]   = lambda k: k.encode("utf-8") if k else None
        return KafkaProducer(**config)

    def _wrap(self, data: dict) -> dict:
        return {
            "source":      self.source_name,
            "fetched_at":  datetime.now(timezone.utc).isoformat(),
            "data":        data,
        }

    def send(self, data: dict, key: str = None):
        topic = self.get_topic()
        try:
            self.producer.send(topic, value=self._wrap(data), key=key)
            self.stats["sent"] += 1
        except KafkaError as e:
            self.stats["errors"] += 1
            self.logger.error(f"Send failed → {topic}: {e}")
            self._send_error(data, str(e))

    def send_batch(self, records: list):
        self.logger.info(f"Sending {len(records)} records → {self.get_topic()}")
        for r in records:
            key = (
                str(r.get("vehicle_id") or r.get("trip_id") or
                    r.get("alert_id")   or r.get("train_number") or "")
            )
            self.send(r, key=key or None)
        self.producer.flush()
        self.logger.info(f"Batch done | sent={self.stats['sent']} errors={self.stats['errors']}")

    def _send_error(self, data: dict, error_msg: str):
        try:
            self.producer.send(
                KAFKA_TOPICS["errors"],
                value=json.dumps({
                    "source": self.source_name,
                    "error":  error_msg,
                    "data":   data,
                    "ts":     datetime.now(timezone.utc).isoformat(),
                }).encode("utf-8")
            )
        except Exception:
            pass

    def close(self):
        self.producer.flush()
        self.producer.close()

    def run_once(self):
        records = self.fetch_data()
        if records:
            self.send_batch(records)
        else:
            self.logger.warning(f"No records fetched from {self.source_name}")

    @abstractmethod
    def fetch_data(self) -> list:
        pass

    @abstractmethod
    def get_topic(self) -> str:
        pass