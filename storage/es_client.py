"""
Unified Elasticsearch client factory.

Supports both:
  - Local ES: ELASTICSEARCH_HOST=http://elasticsearch:9200 (or http://localhost:9200)
  - Elastic Cloud: ELASTIC_CLOUD_ID + ELASTIC_API_KEY  (or ELASTIC_USER + ELASTIC_PASSWORD)

Usage:
    from storage.es_client import get_es_client
    es = get_es_client()
"""
import os
import logging
from elasticsearch import Elasticsearch

log = logging.getLogger(__name__)


def get_es_client(request_timeout: int = 10) -> Elasticsearch:
    """
    Build an Elasticsearch client.

    Priority:
      1. ELASTIC_CLOUD_ID + ELASTIC_API_KEY  (recommended for Elastic Cloud)
      2. ELASTIC_CLOUD_ID + ELASTIC_USER/ELASTIC_PASSWORD
      3. ELASTICSEARCH_HOST + (optional) ELASTIC_USER/ELASTIC_PASSWORD
      4. Fallback to http://elasticsearch:9200 (Docker internal)
    """
    cloud_id  = os.getenv("ELASTIC_CLOUD_ID", "").strip()
    api_key   = os.getenv("ELASTIC_API_KEY", "").strip()
    es_user   = os.getenv("ELASTIC_USER", "elastic").strip()
    es_pass   = os.getenv("ELASTIC_PASSWORD", "").strip()
    es_host   = os.getenv("ELASTICSEARCH_HOST", "").strip()

    # 1) Elastic Cloud via API Key
    if cloud_id and api_key:
        log.info("ES connection: Elastic Cloud (API key)")
        return Elasticsearch(
            cloud_id=cloud_id,
            api_key=api_key,
            request_timeout=request_timeout,
        )

    # 2) Elastic Cloud via basic auth
    if cloud_id and es_pass:
        log.info("ES connection: Elastic Cloud (basic auth)")
        return Elasticsearch(
            cloud_id=cloud_id,
            basic_auth=(es_user, es_pass),
            request_timeout=request_timeout,
        )

    # 3) Plain HTTP/HTTPS endpoint (with or without auth)
    if es_host:
        if es_pass:
            log.info("ES connection: %s (basic auth)", es_host)
            return Elasticsearch(
                es_host,
                basic_auth=(es_user, es_pass),
                request_timeout=request_timeout,
            )
        log.info("ES connection: %s (no auth)", es_host)
        return Elasticsearch(es_host, request_timeout=request_timeout)

    # 4) Default fallback (Docker internal network)
    fallback = "http://elasticsearch:9200"
    log.warning("ES connection: no env vars set, falling back to %s", fallback)
    return Elasticsearch(fallback, request_timeout=request_timeout)


def get_kibana_url() -> str:
    """Return the Kibana URL (cloud or local)."""
    return os.getenv("KIBANA_URL", "http://kibana:5601").strip()
