"""
dag_traffic_to_minio.py — SUPERSEDED

Traffic → MinIO is now handled by dag_direct_traffic inside dag_direct_to_minio.py,
which writes HERE traffic directly to israel-transit-lake every 5 minutes
without a separate bucket or Kafka dependency.

The original storage.traffic_to_minio module was removed.
This file is kept as an empty stub so Airflow does not raise an ImportError.
"""
# No DAGs defined here — see dag_direct_to_minio.py → dag_direct_traffic
