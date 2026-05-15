#!/usr/bin/env python3
"""One-time: create the 6 transit indices on Elastic Cloud with correct mappings."""
import warnings; warnings.filterwarnings("ignore")
from elasticsearch import Elasticsearch

d = open(".env").read()
host = d.split("ELASTICSEARCH_HOST=")[1].split("\n")[0].strip()
key  = d.split("ELASTIC_API_KEY=")[1].split("\n")[0].strip()

TOPIC_INDEX_MAP = {
    "bus_positions":   "transit-bus-positions",
    "trip_updates":    "transit-trip-updates",
    "train_positions": "transit-train-positions",
    "service_alerts":  "transit-service-alerts",
    "delay_events":    "transit-delay-events",
    "traffic_data":    "transit-traffic",
}

INDEX_SETTINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "5s"},
    "mappings": {
        "dynamic": "true",
        "properties": {
            "timestamp":        {"type": "date", "ignore_malformed": True},
            "recorded_at":      {"type": "date", "ignore_malformed": True},
            "_indexed_at":      {"type": "date"},
            "latitude":         {"type": "float"},
            "longitude":        {"type": "float"},
            "lat":              {"type": "float"},
            "lon":              {"type": "float"},
            "location":         {"type": "geo_point"},
            "operator_name":    {"type": "keyword"},
            "operator_id":      {"type": "keyword"},
            "operator_ref":     {"type": "keyword"},
            "route_id":         {"type": "keyword"},
            "route_short_name": {"type": "keyword"},
            "vehicle_id":       {"type": "keyword"},
            "line_ref":         {"type": "keyword"},
            "trip_id":          {"type": "keyword"},
            "stop_id":          {"type": "keyword"},
            "train_number":     {"type": "keyword"},
            "area":             {"type": "keyword"},
            "speed_kmh":        {"type": "float"},
            "velocity":         {"type": "float"},
            "is_moving":        {"type": "boolean"},
            "speed_category":   {"type": "keyword"},
            "delay_minutes":    {"type": "float"},
            "delay_seconds":    {"type": "integer"},
            "delay_category":   {"type": "keyword"},
            "is_delayed":       {"type": "boolean"},
            "bearing":          {"type": "float"},
            "jam_factor":       {"type": "float"},
            "congestion":       {"type": "keyword"},
            "congestion_score": {"type": "integer"},
            "severity":         {"type": "keyword"},
            "speed_ratio":      {"type": "float"},
            "free_flow_kmh":    {"type": "float"},
            "region":           {"type": "keyword"},
            "road_type":        {"type": "keyword"},
            "time_period":      {"type": "keyword"},
            "is_congested":     {"type": "boolean"},
            "is_blocked":       {"type": "boolean"},
            "delay_min_per_km": {"type": "float"},
        },
    },
}

es = Elasticsearch(host, api_key=key, request_timeout=30,
                   verify_certs=False, ssl_show_warn=False)
print("PING:", es.ping())
for topic, index in TOPIC_INDEX_MAP.items():
    if es.indices.exists(index=index):
        print("exists :", index)
    else:
        es.indices.create(index=index, body=INDEX_SETTINGS)
        print("CREATED:", index)
print("---")
print("All:", sorted(i for i in es.indices.get_alias(index="*")
                      if not i.startswith(".")))
