"""
warehouse/redshift_writer.py
Redshift DDL schema + writer for Israel Transit data warehouse.
Schema: transit
Tables: fact_bus_positions, fact_trip_updates, fact_train_positions,
        fact_service_alerts, agg_delay_stats, agg_route_performance
"""

import logging
import psycopg2
from psycopg2.extras import execute_values
import sys
sys.path.append("..")
from config.settings import REDSHIFT_CONFIG, REDSHIFT_SCHEMA

logger = logging.getLogger("warehouse.redshift")

# ─────────────────────────────────────────
# SCHEMA DDL
# ─────────────────────────────────────────
CREATE_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA};"

DDL_STATEMENTS = [

    # ── Real-time bus positions ─────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.fact_bus_positions (
        vehicle_id          VARCHAR(50),
        trip_id             VARCHAR(100),
        route_id            VARCHAR(100),
        route_short_name    VARCHAR(20),
        operator_id         VARCHAR(20),
        operator_name       VARCHAR(50),
        direction_id        SMALLINT,
        latitude            DECIMAL(9,6),
        longitude           DECIMAL(9,6),
        is_valid_location   BOOLEAN,
        bearing             DECIMAL(6,1),
        speed_kmh           DECIMAL(6,1),
        speed_category      VARCHAR(20),
        stop_id             VARCHAR(50),
        current_status      VARCHAR(20),
        hour_of_day         SMALLINT,
        time_period         VARCHAR(20),
        day_of_week         VARCHAR(15),
        is_weekend          BOOLEAN,
        timestamp           BIGINT,
        processed_at        TIMESTAMP,
        ingested_at         TIMESTAMP
    )
    DISTKEY(route_id)
    SORTKEY(processed_at, operator_name);
    """,

    # ── Trip updates / delays ───────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.fact_trip_updates (
        trip_id             VARCHAR(100),
        route_id            VARCHAR(100),
        route_short_name    VARCHAR(20),
        direction_id        SMALLINT,
        start_date          VARCHAR(10),
        stop_id             VARCHAR(50),
        stop_sequence       SMALLINT,
        arrival_delay_sec   INTEGER,
        departure_delay_sec INTEGER,
        delay_seconds       INTEGER,
        delay_minutes       DECIMAL(6,1),
        delay_category      VARCHAR(30),
        is_delayed          BOOLEAN,
        is_cancelled        BOOLEAN,
        is_early            BOOLEAN,
        schedule_relationship VARCHAR(20),
        hour_of_day         SMALLINT,
        time_period         VARCHAR(20),
        day_of_week         VARCHAR(15),
        is_weekend          BOOLEAN,
        processed_at        TIMESTAMP,
        ingested_at         TIMESTAMP
    )
    DISTKEY(route_id)
    SORTKEY(processed_at, delay_category);
    """,

    # ── Israel Railways positions ───────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.fact_train_positions (
        train_number        VARCHAR(20),
        origin_station_id   VARCHAR(10),
        dest_station_id     VARCHAR(10),
        queried_station_id  VARCHAR(10),
        queried_station_name VARCHAR(100),
        platform            VARCHAR(10),
        scheduled_departure VARCHAR(10),
        actual_departure    VARCHAR(10),
        scheduled_arrival   VARCHAR(10),
        actual_arrival      VARCHAR(10),
        delay_minutes       INTEGER,
        delay_seconds       INTEGER,
        delay_category      VARCHAR(30),
        is_delayed          BOOLEAN,
        is_cancelled        BOOLEAN,
        operator            VARCHAR(30),
        hour_of_day         SMALLINT,
        time_period         VARCHAR(20),
        day_of_week         VARCHAR(15),
        is_weekend          BOOLEAN,
        processed_at        TIMESTAMP,
        ingested_at         TIMESTAMP
    )
    DISTKEY(origin_station_id)
    SORTKEY(processed_at, train_number);
    """,

    # ── Service alerts ──────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.fact_service_alerts (
        alert_id                VARCHAR(50) PRIMARY KEY,
        cause                   VARCHAR(50),
        effect                  VARCHAR(50),
        severity                VARCHAR(20),
        header_text             VARCHAR(500),
        description_text        VARCHAR(1000),
        is_active               BOOLEAN,
        active_start            BIGINT,
        active_end              BIGINT,
        affected_routes_count   SMALLINT,
        affected_stops_count    SMALLINT,
        affected_routes         VARCHAR(500),
        affected_agencies       VARCHAR(200),
        impact_score            DECIMAL(6,1),
        hour_of_day             SMALLINT,
        time_period             VARCHAR(20),
        processed_at            TIMESTAMP,
        ingested_at             TIMESTAMP
    )
    SORTKEY(processed_at, severity);
    """,

    # ── Hourly delay aggregation ────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.agg_delay_stats (
        stat_date           DATE,
        stat_hour           SMALLINT,
        time_period         VARCHAR(20),
        route_id            VARCHAR(100),
        route_short_name    VARCHAR(20),
        operator_name       VARCHAR(50),
        total_trips         INTEGER,
        delayed_trips       INTEGER,
        cancelled_trips     INTEGER,
        avg_delay_seconds   DECIMAL(8,1),
        max_delay_seconds   INTEGER,
        p90_delay_seconds   INTEGER,
        delay_rate_pct      DECIMAL(5,2),
        cancellation_rate   DECIMAL(5,2),
        created_at          TIMESTAMP DEFAULT GETDATE(),
        PRIMARY KEY (stat_date, stat_hour, route_id)
    )
    SORTKEY(stat_date, stat_hour);
    """,

    # ── Route performance summary ───────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.agg_route_performance (
        perf_date           DATE,
        route_id            VARCHAR(100),
        route_short_name    VARCHAR(20),
        operator_name       VARCHAR(50),
        total_vehicles      INTEGER,
        active_vehicles     INTEGER,
        avg_speed_kmh       DECIMAL(6,1),
        total_delays        INTEGER,
        avg_delay_min       DECIMAL(6,1),
        worst_delay_min     INTEGER,
        on_time_rate_pct    DECIMAL(5,2),
        alerts_count        SMALLINT,
        created_at          TIMESTAMP DEFAULT GETDATE(),
        PRIMARY KEY (perf_date, route_id)
    )
    SORTKEY(perf_date, operator_name);
    """,
]


class RedshiftWriter:

    def __init__(self):
        self.conn = None
        self._connect()

    def _connect(self):
        try:
            self.conn = psycopg2.connect(**REDSHIFT_CONFIG)
            self.conn.autocommit = False
            logger.info("Connected to Redshift")
        except Exception as e:
            logger.error(f"Redshift connection failed: {e}")
            raise

    def create_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(CREATE_SCHEMA)
            for stmt in DDL_STATEMENTS:
                cur.execute(stmt)
        self.conn.commit()
        logger.info("Transit schema and all tables created")

    def bulk_insert(self, table: str, records: list):
        if not records:
            return
        columns  = list(records[0].keys())
        col_list = ", ".join(columns)
        values   = [tuple(r.get(c) for c in columns) for r in records]
        sql = f"INSERT INTO {table} ({col_list}) VALUES %s"
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, values, page_size=500)
            self.conn.commit()
            logger.info(f"Bulk inserted {len(records)} rows → {table}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Bulk insert failed {table}: {e}")
            raise

    def upsert(self, table: str, record: dict, conflict_key: str):
        columns      = list(record.keys())
        col_list     = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table} WHERE {conflict_key} = %s", (record[conflict_key],))
                cur.execute(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})", list(record.values()))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Upsert failed {table}: {e}")
            raise

    def execute_query(self, sql: str, params=None) -> list:
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                if cur.description:
                    cols = [d[0] for d in cur.description]
                    return [dict(zip(cols, row)) for row in cur.fetchall()]
            self.conn.commit()
            return []
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Query failed: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()