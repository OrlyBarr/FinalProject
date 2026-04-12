"""
airflow/dags/dag_daily_analytics.py
DAG 3: Daily KPI aggregations, performance reports, HTML summary
Schedule: 04:00 IL time (01:00 UTC) - after end of service day
"""

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
sys.path.append("/opt/airflow")

default_args = {
    "owner":            "transit-team",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 1, 1),
    "email_on_failure": True,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}


def aggregate_delay_stats(**context):
    """
    Aggregate yesterday's delay statistics by route, operator, and time period.
    Writes into agg_delay_stats table.
    """
    from warehouse.redshift_writer import RedshiftWriter

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    rw = RedshiftWriter()

    rw.execute_query(f"""
        DELETE FROM transit.agg_delay_stats WHERE stat_date = '{yesterday}';

        INSERT INTO transit.agg_delay_stats
            (stat_date, stat_hour, time_period, route_id, route_short_name,
             total_trips, delayed_trips, cancelled_trips,
             avg_delay_seconds, max_delay_seconds, delay_rate_pct, cancellation_rate)
        SELECT
            '{yesterday}'::DATE                   AS stat_date,
            hour_of_day                            AS stat_hour,
            time_period,
            route_id,
            route_short_name,
            COUNT(*)                               AS total_trips,
            SUM(CASE WHEN is_delayed   THEN 1 ELSE 0 END)  AS delayed_trips,
            SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)  AS cancelled_trips,
            AVG(delay_seconds)                     AS avg_delay_seconds,
            MAX(delay_seconds)                     AS max_delay_seconds,
            ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) AS delay_rate_pct,
            ROUND(100.0 * SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
        FROM transit.fact_trip_updates
        WHERE DATE(processed_at) = '{yesterday}'
        GROUP BY hour_of_day, time_period, route_id, route_short_name;
    """)

    rw.close()
    print(f"Delay stats aggregated for {yesterday}")
    context["ti"].xcom_push(key="report_date", value=yesterday)


def aggregate_route_performance(**context):
    """Aggregate route-level performance metrics for yesterday."""
    from warehouse.redshift_writer import RedshiftWriter

    yesterday = context["ti"].xcom_pull(task_ids="aggregate_delay_stats", key="report_date")
    rw = RedshiftWriter()

    rw.execute_query(f"""
        DELETE FROM transit.agg_route_performance WHERE perf_date = '{yesterday}';

        INSERT INTO transit.agg_route_performance
            (perf_date, route_id, route_short_name, operator_name,
             total_vehicles, avg_speed_kmh, total_delays,
             avg_delay_min, worst_delay_min, on_time_rate_pct)
        SELECT
            '{yesterday}'::DATE     AS perf_date,
            b.route_id,
            b.route_short_name,
            b.operator_name,
            COUNT(DISTINCT b.vehicle_id)             AS total_vehicles,
            AVG(b.speed_kmh)                         AS avg_speed_kmh,
            SUM(CASE WHEN t.is_delayed THEN 1 ELSE 0 END) AS total_delays,
            AVG(t.delay_minutes)                     AS avg_delay_min,
            MAX(t.delay_minutes)                     AS worst_delay_min,
            ROUND(100.0 - (100.0 * SUM(CASE WHEN t.is_delayed THEN 1 ELSE 0 END)
                / NULLIF(COUNT(t.trip_id), 0)), 2)  AS on_time_rate_pct
        FROM transit.fact_bus_positions b
        LEFT JOIN transit.fact_trip_updates t
            ON b.route_id = t.route_id
            AND DATE(b.processed_at) = DATE(t.processed_at)
        WHERE DATE(b.processed_at) = '{yesterday}'
        GROUP BY b.route_id, b.route_short_name, b.operator_name;
    """)

    rw.close()
    print("Route performance aggregation complete")


def calculate_kpis(**context):
    """Calculate network-level KPIs for the daily report."""
    from warehouse.redshift_writer import RedshiftWriter

    yesterday = context["ti"].xcom_pull(task_ids="aggregate_delay_stats", key="report_date")
    rw = RedshiftWriter()

    # Overall network KPIs
    kpis = rw.execute_query(f"""
        SELECT
            COUNT(DISTINCT route_id)                AS total_routes,
            SUM(total_trips)                        AS total_trips,
            SUM(delayed_trips)                      AS total_delayed,
            SUM(cancelled_trips)                    AS total_cancelled,
            ROUND(AVG(avg_delay_seconds) / 60.0, 1) AS avg_delay_min,
            ROUND(AVG(delay_rate_pct), 2)           AS network_delay_rate,
            ROUND(100 - AVG(delay_rate_pct), 2)     AS network_on_time_rate
        FROM transit.agg_delay_stats
        WHERE stat_date = '{yesterday}';
    """)

    # Worst routes (most delayed)
    worst_routes = rw.execute_query(f"""
        SELECT route_short_name, operator_name,
               AVG(delay_rate_pct) AS avg_delay_rate,
               AVG(avg_delay_seconds)/60.0 AS avg_delay_min
        FROM transit.agg_delay_stats
        WHERE stat_date = '{yesterday}'
        GROUP BY route_short_name, operator_name
        ORDER BY avg_delay_rate DESC
        LIMIT 10;
    """)

    # Best routes (most punctual)
    best_routes = rw.execute_query(f"""
        SELECT route_short_name, operator_name,
               ROUND(100 - AVG(delay_rate_pct), 2) AS on_time_pct
        FROM transit.agg_delay_stats
        WHERE stat_date = '{yesterday}'
        GROUP BY route_short_name, operator_name
        ORDER BY on_time_pct DESC
        LIMIT 10;
    """)

    # Peak hour analysis
    peak_hours = rw.execute_query(f"""
        SELECT stat_hour, time_period,
               SUM(delayed_trips) AS delayed_count,
               ROUND(AVG(delay_rate_pct), 2) AS delay_rate
        FROM transit.agg_delay_stats
        WHERE stat_date = '{yesterday}'
        GROUP BY stat_hour, time_period
        ORDER BY stat_hour;
    """)

    # Train performance
    train_kpis = rw.execute_query(f"""
        SELECT
            COUNT(DISTINCT train_number)               AS trains_monitored,
            SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)   AS delayed_trains,
            SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)  AS cancelled_trains,
            ROUND(AVG(delay_minutes), 1)               AS avg_delay_min,
            MAX(delay_minutes)                         AS max_delay_min
        FROM transit.fact_train_positions
        WHERE DATE(processed_at) = '{yesterday}';
    """)

    rw.close()

    context["ti"].xcom_push(key="kpis",         value=kpis[0] if kpis else {})
    context["ti"].xcom_push(key="worst_routes",  value=worst_routes)
    context["ti"].xcom_push(key="best_routes",   value=best_routes)
    context["ti"].xcom_push(key="peak_hours",    value=peak_hours)
    context["ti"].xcom_push(key="train_kpis",    value=train_kpis[0] if train_kpis else {})

    print(f"KPIs: {kpis}")


def generate_daily_report(**context):
    """Build HTML daily report and save to file."""
    ti        = context["ti"]
    yesterday = ti.xcom_pull(task_ids="aggregate_delay_stats",  key="report_date")
    kpis      = ti.xcom_pull(task_ids="calculate_kpis",         key="kpis")         or {}
    worst     = ti.xcom_pull(task_ids="calculate_kpis",         key="worst_routes") or []
    best      = ti.xcom_pull(task_ids="calculate_kpis",         key="best_routes")  or []
    peak      = ti.xcom_pull(task_ids="calculate_kpis",         key="peak_hours")   or []
    train_kpis = ti.xcom_pull(task_ids="calculate_kpis",        key="train_kpis")   or {}

    # Peak hour bar chart (ASCII-style for email)
    peak_chart = ""
    for ph in peak:
        bar_len = int(float(ph.get("delay_rate", 0)) * 2)
        bar = "█" * bar_len
        peak_chart += f"<tr><td>{ph.get('stat_hour', '')}:00</td><td>{ph.get('time_period','')}</td><td style='font-family:monospace;color:#e74c3c'>{bar}</td><td>{ph.get('delay_rate', 0)}%</td></tr>"

    worst_rows = "".join(
        f"<tr><td>🚌 {r.get('route_short_name')}</td><td>{r.get('operator_name','')}</td>"
        f"<td style='color:#e74c3c'>{r.get('avg_delay_rate', 0)}%</td>"
        f"<td>{round(float(r.get('avg_delay_min', 0)), 1)} min</td></tr>"
        for r in worst
    )
    best_rows = "".join(
        f"<tr><td>🚌 {r.get('route_short_name')}</td><td>{r.get('operator_name','')}</td>"
        f"<td style='color:#27ae60'>{r.get('on_time_pct', 0)}%</td></tr>"
        for r in best
    )

    on_time_rate = kpis.get("network_on_time_rate", 0)
    on_time_color = "#27ae60" if float(on_time_rate) >= 80 else "#e74c3c"

    html = f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="he">
    <head><meta charset="UTF-8">
    <style>
      body {{ font-family: Arial, sans-serif; background: #f5f5f5; color: #333; direction: rtl; }}
      .header {{ background: #1a3a5c; color: white; padding: 20px; text-align: center; }}
      .kpi-grid {{ display: flex; gap: 15px; padding: 20px; flex-wrap: wrap; }}
      .kpi-card {{ background: white; border-radius: 8px; padding: 15px; min-width: 160px;
                   text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1); flex: 1; }}
      .kpi-value {{ font-size: 32px; font-weight: bold; margin: 8px 0; }}
      .kpi-label {{ font-size: 12px; color: #666; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th {{ background: #1a3a5c; color: white; padding: 8px 12px; }}
      td {{ padding: 7px 12px; border-bottom: 1px solid #eee; }}
      .section {{ background: white; margin: 10px 20px; border-radius: 8px;
                  padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
      h3 {{ color: #1a3a5c; border-right: 4px solid #e74c3c; padding-right: 10px; }}
    </style>
    </head>
    <body>
    <div class="header">
      <h1>🚌🚆 דוח יומי - ניטור תחבורה ציבורית ישראל</h1>
      <p>תאריך: {yesterday} | נוצר אוטומטית על ידי מערכת ניטור בזמן אמת</p>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-value" style="color:{on_time_color}">{on_time_rate}%</div>
        <div class="kpi-label">אחוז דיוק ברשת</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#e67e22">{kpis.get('avg_delay_min', 0)}</div>
        <div class="kpi-label">ממוצע איחור (דקות)</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#2980b9">{kpis.get('total_trips', 0):,}</div>
        <div class="kpi-label">סה"כ נסיעות</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#e74c3c">{kpis.get('total_delayed', 0):,}</div>
        <div class="kpi-label">נסיעות מאוחרות</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#8e44ad">{kpis.get('total_routes', 0)}</div>
        <div class="kpi-label">קווים פעילים</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value" style="color:#c0392b">{kpis.get('total_cancelled', 0)}</div>
        <div class="kpi-label">נסיעות שבוטלו</div>
      </div>
    </div>

    <div class="section">
      <h3>🚆 ביצועי רכבת ישראל</h3>
      <table>
        <tr><th>רכבות שנוטרו</th><th>מאוחרות</th><th>בוטלו</th><th>ממוצע איחור</th><th>מקסימום איחור</th></tr>
        <tr>
          <td>{train_kpis.get('trains_monitored', 0)}</td>
          <td style="color:#e74c3c">{train_kpis.get('delayed_trains', 0)}</td>
          <td style="color:#c0392b">{train_kpis.get('cancelled_trains', 0)}</td>
          <td>{train_kpis.get('avg_delay_min', 0)} דק'</td>
          <td>{train_kpis.get('max_delay_min', 0)} דק'</td>
        </tr>
      </table>
    </div>

    <div class="section">
      <h3>⏱️ ניתוח לפי שעות - שיעור איחורים</h3>
      <table><tr><th>שעה</th><th>תקופה</th><th>גרף</th><th>אחוז</th></tr>{peak_chart}</table>
    </div>

    <div style="display:flex; gap:20px; margin:0 20px;">
      <div class="section" style="flex:1">
        <h3>🔴 הקווים הבעייתיים ביותר</h3>
        <table><tr><th>קו</th><th>מפעיל</th><th>% איחורים</th><th>ממוצע</th></tr>{worst_rows}</table>
      </div>
      <div class="section" style="flex:1">
        <h3>🟢 הקווים הדייקנים ביותר</h3>
        <table><tr><th>קו</th><th>מפעיל</th><th>% בזמן</th></tr>{best_rows}</table>
      </div>
    </div>

    <div style="text-align:center;color:#999;font-size:11px;padding:20px">
      נוצר על ידי Israel Transit Intelligence Platform | Naya College Final Project 2025
    </div>
    </body></html>
    """

    output_path = f"/opt/airflow/logs/transit_report_{yesterday}.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Daily report saved → {output_path}")
    print(f"Network on-time rate: {on_time_rate}% | Avg delay: {kpis.get('avg_delay_min')} min")


# ─────────────────────────────────────────
# DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="dag_daily_analytics",
    default_args=default_args,
    description="Daily KPIs, aggregations, route performance report",
    schedule_interval=timedelta(hours=24),     # run once per day
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "daily", "transit", "israel"],
) as dag:

    t_agg     = PythonOperator(task_id="aggregate_delay_stats",   python_callable=aggregate_delay_stats)
    t_routes  = PythonOperator(task_id="aggregate_route_perf",    python_callable=aggregate_route_performance)
    t_kpis    = PythonOperator(task_id="calculate_kpis",          python_callable=calculate_kpis)
    t_report  = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_agg >> [t_routes, t_kpis] >> t_report