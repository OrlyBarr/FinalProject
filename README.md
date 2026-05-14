# 🚌🚆 Israel Public Transit - Real-Time Monitoring Platform
### Naya College — Cloud Big Data Engineer — Final Project

---

## 📁 Project Structure

```
transit_project/
├── docker-compose.yml              # 9 services
├── .env.example                    # Environment variables
├── requirements.txt
│
├── config/
│   └── settings.py                 # Central config (operators, stations, thresholds)
│
├── producers/                      # STEP 2: APIs → Kafka
│   ├── base_producer.py
│   ├── bus_positions_producer.py   # GTFS-RT VehiclePositions.pb
│   ├── trip_updates_producer.py    # GTFS-RT TripUpdates.pb (delays)
│   ├── train_positions_producer.py # Israel Railways station board API
│   └── service_alerts_producer.py  # GTFS-RT ServiceAlerts.pb
│
├── etl/
│   └── transformers.py             # STEP 3: Clean + enrich + classify
│
├── storage/
│   └── s3_writer.py                # STEP 4: S3/MinIO (hour-partitioned)
│
├── warehouse/
│   └── redshift_writer.py          # STEP 5: 6-table Redshift schema
│
└── airflow/
    └── dags/
        ├── dag_realtime_ingestion.py  # Every minute  → 4 producers
        ├── dag_etl_transform.py       # Every 10 min  → ETL + delay detection
        └── dag_daily_analytics.py     # Daily 04:00   → KPIs + HTML report
```

---

## 🚀 Quick Start

```bash
# 1. Configure
cp .env.example .env
# (GTFS-RT feeds require NO API key - public data from MOT)

# 2. Start
docker-compose up -d

# 3. Initialize Airflow DB
docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@example.com

# 4. Create Redshift schema
docker-compose exec airflow-webserver python -c \
    "import sys; sys.path.append('/opt/airflow'); from warehouse.redshift_writer import RedshiftWriter; RedshiftWriter().create_schema()"

# 5. Enable DAGs in Airflow UI → http://localhost:8081
```

---

## 🌐 Data Sources (All Free!)

| Source | URL | Key Required |
|--------|-----|-------------|
| GTFS-RT Bus Positions | gtfs.mot.gov.il/gtfsfiles/VehiclePositions.pb | ❌ No |
| GTFS-RT Trip Updates | gtfs.mot.gov.il/gtfsfiles/TripUpdates.pb | ❌ No |
| GTFS-RT Service Alerts | gtfs.mot.gov.il/gtfsfiles/ServiceAlerts.pb | ❌ No |
| Israel Railways API | israelrail.azurewebsites.net | ❌ No |
| Open Bus Stride (hasadna) | open-bus-stride-api.hasadna.org.il | ❌ No |

---

## 📊 Kafka Topics

| Topic | Producer | Frequency | Content |
|-------|---------|-----------|---------|
| `bus-positions` | BusPositionsProducer | 30s | מיקומי אוטובוסים GPS |
| `trip-updates` | TripUpdatesProducer | 60s | איחורים לפי עצירה |
| `train-positions` | TrainPositionsProducer | 30s | רכבות + פלטפורמות |
| `service-alerts` | ServiceAlertsProducer | 120s | הפרעות שירות |
| `delay-events` | ETL DAG | on-detect | אירועי איחור חמור |
| `pipeline-errors` | All producers | on-error | שגיאות pipeline |

---

## 🗄️ Redshift Schema: `transit`

| Table | Description |
|-------|-------------|
| `fact_bus_positions` | כל מיקוב GPS של אוטובוס |
| `fact_trip_updates` | איחורים לפי עצירה + קו |
| `fact_train_positions` | ביצועי רכבות ישראל |
| `fact_service_alerts` | התראות שירות (upserted) |
| `agg_delay_stats` | סטטיסטיקות איחור לפי שעה + קו |
| `agg_route_performance` | ביצועי קו יומיים |

---

## 📈 KPIs Tracked

- אחוז דיוק ברשת (On-Time Rate %)
- ממוצע איחור לפי קו, מפעיל, שעה
- הקווים הבעייתיים / הדייקנים ביותר
- ניתוח פיק בוקר vs פיק ערב
- שיעור ביטולים לפי מפעיל
- ניטור רציף של 14 תחנות רכבת מרכזיות

---


---

## 📦 Data Population (After First Setup)

After starting the services, MinIO will be **empty** — the data lake starts fresh.
You have two options to populate it with historical transit data:

### Option A: Full Historical Backfill (Recommended)

Pull historical data from the [Open Bus Stride API](https://open-bus-stride-api.hasadna.org.il/) (free, no API key needed):

```bash
# Activate the virtual environment
source venv/bin/activate

# Run full backfill — fetches data for all missing days (takes ~15-20 min)
python3 scripts/full_backfill.py
```

This will scan all days since March 14, 2026, identify gaps, and backfill bus positions and ride data from the public Stride API into MinIO.

### Option B: Let It Collect Organically

Once the pipeline is running (`bash run.sh`), new data will be collected automatically every 2 minutes via Airflow DAGs. After a few hours, you'll have meaningful data to work with.

### Automatic Gap Recovery

A `wake_backfill.py` cron job runs every 15 minutes and automatically detects + fills data gaps caused by laptop sleep mode or VM downtime. No manual intervention needed.

```
# Crontab entry (already configured):
*/15 * * * * cd /home/local_admin/finalproject && flock -n /tmp/wake_backfill.lock venv/bin/python3 scripts/wake_backfill.py >> /tmp/wake_backfill.log 2>&1
```

## 🔧 Services

| Service | URL | Description |
|---------|-----|-------------|
| Airflow | localhost:8081 | תזמון DAGs |
| Kafka UI | localhost:8080 | ניטור topics |
| MinIO | localhost:9001 | Data Lake מקומי |
| Kibana | localhost:5601 | דשבורד חי |

---

## ✅ Project Requirements Coverage

| Requirement | Status |
|-------------|--------|
| Solution Architecture | ✅ PPTX + README |
| API Data Ingestion | ✅ 4 producers (GTFS-RT + Railways) |
| ETL Pipeline | ✅ transformers.py + consumers |
| Real-Time Kafka Streaming | ✅ 6 topics |
| Data Warehouse (Redshift) | ✅ 6-table schema |
| Partitioned Storage (S3) | ✅ year/month/day/hour |
| Airflow DAGs | ✅ 3 DAGs |
| ELK Stack (Bonus) | ✅ Elasticsearch + Kibana |
| Docker | ✅ 9 services |