# CLAUDE.md — Israel Public Transit Monitoring Platform

קרא קובץ זה בתחילת כל שיחה חדשה לקבלת הקשר מלא על הפרויקט.

---

## מה הפרויקט?

פלטפורמת ניטור תחבורה ציבורית בזמן אמת לגוש דן / תל אביב.  
נתונים מ-APIs ציבוריים → Kafka → ETL → Elasticsearch / MinIO / Redshift.  
ממשק משתמש: שרת HTTP פשוט (`bot.py`) עם צ'אט AI ולוח שאילתות GTFS.

---

## מבנה הפרויקט

```
finalproject-main/
├── bot.py                    # שרת HTTP (port 5000) — נקודת כניסה ראשית
├── index.html                # צ'אט AI (Gemini API דרך /ask)
├── agent_transit.html        # לוח שאילתות GTFS (אוטובוסים, רכבת, התראות)
├── extractdata.py            # מוריד נתוני אוטובוסים → buses_with_nearest_stops.json
├── transit_query.py          # CLI tool לשאילתות תחבורה
├── Gtfs_query.py             # שאילתות GTFS מ-PostgreSQL
├── check_delays.py           # בדיקת עיכובים
├── resilient_pipeline.py     # pipeline עמיד לשגיאות
├── .env                      # מפתחות API (לא ב-git!) ← חובה ליצור מקומית
├── .env.example              # תבנית לקובץ .env
│
├── config/
│   ├── setting.py            # הגדרות מרכזיות (נטען על ידי settings.py)
│   └── settings.py           # alias ל-setting.py (ייבוא תואמות לאחור)
│
├── producers/                # Kafka producers — אחד לכל מקור נתונים
│   ├── base_producer.py      # מחלקת בסיס לכל ה-producers
│   ├── bus_positions_producer.py    # אוטובוסים מ-Stride SIRI API
│   ├── train_positions_producer.py  # רכבות מ-Stride (operator_ref=2)
│   ├── trip_updates_producer.py     # עדכוני נסיעה מ-MOT GTFS-RT (.pb)
│   ├── service_alert_producer.py    # התראות מ-MOT GTFS-RT (.pb)
│   └── traffic_producer.py          # תנועה מ-HERE Traffic API
│
├── collectors/               # אוספי עיכובים (Kafka consumers + producers)
│   ├── config.py             # הגדרות collectors
│   ├── bus_delay_collector.py
│   ├── train_delay_collector.py
│   ├── delay_kafka_consumer.py
│   └── historical_fetcher.py
│
├── etl/
│   ├── transformers.py       # BusPositionTransformer, TripUpdateTransformer...
│   └── traffic_transformer.py
│
├── storage/
│   ├── s3_writer.py
│   ├── minio_uploader.py
│   ├── es_indexer.py
│   ├── direct_to_minio.py
│   └── traffic_to_minio.py
│
├── warehouse/
│   └── redshift_writer.py
│
├── airflow/dags/
│   ├── dag_realtime_ingestion.py   # כל 2 דקות
│   ├── dag_ETL_transform.py        # כל 10 דקות
│   ├── dag_daily_analysis.py       # יומי 04:00 UTC
│   ├── dag_traffic_ingestion.py    # כל 5 דקות
│   ├── dag_es_indexer.py
│   ├── dag_direct_to_minio.py
│   └── dag_traffic_to_minio.py
│
└── docker-compose.yml        # Kafka, Zookeeper, Airflow, PostgreSQL, MinIO, ES, Kibana
```

---

## APIs ומקורות נתונים

### פעיל ✅

| מקור | URL | מפתח | הערות |
|------|-----|-------|-------|
| Open Bus Stride (Hasadna) | `https://open-bus-stride-api.hasadna.org.il` | לא נדרש | נתוני אוטובוסים + רכבות בזמן אמת |
| MOT GTFS-RT Trip Updates | `https://gtfs.mot.gov.il/gtfsfiles/TripUpdates.pb` | לא נדרש | protobuf בינארי |
| MOT GTFS-RT Vehicle Positions | `https://gtfs.mot.gov.il/gtfsfiles/VehiclePositions.pb` | לא נדרש | protobuf בינארי |
| MOT GTFS-RT Service Alerts | `https://gtfs.mot.gov.il/gtfsfiles/ServiceAlerts.pb` | לא נדרש | protobuf בינארי |
| Nominatim (geocoding) | `https://nominatim.openstreetmap.org/search` | לא נדרש | geocoding חינמי |
| HERE Traffic API | `https://data.traffic.hereapi.com/v7/flow` | `HERE_API_KEY` | תנועה בזמן אמת |
| Gemini AI | `generativelanguage.googleapis.com` | `GEMINI_API_KEY` | צ'אט AI ב-index.html |

### מת ❌ — לא להשתמש!

| API | סיבה | חלופה |
|-----|-------|-------|
| `israelrail.azurewebsites.net` | נחטף | Stride עם `siri_route__operator_ref=2` |
| `www.rail.co.il/apiinfo` | חסום ע"י Cloudflare | Stride עם `siri_route__operator_ref=2` |

---

## Stride API — פרמטרים חשובים

```
# פרמטרי סינון נכונים לsiri_vehicle_locations/list:
siri_route__operator_ref=2        # רכבת ישראל
siri_route__line_ref=89           # מספר קו (יחיד! לא line_refs)
siri_route__operator_ref=3        # דן (יחיד! לא operator_refs)

# פרמטרי bbox נכונים:
lat__greater_or_equal=31.97       # ✅ נכון
lat__lower_or_equal=32.19         # ✅ נכון
lat__gte=31.97                    # ❌ שגוי — Stride לא מכיר
lat__lte=32.19                    # ❌ שגוי
```

---

## מפעילי אוטובוסים — IDs נכונים

```python
OPERATORS = {
    "2":  "רכבת ישראל",
    "3":  "דן",
    "5":  "אגד",
    "6":  "NTA מטרופולין",    # רכבת קלה — לא אגד תעבורה!
    "14": "מטרופולין",         # לא קווים! קווים = 21
    "15": "אגד תעבורה",       # לא נתיב אקספרס! נתיב = 25
    "16": "תנופה",
    "18": "סופרבוס",           # לא גולן!
    "21": "קווים",
    "25": "נתיב אקספרס",
    "32": "V-Line",
    "42": "אפיקים",            # לא Extra!
}
```

---

## תחנות רכבת — IDs נכונים (Israel Railways)

```python
MAJOR_TRAIN_STATIONS = {
    "2300": "תל אביב מרכז (ע\"ש ארלוזרוב)",
    "2820": "תל אביב השלום",
    "3400": "ירושלים יצחק נבון",
    "3600": "חיפה מרכז השמיר",   # לא ת"א!
    "4600": "באר שבע מרכז",
    "3100": "נתניה",
    "5000": "הרצליה",
    "700":  "חדרה מערב",
    "1220": "נהריה",
}
```

---

## קובץ .env — מה נדרש

```env
# חובה לצ'אט AI
GEMINI_API_KEY=your_key_here        # מ-aistudio.google.com (חינמי, 1500/יום)

# אופציונלי
GOOGLE_MAPS_API_KEY=...             # לרחובות מ-Google Places ב-/streets
HERE_API_KEY=...                    # לנתוני תנועה

# Kafka (ברירת מחדל: localhost:9092)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# MinIO (ברירת מחדל: localhost:9000)
USE_MINIO=true
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123

# AWS (אופציונלי)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET_NAME=israel-transit-lake

# Redshift (אופציונלי)
REDSHIFT_HOST=...
REDSHIFT_DB=transit_dw
REDSHIFT_USER=admin
REDSHIFT_PASSWORD=...
```

> ⚠️ `.env` ב-`.gitignore` — לעולם לא להעלות ל-GitHub!

---

## הפעלה מקומית

```bash
# התקנת תלויות
pip install -r requirements.txt

# הפעלת שירותי Docker (Kafka, MinIO, Elasticsearch וכו')
docker-compose up -d

# הפעלת שרת ה-bot
python bot.py

# גישה:
# http://localhost:5000       → צ'אט AI (index.html)
# http://localhost:5000/agent → לוח שאילתות (agent_transit.html)
# http://localhost:8080       → Kafka UI
# http://localhost:9001       → MinIO Console
# http://localhost:5601       → Kibana
# http://localhost:8081       → Airflow UI
```

---

## endpoints של bot.py (port 5000)

| Method | Path | תיאור |
|--------|------|-------|
| GET | `/` | index.html (צ'אט AI) |
| GET | `/agent` | agent_transit.html (לוח שאילתות) |
| GET | `/health` | בדיקת תקינות |
| GET | `/status` | סטטוס מערכת + URLs |
| GET | `/buses` | מיקומי אוטובוסים מ-cache |
| GET | `/stops` | אוטובוסים + תחנות קרובות |
| GET | `/geocode?q=ADDRESS` | כתובת → GPS (Nominatim proxy) |
| GET | `/streets?city=תל+אביב` | רשימת רחובות (Google Places / static) |
| GET | `/gtfs/routes?q=63` | חיפוש קווים מ-PostgreSQL |
| GET | `/gtfs/route_stops?short_name=63` | תחנות קו |
| GET | `/gtfs/nearby?lat=32.08&lon=34.78` | תחנות קרובות |
| GET | `/gtfs/schedule?stop_id=X` | לוח זמנים לתחנה |
| GET | `/gtfs/stops?q=דיזנגוף` | חיפוש תחנות |
| GET | `/proxy/stride/*` | proxy לHasadna Stride API |
| GET | `/proxy/hasadna/*` | alias ל-/proxy/stride |
| GET | `/proxy/rail?stationId=X` | רכבות פעילות ליד תחנה (Stride operator=2) |
| POST | `/ask` | צ'אט AI דרך Gemini |

---

## Kafka Topics

| Topic | מקור | Partitions |
|-------|------|-----------|
| `bus-positions` | BusPositionsProducer | 4 |
| `train-positions` | TrainPositionsProducer | 4 |
| `trip-updates` | TripUpdatesProducer | 2 |
| `service-alerts` | ServiceAlertsProducer | 2 |
| `traffic-data` | TrafficProducer | 2 |
| `delay-events` | ETL DAG | 1 |
| `pipeline-errors` | כל ה-producers | 1 |
| `bus-delays` | BusDelayCollector | 3 |
| `train-delays` | TrainDelayCollector | 3 |

---

## אזור גיאוגרפי — גוש דן

```python
GUSH_DAN = {
    "lat_min": 31.97,  # דרום — בת ים / חולון
    "lat_max": 32.19,  # צפון — הרצליה / רמת השרון
    "lon_min": 34.73,  # מערב — חוף הים
    "lon_max": 34.93,  # מזרח — פתח תקווה / בני ברק
}
```

---

## תיקונים שבוצעו (אפריל 2026)

1. **`bot.py`** — `log` לא היה מוגדר → `NameError` ב-`/streets`
2. **`bot.py`** — bbox params שגויים: `lat__gte` → `lat__greater_or_equal`
3. **`bot.py`** — `/proxy/rail` התעלם מ-`stationId` → עכשיו מסנן לפי קואורדינטות התחנה
4. **`agent_transit.html`** — לוח רכבת תמיד ריק → נוסף טיפול בפורמט Stride array + `renderStrideTrainCard()`
5. **`agent_transit.html`** — מזהי מפעילים שגויים בכל הטבלה ו-dropdown
6. **`agent_transit.html`** — פרמטרי Stride שגויים: `line_ref` → `siri_route__line_ref`
7. **`transit_query.py`** — החליף Israel Railways API מת ב-Stride
8. **`transit_query.py`** — `siri_routes__` (רבים) → `siri_route__` (יחיד)
9. **`.env.example`** — נוסף `GEMINI_API_KEY` ו-`GOOGLE_MAPS_API_KEY`

---

## GitHub Repository

```
https://github.com/zivversano/finalproject.git
branch: main
```
