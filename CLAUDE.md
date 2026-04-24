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
# http://localhost:5000         → צ'אט AI (index.html)
# http://localhost:5000/agent   → לוח שאילתות (agent_transit.html)
# http://localhost:5000/gushdan → אפליקציית גוש דן (Moovit-style)
# http://localhost:8085         → Kafka UI  ← שונה מ-8080!
# http://localhost:9001         → MinIO Console
# http://localhost:5601         → Kibana
# http://localhost:8081         → Airflow UI
```

### הפעלה על linub-vm (שרת Linux)

```bash
# SSH אל השרת
ssh linub-vm   # ~/.ssh/config מוגדר: Host linub-vm → 192.168.1.110, User local_admin

# הרצת run.sh (מאתחל את כל השירותים)
cd /home/local_admin/finalproject
bash run.sh

# אם Docker containers לא נטענים — הרץ עם sudo (containers הופעלו ע"י root)
sudo docker-compose up -d
sudo docker stop <container>
```

---

## endpoints של bot.py (port 5000)

| Method | Path | תיאור |
|--------|------|-------|
| GET | `/` | index.html (צ'אט AI) |
| GET | `/agent` | agent_transit.html (לוח שאילתות) |
| GET | `/gushdan` | gushdan_app.html (אפליקציית Moovit-style) ← **חדש** |
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
| GET | `/api/dankal` | רכבת קלה NTA (operator=6) — bbox מסלול האדום ← **חדש** |
| POST | `/ask` | צ'אט AI דרך Gemini |

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

### גל 1 — תיקוני API ו-UI

1. **`bot.py`** — `log` לא היה מוגדר → `NameError` ב-`/streets`
2. **`bot.py`** — bbox params שגויים: `lat__gte` → `lat__greater_or_equal`
3. **`bot.py`** — `/proxy/rail` התעלם מ-`stationId` → עכשיו מסנן לפי קואורדינטות התחנה
4. **`agent_transit.html`** — לוח רכבת תמיד ריק → נוסף טיפול בפורמט Stride array + `renderStrideTrainCard()`
5. **`agent_transit.html`** — מזהי מפעילים שגויים בכל הטבלה ו-dropdown
6. **`agent_transit.html`** — פרמטרי Stride שגויים: `line_ref` → `siri_route__line_ref`
7. **`transit_query.py`** — החליף Israel Railways API מת ב-Stride
8. **`transit_query.py`** — `siri_routes__` (רבים) → `siri_route__` (יחיד)
9. **`.env.example`** — נוסף `GEMINI_API_KEY` ו-`GOOGLE_MAPS_API_KEY`

### גל 2 — run.sh (תיקון Step 4)

10. **`run.sh`** — Step 4 נתקע לנצח: פקודת `timeout` לא קיימת ב-macOS/Git Bash
    - **פתרון**: פונקציית `_kafka()` נוספת — מריצה את הפקודה ב-background וממתינה לה עם `kill -0`
    - לולאת readiness של 90 שניות (30 × sleep 3) לפני יצירת topics
    - `--if-not-exists` בכל יצירת topic (idempotent)
    - commit `28aeb57` — pushed to GitHub

### גל 3 — אפליקציית גוש דן (`gushdan_app.html`)

11. **`gushdan_app.html`** — **קובץ חדש** (947 שורות) — אפליקציית Moovit-style לגוש דן:
    - 5 מסכים + ניווט תחתון: בית (מפה + תחנות קרובות), תכנון מסלול, ניווט פעיל, דנקל קו אדום, לוח עזיבות, מועדפים
    - 22 תחנות דנקל עם קואורדינטות מקורבות (בת ים → פתח תקווה)
    - `BBOX = {latMin:31.87, latMax:32.21, lonMin:34.72, lonMax:34.95}`
    - guard "מחוץ לתחום" — overlay אם המשתמש מחוץ לגוש דן
    - צבעי מפעילים: דן כתום, אגד כחול כהה, דנקל/NTA אדום, מטרופולין תכלת
    - Hebrew RTL PWA — `direction:rtl`, mobile-first
    - Leaflet.js עם CartoDB Positron tiles (ללא API key)

12. **`bot.py`** — נוספו endpoints:
    - `/gushdan` — מגיש את `gushdan_app.html`
    - `/api/dankal` — שאילתת Stride ל-NTA vehicles (operator_ref=6) בבbox של קו האדום
    - עדכון `/status` לכלול Gush Dan Transit App URL
    - commit `1caea02` — pushed to GitHub

### גל 4 — linub-vm: הרמת ה-VM

13. **linub-vm** — VM לא עלה עקב Hyper-V Saved State + מחסור בזיכרון:
    - שגיאה: `0x800705AA` — ה-VM ניסה לשחזר 12,265 MB RAM; המארח לא היה עם מספיק זיכרון פנוי
    - **פתרון (PowerShell מורם)**:
      1. `Remove-VMSavedState 'linub-vm'` — מחיקת Saved State
      2. `Set-VMMemory -StartupBytes (6144MB)` — הורדת RAM ל-6 GB
      3. `Start-VM 'linub-vm'` — הפעלה מחדש
    - SSH config עודכן: `IdentityFile ~/.ssh/id_ed25519`, `ServerAliveInterval 60`, `ServerAliveCountMax 3`

### גל 5 — linub-vm: דיבאג run.sh ✅

14. **בעיית Kafka `InconsistentClusterIdException`** — אובחנה ותוקנה:
    - **שורש הבעיה**: volume `finalproject_kafka_data` הכיל cluster ID `2wkTnxsyRpynpAyR3BTDWA` שלא תאם ל-Zookeeper (`PgtNQG1AQ5SiXf40TBOrNg`) → Kafka קורס בכל restart
    - **פתרון**: ניקוי ה-volume דרך container זמני:
      ```bash
      docker run --rm -v finalproject_kafka_data:/data \
        confluentinc/cp-kafka:7.4.0 bash -c 'rm -rf /data/* && echo VOLUME_CLEARED'
      ```
    - ✅ בוצע בהצלחה — `VOLUME_CLEARED` אושר

15. **בעיית זיכרון בשרת** — VM (6 GB RAM) עמוס מדי ✅ **נפתרה**:
    - 20 containers שאינם של הפרויקט הוסרו (hive, mariadb, mongo, nifi, logistic-*)
    - Elasticsearch + Kibana נעצרו זמנית לפנות זיכרון (~1.5 GB)
    - Swap הוגדל מ-2 GB ל-6 GB + `vm.swappiness=10`
    - **מצב עכשיו**: RAM זמין 1.5 GB, Swap פנוי 4 GB

### גל 6 — linub-vm: ייצוב זיכרון (אפריל 2026)

16. **Swap הוגדל ל-6 GB** — קובץ swap נוסף `/home/local_admin/swap2.img` (4 GB):
    ```bash
    # יצירת קובץ (ללא sudo, בhome directory):
    fallocate -l 4G /home/local_admin/swap2.img && chmod 600 /home/local_admin/swap2.img
    # הפעלה דרך privileged container (ללא sudo):
    docker run --rm --privileged -u root -v /home/local_admin:/swapdir \
      confluentinc/cp-kafka:7.4.0 bash -c \
      'mkswap /swapdir/swap2.img && swapon /swapdir/swap2.img && echo 10 > /proc/sys/vm/swappiness'
    ```
    - `/etc/fstab` עודכן: `/home/local_admin/swap2.img none swap sw 0 0` ← persistent בין reboots
    - `/etc/sysctl.conf` עודכן: `vm.swappiness=10` ← kernel לא יוציא ל-swap מוקדם מדי

17. **docker-compose.yml — הגבלות זיכרון ל-Airflow** (commit `b53812b`):
    - `airflow-webserver: mem_limit: 800m` + `AIRFLOW__WEBSERVER__WORKERS: '1'`
    - `airflow-scheduler: mem_limit: 600m` + `AIRFLOW__SCHEDULER__MAX_THREADS: '2'` + `PARSING_PROCESSES: '1'`
    - `AIRFLOW__CORE__PARALLELISM: '4'` + `MAX_ACTIVE_TASKS_PER_DAG: '2'` בשניהם

18. **טריק לעצירת containers ע"י root** (ללא sudo):
    ```bash
    docker exec <container> kill 1    # שולח SIGTERM ל-PID 1 → container מסתיים בעצמו
    ```

---

## בעיות ידועות ופתרונות

### מצב containers נוכחי על linub-vm

```
✅ kafka, zookeeper, postgres, minio, airflow-webserver, airflow-scheduler, kafka-ui
⏸️  elasticsearch, kibana — נעצרו זמנית לחסוך זיכרון (ניתן להפעיל כשצריך)
```

להפעיל elasticsearch+kibana:
```bash
cd /home/local_admin/finalproject && sudo docker-compose up -d elasticsearch kibana
```

### Kafka — הערות חשובות

- **kafka-ui** על port **8085** (לא 8080! — 8080 תפוס ע"י NiFi)
- **kafka-init** יוצר topics דרך `kafka:29092` (internal Docker network)
- **run.sh** יוצר topics דרך `localhost:9092` (external) — שניהם עובדים
- אם Kafka לא עולה → בדוק `docker logs kafka --tail 30` לאיתור שגיאת cluster ID

### Docker permissions על linub-vm

```
# Containers הופעלו ע"י root:
docker stop <container>  → "permission denied"
docker exec <container>  → עובד ✅ (exec מותר)
docker run               → עובד ✅ (יצירת containers חדשים מותרת)
sudo docker stop         → דורש password sudo interactively
```

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

> ⚠️ `kafka-ui` port = **8085** (לא 8080)

---

## GitHub Repository

```
https://github.com/zivversano/finalproject.git
branch: main

commits:
  28aeb57 — run.sh Step 4 portable fix (_kafka() helper + 90s readiness loop)
  1caea02 — bot.py /gushdan + /api/dankal + gushdan_app.html (947 lines)
```
