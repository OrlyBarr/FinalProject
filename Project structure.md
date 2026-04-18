# מבנה פרויקט מומלץ — Israel Public Transit Real-Time Monitoring

## עץ תיקיות מלא

```
israel-transit-monitor/
│
├── docker-compose.yml               # Kafka, Zookeeper, MinIO, ES, Kibana
├── .env                             # משתני סביבה (לא ב-git)
├── README.md
│
├── collectors/                      ← ✅ הקבצים החדשים שקיבלת
│   ├── config.py
│   ├── bus_delay_collector.py       # real-time אוטובוסים
│   ├── train_delay_collector.py     # real-time רכבות
│   ├── historical_fetcher.py        # batch היסטורי
│   ├── delay_kafka_consumer.py      # consumer → ES + MinIO
│   └── requirements.txt
│
├── proxy/                           ← קיים (bot.py)
│   └── bot.py
│
├── frontend/                        ← קיים (תצוגת מסלולים)
│   └── ...
│
├── kibana/                          ← ✅ חדש
│   └── kibana_dashboard.ndjson      # import לדשבורד
│
└── scripts/                         ← ✅ חדש (שורות הפקודה)
    ├── start_collectors.sh          # מריץ את כל ה-collectors ב-background
    └── import_kibana.sh             # מייבא את הדשבורד ל-Kibana
```

---

## למה collectors/ ולא ישר בשורש?

הפרויקט כבר מכיל שכבות נפרדות (proxy, frontend).
`collectors/` היא שכבת ה-data ingestion — שומר על הפרדה ברורה.

---

## הוראות שילוב

### 1. העברת הקבצים

```bash
mkdir -p collectors kibana scripts
cp bus_delay_collector.py train_delay_collector.py \
   historical_fetcher.py delay_kafka_consumer.py \
   config.py requirements.txt  collectors/

cp kibana_dashboard.ndjson  kibana/
```

### 2. Kafka topics — יצירה ידנית (אם לא קיימים)

```bash
docker exec -it kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic bus-delays --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic train-delays --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic bus-delays-historical --partitions 2 --replication-factor 1

docker exec -it kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic train-delays-historical --partitions 2 --replication-factor 1
```

### 3. הפעלת הכל

```bash
# terminal 1 — אוטובוסים real-time
cd collectors && python bus_delay_collector.py

# terminal 2 — רכבות real-time
cd collectors && python train_delay_collector.py

# terminal 3 — consumer → ES + MinIO
cd collectors && python delay_kafka_consumer.py

# שליפה היסטורית (חד-פעמי, לאחר שהכל עולה)
cd collectors && python historical_fetcher.py --days 7
```

### 4. ייבוא Kibana Dashboard

```bash
curl -X POST "http://localhost:5601/api/saved_objects/_import" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: multipart/form-data" \
  --form file=@kibana/kibana_dashboard.ndjson
```

או דרך ממשק Kibana:
**Stack Management → Saved Objects → Import → בחר kibana_dashboard.ndjson**

---

## אחרי ייבוא הדשבורד

כתובת הדשבורד: `http://localhost:5601/app/dashboards`
חפש: **Israel Transit Delays Dashboard**