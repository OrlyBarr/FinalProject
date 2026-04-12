import sys

# Ensure stdout uses UTF-8 so emoji / Hebrew text print correctly on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import requests
import zipfile
import io
import pandas as pd
from google.transit import gtfs_realtime_pb2
from geopy.distance import geodesic
from datetime import datetime

# Hasadna Open Bus SIRI API (primary — always available)
HASADNA_SIRI_URL = "https://open-bus-stride-api.hasadna.org.il/siri_vehicle_locations/list"

# MOT GTFS-RT feeds (may return HTML error page when unavailable)
GTFS_RT_URL     = "https://gtfs.mot.gov.il/gtfsfiles/VehiclePositions.pb"
GTFS_RT_URL_ALT = "https://gtfs.mot.gov.il/gtfsrt/realtimeVehiclePositions.pb"
GTFS_STATIC_URL  = "https://gtfs.mot.gov.il/gtfsfiles/gtfs.zip"


# -----------------------------
# 1. Fetch bus positions (GTFS-RT)
# -----------------------------
def fetch_bus_positions():
    """Downloads and parses real-time bus positions.
    Primary:   Hasadna Open Bus SIRI API (JSON)
    Secondary: MOT GTFS-RT protobuf feeds
    Fallback:  Generated sample data
    """

    # ── Primary: Hasadna SIRI vehicle locations (JSON, always up) ─────────────
    try:
        print("📡 Trying Hasadna Open Bus SIRI API...")
        resp = requests.get(
            HASADNA_SIRI_URL,
            params={"limit": 200, "order_by": "recorded_at_time desc"},
            timeout=15
        )
        resp.raise_for_status()
        records = resp.json()

        rows = []
        for rec in records:
            lat = rec.get("lat")
            lon = rec.get("lon")
            if lat is None or lon is None:
                continue
            rows.append({
                "vehicle_id":  str(rec.get("id", "")),
                "trip_id":     str(rec.get("siri_ride_stop_id", "")),
                "route_id":    str(rec.get("siri_snapshot_id", "")),
                "lat":         float(lat),
                "lon":         float(lon),
                "bearing":     rec.get("bearing"),
                "velocity":    rec.get("velocity"),
                "timestamp":   rec.get("recorded_at_time"),
                "source":      "hasadna-siri",
            })

        if rows:
            print(f"✅ Hasadna SIRI API: {len(rows)} vehicle positions fetched")
            return pd.DataFrame(rows)
        print("⚠️  Hasadna API returned 0 records — trying fallback...")
    except Exception as e:
        print(f"❌ Hasadna SIRI API error: {e}")

    # ── Secondary: MOT GTFS-RT protobuf (may be down) ─────────────────────────
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/x-protobuf,application/octet-stream'
    }
    for url in [GTFS_RT_URL, GTFS_RT_URL_ALT]:
        try:
            print(f"📡 Trying MOT GTFS-RT: {url}")
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()

            if resp.content.startswith(b'<!DOCTYPE') or resp.content.startswith(b'<html'):
                print(f"❌ MOT returned HTML error page — server is down")
                continue

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(resp.content)

            rows = []
            for entity in feed.entity:
                if not entity.HasField("vehicle"):
                    continue
                v = entity.vehicle
                rows.append({
                    "vehicle_id": v.vehicle.id,
                    "trip_id":    v.trip.trip_id,
                    "route_id":   v.trip.route_id,
                    "lat":        v.position.latitude,
                    "lon":        v.position.longitude,
                    "bearing":    v.position.bearing if v.position.HasField("bearing") else None,
                    "velocity":   None,
                    "timestamp":  datetime.utcfromtimestamp(v.timestamp).isoformat() if v.timestamp else None,
                    "source":     "mot-gtfs-rt",
                })

            if rows:
                print(f"✅ MOT GTFS-RT: {len(rows)} bus positions fetched")
                return pd.DataFrame(rows)
        except Exception as e:
            print(f"❌ MOT GTFS-RT error ({url}): {e}")

    # ── Fallback: generated sample data ───────────────────────────────────────
    print("⚠️  All live APIs failed — using sample data for demonstration")
    return create_sample_bus_data()


# -----------------------------
# 2. Fetch bus stops (GTFS Static)
# -----------------------------
def fetch_stops():
    """Downloads and extracts bus stops from GTFS static data"""
    print("מוריד נתוני תחנות...")
    
    try:
        resp = requests.get(GTFS_STATIC_URL, timeout=20)
        resp.raise_for_status()
        
        # Check if we got HTML instead of zip
        if resp.content.startswith(b'<!DOCTYPE') or resp.content.startswith(b'<html'):
            print("❌ Received HTML instead of zip file")
            raise Exception("Invalid response format")

        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            with z.open("stops.txt") as f:
                df = pd.read_csv(f)

        df = df.rename(columns={
            "stop_lat": "stop_lat",
            "stop_lon": "stop_lon",
            "stop_id": "stop_id",
            "stop_name": "stop_name"
        })

        print(f"✅ Successfully fetched {len(df)} bus stops")
        return df[["stop_id", "stop_name", "stop_lat", "stop_lon"]]
        
    except Exception as e:
        print(f"❌ Error fetching stops: {e}")
        print("⚠️  Using sample stops data for demonstration...")
        return create_sample_stops_data()


def create_sample_stops_data():
    """Creates sample bus stop data for demonstration"""
    import random
    
    # Sample stops around Tel Aviv area
    base_lat, base_lon = 32.0853, 34.7818
    
    stops = []
    stop_names = [
        "תחנה מרכזית", "רחוב דיזנגוף", "כיכר רבין", "תחנת רכבת",
        "קניון", "בית חולים", "אוניברסיטה", "שדרות רוטשילד",
        "נמל תל אביב", "תחנת מרכז", "רמת אביב", "רמת גן",
        "בני ברק", "גבעתיים", "חולון", "בת ים",
        "פלורנטין", "נווה צדק", "יפו", "רמת החייל"
    ]
    
    for i, name in enumerate(stop_names):
        stops.append({
            "stop_id": f"STOP_{10000 + i}",
            "stop_name": name,
            "stop_lat": base_lat + random.uniform(-0.15, 0.15),
            "stop_lon": base_lon + random.uniform(-0.15, 0.15)
        })
    
    return pd.DataFrame(stops)


# -----------------------------
# 3. Match each bus to nearest stop
# -----------------------------
def find_nearest_stop(bus_row, stops_df):
    """Finds the nearest bus stop for a given bus position"""
    bus_loc = (bus_row.lat, bus_row.lon)

    # מחשבים מרחק לכל תחנה
    stops_df["distance_m"] = stops_df.apply(
        lambda row: geodesic(bus_loc, (row.stop_lat, row.stop_lon)).meters,
        axis=1
    )

    # מוצאים את התחנה הקרובה ביותר
    nearest = stops_df.loc[stops_df["distance_m"].idxmin()]

    return pd.Series({
        "nearest_stop_id": nearest.stop_id,
        "nearest_stop_name": nearest.stop_name,
        "distance_to_stop_m": nearest.distance_m
    })


def create_sample_bus_data():
    """Creates sample vehicle position data for demonstration"""
    from datetime import datetime, timedelta
    import random
    
    # Sample data for Tel Aviv area
    base_lat, base_lon = 32.0853, 34.7818
    
    rows = []
    for i in range(20):
        rows.append({
            "vehicle_id": f"BUS_{1000 + i}",
            "trip_id": f"TRIP_{5000 + i}",
            "route_id": f"ROUTE_{random.randint(1, 50)}",
            "lat": base_lat + random.uniform(-0.1, 0.1),
            "lon": base_lon + random.uniform(-0.1, 0.1),
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat()
        })
    
    return pd.DataFrame(rows)



# -----------------------------
# MAIN
# -----------------------------
def main():
    print("מוריד נתוני אוטובוסים...")
    buses = fetch_bus_positions()

    print("מוריד נתוני תחנות...")
    stops = fetch_stops()

    print("מחשב תחנה קרובה לכל אוטובוס...")
    merged = buses.join(
        buses.apply(lambda row: find_nearest_stop(row, stops.copy()), axis=1)
    )

    print("\n📌 טבלה רלציונית (5 שורות ראשונות):")
    print(merged.head())

    merged.to_json("buses_with_nearest_stops.json", orient="records", indent=2, force_ascii=False)
    print("\n📁 נשמר קובץ JSON בשם buses_with_nearest_stops.json")


if __name__ == "__main__":
    main()
