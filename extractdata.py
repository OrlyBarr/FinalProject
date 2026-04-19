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
from datetime import datetime, timezone

sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent))
try:
    from config.setting import OPERATORS as _OPERATORS
except Exception:
    _OPERATORS = {}

# Fallback operator name map (covers the most common Israeli transit operators)
_OP_NAMES = {
    "1": "Egged", "2": "Israel Railways", "3": "Dan", "4": "Kavim",
    "5": "Metropoline", "6": "NTA (Metro)", "7": "Superbus", "8": "GB Tours",
    "14": "Nateev Express", "15": "Bus.co.il", "18": "Afikim", "25": "Connex",
    **_OPERATORS,
}

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
            params={"limit": 200, "order_by": "id desc"},
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
            # Skip sentinel timestamps (2037+)
            ts = rec.get("recorded_at_time", "")
            if ts and ts[:4] >= "2037":
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

            operator_id = str(rec.get("siri_route__operator_ref") or "").strip()
            line_ref     = str(rec.get("siri_route__line_ref") or "").strip()
            route_short  = (
                str(rec.get("siri_route__gtfs_route__route_short_name") or "").strip()
                or line_ref
            )

            rows.append({
                "vehicle_id":       str(rec.get("siri_ride__vehicle_ref") or rec.get("id") or ""),
                "trip_id":          str(rec.get("siri_ride__id") or ""),
                "route_id":         line_ref,
                "line_ref":         line_ref,
                "route_short_name": route_short,
                "operator_id":      operator_id,
                "operator_name":    _OP_NAMES.get(operator_id, operator_id or "Unknown"),
                "lat":              float(lat),
                "lon":              float(lon),
                "bearing":          rec.get("bearing"),
                "velocity":         rec.get("velocity"),
                "timestamp":        ts,
                "source":           "hasadna-siri",
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
    print("Downloading stop data...")
    
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
        "Central Station", "Dizengoff Street", "Rabin Square", "Train Station",
        "Shopping Mall", "Hospital", "University", "Rothschild Blvd",
        "Tel Aviv Port", "Central Bus Stop", "Ramat Aviv", "Ramat Gan",
        "Bnei Brak", "Givatayim", "Holon", "Bat Yam",
        "Florentin", "Neve Tzedek", "Jaffa", "Ramat HaChayal"
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

    # Calculate distance to each stop
    stops_df["distance_m"] = stops_df.apply(
        lambda row: geodesic(bus_loc, (row.stop_lat, row.stop_lon)).meters,
        axis=1
    )

    # Find the nearest stop
    nearest = stops_df.loc[stops_df["distance_m"].idxmin()]

    return pd.Series({
        "nearest_stop_id": nearest.stop_id,
        "nearest_stop_name": nearest.stop_name,
        "distance_to_stop_m": nearest.distance_m
    })


def create_sample_bus_data():
    """Creates sample vehicle position data for demonstration — mirrors real schema."""
    import random

    # Real Israeli operator codes + sample route numbers
    sample_operators = [
        ("3",  "Dan",          ["5", "51", "89", "240"]),
        ("1",  "Egged",        ["480", "900", "16", "37"]),
        ("5",  "Metropoline",  ["63", "67", "189", "545"]),
        ("4",  "Kavim",        ["10", "20", "30", "40"]),
        ("18", "Afikim",       ["350", "400", "420", "430"]),
    ]
    # Sample stops around Tel Aviv / Gush Dan
    base_lat, base_lon = 32.0853, 34.7818

    rows = []
    for i in range(20):
        op_id, op_name, routes = random.choice(sample_operators)
        route = random.choice(routes)
        rows.append({
            "vehicle_id":       f"VEH_{7000000 + i}",
            "trip_id":          f"{8000000 + i}",
            "route_id":         route,
            "line_ref":         route,
            "route_short_name": route,
            "operator_id":      op_id,
            "operator_name":    op_name,
            "lat":              base_lat + random.uniform(-0.1, 0.1),
            "lon":              base_lon + random.uniform(-0.1, 0.1),
            "bearing":          random.randint(0, 359),
            "velocity":         random.uniform(5, 60),
            "timestamp":        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "source":           "sample-fallback",
        })

    return pd.DataFrame(rows)



# -----------------------------
# MAIN
# -----------------------------
def main():
    print("Downloading bus data...")
    buses = fetch_bus_positions()

    print("Downloading stop data...")
    stops = fetch_stops()

    print("Computing nearest stop for each bus...")
    merged = buses.join(
        buses.apply(lambda row: find_nearest_stop(row, stops.copy()), axis=1)
    )

    print("\n📌 Relational table (first 5 rows):")
    print(merged.head())

    merged.to_json("buses_with_nearest_stops.json", orient="records", indent=2, force_ascii=False)
    print("\n📁 Saved JSON file: buses_with_nearest_stops.json")


if __name__ == "__main__":
    main()
