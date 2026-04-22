#!/usr/bin/env python3
"""
transit_query.py
Israel Public Transit CLI tool.

Usage:
  python3 transit_query.py --buses-near "תל אביב, דיזנגוף 50" [--radius 500]
  python3 transit_query.py --track-line 480
  python3 transit_query.py --train-station 3700
  python3 transit_query.py --track-train 123
  python3 transit_query.py --alerts

Data sources:
  Bus positions   → Open Bus Stride (Hasadna): https://open-bus-stride-api.hasadna.org.il
  Train info      → Israel Railways API: https://israelrail.azurewebsites.net
  Geocoding       → OpenStreetMap Nominatim (free, no key required)
"""

import argparse
import math
import sys
from datetime import datetime

# Ensure stdout uses UTF-8 so emoji / Hebrew text print correctly on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

try:
    import requests
except ImportError:
    print("ERROR: 'requests' not installed. Run: pip install requests")
    sys.exit(1)

# ── Constants ────────────────────────────────────────────────────────────────

HASADNA_URL  = "https://open-bus-stride-api.hasadna.org.il"
RAIL_API_URL = ""   # israelrail.azurewebsites.net is hijacked — using Stride instead
NOMINATIM    = "https://nominatim.openstreetmap.org/search"

OPERATORS = {
    "2":   "Israel Railways",
    "3":   "Dan",
    "5":   "Egged",
    "6":   "NTA Metropolitan",
    "14":  "Metropoline",
    "15":  "Egged Taavura",
    "16":  "Tnufa",
    "18":  "Superbus",
    "21":  "Kavim",
    "25":  "Nateev Express",
    "32":  "V-Line",
    "42":  "Afikim",
}

TRAIN_STATIONS = {
    "1500": "Tel Aviv - Savidor Center",
    "1600": "Tel Aviv - HaShalom",
    "1700": "Tel Aviv - HaRakevet",
    "1800": "Tel Aviv - Pardes Hanna",
    "2100": "Rehovot",
    "2200": "Nes Ziona - Weizmann",
    "2300": "Rishon LeZion - Moshe Dayan",
    "2400": "Rishon LeZion HaYashan",
    "2500": "Bat Yam Komemiyut",
    "2600": "Bat Yam Yoseftal",
    "3100": "Jerusalem - Yitzhak Navon",
    "3200": "Jerusalem - Big",
    "3300": "Beit Shemesh",
    "3400": "Neve Sha'anan",
    "3500": "Lod",
    "3600": "Ramla",
    "3700": "Ben Gurion Airport",
    "4100": "Haifa - Merkaz HaKarmel",
    "4200": "Haifa - Bat Galim",
    "4250": "Haifa - Hof HaKarmel",
    "4600": "Nahariya",
    "4640": "Akko",
    "4680": "Kiryat Motzkin",
    "4700": "Kiryat Yam",
    "4900": "Hadera West",
    "5000": "Netanya",
    "5200": "Petah Tikva - Kiryat Arye",
    "5300": "Petah Tikva - Segula",
    "5410": "Kfar Saba - Nordau",
    "5900": "Hod HaSharon - Sokolov",
    "6300": "Rosh HaAyin - Ariel Sharon",
    "6700": "Ra'anana B",
    "6900": "Kfar Saba - Sapir",
    "7000": "Herzliya",
    "7100": "Bnei Brak",
    "7200": "Kiryat Gat",
    "7300": "Beer Sheva - Center",
    "7400": "Dimona",
    "7500": "Ashdod - Ad Halom",
    "7600": "Ashkelon",
    "8600": "Modi'in - Yitzhak Navon",
    "8700": "ORT Braude College",
    "9200": "Karmiel",
    "9300": "Akko",
    "9600": "Nahariya - Gav-Yam",
}

# ── Utilities ────────────────────────────────────────────────────────────────

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Return distance in metres between two lat/lon points."""
    R = 6_371_000
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(d_lon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def geocode(address: str) -> tuple[float, float]:
    """Geocode an address using Nominatim. Returns (lat, lon)."""
    params = {
        "q": address,
        "format": "json",
        "limit": 1,
        "countrycodes": "il",
    }
    headers = {"User-Agent": "TransitQuery/1.0"}
    resp = requests.get(NOMINATIM, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    results = resp.json()
    if not results:
        raise ValueError(f"Address not found: {address}")
    return float(results[0]["lat"]), float(results[0]["lon"])


def operator_name(op_ref: str) -> str:
    return OPERATORS.get(str(op_ref), f"Operator {op_ref}")


def delay_label(minutes) -> str:
    if minutes is None:
        return ""
    m = int(minutes)
    if m <= 0:
        return "  on time"
    if m < 5:
        return f"  +{m}′"
    if m < 15:
        return f"  ⚠ +{m}′"
    return f"  🔴 +{m}′"

# ── Commands ─────────────────────────────────────────────────────────────────

def cmd_buses_near(address: str, radius_m: int = 500) -> None:
    """Find buses currently within radius_m metres of the given address."""
    print(f"\n🔍 Locating address: {address} ...")
    lat, lon = geocode(address)
    print(f"   📍 Coordinates: {lat:.5f}, {lon:.5f}")

    # Bounding box slightly wider than radius
    deg = radius_m / 111_000
    params = {
        "siri_vehicle_location__lat__gte": lat - deg,
        "siri_vehicle_location__lat__lte": lat + deg,
        "siri_vehicle_location__lon__gte": lon - deg,
        "siri_vehicle_location__lon__lte": lon + deg,
        "limit": 200,
        "order_by": "recorded_at_time desc",
    }
    url = f"{HASADNA_URL}/siri_rides__with_related/list"
    resp = requests.get(url, params=params, timeout=20)
    if not resp.ok:
        # Fallback to simpler vehicle locations endpoint
        url = f"{HASADNA_URL}/siri_vehicle_locations/list"
        params.pop("siri_vehicle_location__lat__gte", None)
        params.update({
            "lat__gte": lat - deg,
            "lat__lte": lat + deg,
            "lon__gte": lon - deg,
            "lon__lte": lon + deg,
        })
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()

    items = resp.json()

    # Precise haversine filter
    nearby = []
    for v in items:
        vlat = v.get("lat") or v.get("siri_vehicle_location__lat")
        vlon = v.get("lon") or v.get("siri_vehicle_location__lon")
        if vlat is None or vlon is None:
            continue
        dist = haversine_m(lat, lon, float(vlat), float(vlon))
        if dist <= radius_m:
            v["_dist_m"] = round(dist)
            nearby.append(v)

    nearby.sort(key=lambda v: v["_dist_m"])

    if not nearby:
        print(f"\n⚠️  No buses found within {radius_m}m of the address.")
        return

    print(f"\n🚌 {len(nearby)} buses within {radius_m}m:\n")
    for v in nearby[:20]:
        op   = operator_name(v.get("siri_routes__operator_ref") or v.get("operator_ref") or "")
        line = v.get("siri_routes__line_ref") or v.get("line_ref") or "?"
        dist = v["_dist_m"]
        print(f"  Line {line:>4}  ({op:15})  {dist:>4}m away")


def cmd_track_line(line_ref: str) -> None:
    """Show all active vehicles on a bus line, grouped by direction."""
    print(f"\n🚌 Tracking line {line_ref} ...\n")
    params = {
        "siri_route__line_ref": line_ref,   # singular: siri_route__ not siri_routes__
        "limit": 200,
        "order_by": "id desc",
    }
    resp = requests.get(f"{HASADNA_URL}/siri_vehicle_locations/list", params=params, timeout=20)
    resp.raise_for_status()
    items = resp.json()

    if not items:
        print(f"  ⚠️  No active vehicles found on line {line_ref}")
        return

    by_dir: dict[str, list] = {}
    for v in items:
        d = str(v.get("siri_route__direction_id") or "?")
        by_dir.setdefault(d, []).append(v)

    for direction, vehicles in sorted(by_dir.items()):
        label = "Direction A" if direction == "1" else ("Direction B" if direction == "2" else f"Direction {direction}")
        op    = operator_name(vehicles[0].get("siri_route__operator_ref") or "")
        print(f"  {'─'*50}")
        print(f"  {label}  |  {op}  |  {len(vehicles)} buses")
        print(f"  {'─'*50}")
        for v in vehicles[:10]:
            lat  = v.get("lat", "?")
            lon  = v.get("lon", "?")
            t    = (v.get("recorded_at_time") or "")[:19].replace("T", " ")
            vid  = v.get("siri_ride__vehicle_ref") or v.get("id") or "?"
            print(f"    Vehicle {str(vid):>8}   📍 {lat}, {lon}   ⏰ {t}")
        if len(vehicles) > 10:
            print(f"    ... and {len(vehicles) - 10} more vehicles")
        print()


def cmd_train_station(station_id: str) -> None:
    """Show active trains near a station using Open Bus Stride (operator_ref=2)."""
    name = TRAIN_STATIONS.get(station_id, f"Station {station_id}")
    print(f"\n🚆 Active Trains near {name} (Station {station_id})\n")
    print("  ℹ️  Israel Railways API is unavailable — showing active trains from Open Bus Stride.\n")

    # Station coordinates for bbox filtering
    STATION_COORDS = {
        "700":  (32.4493, 34.9205), "1220": (33.0063, 35.0972),
        "1500": (32.0893, 34.7773), "1600": (32.0648, 34.7751),
        "1700": (32.0858, 34.7784), "2300": (32.0993, 34.7977),
        "3100": (31.8952, 35.0186), "3400": (31.7919, 35.2040),
        "3600": (32.8145, 34.9886), "3700": (31.9993, 34.8849),
        "4100": (32.8145, 34.9886), "4600": (33.0063, 35.0972),
        "4900": (32.4493, 34.9205), "5000": (32.3155, 34.8519),
        "5200": (32.0912, 34.8560), "7300": (31.2435, 34.8018),
        "7500": (31.8052, 34.6468), "7600": (31.6598, 34.5714),
        "8600": (31.8952, 35.0186),
    }

    params: dict = {"siri_route__operator_ref": "2", "limit": 50, "order_by": "id desc"}
    if station_id in STATION_COORDS:
        lat, lon = STATION_COORDS[station_id]
        r = 0.20  # ~22 km radius
        params.update({
            "lat__greater_or_equal": lat - r, "lat__lower_or_equal": lat + r,
            "lon__greater_or_equal": lon - r, "lon__lower_or_equal": lon + r,
        })

    try:
        resp = requests.get(
            f"{HASADNA_URL}/siri_vehicle_locations/list",
            params=params, timeout=15,
        )
        resp.raise_for_status()
        vehicles = resp.json()
    except Exception as e:
        print(f"  ❌ Error fetching from Stride: {e}")
        return

    if not vehicles:
        print(f"  ⚠️  No active trains found near {name} at this time.")
        return

    print(f"  {'Train':>8}  {'Line':>6}  {'Speed':>7}  {'Scheduled':>10}  {'Updated':>12}")
    print(f"  {'─'*55}")
    for v in vehicles[:15]:
        tn      = str(v.get("siri_ride__vehicle_ref") or v.get("id") or "?")
        line    = str(v.get("siri_route__line_ref") or "?")
        speed   = f"{float(v['velocity']):.0f} km/h" if v.get("velocity") else "—"
        sched   = (v.get("siri_ride__scheduled_start_time") or "")[:16].replace("T", " ")
        updated = (v.get("recorded_at_time") or "")[:16].replace("T", " ")
        print(f"  {tn:>8}  {line:>6}  {speed:>7}  {sched:>10}  {updated:>12}")


def cmd_track_train(train_number: str) -> None:
    """Find a train by vehicle_ref using Open Bus Stride."""
    print(f"\n🔎 Searching for train {train_number} via Open Bus Stride...\n")
    print("  ℹ️  Israel Railways API is unavailable — using Stride vehicle locations.\n")

    try:
        resp = requests.get(
            f"{HASADNA_URL}/siri_vehicle_locations/list",
            params={
                "siri_route__operator_ref": "2",
                "limit": 200,
                "order_by": "id desc",
            },
            timeout=15,
        )
        resp.raise_for_status()
        vehicles = resp.json()
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return

    found = [
        v for v in vehicles
        if str(v.get("siri_ride__vehicle_ref") or "") == str(train_number)
    ]

    if not found:
        print(f"  ⚠️  Train {train_number} not found in active vehicles.")
        print(f"  Tip: Train numbers in Stride are vehicle_ref values (e.g. 145, 203).")
        return

    print(f"  ✅ Found train {train_number}:\n")
    for v in found:
        lat     = v.get("lat", "?")
        lon     = v.get("lon", "?")
        speed   = f"{float(v['velocity']):.0f} km/h" if v.get("velocity") else "—"
        line    = v.get("siri_route__line_ref") or "?"
        sched   = (v.get("siri_ride__scheduled_start_time") or "")[:16].replace("T", " ")
        updated = (v.get("recorded_at_time") or "")[:16].replace("T", " ")
        print(f"  Line {line} | 📍 {lat}, {lon} | {speed} | Scheduled: {sched} | Updated: {updated}")


def cmd_alerts() -> None:
    """
    Display GTFS-RT service alerts.
    NOTE: The MOT GTFS-RT server (gtfs.mot.gov.il) is currently returning HTML
    error pages. Alerts are not available via HTTP. As a workaround, the Kafka
    topic 'service-alerts' is populated by the platform's ServiceAlertsProducer
    when it manages to fetch data.
    """
    print("\n⚠️  Service Alerts\n")
    print("  Source: MOT GTFS-RT — gtfs.mot.gov.il/gtfsfiles/Alerts.pb")
    print()
    print("  ❌ The MOT GTFS-RT server is not responding (returns HTML page).")
    print("     Cannot fetch service alerts in real time via this method.")
    print()
    print("  💡 Alternatives:")
    print("     1. Check topic 'service-alerts' in Kafka UI: http://localhost:8080")
    print("     2. Check index transit-service-alerts in Kibana: http://localhost:5601")
    print("     3. Check updates on Waze / Moovit / Israel Railways website")


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Israel Public Transit CLI",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="\nExamples:\n"
               "  python3 transit_query.py --buses-near 'Tel Aviv, Dizengoff 50'\n"
               "  python3 transit_query.py --track-line 480\n"
               "  python3 transit_query.py --train-station 3700\n"
               "  python3 transit_query.py --track-train 123\n"
               "  python3 transit_query.py --alerts\n",
    )
    parser.add_argument("--buses-near",    metavar="ADDRESS",      help="Find buses near an address")
    parser.add_argument("--radius",        metavar="METRES", type=int, default=500,
                        help="Search radius in metres (default: 500)")
    parser.add_argument("--track-line",    metavar="LINE_REF",     help="Track a bus line")
    parser.add_argument("--train-station", metavar="STATION_ID",   help="Departures board for a train station")
    parser.add_argument("--track-train",   metavar="TRAIN_NUMBER", help="Find a train by number")
    parser.add_argument("--alerts",        action="store_true",    help="Show service alerts")

    args = parser.parse_args()

    if args.buses_near:
        cmd_buses_near(args.buses_near, radius_m=args.radius)
    elif args.track_line:
        cmd_track_line(args.track_line)
    elif args.train_station:
        cmd_train_station(args.train_station)
    elif args.track_train:
        cmd_track_train(args.track_train)
    elif args.alerts:
        cmd_alerts()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
