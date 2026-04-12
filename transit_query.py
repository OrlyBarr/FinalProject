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
RAIL_API_URL = "https://israelrail.azurewebsites.net"
NOMINATIM    = "https://nominatim.openstreetmap.org/search"

OPERATORS = {
    "3":   "דן",
    "5":   "אגד",
    "6":   "אגד תעבורה",
    "7":   "קוופרטיב",
    "10":  "מטרופולין",
    "14":  "קווים",
    "15":  "נתיב אקספרס",
    "16":  "ירושלים",
    "18":  "גולן",
    "20":  "Kavim",
    "21":  "מועצה אזורית",
    "23":  "חיפה",
    "25":  "נצרת",
    "31":  "גן שמואל",
    "32":  "אפיקים",
    "34":  "ניר שחק",
    "37":  "מחוז הדרום",
    "41":  "Veolia",
    "42":  "Extra",
    "44":  "Egged Tavura",
    "91":  "ישראל רכבת",
}

TRAIN_STATIONS = {
    "1500": "תל אביב - סבידור מרכז",
    "1600": "תל אביב - השלום",
    "1700": "תל אביב - הרכבת",
    "1800": "תל אביב - פרדס חנה",
    "2100": "רחובות",
    "2200": "נס ציונה - מכון וייצמן",
    "2300": "ראשון לציון - משה דיין",
    "2400": "ראשון לציון הוישן",
    "2500": "בת ים קוממיות",
    "2600": "בת ים יוספטל",
    "3100": "ירושלים - יצחק נבון",
    "3200": "ירושלים - ביג",
    "3300": "בית שמש",
    "3400": "נוה שאנן",
    "3500": "לוד",
    "3600": "רמלה",
    "3700": "בן גוריון",
    "4100": "חיפה - מרכז הכרמל",
    "4200": "חיפה - בת גלים",
    "4250": "חיפה - חוף הכרמל",
    "4600": "נהריה",
    "4640": "עכו",
    "4680": "קריית מוצקין",
    "4700": "קריית ים",
    "4900": "חדרה מערב",
    "5000": "נתניה",
    "5200": "פתח תקווה - קריית אריה",
    "5300": "פתח תקווה - סגולה",
    "5410": "כפר סבא - נורדאו",
    "5900": "הוד השרון - סוקולוב",
    "6300": "ראש העין - אריאל שרון",
    "6700": "רעננה ב",
    "6900": "כפר סבא - ספיר",
    "7000": "הרצליה",
    "7100": "בני ברק",
    "7200": "קריית גת",
    "7300": "באר שבע - מרכז",
    "7400": "דימונה",
    "7500": "אשדוד - עד הלום",
    "7600": "אשקלון",
    "8600": "מודיעין - יצחק נבון",
    "8700": "ביה\"ס אורט בראודה",
    "9200": "כרמיאל",
    "9300": "עכו",
    "9600": "נהריה - גברעם",
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
    return OPERATORS.get(str(op_ref), f"מפעיל {op_ref}")


def delay_label(minutes) -> str:
    if minutes is None:
        return ""
    m = int(minutes)
    if m <= 0:
        return "  בזמן"
    if m < 5:
        return f"  +{m}′"
    if m < 15:
        return f"  ⚠ +{m}′"
    return f"  🔴 +{m}′"

# ── Commands ─────────────────────────────────────────────────────────────────

def cmd_buses_near(address: str, radius_m: int = 500) -> None:
    """Find buses currently within radius_m metres of the given address."""
    print(f"\n🔍 מגדיר מיקום: {address} …")
    lat, lon = geocode(address)
    print(f"   📍 קואורדינטות: {lat:.5f}, {lon:.5f}")

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
        print(f"\n⚠️  לא נמצאו אוטובוסים בטווח {radius_m}מ' מהכתובת.")
        return

    print(f"\n🚌 {len(nearby)} אוטובוסים בטווח {radius_m}מ':\n")
    for v in nearby[:20]:
        op   = operator_name(v.get("siri_routes__operator_ref") or v.get("operator_ref") or "")
        line = v.get("siri_routes__line_ref") or v.get("line_ref") or "?"
        dist = v["_dist_m"]
        print(f"  קו {line:>4}  ({op:15})  {dist:>4}מ' ממך")


def cmd_track_line(line_ref: str) -> None:
    """Show all active vehicles on a bus line, grouped by direction."""
    print(f"\n🚌 מעקב אחרי קו {line_ref} …\n")
    params = {
        "siri_routes__line_ref": line_ref,
        "limit": 200,
        "order_by": "recorded_at_time desc",
    }
    resp = requests.get(f"{HASADNA_URL}/siri_vehicle_locations/list", params=params, timeout=20)
    resp.raise_for_status()
    items = resp.json()

    if not items:
        print(f"  ⚠️  לא נמצאו כלי רכב פעילים בקו {line_ref}")
        return

    by_dir: dict[str, list] = {}
    for v in items:
        d = str(v.get("siri_routes__direction_id") or "?")
        by_dir.setdefault(d, []).append(v)

    for direction, vehicles in sorted(by_dir.items()):
        label = "כיוון א'" if direction == "1" else ("כיוון ב'" if direction == "2" else f"כיוון {direction}")
        op    = operator_name(vehicles[0].get("siri_routes__operator_ref") or "")
        print(f"  {'─'*50}")
        print(f"  {label}  |  {op}  |  {len(vehicles)} אוטובוסים")
        print(f"  {'─'*50}")
        for v in vehicles[:10]:
            lat  = v.get("lat", "?")
            lon  = v.get("lon", "?")
            t    = (v.get("recorded_at_time") or "")[:19].replace("T", " ")
            vid  = v.get("vehicle_ref") or v.get("id") or "?"
            print(f"    רכב {str(vid):>8}   📍 {lat}, {lon}   ⏰ {t}")
        if len(vehicles) > 10:
            print(f"    … ועוד {len(vehicles) - 10} רכבים")
        print()


def cmd_train_station(station_id: str) -> None:
    """Show the departure board for a train station."""
    name = TRAIN_STATIONS.get(station_id, f"תחנה {station_id}")
    print(f"\n🚆 לוח נסיעות — {name} (תחנה {station_id})\n")

    today = datetime.now().strftime("%Y-%m-%d")
    hour  = datetime.now().hour

    url    = f"{RAIL_API_URL}/stations/GetStationBoard"
    params = {"stationId": station_id, "date": today, "hour": hour}
    headers = {"Accept": "application/json"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ❌ שגיאה: {e}")
        return

    trains = (data.get("result", {}).get("trains")
              or data.get("Data")
              or data.get("trains")
              or [])

    if not trains:
        print(f"  ⚠️  אין נסיעות בתחנת {name} בשעה הנוכחית.")
        return

    print(f"  {'קו':>5}  {'יעד':20}  {'מתוכנן':8}  {'מציאות':8}  {'עיכוב':8}  {'רציף':>6}")
    print(f"  {'─'*65}")
    for t in trains[:15]:
        line    = str(t.get("trainno") or t.get("LineNumber") or "?")
        dest    = str(t.get("orign")   or t.get("Destination") or "?")[:20]
        planned = str(t.get("departtime") or t.get("DepartureTime") or "?")[:5]
        actual  = str(t.get("actualdeparttime") or t.get("ActualDepartureTime") or planned)[:5]
        delay   = t.get("delay") or t.get("Delay") or 0
        platform = str(t.get("platform") or t.get("Platform") or "?")
        dl      = delay_label(delay)
        print(f"  {line:>5}  {dest:20}  {planned:8}  {actual:8}  {dl:10}  {platform:>6}")


def cmd_track_train(train_number: str) -> None:
    """Scan stations to find a train by number and show its current status."""
    print(f"\n🔎 מחפש רכבת מספר {train_number} …\n")

    today      = datetime.now().strftime("%Y-%m-%d")
    hour       = datetime.now().hour
    scan_stations = list(TRAIN_STATIONS.keys())[:15]   # scan first 15 major stations
    found      = []

    for sid in scan_stations:
        name = TRAIN_STATIONS.get(sid, sid)
        try:
            params = {"stationId": sid, "date": today, "hour": hour}
            resp   = requests.get(
                f"{RAIL_API_URL}/stations/GetStationBoard",
                params=params,
                headers={"Accept": "application/json"},
                timeout=10,
            )
            if not resp.ok:
                continue
            data   = resp.json()
            trains = (data.get("result", {}).get("trains")
                      or data.get("Data") or data.get("trains") or [])
            for t in trains:
                tno = str(t.get("trainno") or t.get("LineNumber") or "")
                if tno == str(train_number):
                    t["_station_name"] = name
                    t["_station_id"]   = sid
                    found.append(t)
        except Exception:
            continue

    if not found:
        print(f"  ⚠️  לא נמצאה רכבת {train_number} בתחנות שנסרקו.")
        return

    print(f"  ✅ נמצאה רכבת {train_number} ב-{len(found)} תחנות:")
    print()
    for t in found:
        station  = t["_station_name"]
        planned  = str(t.get("departtime") or t.get("DepartureTime") or "?")[:5]
        actual   = str(t.get("actualdeparttime") or t.get("ActualDepartureTime") or planned)[:5]
        delay    = t.get("delay") or t.get("Delay") or 0
        platform = str(t.get("platform") or t.get("Platform") or "?")
        dl       = delay_label(delay)
        print(f"  📍 {station:30} | יציאה {planned} ({actual:5}) {dl} | רציף {platform}")


def cmd_alerts() -> None:
    """
    Display GTFS-RT service alerts.
    NOTE: The MOT GTFS-RT server (gtfs.mot.gov.il) is currently returning HTML
    error pages. Alerts are not available via HTTP. As a workaround, the Kafka
    topic 'service-alerts' is populated by the platform's ServiceAlertsProducer
    when it manages to fetch data.
    """
    print("\n⚠️  התראות שירות (Service Alerts)\n")
    print("  מקור: MOT GTFS-RT — gtfs.mot.gov.il/gtfsfiles/Alerts.pb")
    print()
    print("  ❌ שרת ה-GTFS-RT של משרד התחבורה אינו מגיב (מחזיר עמוד HTML).")
    print("     לא ניתן לאחזר התראות שירות בזמן אמת בשיטה זו.")
    print()
    print("  💡 חלופות:")
    print("     1. בדוק את topic 'service-alerts' ב-Kafka UI: http://localhost:8080")
    print("     2. בדוק מדד transit-service-alerts ב-Kibana: http://localhost:5601")
    print("     3. בדוק עדכונים ב-Waze / Moovit / אתר רכבת ישראל")


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Israel Public Transit CLI — שאל על תחבורה ציבורית",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="\nדוגמאות:\n"
               "  python3 transit_query.py --buses-near 'תל אביב, דיזנגוף 50'\n"
               "  python3 transit_query.py --track-line 480\n"
               "  python3 transit_query.py --train-station 3700\n"
               "  python3 transit_query.py --track-train 123\n"
               "  python3 transit_query.py --alerts\n",
    )
    parser.add_argument("--buses-near",    metavar="ADDRESS",      help="מצא אוטובוסים ליד כתובת")
    parser.add_argument("--radius",        metavar="METRES", type=int, default=500,
                        help="רדיוס חיפוש במטרים (ברירת מחדל: 500)")
    parser.add_argument("--track-line",    metavar="LINE_REF",     help="עקוב אחרי קו אוטובוס")
    parser.add_argument("--train-station", metavar="STATION_ID",   help="לוח נסיעות לתחנת רכבת")
    parser.add_argument("--track-train",   metavar="TRAIN_NUMBER", help="מצא רכבת לפי מספר")
    parser.add_argument("--alerts",        action="store_true",    help="הצג התראות שירות")

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
