"""
bot.py
Simple REST API bot for Israel Public Transit Monitoring Platform.
Exposes real-time transit data over HTTP on port 5000.

Endpoints:
  GET /                - serve agent_transit.html (Transit Query Tool)
  GET /health          - health check
  GET /status          - system status
  GET /buses           - latest bus positions from bus_positions.json
  GET /stops           - bus stops with nearest stop info
  GET /agent           - serve agent_transit.html dashboard
  GET /geocode         - address → GPS (Nominatim proxy, bypasses CORS)
  GET /proxy/stride/*  - proxy to Hasadna Open Bus Stride API (bypasses CORS)
  GET /proxy/hasadna/* - alias for /proxy/stride (backwards compat)
  GET /proxy/rail      - proxy to Israel Railways API (bypasses CORS)
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import sys
import threading
import time
import subprocess
import urllib.parse
import urllib.request
import urllib.error

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BOT_PORT       = int(os.getenv("BOT_PORT", 5000))
# israelrail.azurewebsites.net is hijacked — disabled
RAIL_BOARD_URL = ""
HASADNA_URL    = "https://open-bus-stride-api.hasadna.org.il"
NOMINATIM_URL  = "https://nominatim.openstreetmap.org/search"
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))

# ── גוש דן ותל אביב — Bounding Box ──────────────────────────────────────────
GUSH_DAN = {
    "lat_min": 31.97, "lat_max": 32.19,
    "lon_min": 34.73, "lon_max": 34.93,
}

# ── רחובות לכל עיר בגוש דן ───────────────────────────────────────────────────
CITY_STREETS = {
    "תל אביב-יפו": sorted([
        "אבן גבירול","אלנבי","בוגרשוב","בן יהודה","בן גוריון","גורדון",
        "דיזנגוף","דרך יפו","הארבעה","הברזל","הגליל","הירקון","הנביאים",
        "הרצל","השייטת","ויצמן","זמנהוף","חיים לבנון","יהודה הלוי",
        "יהושע בן נון","יצחק שדה","כנפי נשרים","לה גווארדיה","לילנבלום",
        'מזא"ה',"מנחם בגין","נחלת בנימין","נחמני","נורדאו","סוקולוב",
        "פינסקר","פלורנטין","פרישמן","קינג ג'ורג'","רוטשילד","שד' שאול המלך",
        "שד' ירושלים","שדרות חן","שלמה המלך","שלמה לבין","שינקין","תובל",
        "ארלוזורוב","אחד העם","ביאליק","חסן בק","יפת","שד' רוקח",
        "מסגר","הצורן","האורגים","הרקמה","הנחושת","החרושת","דרך בגין",
        "קיבוץ גלויות","שד' לוי אשכול","חובבי ציון","שד' נורדאו",
    ]),
    "רמת גן": sorted([
        "אבא הלל","ביאליק","בן גוריון","ברודצקי","ז'בוטינסקי","חזון איש",
        "יהודה הנשיא","יוספטל","כצנלסון","מאפו","מוצקין","עמל",
        "פועה","ריינס","שד' ירושלים","שד' מנחם בגין","תל חי",
        "הרב קוק","הצנחנים","אלוף שדה","בעל שם טוב","גנסין",
        "אוסישקין","ארלוזורוב","בלפור","קרליבך","קפלן",
    ]),
    "גבעתיים": sorted([
        "ארלוזורוב","בורוכוב","גרוזנברג","הנשיא","וייצמן","יהלום",
        "כצנלסון","קוגל","שד' העצמאות","שד' הציונות","שינקין",
        'תרצ"ה',"הרב מימון","אחד העם","גאולה","חיבת ציון",
        "קוסובסקי","פלוגת הבלתי מחדיל","בן גוריון",
    ]),
    "בני ברק": sorted([
        "אחיעזר","הרב אלפנדרי","הרב דסלר","הרב הרצוג","הרב שך",
        "ז'בוטינסקי","חזון איש","יצחק קפלן","כהנמן","מוהליבר",
        "מנחם בגין","עמיאל","פועלי צדק","רבי עקיבא","שמחה",
        "תחכמוני","בר אילן","בית יעקב","ישיבת פוניבז'","נחל קדרון",
    ]),
    "פתח תקווה": sorted([
        "אחד העם","ארלוזורוב","בילינסון","גנות","הגדוד העברי","הרצל",
        "ויצמן","חיים עוזר","יוספטל","יחזקאל","כצנלסון","מוהליבר",
        "מנחם בגין","נורדאו","עגנון","פינס","קפלן",
        "שד' העצמאות","שיפר","שמחה","תמר","אוסישקין","בלפור",
        "סירקין","אחוזה","הגפן","המייסדים",
    ]),
    "חולון": sorted([
        "אבן גבירול","בן גוריון","גולדה מאיר","הלח\"י","הנשיא","ויצמן",
        "יוספטל","ינאי","כצנלסון","מנחם בגין","סוקולוב",
        "עציון","פלמ\"ח","קוגן","רבין","שד' חולון","שוהם","תל גיבורים",
        "אלתרמן","בלפור","גורדון","דרך בן צבי","פיקר","ז'בוטינסקי",
    ]),
    "בת ים": sorted([
        "אחד העם","בלפור","בן גוריון","גורדון","הגבורה","הנשיא",
        "ויצמן","חורגין","יוספטל","לסקוב","מנחם בגין","סוקולוב",
        "עציון","פלמ\"ח","רוטשילד","שד' ירושלים","שינקין","תמר",
        "הרצל","ביאליק","בן יהודה","דרך יפו","קפלן","בלפור",
    ]),
    "הרצליה": sorted([
        "אבן גבירול","בן גוריון","גיבורי ישראל","דרך הים","הגלים","הנשיא",
        "ויצמן","יהלום","כוכב הצפון","מנחם בגין","נורדאו","סוקולוב",
        "עגנון","פלמ\"ח","קפלן","שד' ירושלים","שד' בן ציון","תמר",
        "אחד העם","בלפור","רוטשילד","הרב קוק","שד' ההסתדרות",
    ]),
    "רמת השרון": sorted([
        "אבא הלל","ביאליק","בן גוריון","גיבורי ישראל","הגלים","הנשיא",
        "כצנלסון","מנחם בגין","סוקולוב","עגנון","קפלן",
        "שד' ירושלים","תמר","אחד העם","בלפור","רוטשילד",
        "הרב קוק","בעל שם טוב","שד' ויצמן",
    ]),
    "גבעת שמואל": sorted([
        "בן גוריון","גיבורי ישראל","הגבורה","הנשיא","ויצמן","כצנלסון",
        "מנחם בגין","סוקולוב","עגנון","קפלן","שד' ירושלים","תמר",
        "אחד העם","בלפור","רוטשילד","הרב קוק","שינקין",
    ]),
    "קריית אונו": sorted([
        "אבא הלל","ביאליק","בן גוריון","גיבורי ישראל","הגבורה","הנשיא",
        "ויצמן","כצנלסון","מנחם בגין","סוקולוב","עגנון","קפלן",
        "שד' ירושלים","תמר","אחד העם","בלפור","רוטשילד","דרך השלום",
    ]),
    "אור יהודה": sorted([
        "בן גוריון","גיבורי ישראל","הגבורה","הנשיא","ויצמן","כצנלסון",
        "מנחם בגין","סוקולוב","עגנון","קפלן","שד' ירושלים","תמר",
        "אחד העם","בלפור","רוטשילד","הרב קוק","דרך השלום","הציונות",
    ]),
    "אזור": sorted([
        "בן גוריון","הגבורה","הנשיא","ויצמן","כצנלסון","מנחם בגין",
        "סוקולוב","עגנון","קפלן","שד' ירושלים","תמר","אחד העם",
        "בלפור","רוטשילד","דרך השלום","הציונות","הרצל",
    ]),
    "גבעת עדה": sorted([
        "בן גוריון","הגבורה","הנשיא","ויצמן","כצנלסון","מנחם בגין",
        "סוקולוב","עגנון","קפלן","שד' ירושלים","תמר","אחד העם",
        "בלפור","רוטשילד","דרך השלום","הרצל",
    ]),
}

def _in_gush_dan(lat, lon) -> bool:
    """בדיקה אם נקודה נמצאת בגוש דן / תל אביב."""
    try:
        return (GUSH_DAN["lat_min"] <= float(lat) <= GUSH_DAN["lat_max"] and
                GUSH_DAN["lon_min"] <= float(lon) <= GUSH_DAN["lon_max"])
    except (TypeError, ValueError):
        return False


# ── Background auto-refresh ───────────────────────────────────────────────────

def _refresh_buses_loop(interval_seconds: int = 120) -> None:
    script     = os.path.join(BASE_DIR, "extractdata.py")
    python_bin = os.path.join(BASE_DIR, "venv", "bin", "python3")
    if not os.path.exists(python_bin):
        python_bin = "python3"

    while True:
        time.sleep(interval_seconds)
        try:
            result = subprocess.run(
                [python_bin, script],
                cwd=BASE_DIR, timeout=60,
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                print("[BOT-refresh] buses_with_nearest_stops.json refreshed")
            else:
                print(f"[BOT-refresh] extractdata.py error: {result.stderr[:200]}")
        except subprocess.TimeoutExpired:
            print("[BOT-refresh] extractdata.py timed out — skipping")
        except Exception as e:
            print(f"[BOT-refresh] error: {e}")


def load_json(filename):
    path = os.path.join(BASE_DIR, filename)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


# ── HTTP Handler ──────────────────────────────────────────────────────────────

class BotHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        print(f"[BOT] {self.address_string()} - {format % args}")

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                body = f.read().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_json({"error": f"HTML file not found: {filepath}"}, status=404)

    def _proxy(self, target_url):
        """Generic server-side proxy — forwards GET, adds CORS headers."""
        try:
            req = urllib.request.Request(
                target_url,
                headers={
                    "Accept":     "application/json",
                    "User-Agent": "IsraelTransitBot/1.0",
                },
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                body   = resp.read()
                status = resp.status
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except urllib.error.HTTPError as e:
            body = e.read() or json.dumps({"error": str(e)}).encode()
            self.send_response(e.code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            self.send_json({"error": f"proxy error: {e}"}, status=502)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        qs     = urllib.parse.parse_qs(parsed.query)

        # ── Transit Query Tool ────────────────────────────────────────────────
        # מגיש index.html (גרסה חדשה) עם fallback ל-agent_transit.html
        if path == "/" or path == "/transit":
            html = os.path.join(BASE_DIR, "index.html")
            if not os.path.exists(html):
                html = os.path.join(BASE_DIR, "agent_transit.html")
            self.send_html(html)

        # ── Agent dashboard ───────────────────────────────────────────────────
        elif path == "/agent":
            html = os.path.join(BASE_DIR, "index.html")
            if not os.path.exists(html):
                html = os.path.join(BASE_DIR, "agent_transit.html")
            self.send_html(html)

        # ── health ────────────────────────────────────────────────────────────
        elif path == "/health":
            self.send_json({"status": "ok", "service": "Israel Transit Bot", "port": BOT_PORT})

        # ── status ────────────────────────────────────────────────────────────
        elif path == "/status":
            self.send_json({
                "status": "running",
                "service": "Israel Public Transit Monitoring — גוש דן ותל אביב",
                "area": {
                    "name": "גוש דן ותל אביב",
                    "lat": f"{GUSH_DAN['lat_min']}–{GUSH_DAN['lat_max']}",
                    "lon": f"{GUSH_DAN['lon_min']}–{GUSH_DAN['lon_max']}",
                },
                "endpoints": {
                    "Transit Query Tool": f"http://localhost:{BOT_PORT}/",
                    "Agent Dashboard":    f"http://localhost:{BOT_PORT}/agent",
                    "Airflow UI":         "http://localhost:8081",
                    "Kafka UI":           "http://localhost:8080",
                    "MinIO Console":      "http://localhost:9001",
                    "Kibana":             "http://localhost:5601",
                    "Bot API":            f"http://localhost:{BOT_PORT}",
                }
            })

        # ── buses — גוש דן ותל אביב בלבד ────────────────────────────────────
        elif path == "/buses":
            all_data = load_json("buses_with_nearest_stops.json") or load_json("bus_positions.json")
            # סינון לגוש דן בלבד
            filtered = [
                b for b in all_data
                if _in_gush_dan(
                    b.get("lat") or b.get("latitude"),
                    b.get("lon") or b.get("longitude")
                )
            ]
            self.send_json(filtered[:200])

        # ── stops — גוש דן ותל אביב בלבד ────────────────────────────────────
        elif path == "/stops":
            all_data = load_json("buses_with_nearest_stops.json")
            filtered = [
                b for b in all_data
                if _in_gush_dan(
                    b.get("lat") or b.get("latitude"),
                    b.get("lon") or b.get("longitude")
                )
            ]
            self.send_json(filtered[:200])

        # ── GTFS — routes by operator ─────────────────────────────────────────
        # GET /gtfs/routes?operator_id=6&q=63
        elif path == "/gtfs/routes":
            op  = (qs.get("operator_id") or [""])[0].strip()
            q_  = (qs.get("q") or [""])[0].strip()
            lim = int((qs.get("limit") or ["100"])[0])
            try:
                from gtfs_query import search_routes
                rows = search_routes(query=q_, operator_id=op, limit=lim)
                self.send_json({"routes": rows, "count": len(rows)})
            except Exception as e:
                self.send_json({"error": str(e), "routes": []}, status=500)

        # ── GTFS — stops of a route ───────────────────────────────────────────
        # GET /gtfs/route_stops?route_id=XXX  OR  ?short_name=63
        elif path == "/gtfs/route_stops":
            route_id   = (qs.get("route_id")   or [""])[0].strip()
            short_name = (qs.get("short_name")  or [""])[0].strip()
            try:
                from gtfs_query import get_route_stops, get_route_stops_by_short_name
                if short_name:
                    rows = get_route_stops_by_short_name(short_name)
                elif route_id:
                    rows = get_route_stops(route_id)
                else:
                    self.send_json({"error": "?route_id= or ?short_name= required"}, status=400)
                    return
                self.send_json({"stops": rows, "count": len(rows)})
            except Exception as e:
                self.send_json({"error": str(e), "stops": []}, status=500)

        # ── GTFS — nearby stops ───────────────────────────────────────────────
        # GET /gtfs/nearby?lat=32.08&lon=34.78&radius=500
        elif path == "/gtfs/nearby":
            try:
                lat    = float((qs.get("lat")    or ["0"])[0])
                lon    = float((qs.get("lon")    or ["0"])[0])
                radius = int((qs.get("radius")   or ["500"])[0])
                from gtfs_query import get_nearby_stops, get_routes_at_stop
                stops = get_nearby_stops(lat, lon, radius)
                # הוסף קווים לכל תחנה
                for s in stops[:5]:
                    s["routes"] = get_routes_at_stop(s["stop_id"])
                self.send_json({"stops": stops, "count": len(stops)})
            except Exception as e:
                self.send_json({"error": str(e), "stops": []}, status=500)

        # ── GTFS — stop schedule ──────────────────────────────────────────────
        # GET /gtfs/schedule?stop_id=XXX&from=08:00
        elif path == "/gtfs/schedule":
            stop_id  = (qs.get("stop_id")  or [""])[0].strip()
            from_t   = (qs.get("from")     or ["00:00:00"])[0].strip()
            day_type = (qs.get("day")      or ["weekday"])[0].strip()
            if not stop_id:
                self.send_json({"error": "?stop_id= required"}, status=400)
                return
            try:
                from gtfs_query import get_stop_schedule
                rows = get_stop_schedule(stop_id, from_t, day_type)
                self.send_json({"schedule": rows, "count": len(rows)})
            except Exception as e:
                self.send_json({"error": str(e), "schedule": []}, status=500)

        # ── GTFS — search stops ───────────────────────────────────────────────
        # GET /gtfs/stops?q=דיזנגוף
        elif path == "/gtfs/stops":
            q_ = (qs.get("q") or [""])[0].strip()
            try:
                from gtfs_query import search_stops
                rows = search_stops(q_) if q_ else []
                self.send_json({"stops": rows})
            except Exception as e:
                self.send_json({"error": str(e), "stops": []}, status=500)

        # ── GTFS — operators ─────────────────────────────────────────────────
        # GET /gtfs/operators
        elif path == "/gtfs/operators":
            try:
                from gtfs_query import get_operators
                rows = get_operators()
                self.send_json({"operators": rows})
            except Exception as e:
                self.send_json({"error": str(e), "operators": []}, status=500)

        # ── GTFS — status ─────────────────────────────────────────────────────
        # GET /gtfs/status
        elif path == "/gtfs/status":
            try:
                from gtfs_query import get_gtfs_summary, is_available
                self.send_json({
                    "available": is_available(),
                    "summary":   get_gtfs_summary(),
                })
            except Exception as e:
                self.send_json({"available": False, "error": str(e)})

        # ── geocode → Nominatim proxy ─────────────────────────────────────────
        elif path == "/geocode":
            address = urllib.parse.unquote_plus((qs.get("q") or [""])[0]).strip()
            if not address:
                self.send_json({"error": "?q=address parameter required"}, status=400)
                return
            q = address if "israel" in address.lower() else address + ", Israel"
            target = NOMINATIM_URL + "?" + urllib.parse.urlencode({
                "q": q, "format": "json", "limit": 1,
                "countrycodes": "il", "accept-language": "he",
            })
            try:
                req = urllib.request.Request(
                    target,
                    headers={"User-Agent": "IsraelTransitBot/1.0 (transit-project)"},
                )
                with urllib.request.urlopen(req, timeout=8) as resp:
                    results = json.loads(resp.read())
                if not results:
                    self.send_json({"error": f"Address not found: {address}"}, status=404)
                    return
                r = results[0]
                self.send_json({
                    "lat":          float(r["lat"]),
                    "lon":          float(r["lon"]),
                    "display_name": r.get("display_name", address),
                })
            except Exception as e:
                self.send_json({"error": f"geocoding error: {e}"}, status=500)

        # ── streets → רשימת רחובות סטטית לכל עיר ────────────────────────────
        # GET /streets?city=תל+אביב
        elif path == "/streets":
            city = urllib.parse.unquote_plus((qs.get("city") or [""])[0]).strip()
            if not city:
                self.send_json({"error": "?city= required"}, status=400)
                return
            streets = CITY_STREETS.get(city, [])
            if not streets:
                # fallback: חפש עיר דומה
                for k in CITY_STREETS:
                    if city in k or k in city:
                        streets = CITY_STREETS[k]
                        break
            self.send_json({"city": city, "streets": sorted(streets)})

        # ── Stride proxy — מסנן לגוש דן ותל אביב ────────────────────────────
        elif path.startswith("/proxy/stride"):
            api_path = path[len("/proxy/stride"):]
            if not api_path.startswith("/"):
                api_path = "/" + api_path
            target = HASADNA_URL.rstrip("/") + api_path

            # הוסף פרמטרי bbox לגוש דן לכל קריאת siri_vehicle_locations
            query = parsed.query
            if "siri_vehicle_locations" in api_path:
                bbox_params = (
                    f"lat__gte={GUSH_DAN['lat_min']}&lat__lte={GUSH_DAN['lat_max']}"
                    f"&lon__gte={GUSH_DAN['lon_min']}&lon__lte={GUSH_DAN['lon_max']}"
                )
                query = f"{query}&{bbox_params}" if query else bbox_params

            if query:
                target += "?" + query
            self._proxy(target)

        # ── Hasadna alias ─────────────────────────────────────────────────────
        elif path.startswith("/proxy/hasadna"):
            api_path = path[len("/proxy/hasadna"):]
            if not api_path.startswith("/"):
                api_path = "/" + api_path
            target = HASADNA_URL.rstrip("/") + api_path
            query  = parsed.query
            if "siri_vehicle_locations" in api_path:
                bbox_params = (
                    f"lat__gte={GUSH_DAN['lat_min']}&lat__lte={GUSH_DAN['lat_max']}"
                    f"&lon__gte={GUSH_DAN['lon_min']}&lon__lte={GUSH_DAN['lon_max']}"
                )
                query = f"{query}&{bbox_params}" if query else bbox_params
            if query:
                target += "?" + query
            self._proxy(target)

        # ── Israel Railways → Stride (israelrail.azurewebsites.net is dead) ──
        elif path.startswith("/proxy/rail"):
            stride_params = (
                f"siri_route__operator_ref=2&limit=100&order_by=id+desc"
                f"&lat__gte={GUSH_DAN['lat_min']}&lat__lte={GUSH_DAN['lat_max']}"
                f"&lon__gte={GUSH_DAN['lon_min']}&lon__lte={GUSH_DAN['lon_max']}"
            )
            target = f"{HASADNA_URL.rstrip('/')}/siri_vehicle_locations/list?{stride_params}"
            self._proxy(target)

        else:
            self.send_json({"error": "Not found", "path": self.path}, status=404)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    threading.Thread(
        target=_refresh_buses_loop, args=(120,),
        daemon=True, name="bus-refresh"
    ).start()
    print("🔄 Background bus refresh started (every 2 min)")

    server = ThreadingHTTPServer(("0.0.0.0", BOT_PORT), BotHandler)
    print(f"🤖 Transit Bot API → http://0.0.0.0:{BOT_PORT}")
    print(f"   /                         → Transit Query Tool (index.html)")
    print(f"   /agent                    → Agent Transit Dashboard")
    print(f"   /health                   → health check")
    print(f"   /status                   → system status")
    print(f"   /buses                    → bus positions")
    print(f"   /stops                    → buses + nearest stops")
    print(f"   /geocode?q=address        → address → GPS (Nominatim proxy)")
    print(f"   /proxy/stride/<path>?...  → Hasadna Open Bus Stride API proxy")
    print(f"   /proxy/rail?...           → Israel Railways API proxy")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped.")
        server.server_close()


if __name__ == "__main__":
    main()