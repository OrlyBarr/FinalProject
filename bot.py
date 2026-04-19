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

    def _fetch_url(self, target_url, timeout=8):
        """Fetch URL, return (status_int, bytes_body). Raises on error."""
        req = urllib.request.Request(
            target_url,
            headers={"Accept": "application/json", "User-Agent": "IsraelTransitBot/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()

    def _send_raw(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _proxy(self, target_url):
        """Generic server-side proxy — 8 s timeout, no fallback."""
        try:
            status, body = self._fetch_url(target_url)
            self._send_raw(status, body)
        except urllib.error.HTTPError as e:
            body = e.read() or json.dumps({"error": str(e)}).encode()
            self._send_raw(e.code, body)
        except Exception as e:
            self.send_json({"error": f"proxy error: {e}"}, status=502)

    def _proxy_with_fallback(self, target_url, fallback_fn):
        """Try live proxy; on any failure serve fallback_fn() as local JSON."""
        try:
            status, body = self._fetch_url(target_url)
            self._send_raw(status, body)
        except Exception as e:
            print(f"[BOT-proxy] upstream failed ({e}), serving local fallback")
            data = fallback_fn()
            self.send_json(data)

    # ── Local Stride-format fallback ──────────────────────────────────────────

    def _stride_local(self, api_path, qs):
        """Return local cached data in Stride API response format."""
        buses = load_json("buses_with_nearest_stops.json") or load_json("bus_positions.json")

        if "gtfs_routes" in api_path:
            line_filters = set(filter(None, (qs.get("line_refs") or [""])[0].split(",")))
            op_filters   = set(filter(None, (qs.get("operator_refs") or [""])[0].split(",")))
            seen, routes = set(), []
            for b in buses:
                lr = str(b.get("line_ref") or b.get("route_short_name") or "")
                op = str(b.get("operator_id") or "")
                if not lr: continue
                if line_filters and lr not in line_filters: continue
                if op_filters   and op not in op_filters:   continue
                key = (lr, op)
                if key in seen: continue
                seen.add(key)
                routes.append({
                    "line_ref":         lr,
                    "route_short_name": lr,
                    "operator_ref":     op,
                    "route_long_name":  b.get("operator_name", ""),
                })
            routes.sort(key=lambda r: r["line_ref"])
            return routes

        if "siri_vehicle_locations" in api_path:
            lr_filter = (qs.get("line_ref") or [""])[0]
            op_filter = (qs.get("operator_ref") or [""])[0]
            result = []
            for b in buses:
                lr = str(b.get("line_ref") or "")
                op = str(b.get("operator_id") or "")
                if lr_filter and lr != lr_filter: continue
                if op_filter and op != op_filter: continue
                if not _in_gush_dan(b.get("lat"), b.get("lon")): continue
                result.append({
                    "siri_route__line_ref":    lr,
                    "siri_route__operator_ref": op,
                    "lat":                     b.get("lat"),
                    "lon":                     b.get("lon"),
                    "velocity":                b.get("velocity"),
                    "bearing":                 b.get("bearing"),
                    "recorded_at_time":        b.get("timestamp"),
                    "direction_ref":           "0",
                    "siri_ride__id":           b.get("trip_id"),
                    "siri_ride__vehicle_ref":  b.get("vehicle_id"),
                    "operator_name":           b.get("operator_name"),
                })
            return result[:100]

        return []

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
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
            self._proxy_with_fallback(target, lambda: self._stride_local(api_path, qs))

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
            self._proxy_with_fallback(target, lambda: self._stride_local(api_path, qs))

        # ── Israel Railways → Stride (israelrail.azurewebsites.net is dead) ──
        elif path.startswith("/proxy/rail"):
            stride_params = (
                f"siri_route__operator_ref=2&limit=100&order_by=id+desc"
                f"&lat__gte={GUSH_DAN['lat_min']}&lat__lte={GUSH_DAN['lat_max']}"
                f"&lon__gte={GUSH_DAN['lon_min']}&lon__lte={GUSH_DAN['lon_max']}"
            )
            target = f"{HASADNA_URL.rstrip('/')}/siri_vehicle_locations/list?{stride_params}"
            rail_api_path = "/siri_vehicle_locations/list"
            self._proxy_with_fallback(target, lambda: self._stride_local(rail_api_path, qs))

        else:
            self.send_json({"error": "Not found", "path": self.path}, status=404)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    threading.Thread(
        target=_refresh_buses_loop, args=(120,),
        daemon=True, name="bus-refresh"
    ).start()
    print("🔄 Background bus refresh started (every 2 min)")

    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer(("0.0.0.0", BOT_PORT), BotHandler)
    print(f"🤖 Transit Bot API → http://0.0.0.0:{BOT_PORT}")
    print(f"   /                         → Transit Query Tool (agent_transit.html)")
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