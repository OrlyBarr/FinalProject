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
# Train data served via Open Bus Stride with operator_ref=2
RAIL_BOARD_URL = ""   # dead — see /proxy/rail handler below
HASADNA_URL    = "https://open-bus-stride-api.hasadna.org.il"
NOMINATIM_URL  = "https://nominatim.openstreetmap.org/search"
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))


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
                "service": "Israel Public Transit Monitoring Platform",
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

        # ── buses ─────────────────────────────────────────────────────────────
        elif path == "/buses":
            data = load_json("buses_with_nearest_stops.json") or load_json("bus_positions.json")
            self.send_json(data[:100])

        # ── stops ─────────────────────────────────────────────────────────────
        elif path == "/stops":
            data = load_json("buses_with_nearest_stops.json")
            self.send_json(data[:100])

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

        # ── Stride proxy: /proxy/stride/<api_path>?<query> ────────────────────
        # מטפל בכל ה-Stride API calls מה-frontend:
        #   /proxy/stride/siri_vehicle_locations/list?...
        #   /proxy/stride/siri_ride_stops/list?...
        #   /proxy/stride/gtfs_routes/list?...
        elif path.startswith("/proxy/stride"):
            api_path = path[len("/proxy/stride"):]   # e.g. /siri_vehicle_locations/list
            if not api_path.startswith("/"):
                api_path = "/" + api_path
            target = HASADNA_URL + api_path
            if parsed.query:
                target += "?" + parsed.query
            self._proxy(target)

        # ── Hasadna alias (backwards compat) ──────────────────────────────────
        elif path.startswith("/proxy/hasadna"):
            api_path = path[len("/proxy/hasadna"):]
            if not api_path.startswith("/"):
                api_path = "/" + api_path
            target = HASADNA_URL + api_path
            if parsed.query:
                target += "?" + parsed.query
            self._proxy(target)

        # ── Israel Railways proxy → Stride (israelrail.azurewebsites.net is dead) ──
        # מפנה ל-Open Bus Stride עם operator_ref=2 (רכבת ישראל)
        elif path.startswith("/proxy/rail"):
            station_id = urllib.parse.parse_qs(parsed.query).get("stationId", [""])[0]
            # בנה query ל-Stride
            stride_params = "operator_ref=2&limit=100&order_by=recorded_at_time+desc"
            target = f"{HASADNA_URL}/siri_vehicle_locations/list?{stride_params}"
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