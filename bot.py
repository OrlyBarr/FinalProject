"""
bot.py
Simple REST API bot for Israel Public Transit Monitoring Platform.
Exposes real-time transit data over HTTP on port 5000.

Endpoints:
  GET /            - health check
  GET /status      - system status
  GET /buses       - latest bus positions from bus_positions.json
  GET /stops       - bus stops with nearest stop info
  GET /proxy/rail  - server-side proxy for Israel Railways API (bypasses CORS)
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import time
import subprocess
import urllib.parse
import urllib.request

BOT_PORT = int(os.getenv("BOT_PORT", 5000))

# Israel Railways station board base URL (proxied server-side to bypass CORS)
RAIL_BOARD_URL = "https://israelrail.azurewebsites.net/stations/GetStationBoard"

# ── Background auto-refresh ──────────────────────────────────────────────────

def _refresh_buses_loop(interval_seconds: int = 120) -> None:
    """
    Background thread: re-run extractdata.py every `interval_seconds`
    so /buses and /stops always serve fresh data.
    """
    project_dir = os.path.dirname(os.path.abspath(__file__))
    script      = os.path.join(project_dir, "extractdata.py")
    python_bin  = os.path.join(project_dir, "venv", "bin", "python3")
    if not os.path.exists(python_bin):
        python_bin = "python3"

    while True:
        time.sleep(interval_seconds)
        try:
            result = subprocess.run(
                [python_bin, script],
                cwd=project_dir,
                timeout=60,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                print("[BOT-refresh] buses_with_nearest_stops.json refreshed")
            else:
                print(f"[BOT-refresh] extractdata.py exited {result.returncode}: {result.stderr[:200]}")
        except subprocess.TimeoutExpired:
            print("[BOT-refresh] extractdata.py timed out (>60s) — skipping")
        except Exception as e:
            print(f"[BOT-refresh] Unexpected error: {e}")


def load_json(filename):
    path = os.path.join(os.path.dirname(__file__), filename)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


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
            self.send_json({"error": "HTML file not found"}, status=404)

    def do_GET(self):
        if self.path == "/health":
            self.send_json({"status": "ok", "service": "Israel Transit Bot", "port": BOT_PORT})

        elif self.path == "/" or self.path == "/agent":
            # Serve the transit agent dashboard
            html_path = os.path.join(os.path.dirname(__file__), "agent_transit.html")
            self.send_html(html_path)

        elif self.path == "/status":
            self.send_json({
                "status": "running",
                "service": "Israel Public Transit Monitoring Platform",
                "endpoints": {
                    "Agent Dashboard": f"http://localhost:{BOT_PORT}/agent",
                    "Airflow UI":      "http://localhost:8081",
                    "Kafka UI":        "http://localhost:8080",
                    "MinIO Console":   "http://localhost:9001",
                    "Kibana":          "http://localhost:5601",
                    "Bot API":         f"http://localhost:{BOT_PORT}",
                }
            })

        elif self.path == "/buses":
            # Return array directly (expected by agent_transit.html)
            data = load_json("buses_with_nearest_stops.json") or load_json("bus_positions.json")
            self.send_json(data[:100])

        elif self.path == "/stops":
            data = load_json("buses_with_nearest_stops.json")
            self.send_json(data[:100])

        elif self.path.startswith("/proxy/rail"):
            # Server-side proxy for Israeli Railways API — avoids browser CORS
            parsed  = urllib.parse.urlparse(self.path)
            params  = parsed.query  # forward query string as-is
            target  = f"{RAIL_BOARD_URL}?{params}" if params else RAIL_BOARD_URL
            try:
                req = urllib.request.Request(
                    target,
                    headers={"Accept": "application/json", "User-Agent": "TransitBot/1.0"},
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    body   = resp.read()
                    status = resp.status
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", len(body))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)
            except urllib.error.HTTPError as e:
                # Forward the upstream HTTP error transparently
                body = e.read() or json.dumps({"error": str(e)}).encode()
                self.send_response(e.code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", len(body))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                self.send_json({"error": f"proxy error: {e}"}, status=502)

        else:
            self.send_json({"error": "Not found", "path": self.path}, status=404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.end_headers()


def main():
    # Start background bus data refresh every 2 minutes
    refresh_thread = threading.Thread(
        target=_refresh_buses_loop, args=(120,), daemon=True, name="bus-refresh"
    )
    refresh_thread.start()
    print("🔄 Background bus refresh started (every 2 min)")

    server = ThreadingHTTPServer(("0.0.0.0", BOT_PORT), BotHandler)
    print(f"🤖 Transit Bot API running on http://0.0.0.0:{BOT_PORT}")
    print(f"   GET /agent      → Agent Transit Dashboard (HTML)")
    print(f"   GET /health     → health check (JSON)")
    print(f"   GET /status     → system status & service URLs")
    print(f"   GET /buses      → latest bus positions (array)")
    print(f"   GET /stops      → buses with nearest stops (array)")
    print(f"   GET /proxy/rail → Israel Railways API proxy (bypasses CORS)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
