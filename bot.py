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
import logging
import os
import sys
import threading
import time
import subprocess
import urllib.parse
import urllib.request
import urllib.error

log = logging.getLogger(__name__)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")
# תיקון encoding ל-Windows/ASCII environments
import locale
try:
    locale.setlocale(locale.LC_ALL, '')
except Exception:
    pass

BOT_PORT       = int(os.getenv("BOT_PORT", 5000))
# israelrail.azurewebsites.net is hijacked — disabled
RAIL_BOARD_URL = ""
HASADNA_URL    = "https://open-bus-stride-api.hasadna.org.il"
NOMINATIM_URL  = "https://nominatim.openstreetmap.org/search"
GOOGLE_PLACES_URL = "https://maps.googleapis.com/maps/api/place"
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))

# טעינת Google Maps API Key מ-.env
from dotenv import load_dotenv
load_dotenv(os.path.join(BASE_DIR, ".env"))
GOOGLE_MAPS_API_KEY  = os.getenv("GOOGLE_MAPS_API_KEY", "")
ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY       = os.getenv("GEMINI_API_KEY", "")

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

# ── GTFS LineRef → Route Short Name cache ────────────────────────────────────
# SIRI מחזיר line_ref (מספר פנימי כמו 16542). מטפסים לשם קו קריא ("63") דרך
# שני מסלולים: (1) PostgreSQL GTFS בעת האתחול, (2) לימוד מ-Stride בזמן ריצה.

_LINECACHE: dict[str, str] = {}
_LINECACHE_LOCK = threading.Lock()

_LONGCACHE: dict[str, str] = {}      # line_ref → route_long_name (cleaned, Hebrew)
_STOPCACHE: dict[str, str] = {}      # stop_code (str) → "name, city"
_STOPCACHE_LOCK = threading.Lock()


def _clean_train_name(raw: str) -> str:
    """'נהריה-נהריה<->תל אביב מרכז-תל אביב יפו' → 'נהריה ↔ תל אביב מרכז'"""
    if not raw:
        return raw
    raw = raw.strip()
    if "<->" in raw:
        parts = raw.split("<->")
        def _st(p):
            idx = p.rfind("-")
            return p[:idx].strip() if idx > 0 else p.strip()
        a, b = _st(parts[0]), _st(parts[1])
        return f"{a} ↔ {b}"
    # single station: "תל אביב מרכז-תל אביב יפו" → "תל אביב מרכז"
    idx = raw.rfind("-")
    return raw[:idx].strip() if idx > 0 else raw


def _resolve_long(line_ref: str) -> str:
    """מחזיר שם מסלול ארוך לפי line_ref (לרכבות בעיקר)."""
    return _LONGCACHE.get(str(line_ref), "")


def _resolve_stop_name(code: str, city_hint: str = "") -> str:
    """מחפש שם תחנה לפי קוד מ-cache או Stride gtfs_stops."""
    if not code:
        return city_hint
    key = str(code)
    with _STOPCACHE_LOCK:
        cached = _STOPCACHE.get(key)
    if cached is not None:
        return cached or city_hint
    try:
        import urllib.request as _ur2
        url2 = f"{HASADNA_URL}/gtfs_stops/list?code={key}&limit=1&order_by=id+desc"
        with _ur2.urlopen(url2, timeout=5) as _r2:
            _d2 = json.loads(_r2.read())
        if _d2:
            nm = (_d2[0].get("name") or "").strip()
            ct = (_d2[0].get("city") or city_hint or "").strip()
            result = (f"{nm}, {ct}" if nm and ct and nm.lower() != ct.lower()
                      else nm or ct or "")
        else:
            result = city_hint or ""
        with _STOPCACHE_LOCK:
            _STOPCACHE[key] = result
        return result or city_hint
    except Exception:
        with _STOPCACHE_LOCK:
            _STOPCACHE[key] = city_hint or ""
        return city_hint or ""


def _build_line_cache() -> None:
    """
    טוען route_id → route_short_name cache בשני שלבים:
    1. Stride API — מיידי, ללא DB (עובד תמיד)
    2. GTFS PostgreSQL — מקיף יותר (אם psycopg2 מותקן)
    """
    import sys as _sys
    if BASE_DIR not in _sys.path:
        _sys.path.insert(0, BASE_DIR)

    cache_path = os.path.join(BASE_DIR, "line_ref_cache.json")

    # ── שלב 0: טעינה מ-JSON cache קיים (מהיר, offline) ──────────────────────
    try:
        if os.path.exists(cache_path):
            with open(cache_path, encoding="utf-8") as _f:
                loaded = json.load(_f)
            with _LINECACHE_LOCK:
                _LINECACHE.update(loaded)
            print(f"[LINECACHE] pre-loaded {len(loaded)} routes from JSON cache")
    except Exception:
        pass

    # ── שלב 1: Stride gtfs_routes — מיפוי מלא לתאריך היום ──────────────────
    # /gtfs_routes/list?date=TODAY&limit=5000 = ~4700 קווים ייחודיים
    try:
        import urllib.request as _ur
        from datetime import date as _date
        today = _date.today().isoformat()
        gtfs_url = (
            f"{HASADNA_URL}/gtfs_routes/list"
            f"?date={today}&limit=5000&order_by=id+desc"
        )
        with _ur.urlopen(gtfs_url, timeout=30) as _resp:
            routes = json.loads(_resp.read())
        count_stride = 0
        for _r in routes:
            lr  = str(_r.get("line_ref") or "").strip()
            rsn = str(_r.get("route_short_name") or "").strip()
            if lr and rsn and rsn not in ("NaN", "nan", ""):
                _learn_line(lr, rsn)
                count_stride += 1
        print(f"[LINECACHE] {count_stride} routes loaded from Stride gtfs_routes ({today})")

        # Store long names for all routes (trains have NaN short_name but valid long_name)
        for _r in routes:
            lr2  = str(_r.get("line_ref") or "").strip()
            rln2 = str(_r.get("route_long_name") or "").strip()
            if lr2 and rln2 and rln2 not in ("NaN","nan","None",""):
                _LONGCACHE[lr2] = _clean_train_name(rln2)

        # Fetch train routes specifically (operator=2) for long names
        try:
            train_url = (f"{HASADNA_URL}/gtfs_routes/list"
                         f"?operator_refs=2&limit=300&order_by=id+desc")
            with _ur.urlopen(train_url, timeout=20) as _rt:
                train_routes = json.loads(_rt.read())
            for _r in train_routes:
                lr2  = str(_r.get("line_ref") or "").strip()
                rln2 = str(_r.get("route_long_name") or "").strip()
                rsn2 = str(_r.get("route_short_name") or "").strip()
                if lr2 and rln2 and rln2 not in ("NaN","nan","None",""):
                    _LONGCACHE[lr2] = _clean_train_name(rln2)
                if lr2 and rsn2 and rsn2 not in ("NaN","nan","None",""):
                    _learn_line(lr2, rsn2)
            print(f"[LONGCACHE] loaded {len(_LONGCACHE)} long route names")
        except Exception as _et:
            print(f"[LONGCACHE] failed: {_et}")

        # Pre-populate stop name cache
        try:
            stops_url = f"{HASADNA_URL}/gtfs_stops/list?limit=2000&order_by=id+desc"
            with _ur.urlopen(stops_url, timeout=30) as _rs:
                stops_bulk = json.loads(_rs.read())
            with _STOPCACHE_LOCK:
                for _s in stops_bulk:
                    _scode = str(_s.get("code") or "").strip()
                    _snm   = (_s.get("name") or "").strip()
                    _sct   = (_s.get("city") or "").strip()
                    if _scode and _snm:
                        _STOPCACHE[_scode] = (
                            f"{_snm}, {_sct}" if _sct and _snm.lower() != _sct.lower()
                            else _snm
                        )
            print(f"[STOPCACHE] pre-loaded {len(stops_bulk)} stop names")
        except Exception as _es:
            print(f"[STOPCACHE] failed: {_es}")

    except Exception as _e:
        print(f"[LINECACHE] Stride gtfs_routes failed: {_e}")

    # ── שלב 2: GTFS PostgreSQL — מיפוי מקיף (optional) ──────────────────────
    try:
        from gtfs_query import get_line_ref_map
        m = get_line_ref_map()
        if m:
            with _LINECACHE_LOCK:
                _LINECACHE.update(m)
            print(f"[LINECACHE] enriched with {len(m)} routes from GTFS DB")
    except Exception as e:
        print(f"[LINECACHE] GTFS DB skip ({e})")

    # ── שמירת cache ל-JSON ────────────────────────────────────────────────────
    try:
        with _LINECACHE_LOCK:
            snapshot = dict(_LINECACHE)
        with open(cache_path, "w", encoding="utf-8") as _f:
            json.dump(snapshot, _f, ensure_ascii=False)
        print(f"[LINECACHE] saved {len(snapshot)} entries to {cache_path}")
    except Exception as e:
        print(f"[LINECACHE] save failed: {e}")


def _resolve_line(line_ref: str) -> str:
    """
    מחזיר שם קו קריא לפי line_ref פנימי.
    דוגמה: '16542' → '63'   |   '5' → '5' (אם אין מיפוי)
    """
    if not line_ref:
        return ""
    s = str(line_ref)
    with _LINECACHE_LOCK:
        name = _LINECACHE.get(s, "")
    return name if name else s


def _learn_line(line_ref: str, route_short_name: str) -> None:
    """
    שומר מיפוי שנלמד מ-Stride API בזמן ריצה.
    מונע קריאות DB חוזרות לאותו קו.
    """
    if line_ref and route_short_name and str(route_short_name) != str(line_ref):
        with _LINECACHE_LOCK:
            _LINECACHE[str(line_ref)] = str(route_short_name)


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

    def log_message(self, fmt, *args):
        """Override to prevent ASCII encoding errors with Hebrew URLs."""
        try:
            msg = (fmt % args).encode("utf-8", errors="replace").decode("utf-8")
            sys.stderr.buffer.write(
                f"[{self.log_date_time_string()}] {msg}\n".encode("utf-8")
            )
        except Exception:
            pass

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
            with urllib.request.urlopen(req, timeout=30) as resp:
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
        except TimeoutError:
            self.send_json({"error": "timeout — השרת החיצוני לא הגיב בזמן, נסי שוב"}, status=504)
        except Exception as e:
            self.send_json({"error": f"proxy error: {e}"}, status=502)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(body)
        except Exception:
            payload = {}

        # ── /smart_ask — ענייה ישירה מ-Stride/GTFS ללא AI ─────────────────────
        if path == "/smart_ask":
          try:
            import re, requests as _req
            msg = payload.get("message", "").strip()
            ml  = msg.lower()

            STRIDE = "https://open-bus-stride-api.hasadna.org.il"
            OPERATOR_IDS = {
                "דן":"3","אגד תעבורה":"15","אגד":"5","nta":"6","מטרופולין":"6",
                "סופרבוס":"18","קווים":"21","נתיב אקספרס":"25","נתיב":"25",
                "תנופה":"16","v-line":"32","אפיקים":"42",
            }
            TRAIN_STATIONS = {
                "מרכז":("תל אביב מרכז","32.0893","34.7773"),
                "ארלוזורוב":("תל אביב מרכז","32.0893","34.7773"),
                "השלום":("תל אביב השלום","32.0648","34.7751"),
                "הגנה":("תל אביב הגנה","32.0530","34.7637"),
                "אוניברסיטה":("תל אביב אוניברסיטה","32.1134","34.8043"),
                "בני ברק":("בני ברק","32.0896","34.8299"),
                "פתח תקווה":("פתח תקווה סגולה","32.0919","34.8836"),
                "הרצליה":("הרצליה","32.1650","34.8430"),
            }

            reply = None; source = None; route_data = None

            # ── תחנות קו X (גם לרכבות, גם לאוטובוסים) ─────────────────────
            m_stops = re.search(r'(?:תחנות|עצירות)\s*(?:קו\s*)?(\d+)', ml)
            if m_stops and not reply:
                line = m_stops.group(1)
                # נסה GTFS
                try:
                    sd = _req.get(f"http://localhost:{BOT_PORT}/gtfs/route_stops",
                                  params={"short_name": line}, timeout=5).json()
                    stops = sd.get("stops", [])
                    if stops:
                        rd = _req.get(f"http://localhost:{BOT_PORT}/gtfs/routes",
                                      params={"q": line, "limit": 1}, timeout=3).json()
                        route = (rd.get("routes") or [{}])[0]
                        rname = route.get("route_long_name", "")
                        reply = (
                            f"**תחנות קו {line}**"
                            + (f" — {rname}" if rname else "") + "\n"
                            f"סה\"כ {len(stops)} תחנות:\n\n"
                            + "\n".join(f"{i+1}. {s['stop_name']}"
                                        for i, s in enumerate(stops[:30]))
                            + (f"\n... ועוד {len(stops)-30} תחנות" if len(stops) > 30 else "")
                        )
                        source = "gtfs"
                except Exception:
                    pass
                # Stride fallback לרכבות (siri_ride_stops)
                if not reply:
                    try:
                        veh = _req.get(f"{STRIDE}/siri_vehicle_locations/list",
                            params={"siri_routes__line_ref": line,
                                    "siri_routes__operator_ref": "2",
                                    "limit": 1, "order_by": "id desc"}, timeout=8).json()
                        if veh:
                            ride_id = veh[0].get("siri_ride__id") or veh[0].get("ride_id")
                            if ride_id:
                                rs = _req.get(f"{STRIDE}/siri_ride_stops/list",
                                    params={"siri_ride__id": ride_id,
                                            "limit": 50, "order_by": "order asc"}, timeout=8).json()
                                if rs:
                                    names = []
                                    for _i, _s in enumerate(rs):
                                        _nm = _s.get("gtfs_stop__name","") or _resolve_stop_name(
                                            str(_s.get("siri_stop__code") or ""),
                                            _s.get("gtfs_stop__city","")
                                        )
                                        names.append(_nm or f"תח.{_s.get('siri_stop__code','?')}")
                                    reply = (
                                        f"**תחנות רכבת קו {line}**\n"
                                        f"סה\"כ {len(names)} תחנות:\n\n"
                                        + "\n".join(f"{i+1}. {n}" for i,n in enumerate(names))
                                    )
                                    source = "stride"
                    except Exception:
                        pass
                if not reply:
                    reply = f"לא נמצאו תחנות לקו {line} — ייתכן שה-GTFS לא טעון"
                    source = "error"

            # ── קו מספר ─────────────────────────────────────────────────────
            lm = re.search(r'(?:קו|line)[^\d]*(\d+)', ml)
            if lm and not reply:
                line = lm.group(1)
                try:
                    # נסה GTFS קודם
                    gdata = _req.get(
                        f"http://localhost:{BOT_PORT}/gtfs/route_stops",
                        params={"short_name": line}, timeout=4
                    ).json()
                    stops = gdata.get("stops", [])
                    rdata = _req.get(
                        f"http://localhost:{BOT_PORT}/gtfs/routes",
                        params={"q": line, "limit": 1}, timeout=4
                    ).json()
                    route = (rdata.get("routes") or [{}])[0]
                    if stops:
                        first = stops[0]["stop_name"]; last = stops[-1]["stop_name"]
                        reply = (
                            f"**קו {line}** — {route.get('route_long_name','')}\n"
                            f"מפעיל: {route.get('agency_name','לא ידוע')}\n"
                            f"מ: {first} → עד: {last}\n"
                            f"סה\"כ {len(stops)} תחנות\n\n"
                            + "\n".join(f"{i+1}. {s['stop_name']}" for i,s in enumerate(stops[:20]))
                            + (f"\n... ועוד {len(stops)-20} תחנות" if len(stops)>20 else "")
                        )
                        source = "gtfs"
                except Exception:
                    pass

                if not reply:
                    # fallback — Stride live
                    try:
                        sr = _req.get(f"{STRIDE}/siri_vehicle_locations/list",
                            params={"siri_routes__line_ref":line,"limit":20,"order_by":"id desc"},
                            timeout=8).json()
                        if sr:
                            ops = list({v.get("siri_route__operator_ref","?") for v in sr})
                            reply = (
                                f"**קו {line}** — כרגע פעיל\n"
                                f"{len(sr)} כלי רכב בשטח\n"
                                f"מפעיל: {', '.join(str(o) for o in ops)}"
                            )
                            source = "stride"
                    except Exception:
                        pass

            # ── מסלול: מ-X ל-Y  (Moovit style route planning) ───────────────
            m_route = re.match(r'^(?:מסלול\s+)?מ[-\s]+(.+?)\s+ל[-\s]*(.+)$', ml.strip())
            if not m_route:
                m_route = re.search(
                    r'(?:תכנן|מסלול|איך\s+מגיעים).*?מ[-\s]+(.+?)\s+ל[-\s]*(.+)',
                    ml.strip()
                )
            if m_route and not reply:
                origin_q = m_route.group(1).strip()
                dest_q   = m_route.group(2).strip()
                if len(origin_q) >= 2 and len(dest_q) >= 2:
                    try:
                        from concurrent.futures import (ThreadPoolExecutor as _TPEX,
                                                        as_completed as _asc)

                        def _geo_req(addr):
                            res = _req.get(
                                "https://nominatim.openstreetmap.org/search",
                                params={"q": addr + ", ישראל",
                                        "format": "json", "limit": 1},
                                headers={"User-Agent": "IsraelTransitBot/1.0"},
                                timeout=6
                            ).json()
                            if res:
                                return (float(res[0]["lat"]), float(res[0]["lon"]),
                                        res[0].get("display_name","").split(",")[0])
                            return None, None, addr

                        # ── Step 1: Parallel geocoding (saves ~5s) ────────────
                        with _TPEX(max_workers=2) as _gp:
                            _fo = _gp.submit(_geo_req, origin_q)
                            _fd = _gp.submit(_geo_req, dest_q)
                            try:
                                lat1, lon1, olabel = _fo.result(timeout=8)
                            except Exception:
                                lat1, lon1, olabel = None, None, origin_q
                            try:
                                lat2, lon2, dlabel = _fd.result(timeout=8)
                            except Exception:
                                lat2, lon2, dlabel = None, None, dest_q

                        if lat1 and lat2:
                            lat_min = min(lat1,lat2)-0.025; lat_max = max(lat1,lat2)+0.025
                            lon_min = min(lon1,lon2)-0.025; lon_max = max(lon1,lon2)+0.025

                            # ── Step 2: Stride bbox call ───────────────────────
                            sv = _req.get(f"{STRIDE}/siri_vehicle_locations/list", params={
                                "lat__greater_or_equal": lat_min,
                                "lat__lower_or_equal":   lat_max,
                                "lon__greater_or_equal": lon_min,
                                "lon__lower_or_equal":   lon_max,
                                "limit": 200, "order_by": "id desc"
                            }, timeout=10).json()

                            _OP = {
                                "2":"רכבת ישראל","3":"דן","5":"אגד","6":"NTA מטרופולין",
                                "14":"מטרופולין","15":"אגד תעבורה","16":"תנופה",
                                "18":"סופרבוס","21":"קווים","25":"נתיב אקספרס",
                                "32":"V-Line","42":"אפיקים"
                            }

                            # ── Step 3: Group by line, keep first ride_id ──────
                            line_data = {}
                            for v in sv:
                                lr = str(v.get("siri_route__line_ref",""))
                                op = str(v.get("siri_route__operator_ref",""))
                                if not (lr and lr.isdigit()):
                                    continue
                                if lr not in line_data:
                                    line_data[lr] = {
                                        "operator": _OP.get(op, f"מפעיל {op}"),
                                        "vehicles": [],
                                        "ride_id":  v.get("siri_ride__id")
                                    }
                                try:
                                    vlat = float(v.get("lat") or
                                                 v.get("calculated_lat") or 0)
                                    vlon = float(v.get("lon") or
                                                 v.get("calculated_lon") or 0)
                                    if vlat and vlon:
                                        line_data[lr]["vehicles"].append({
                                            "lat": vlat, "lon": vlon,
                                            "vel": int(v.get("velocity") or 0)
                                        })
                                except (TypeError, ValueError):
                                    pass

                            sorted_lr = sorted(
                                line_data.items(),
                                key=lambda x: (-len(x[1]["vehicles"]),
                                               int(x[0]) if x[0].isdigit() else 9999)
                            )[:12]

                            # ── Step 4: Get stop names from Stride ride_stops ──
                            # (no GTFS needed — uses live siri_ride_stops data)
                            def _get_ride_stops(lr_rid):
                                lr_k, rid = lr_rid
                                if not rid:
                                    return (lr_k, "", "")
                                try:
                                    rs = _req.get(
                                        f"{STRIDE}/siri_ride_stops/list",
                                        params={"siri_ride__id": rid,
                                                "limit": 60,
                                                "order_by": "order asc"},
                                        timeout=4
                                    ).json()
                                    names = [s.get("gtfs_stop__name","") or ""
                                             for s in rs]
                                    names = [n for n in names if n]
                                    if names:
                                        return (lr_k, names[0], names[-1])
                                except Exception:
                                    pass
                                return (lr_k, "", "")

                            stop_names = {}
                            top_lr_rids = [
                                (lr, ld.get("ride_id"))
                                for lr, ld in sorted_lr[:8]
                            ]
                            try:
                                with _TPEX(max_workers=6) as _pool:
                                    futs = {_pool.submit(_get_ride_stops, x): x[0]
                                            for x in top_lr_rids}
                                    for f in _asc(futs, timeout=5):
                                        try:
                                            k, os2, fs2 = f.result(timeout=0.1)
                                            if os2:
                                                stop_names[k] = (os2, fs2)
                                        except Exception:
                                            pass
                            except Exception:
                                pass

                            routes_list = []
                            for lr, ldata in sorted_lr:
                                o_stop, f_stop = stop_names.get(lr, ("",""))
                                routes_list.append({
                                    "line_ref":    lr,
                                    "operator":    ldata["operator"],
                                    "origin_stop": o_stop,
                                    "final_stop":  f_stop,
                                    "vehicles":    ldata["vehicles"][:8]
                                })

                            if routes_list:
                                lines_txt = []
                                for r in routes_list:
                                    cnt = len(r.get("vehicles",[]))
                                    lname = r.get('line_name') or _resolve_line(str(r.get('line_ref','')))
                                    ldisp = lname if lname and lname != str(r.get('line_ref','')) else r['line_ref']
                                    s = f"🚌 **קו {ldisp}** — {r['operator']}"
                                    if cnt:
                                        s += f" ({cnt} כלי רכב)"
                                    if r["origin_stop"] and r["final_stop"]:
                                        s += (f"\n   📍 {r['origin_stop']}"
                                              f" → {r['final_stop']}")
                                    lines_txt.append(s)
                                reply = (
                                    f"**מסלול מ-{olabel} ל-{dlabel}**\n"
                                    f"מצאתי {len(routes_list)} קווים פעילים:\n\n"
                                    + "\n".join(lines_txt)
                                )
                                route_data = {
                                    "origin":      {"label": olabel,
                                                    "lat": lat1, "lon": lon1},
                                    "destination": {"label": dlabel,
                                                    "lat": lat2, "lon": lon2},
                                    "routes":      routes_list
                                }
                                source = "stride"
                            else:
                                reply = (f"לא נמצאו אוטובוסים פעילים כרגע "
                                         f"בין {olabel} ל-{dlabel}")
                                source = "stride"
                        else:
                            reply = ("לא הצלחתי לאתר את הכתובות — "
                                     "נסי עם כתובות מפורטות יותר")
                            source = "error"
                    except Exception as _re:
                        reply = f"שגיאה בתכנון מסלול: {_re}"
                        source = "error"

            # ── קווי מפעיל ──────────────────────────────────────────────────
            OP_DISPLAY = {
                "3":"דן","5":"אגד","6":"NTA מטרופולין","15":"אגד תעבורה",
                "16":"תנופה","18":"סופרבוס","21":"קווים","25":"נתיב אקספרס",
                "14":"מטרופולין","32":"V-Line","42":"אפיקים",
            }
            if not reply:
                for op_name, op_id in OPERATOR_IDS.items():
                    if op_name in ml and ("קו" in ml or "רשימ" in ml or "מפעיל" in ml or any(w in ml for w in ["של","מה","קווי"])):
                        # נסה GTFS קודם
                        try:
                            gdata = _req.get(
                                f"http://localhost:{BOT_PORT}/gtfs/routes",
                                params={"operator_id": op_id, "limit": 200}, timeout=4
                            ).json()
                            routes = gdata.get("routes", [])
                            if routes:
                                nums = sorted({r["route_short_name"] for r in routes if r.get("route_short_name")},
                                              key=lambda x: int(x) if x.isdigit() else 9999)
                                agency = routes[0].get("agency_name", op_name)
                                reply = (
                                    f"**קווי {agency}** — {len(nums)} קווים\n"
                                    + ", ".join(str(n) for n in nums[:60])
                                    + ("  ועוד..." if len(nums)>60 else "")
                                )
                                source = "gtfs"
                        except Exception:
                            pass

                        # Stride fallback — כלי רכב פעילים כרגע
                        if not reply:
                            try:
                                sr = _req.get(
                                    f"{STRIDE}/siri_vehicle_locations/list",
                                    params={"siri_routes__operator_ref": op_id,
                                            "limit": 200, "order_by": "id desc"},
                                    timeout=9
                                ).json()
                                agency = OP_DISPLAY.get(op_id, op_name)
                                if sr:
                                    # בנה set של שמות קווים קריאים (לא line_ref פנימי)
                                    line_names = sorted(
                                        {_resolve_line(str(v.get("siri_route__line_ref","")))
                                         for v in sr if v.get("siri_route__line_ref")},
                                        key=lambda x: int(x) if x.isdigit() else x
                                    )
                                    reply = (
                                        f"**קווי {agency}** — {len(line_names)} קווים פעילים כרגע\n"
                                        + ", ".join(line_names[:80])
                                        + ("  ועוד..." if len(line_names)>80 else "")
                                    )
                                else:
                                    reply = f"**{agency}** — אין כלי רכב פעילים כרגע"
                                source = "stride"
                            except Exception:
                                agency = OP_DISPLAY.get(op_id, op_name)
                                reply = f"לא ניתן לשלוף קווי {agency} כרגע — נסי שוב"
                                source = "error"
                        break

            # ── רכבות — תחנה ────────────────────────────────────────────────
            if not reply and ("רכבת" in ml or "תחנת" in ml or "רכבות" in ml):
                matched_station = None
                for kw,(name,lat,lon) in TRAIN_STATIONS.items():
                    if kw in ml:
                        matched_station = (name, lat, lon); break

                if not matched_station and ("מרכז" in ml or "ת\"א" in ml or "תא " in ml):
                    matched_station = ("תל אביב מרכז","32.0893","34.7773")

                if matched_station:
                    name, lat, lon = matched_station
                    try:
                        r_val = 0.15
                        sr = _req.get(f"{STRIDE}/siri_vehicle_locations/list",
                            params={
                                "siri_routes__operator_ref":"2",
                                "lat__greater_or_equal": float(lat)-r_val,
                                "lat__lower_or_equal":   float(lat)+r_val,
                                "lon__greater_or_equal": float(lon)-r_val,
                                "lon__lower_or_equal":   float(lon)+r_val,
                                "limit":30,"order_by":"id desc"
                            }, timeout=10).json()
                        if sr:
                            # קבץ לפי line_ref וספור כמה רכבות בכל קו
                            # וגם שלוף שם מסלול ארוך ישירות מנתוני הרכב
                            line_groups = {}
                            route_names = {}  # lr → route_long_name
                            for v in sr:
                                lr = str(v.get("siri_route__line_ref","?"))
                                line_groups[lr] = line_groups.get(lr, 0) + 1
                                # Stride מחזיר route_long_name ישירות בנתוני הרכב
                                long_n = (v.get("siri_route__gtfs_route__route_long_name") or "").strip()
                                if long_n and lr not in route_names:
                                    route_names[lr] = long_n

                            # אם עדיין חסרים שמות — נסה GTFS endpoint (PostgreSQL)
                            missing = [k for k in list(line_groups.keys())[:8] if k not in route_names]
                            if missing:
                                try:
                                    for lr in missing:
                                        gr = _req.get(
                                            f"http://localhost:{BOT_PORT}/gtfs/routes",
                                            params={"q": lr, "operator_id": "2", "limit": 1},
                                            timeout=3
                                        ).json()
                                        routes = gr.get("routes", [])
                                        if routes and routes[0].get("route_long_name"):
                                            route_names[lr] = routes[0]["route_long_name"]
                                except Exception:
                                    pass

                            # בנה רשימה מסודרת
                            lines_text = []
                            for lr, cnt in sorted(line_groups.items(), key=lambda x: -x[1])[:8]:
                                rname = route_names.get(lr, "") or _resolve_long(lr) or _resolve_line(lr)
                                cnt_s = f"{cnt} רכבת" if cnt == 1 else f"{cnt} רכבות"
                                if rname and rname != lr:
                                    lines_text.append(f"🚆 {rname} ({cnt_s})")
                                else:
                                    lines_text.append(f"🚆 קו {lr} ({cnt_s})")

                            reply = (
                                f"**רכבות ישראל — {name}**\n"
                                f"{len(sr)} רכבות פעילות באזור כרגע:\n\n"
                                + "\n".join(lines_text)
                                + "\n\n💡 לתחנות: כתבי **תחנות קו [מספר]**"
                            )
                        else:
                            reply = f"**{name}** — אין רכבות פעילות באזור כרגע"
                        source = "stride"
                    except Exception as e:
                        reply = f"שגיאה בשליפת רכבות: {e}"
                        source = "error"
                else:
                    reply = (
                        "**תחנות רכבת עיקריות בגוש דן:**\n\n"
                        "🚉 תל אביב מרכז (ארלוזורוב)\n"
                        "🚉 תל אביב השלום\n"
                        "🚉 תל אביב הגנה\n"
                        "🚉 תל אביב אוניברסיטה\n"
                        "🚉 בני ברק\n"
                        "🚉 פתח תקווה סגולה\n"
                        "🚉 הרצליה\n\n"
                        "כתבי שם תחנה לרכבות פעילות, או\n"
                        "**תחנות קו [מספר]** לתחנות מסלול"
                    )
                    source = "static"

            # ── אוטובוסים ליד כתובת / קואורדינטות ─────────────────────────
            if not reply and any(w in ml for w in ["ליד","קרוב","בקרבת","כתובת"]):
                # חלץ קואורדינטות אם קיימות
                coords = re.search(r'(\d+\.\d+)[,\s]+(\d+\.\d+)', msg)
                lat = lon = None
                addr_label = ""
                if coords:
                    lat = float(coords.group(1)); lon = float(coords.group(2))
                    addr_label = f"{lat:.4f},{lon:.4f}"
                else:
                    # Nominatim geocoding
                    addr = re.sub(r'אוטובוסים?\s*(ליד|קרוב\s*ל|בקרבת)?\s*', '', msg).strip()
                    addr = addr or msg
                    try:
                        geo = _req.get(
                            "https://nominatim.openstreetmap.org/search",
                            params={"q": addr+", ישראל","format":"json","limit":1},
                            headers={"User-Agent":"TransitBot/1.0"},
                            timeout=6
                        ).json()
                        if geo:
                            lat = float(geo[0]["lat"]); lon = float(geo[0]["lon"])
                            addr_label = geo[0].get("display_name","").split(",")[0]
                    except Exception:
                        pass

                if lat:
                    try:
                        near = _req.get(
                            f"http://localhost:{BOT_PORT}/gtfs/nearby",
                            params={"lat":lat,"lon":lon,"radius":500}, timeout=5
                        ).json()
                        stops = near.get("stops",[])
                        if stops:
                            lines_set = {}
                            for s in stops[:6]:
                                for r in (s.get("routes") or []):
                                    n = r.get("route_short_name","")
                                    if n: lines_set[n] = f"קו {n} — {s['stop_name']} ({s.get('distance_m',0):.0f}מ')"
                            reply = (
                                f"**אוטובוסים ליד {addr_label}**\n"
                                f"{len(stops)} תחנות ב-500 מטר:\n\n"
                                + "\n".join(list(lines_set.values())[:15])
                            )
                        else:
                            reply = f"לא נמצאו תחנות ב-500 מטר מ-{addr_label}"
                        source = "gtfs"
                    except Exception as e:
                        reply = f"שגיאה: {e}"

            # ── רשימת מפעילים ───────────────────────────────────────────────
            if not reply and any(w in ml for w in ["מפעיל","חברות","חברה","רשימ"]):
                reply = (
                    "**מפעילי תחבורה ציבורית — גוש דן**\n\n"
                    "🚌 **אוטובוסים:**\n"
                    "• דן (3) — תל אביב וסביבה\n"
                    "• אגד (5) — בין-עירוני ותל אביב\n"
                    "• NTA מטרופולין (6) — קו הסגול/אדום\n"
                    "• קווים (21) — מרכז הארץ\n"
                    "• סופרבוס (18) — שרון ומרכז\n"
                    "• נתיב אקספרס (25) — בין-עירוני\n"
                    "• מטרופולין (14) — מרכז וצפון\n"
                    "• אגד תעבורה (15) — פרברים\n"
                    "• תנופה (16) — שרון\n\n"
                    "🚆 **רכבת:**\n"
                    "• רכבת ישראל (2)\n\n"
                    "ציני שם מפעיל לרשימת קוויו"
                )
                source = "static"

            # ── לא זוהה — שלח None כדי ש-frontend ייפול ל-Gemini ─────────────
            if reply:
                resp = {"reply": reply, "source": source}
                if route_data:
                    resp["route_data"] = route_data
                self.send_json(resp)
            else:
                self.send_json({"reply": None, "source": None})

          except Exception as e:
            log.error(f"/smart_ask error: {e}", exc_info=True)
            self.send_json({"reply": None, "source": None})
          return

        # ── /ask — Gemini (primary) → Ollama (fallback) ─────────────────────
        if path == "/ask":
          try:
            import requests as _req
            import socket as _sock
            from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FTout

            messages  = payload.get("messages", [])
            tool_data = payload.get("tool_data", "")

            SYSTEM = (
                "אתה סוכן תחבורה ציבורית חכם לגוש דן ותל אביב, ישראל. "
                "ענה תמיד בעברית בתשובות קצרות וברורות. "
                "שאל שאלות המשך כדי לקבל מידע חסר. "
                "כשמישהו שואל על אוטובוסים ליד כתובת — שאל מה הכתובת. "
                "כשמישהו שואל על קו — שאל מאיפה לאן. "
                "כשמישהו שואל על רכבת — שאל מאיזו תחנה. "
                "אל תמציא מספרי קווים או שעות שאינך בטוח בהם. "
                + (f"\nנתונים: {tool_data}" if tool_data else "")
            )

            # בניית contents תקינה לGemini (חייב להתחיל ב-user, ללא כפילויות)
            def _contents(msgs):
                out = []
                for m in msgs[-6:]:
                    role = "model" if m.get("role") == "assistant" else "user"
                    txt  = (m.get("content") or "").strip()
                    if not txt:
                        continue
                    if out and out[-1]["role"] == role:
                        out[-1]["parts"][0]["text"] += "\n" + txt
                    else:
                        out.append({"role": role, "parts": [{"text": txt}]})
                while out and out[0]["role"] != "user":
                    out.pop(0)
                return out or [{"role": "user", "parts": [{"text": "שלום"}]}]

            # Gemini — מנסה מודלים בסדר עדיפות (free tier)
            # gemini-1.5-flash: 1500 req/day, 15 RPM — הכי אמין בחינמי
            # gemini-2.0-flash-lite: מהיר יותר אבל מכסה קטנה יותר
            # gemini-1.5-flash-8b: הקטן ביותר, גיבוי אחרון
            GEMINI_MODELS = [
                "gemini-1.5-flash",
                "gemini-2.0-flash-lite",
                "gemini-1.5-flash-8b",
            ]

            def _gemini_model(model):
                r = _req.post(
                    f"https://generativelanguage.googleapis.com"
                    f"/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}",
                    json={
                        "system_instruction": {"parts": [{"text": SYSTEM}]},
                        "contents": _contents(messages),
                        "generationConfig": {"maxOutputTokens": 400, "temperature": 0.7},
                    },
                    timeout=(5, 12),
                )
                if r.status_code == 429:
                    raise RuntimeError(f"429_QUOTA")   # quota — try next model
                if r.status_code != 200:
                    raise RuntimeError(r.json().get("error", {}).get("message", r.text[:150]))
                cands = r.json().get("candidates", [])
                if not cands:
                    raise RuntimeError("Gemini: תשובה ריקה")
                parts = cands[0].get("content", {}).get("parts", [])
                return "".join(p.get("text", "") for p in parts).strip() or "לא התקבלה תשובה"

            def _gemini():
                last_err = ""
                for model in GEMINI_MODELS:
                    try:
                        result = _gemini_model(model)
                        log.info(f"/ask answered by {model}")
                        return result
                    except RuntimeError as e:
                        last_err = str(e)
                        if "429_QUOTA" in last_err:
                            log.warning(f"/ask {model} quota exceeded — trying next model")
                            continue
                        raise   # שגיאה אחרת — הפסק מיד
                raise RuntimeError(f"כל מודלי Gemini חרגו ממכסה: {last_err}")

            # Ollama — fallback, only if port 11434 is open
            def _ollama():
                try:
                    _sock.create_connection(("127.0.0.1", 11434), timeout=1).close()
                except OSError:
                    raise RuntimeError("Ollama לא רץ")
                r = _req.post(
                    "http://127.0.0.1:11434/api/chat",
                    json={
                        "model": "tinyllama",
                        "messages": [{"role": "system", "content": SYSTEM}]
                                  + [{"role": m.get("role","user"), "content": m.get("content","")}
                                     for m in messages[-4:]],
                        "stream": False,
                        "options": {"num_predict": 200, "temperature": 0.7, "num_ctx": 512},
                    },
                    timeout=(3, 25),
                )
                return r.json().get("message", {}).get("content", "לא התקבלה תשובה")

            reply = None; source = None; gemini_err = None

            # ── Gemini עם hard timeout של 14 שניות ────────────────────────────
            if GEMINI_API_KEY:
                with ThreadPoolExecutor(max_workers=1) as _pool:
                    _fut = _pool.submit(_gemini)
                    try:
                        reply  = _fut.result(timeout=14)
                        source = "gemini"
                    except _FTout:
                        gemini_err = "Gemini timeout (14s)"
                        log.warning("/ask Gemini hard-timeout")
                    except Exception as e:
                        gemini_err = str(e)
                        log.warning(f"/ask Gemini error: {gemini_err}")
            else:
                gemini_err = "GEMINI_API_KEY חסר ב-.env"

            # ── Ollama fallback ────────────────────────────────────────────────
            if reply is None:
                try:
                    reply  = _ollama()
                    source = "ollama"
                except Exception as e:
                    self.send_json({"error": gemini_err or str(e)}, status=503)
                    return

            self.send_json({"reply": reply, "source": source})

          except Exception as e:
            log.error(f"/ask error: {e}", exc_info=True)
            self.send_json({"error": str(e)}, status=500)



        else:
            self.send_json({"error": "Unknown POST endpoint"}, status=404)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        qs     = urllib.parse.parse_qs(parsed.query)

        # ── Gush Dan Transit App ─────────────────────────────────────────────
        if path == "/gushdan":
            self.send_html(os.path.join(BASE_DIR, "gushdan_app.html"))

        elif path == "/moovit":
            self.send_html(os.path.join(BASE_DIR, "moovit.html"))

        # ── Dankal Red Line — live vehicles + alert status ────────────────────
        elif path == "/api/dankal":
            try:
                import requests as _rq
                # Fetch NTA/Dankal vehicles (operator_ref=6) along Red Line corridor
                # Red Line runs roughly: lat 32.02-32.12, lon 34.74-34.89
                vehs_raw = _rq.get(
                    f"{HASADNA_URL}/siri_vehicle_locations/list",
                    params={
                        "siri_routes__operator_ref": "6",
                        "lat__greater_or_equal": 32.02,
                        "lat__lower_or_equal":   32.13,
                        "lon__greater_or_equal": 34.74,
                        "lon__lower_or_equal":   34.90,
                        "limit": 40, "order_by": "id desc",
                    },
                    timeout=10,
                ).json()
                vehicles = []
                if isinstance(vehs_raw, list):
                    for v in vehs_raw:
                        try:
                            vlat = float(v.get("lat") or v.get("calculated_lat") or 0)
                            vlon = float(v.get("lon") or v.get("calculated_lon") or 0)
                            if vlat and vlon:
                                lr  = str(v.get("siri_route__line_ref") or "")
                                rsn = str(v.get("siri_route__gtfs_route__route_short_name") or "").strip()
                                if rsn:
                                    _learn_line(lr, rsn)
                                line_name = rsn or _resolve_line(lr)
                                vehicles.append({
                                    "lat":       vlat,
                                    "lon":       vlon,
                                    "line":      lr,
                                    "line_name": line_name,
                                    "speed":     int(v.get("velocity") or 0),
                                })
                        except Exception:
                            pass

                # Check service alerts from MOT GTFS-RT
                alert = ""
                try:
                    import urllib.request as _ureq
                    head_resp = _ureq.urlopen(
                        "https://gtfs.mot.gov.il/gtfsfiles/ServiceAlerts.pb",
                        timeout=5,
                    )
                    # If reachable, line is operating
                    head_resp.close()
                    alert = "הקו האדום פועל כסדרו" if not vehicles else ""
                except Exception:
                    alert = ""

                self.send_json({
                    "vehicles": vehicles,
                    "alert":    alert,
                    "count":    len(vehicles),
                    "status":   "ok",
                })
            except Exception as e:
                self.send_json({
                    "vehicles": [], "alert": "",
                    "count": 0, "status": "error", "error": str(e),
                })

        # ── ML delay prediction (lazy-loaded) ─────────────────────────────────
        # GET /api/predict_delay?line=171&operator_ref=3[&hour=8&day_of_week=1&jam_factor=3.5]
        # Returns the model's expected delay (minutes) for the given line/time.
        elif path == "/api/predict_delay":
            try:
                line = (qs.get("line") or [""])[0].strip()
                operator_ref = (qs.get("operator_ref") or [""])[0].strip()
                operator_name = (qs.get("operator") or [""])[0].strip()
                hour_param = (qs.get("hour") or [""])[0].strip()
                dow_param = (qs.get("day_of_week") or [""])[0].strip()
                jam_param = (qs.get("jam_factor") or [""])[0].strip()

                # Map operator_ref -> operator_name used at training time
                _OP_NAME_MAP = {
                    "2": "Israel Railways", "3": "Dan", "5": "Egged",
                    "6": "NTA", "14": "Metropoline", "15": "Egged Taavura",
                    "16": "Tnufa", "18": "Superbus", "21": "Kavim",
                    "25": "Nateev Express", "32": "V-Line", "42": "Afikim",
                }
                if not operator_name and operator_ref:
                    operator_name = _OP_NAME_MAP.get(operator_ref, "Unknown")

                hour_val = int(hour_param) if hour_param.isdigit() else None
                dow_val = int(dow_param) if dow_param.isdigit() else None
                jam_val = float(jam_param) if jam_param else None

                # If jam_factor not given, fetch real-time average from ES
                if jam_val is None:
                    try:
                        import urllib.request as _ureq
                        body = json.dumps({
                            "size": 0,
                            "query": {"range": {"recorded_at": {"gte": "now-1h"}}},
                            "aggs": {"jam": {"avg": {"field": "jam_factor"}}},
                        }).encode("utf-8")
                        req = _ureq.Request(
                            "http://localhost:9200/transit-traffic/_search",
                            data=body,
                            headers={"Content-Type": "application/json"},
                        )
                        with _ureq.urlopen(req, timeout=5) as resp:
                            d = json.loads(resp.read())
                            jam_val = float(
                                d.get("aggregations", {}).get("jam", {}).get("value")
                                or 2.0
                            )
                    except Exception:
                        jam_val = 2.0

                # Lazy import — keeps bot.py startup fast and tolerates
                # missing pkl gracefully.
                try:
                    sys.path.insert(0, os.path.join(BASE_DIR, "ml"))
                    import predict as _ml_predict  # type: ignore
                except Exception as ex:
                    self.send_json({"error": f"model not loaded: {ex}"}, status=503)
                    return

                result = _ml_predict.predict_delay(
                    line=line or None,
                    operator=operator_name or None,
                    hour=hour_val,
                    day_of_week=dow_val,
                    jam_factor=jam_val,
                )
                if result is None:
                    self.send_json({"error": "model not loaded"}, status=503)
                else:
                    self.send_json(result)
            except Exception as e:
                self.send_json({"error": str(e)}, status=500)

        # ── Real-time active lines per operator from Stride SIRI ─────────────
        # GET /api/active_lines?operator_ref=3
        # Returns unique bus lines currently active (vehicles on road right now).
        # Same data source as Waze/Google Maps Israel — MOT SIRI via Hasadna.
        elif path == "/api/active_lines":
            operator_ref = (qs.get("operator_ref") or [""])[0].strip()
            if not operator_ref:
                self.send_json({"error": "operator_ref required", "routes": []}, status=400)
            else:
                try:
                    import requests as _rq
                    raw = _rq.get(
                        f"{HASADNA_URL}/siri_vehicle_locations/list",
                        params={
                            "siri_routes__operator_ref": operator_ref,
                            "lat__greater_or_equal": 31.87,
                            "lat__lower_or_equal":   32.21,
                            "lon__greater_or_equal": 34.72,
                            "lon__lower_or_equal":   34.95,
                            "limit": 500,
                            "order_by": "id desc",
                        },
                        timeout=12,
                        headers={"User-Agent": "IsraelTransitBot/1.0"},
                    ).json()
                    seen = {}
                    line_velocity = {}  # line_ref → max velocity seen
                    if isinstance(raw, list):
                        for v in raw:
                            lr  = str(v.get("siri_route__line_ref") or "").strip()
                            rsn = str(v.get("siri_route__gtfs_route__route_short_name") or "").strip()
                            rln = str(v.get("siri_route__gtfs_route__route_long_name") or "").strip()
                            vel = int(v.get("velocity") or 0)
                            if not lr:
                                continue
                            # Track max velocity per line
                            if vel > line_velocity.get(lr, 0):
                                line_velocity[lr] = vel
                            if rsn:
                                _learn_line(lr, rsn)
                            if lr not in seen:
                                seen[lr] = {
                                    "route_id":         lr,
                                    "route_short_name": rsn or _resolve_line(lr),
                                    "route_long_name":  rln,
                                    "agency_id":        operator_ref,
                                    "agency_name":      (qs.get("op_name") or [""])[0] or operator_ref,
                                }
                    # Filter to only lines with at least one moving vehicle (real "active")
                    active_routes = [
                        r for lr, r in seen.items()
                        if line_velocity.get(lr, 0) > 0
                    ]
                    # Attach max_velocity for each line (so frontend can show "live" badge)
                    for r in active_routes:
                        r["max_velocity"] = line_velocity.get(r["route_id"], 0)
                    routes = sorted(
                        active_routes,
                        key=lambda x: int(x["route_short_name"]) if x["route_short_name"].isdigit() else 9999
                    )
                    self.send_json({
                        "routes": routes,
                        "count":  len(routes),
                        "total_lines_seen": len(seen),
                        "source": "stride_realtime_active_only",
                    })
                except Exception as e:
                    self.send_json({"error": str(e), "routes": []}, status=500)

        # ── PWA: manifest.json ───────────────────────────────────────────────
        elif path == "/manifest.json":
            try:
                manifest_path = os.path.join(BASE_DIR, "manifest.json")
                with open(manifest_path, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/manifest+json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "public, max-age=3600")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                self.send_json({"error": "manifest.json not found"}, status=404)
            except Exception as e:
                self.send_json({"error": str(e)}, status=500)

        # ── PWA: service worker ──────────────────────────────────────────────
        # Must be served from the project root with Service-Worker-Allowed: /
        # so the SW can claim scope "/" even though the file path is /service-worker.js.
        elif path == "/service-worker.js":
            try:
                sw_path = os.path.join(BASE_DIR, "service-worker.js")
                with open(sw_path, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/javascript; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                # SW updates: don't let the browser hold an old copy forever.
                self.send_header("Cache-Control", "no-cache, max-age=0")
                self.send_header("Service-Worker-Allowed", "/")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                self.send_json({"error": "service-worker.js not found"}, status=404)
            except Exception as e:
                self.send_json({"error": str(e)}, status=500)

        # ── PWA: icons ───────────────────────────────────────────────────────
        elif path.startswith("/icons/"):
            # Reject path traversal — only allow basenames inside /icons/.
            fname = path[len("/icons/"):]
            if not fname or "/" in fname or ".." in fname or "\\" in fname:
                self.send_json({"error": "invalid icon path"}, status=400)
            else:
                icon_path = os.path.join(BASE_DIR, "icons", fname)
                if not os.path.isfile(icon_path):
                    self.send_json({"error": f"icon not found: {fname}"}, status=404)
                else:
                    ext = os.path.splitext(fname)[1].lower()
                    ctype = {
                        ".png":  "image/png",
                        ".svg":  "image/svg+xml",
                        ".ico":  "image/x-icon",
                        ".jpg":  "image/jpeg",
                        ".jpeg": "image/jpeg",
                        ".webp": "image/webp",
                    }.get(ext, "application/octet-stream")
                    try:
                        with open(icon_path, "rb") as f:
                            body = f.read()
                        self.send_response(200)
                        self.send_header("Content-Type", ctype)
                        self.send_header("Content-Length", str(len(body)))
                        self.send_header("Cache-Control", "public, max-age=86400")
                        self.send_header("Access-Control-Allow-Origin", "*")
                        self.end_headers()
                        self.wfile.write(body)
                    except Exception as e:
                        self.send_json({"error": str(e)}, status=500)

        # ── moovit.html (ממשק ראשי) ──────────────────────────────────────────
        elif path == "/":
            html_path = os.path.join(BASE_DIR, "moovit.html")
            if not os.path.exists(html_path):
                html_path = os.path.join(BASE_DIR, "index.html")
            try:
                with open(html_path, "r", encoding="utf-8") as f:
                    html_content = f.read()
                body = html_content.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                self.send_json({"error": str(e)}, status=500)

        # ── index.html (צ'אט ישן) ────────────────────────────────────────────
        elif path == "/chat" or path == "/transit" or path == "/agent":
            html_path = os.path.join(BASE_DIR, "index.html")
            if not os.path.exists(html_path):
                html_path = os.path.join(BASE_DIR, "agent_transit.html")
            try:
                with open(html_path, "r", encoding="utf-8") as f:
                    html_content = f.read()
                body = html_content.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                # מונע cache בדפדפן — כל טעינה מחדש תביא את הגרסה העדכנית
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                self.send_json({"error": str(e)}, status=500)

        # ── health ────────────────────────────────────────────────────────────
        elif path == "/health":
            self.send_json({"status": "ok", "service": "Israel Transit Bot", "port": BOT_PORT})

        # ── status ────────────────────────────────────────────────────────────
        elif path == "/status":
            # Fetch ES freshness quickly
            es_freshness = {}
            for _key, _idx, _tfield in [
                ("bus",     "transit-bus-positions",   "fetched_at"),
                ("train",   "transit-train-positions", "recorded_at"),
                ("traffic", "transit-traffic",         "recorded_at"),
            ]:
                try:
                    # Filter to reasonable date range to avoid bogus synthetic timestamps (e.g. 2038)
                    _ts_filter = {"range": {_tfield: {"gte": "2025-01-01", "lte": "2027-12-31"}}}
                    _req = urllib.request.Request(
                        f"http://localhost:9200/{_idx}/_search",
                        data=json.dumps({
                            "size": 1,
                            "query": _ts_filter,
                            "sort": [{_tfield: {"order": "desc"}}],
                            "_source": [_tfield],
                        }).encode(),
                        headers={"Content-Type": "application/json"},
                    )
                    with urllib.request.urlopen(_req, timeout=3) as _r:
                        _d = json.loads(_r.read())
                        _hits = _d.get("hits", {}).get("hits", [])
                        es_freshness[_key] = {
                            "total": _d["hits"]["total"]["value"],
                            "latest": _hits[0]["_source"].get(_tfield) if _hits else None,
                        }
                except Exception:
                    es_freshness[_key] = {"total": 0, "latest": None}

            self.send_json({
                "status": "running",
                "service": "Israel Public Transit Monitoring — גוש דן ותל אביב",
                "server_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "area": {
                    "name": "גוש דן ותל אביב",
                    "lat": f"{GUSH_DAN['lat_min']}–{GUSH_DAN['lat_max']}",
                    "lon": f"{GUSH_DAN['lon_min']}–{GUSH_DAN['lon_max']}",
                },
                "data_freshness": es_freshness,
                "endpoints": {
                    "Gush Dan Transit App":  f"http://linub-vm:{BOT_PORT}/gushdan",
                    "Transit Query Tool":    f"http://linub-vm:{BOT_PORT}/",
                    "Agent Dashboard":       f"http://linub-vm:{BOT_PORT}/agent",
                    "Airflow UI":            "http://linub-vm:8081",
                    "Kafka UI":              "http://linub-vm:8085",
                    "MinIO Console":         "http://linub-vm:9001",
                    "Kibana":                "http://linub-vm:5601",
                    "Pipeline Status":       f"http://linub-vm:{BOT_PORT}/api/pipeline_status",
                    "ES Summary":            f"http://linub-vm:{BOT_PORT}/api/es_summary",
                    "Active Lines (Dan)":    f"http://linub-vm:{BOT_PORT}/api/active_lines?operator_ref=3",
                },
            })

        # ── buses — גוש דן ותל אביב בלבד ────────────────────────────────────
        elif path == "/buses":
            all_data = load_json("buses_with_nearest_stops.json") or load_json("bus_positions.json")
            filtered = []
            for b in all_data:
                if not _in_gush_dan(
                    b.get("lat") or b.get("latitude"),
                    b.get("lon") or b.get("longitude")
                ):
                    continue
                # הוסף line_name קריא אם לא קיים או זהה ל-line_ref
                lr = str(b.get("line_ref") or b.get("route_id") or "")
                rsn = str(b.get("route_short_name") or "")
                if not rsn or rsn == lr:
                    rsn = _resolve_line(lr)
                b = dict(b)   # shallow copy — לא לשנות את ה-cache
                b["line_name"] = rsn or lr
                filtered.append(b)
            self.send_json(filtered[:200])

        # ── stops — גוש דן ותל אביב בלבד ────────────────────────────────────
        elif path == "/stops":
            all_data = load_json("buses_with_nearest_stops.json")
            filtered = []
            for b in all_data:
                if not _in_gush_dan(
                    b.get("lat") or b.get("latitude"),
                    b.get("lon") or b.get("longitude")
                ):
                    continue
                lr  = str(b.get("line_ref") or b.get("route_id") or "")
                rsn = str(b.get("route_short_name") or "")
                if not rsn or rsn == lr:
                    rsn = _resolve_line(lr)
                b = dict(b)
                b["line_name"] = rsn or lr
                filtered.append(b)
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

        # ── Nearby buses via Stride (fallback when GTFS unavailable) ─────────
        # GET /nearby_buses?lat=32.08&lon=34.78&radius=600
        elif path == "/nearby_buses":
            try:
                lat    = float((qs.get("lat")    or ["0"])[0])
                lon    = float((qs.get("lon")    or ["0"])[0])
                radius = int((qs.get("radius")   or ["600"])[0])
                deg = radius / 111000.0  # rough m → degrees
                target = (
                    f"{HASADNA_URL}/siri_vehicle_locations/list?"
                    f"lat__greater_or_equal={lat-deg}&lat__lower_or_equal={lat+deg}"
                    f"&lon__greater_or_equal={lon-deg}&lon__lower_or_equal={lon+deg}"
                    f"&limit=50&order_by=id+desc"
                )
                self._proxy(target)
            except Exception as e:
                self.send_json({"error": str(e)}, status=400)

        # ── Reverse geocode — lat,lon → address ──────────────────────────────
        # GET /reverse_geocode?lat=32.08&lon=34.78
        elif path == "/reverse_geocode":
            try:
                import requests as _rq
                rlat = float((qs.get("lat") or ["0"])[0])
                rlon = float((qs.get("lon") or ["0"])[0])
                res = _rq.get(
                    "https://nominatim.openstreetmap.org/reverse",
                    params={"lat": rlat, "lon": rlon, "format": "json",
                            "accept-language": "he"},
                    headers={"User-Agent": "IsraelTransitBot/1.0"},
                    timeout=6
                ).json()
                parts = res.get("address", {})
                road  = parts.get("road") or parts.get("pedestrian") or parts.get("path") or ""
                house = parts.get("house_number", "")
                city  = (parts.get("city") or parts.get("town")
                         or parts.get("village") or parts.get("suburb") or "")
                if road and city:
                    short = f"{road} {house}, {city}".strip(", ")
                elif city:
                    short = city
                else:
                    short = res.get("display_name", "").split(",")[0]
                self.send_json({"address": short, "lat": rlat, "lon": rlon})
            except Exception as e:
                rlat2 = float((qs.get("lat") or ["0"])[0])
                rlon2 = float((qs.get("lon") or ["0"])[0])
                self.send_json({"address": f"{rlat2:.4f},{rlon2:.4f}", "error": str(e)})

        # ── Route plan — מסלול מ-X ל-Y ──────────────────────────────────────
        # GET /route_plan?origin=ADDR&destination=ADDR
        elif path == "/route_plan":
            origin = urllib.parse.unquote_plus((qs.get("origin") or [""])[0]).strip()
            destination = urllib.parse.unquote_plus((qs.get("destination") or [""])[0]).strip()
            if not origin or not destination:
                self.send_json({"error": "?origin= and ?destination= required"}, status=400)
                return
            try:
                import requests as _rq
                _OP = {
                    "2":"רכבת ישראל","3":"דן","5":"אגד","6":"NTA מטרופולין",
                    "14":"מטרופולין","15":"אגד תעבורה","16":"תנופה",
                    "18":"סופרבוס","21":"קווים","25":"נתיב אקספרס",
                    "32":"V-Line","42":"אפיקים"
                }
                def _geo(addr):
                    res = _rq.get(
                        NOMINATIM_URL,
                        params={"q": addr+", ישראל","format":"json","limit":1,
                                "countrycodes":"il","accept-language":"he"},
                        headers={"User-Agent":"IsraelTransitBot/1.0"}, timeout=5
                    ).json()
                    if res:
                        parts = res[0].get("display_name","").split(",")
                        label = parts[0].strip() if parts else addr
                        return (float(res[0]["lat"]), float(res[0]["lon"]), label)
                    return None, None, addr

                # Geocode both addresses in parallel
                from concurrent.futures import ThreadPoolExecutor as _GeoTPX
                with _GeoTPX(max_workers=2) as _gtp:
                    _f1 = _gtp.submit(_geo, origin)
                    _f2 = _gtp.submit(_geo, destination)
                    lat1, lon1, olabel = _f1.result()
                    lat2, lon2, dlabel = _f2.result()
                if not lat1 or not lat2:
                    self.send_json({"error": "לא ניתן לאתר כתובות", "routes": []})
                    return

                lat_min=min(lat1,lat2)-0.025; lat_max=max(lat1,lat2)+0.025
                lon_min=min(lon1,lon2)-0.025; lon_max=max(lon1,lon2)+0.025

                sv = _rq.get(f"{HASADNA_URL}/siri_vehicle_locations/list", params={
                    "lat__greater_or_equal": lat_min, "lat__lower_or_equal": lat_max,
                    "lon__greater_or_equal": lon_min, "lon__lower_or_equal": lon_max,
                    "limit": 200, "order_by": "id desc"
                }, timeout=12).json()

                line_map = {}
                for v in sv:
                    lr  = str(v.get("siri_route__line_ref",""))
                    rsn = str(v.get("siri_route__gtfs_route__route_short_name") or "").strip()
                    op  = str(v.get("siri_route__operator_ref",""))
                    if lr and lr not in line_map:
                        if rsn:
                            _learn_line(lr, rsn)
                        line_map[lr] = (_OP.get(op, f"מפעיל {op}"), rsn or _resolve_line(lr))

                routes_list = []
                for lr, (op_name, line_name) in sorted(
                    line_map.items(),
                    key=lambda x: (int(x[1][1]) if x[1][1].isdigit() else 9999,
                                   int(x[0])    if x[0].isdigit()    else 9999)
                )[:15]:
                    rinfo = {"line_ref": lr, "line_name": line_name,
                             "operator": op_name,
                             "origin_stop": "", "final_stop": ""}
                    try:
                        sd = _rq.get(
                            f"http://localhost:{BOT_PORT}/gtfs/route_stops",
                            params={"short_name": lr}, timeout=3
                        ).json()
                        sts = sd.get("stops", [])
                        if sts:
                            rinfo["origin_stop"] = sts[0].get("stop_name","")
                            rinfo["final_stop"]  = sts[-1].get("stop_name","")
                    except Exception:
                        pass
                    routes_list.append(rinfo)

                self.send_json({
                    "origin":      {"label": olabel, "lat": lat1, "lon": lon1},
                    "destination": {"label": dlabel, "lat": lat2, "lon": lon2},
                    "routes":      routes_list,
                    "count":       len(routes_list)
                })
            except Exception as e:
                self.send_json({"error": str(e), "routes": []}, status=500)

        # ── Live vehicles for a specific line ─────────────────────────────────
        # GET /line_vehicles?line_ref=X
        elif path == "/line_vehicles":
            line_ref = (qs.get("line_ref") or [""])[0].strip()
            if not line_ref:
                self.send_json({"error": "?line_ref= required"}, status=400)
                return
            target = (
                f"{HASADNA_URL}/siri_vehicle_locations/list?"
                f"siri_routes__line_ref={urllib.parse.quote(line_ref)}"
                f"&limit=30&order_by=id+desc"
            )
            self._proxy(target)

        # ── Future stops for a line / ride ───────────────────────────────────
        # GET /line_stops?line_ref=X   OR   ?ride_id=Y
        elif path == "/line_stops":
            line_ref = (qs.get("line_ref") or [""])[0].strip()
            ride_id  = (qs.get("ride_id")  or [""])[0].strip()
            try:
                import requests as _rq
                STRIDE = HASADNA_URL
                if not ride_id and line_ref:
                    # Get most recent ride for this line
                    veh = _rq.get(f"{STRIDE}/siri_vehicle_locations/list",
                        params={"siri_routes__line_ref": line_ref, "limit": 1,
                                "order_by": "id desc"}, timeout=8).json()
                    if veh:
                        ride_id = str(veh[0].get("siri_ride__id",""))

                if ride_id:
                    rs = _rq.get(f"{STRIDE}/siri_ride_stops/list",
                        params={"siri_ride__id": ride_id, "limit": 60,
                                "order_by": "order asc"}, timeout=10).json()
                    stops = [{"name": (s.get("gtfs_stop__name")
                                       or _resolve_stop_name(str(s.get("siri_stop__code") or ""),
                                                             s.get("gtfs_stop__city",""))
                                       or f"תח.{s.get('siri_stop__code','?')}"),
                              "order": s.get("order",i),
                              "lat": s.get("gtfs_stop__lat"),
                              "lon": s.get("gtfs_stop__lon")}
                             for i,s in enumerate(rs)]
                    self.send_json({"stops": stops, "count": len(stops),
                                   "ride_id": ride_id, "line_ref": line_ref})
                else:
                    self.send_json({"stops": [], "error": "no active ride found"})
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
            # Don't add Israel if already present (English or Hebrew)
            if "israel" in address.lower() or "ישראל" in address:
                q = address
            else:
                q = address + ", Israel"
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

        # ── streets → רשימת רחובות מ-Google Places ──────────────────────────
        # GET /streets?city=תל+אביב
        # משתמש ב-Google Places Autocomplete לקבלת רחובות אמיתיים
        elif path == "/streets":
            city = urllib.parse.unquote_plus((qs.get("city") or [""])[0]).strip()
            if not city:
                self.send_json({"error": "?city= required"}, status=400)
                return

            # אם אין Google API Key — fallback לנתונים סטטיים
            if not GOOGLE_MAPS_API_KEY:
                streets = CITY_STREETS.get(city, [])
                for k in CITY_STREETS:
                    if city in k or k in city:
                        streets = CITY_STREETS[k]
                        break
                self.send_json({"city": city, "streets": sorted(streets), "source": "static"})
                return

            try:
                # שלב 1: מצא את place_id של העיר
                geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?" + urllib.parse.urlencode({
                    "address": city + ", ישראל",
                    "key":     GOOGLE_MAPS_API_KEY,
                    "language":"he",
                    "region":  "il",
                })
                req = urllib.request.Request(geocode_url,
                    headers={"User-Agent": "IsraelTransitBot/1.0"})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    geo_data = json.loads(resp.read())

                if geo_data.get("status") != "OK":
                    raise Exception(f"Geocode failed: {geo_data.get('status')}")

                loc    = geo_data["results"][0]["geometry"]["location"]
                city_lat = loc["lat"]
                city_lon = loc["lng"]

                # שלב 2: חפש רחובות עם Places API (Nearby Search + Text Search)
                # שימוש ב-Text Search עם שאילתות רחוב
                streets = set()

                # חיפוש רחובות נפוצים בעיר
                for prefix in ["רחוב", "שדרות", "דרך"]:
                    places_url = f"{GOOGLE_PLACES_URL}/textsearch/json?" + urllib.parse.urlencode({
                        "query":    f"{prefix} {city}",
                        "key":      GOOGLE_MAPS_API_KEY,
                        "language": "he",
                        "region":   "il",
                        "location": f"{city_lat},{city_lon}",
                        "radius":   "3000",
                        "type":     "route",
                    })
                    req2 = urllib.request.Request(places_url,
                        headers={"User-Agent": "IsraelTransitBot/1.0"})
                    with urllib.request.urlopen(req2, timeout=10) as resp2:
                        places_data = json.loads(resp2.read())

                    for place in places_data.get("results", []):
                        name = place.get("name", "")
                        # נקה את שם הרחוב
                        for strip in ["רחוב ", "שדרות ", "דרך ", "שד' "]:
                            if name.startswith(strip):
                                name = name[len(strip):]
                        if name and len(name) > 1:
                            streets.add(name.strip())

                # שלב 3: אם קיבלנו מעט תוצאות — השלם מהנתונים הסטטיים
                static = CITY_STREETS.get(city, [])
                for k in CITY_STREETS:
                    if city in k or k in city:
                        static = CITY_STREETS[k]
                        break
                streets.update(static)

                result = sorted(streets)
                self.send_json({
                    "city":    city,
                    "streets": result,
                    "count":   len(result),
                    "source":  "google_places",
                })

            except Exception as e:
                # fallback לנתונים סטטיים אם Google נכשל
                log.warning(f"Google Places failed: {e} — using static data")
                streets = CITY_STREETS.get(city, [])
                for k in CITY_STREETS:
                    if city in k or k in city:
                        streets = CITY_STREETS[k]
                        break
                self.send_json({
                    "city":    city,
                    "streets": sorted(streets),
                    "source":  "static_fallback",
                    "error":   str(e),
                })

        # ── Stride proxy — מסנן לגוש דן ותל אביב ────────────────────────────
        elif path.startswith("/proxy/stride"):
            api_path = path[len("/proxy/stride"):]
            if not api_path.startswith("/"):
                api_path = "/" + api_path
            target = HASADNA_URL.rstrip("/") + api_path

            # הוסף פרמטרי bbox לגוש דן לכל קריאת siri_vehicle_locations
            # (רק אם הפרונטאנד לא שלח bbox משלו)
            query = parsed.query
            if "siri_vehicle_locations" in api_path and "lat__greater_or_equal" not in query:
                bbox_params = (
                    f"lat__greater_or_equal={GUSH_DAN['lat_min']}"
                    f"&lat__lower_or_equal={GUSH_DAN['lat_max']}"
                    f"&lon__greater_or_equal={GUSH_DAN['lon_min']}"
                    f"&lon__lower_or_equal={GUSH_DAN['lon_max']}"
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
            if "siri_vehicle_locations" in api_path and "lat__greater_or_equal" not in query:
                bbox_params = (
                    f"lat__greater_or_equal={GUSH_DAN['lat_min']}"
                    f"&lat__lower_or_equal={GUSH_DAN['lat_max']}"
                    f"&lon__greater_or_equal={GUSH_DAN['lon_min']}"
                    f"&lon__lower_or_equal={GUSH_DAN['lon_max']}"
                )
                query = f"{query}&{bbox_params}" if query else bbox_params
            if query:
                target += "?" + query
            self._proxy(target)

        # ── Israel Railways → Stride (israelrail.azurewebsites.net is dead) ──
        # מקבל פרמטר stationId אופציונלי לסינון לפי אזור התחנה
        elif path.startswith("/proxy/rail"):
            station_id = qs.get("stationId", [""])[0]
            # קואורדינטות תחנות רכבת ישראל
            RAIL_STATION_COORDS = {
                "700":  (32.4493, 34.9205), "1220": (33.0063, 35.0972),
                "1500": (32.0893, 34.7773), "1600": (32.0648, 34.7751),
                "1700": (32.0858, 34.7784), "2100": (31.8952, 34.8018),
                "2200": (31.9651, 34.7984), "2300": (32.0993, 34.7977),
                "3100": (31.8952, 35.0186), "3400": (31.7919, 35.2040),
                "3600": (32.8145, 34.9886), "3700": (31.9993, 34.8849),
                "4100": (32.8145, 34.9886), "4600": (33.0063, 35.0972),
                "4640": (32.9257, 35.0707), "4900": (32.4493, 34.9205),
                "5000": (32.3155, 34.8519), "5200": (32.0912, 34.8560),
                "6700": (32.1837, 34.8700), "6900": (31.8052, 34.6468),
                "7300": (31.2435, 34.8018), "7500": (31.8052, 34.6468),
                "7600": (31.6598, 34.5714), "8600": (31.8952, 35.0186),
            }
            if station_id and station_id in RAIL_STATION_COORDS:
                lat, lon = RAIL_STATION_COORDS[station_id]
                r = 0.20  # ~22 ק"מ סביב התחנה
                lat_min, lat_max = lat - r, lat + r
                lon_min, lon_max = lon - r, lon + r
            else:
                lat_min = GUSH_DAN["lat_min"]
                lat_max = GUSH_DAN["lat_max"]
                lon_min = GUSH_DAN["lon_min"]
                lon_max = GUSH_DAN["lon_max"]
            stride_params = (
                f"siri_routes__operator_ref=2&limit=50&order_by=id+desc"
                f"&lat__greater_or_equal={lat_min}&lat__lower_or_equal={lat_max}"
                f"&lon__greater_or_equal={lon_min}&lon__lower_or_equal={lon_max}"
            )
            target = f"{HASADNA_URL.rstrip('/')}/siri_vehicle_locations/list?{stride_params}"
            self._proxy(target)

        # ── Line info — Moovit-style (stops × 2 directions + live vehicles) ─────
        # GET /line_info?line_ref=X&short_name=2&operator_ref=3
        elif path == "/line_info":
            line_ref     = (qs.get("line_ref")     or [""])[0].strip()
            short_name   = (qs.get("short_name")   or [""])[0].strip()
            operator_ref = (qs.get("operator_ref") or [""])[0].strip()
            if not line_ref and not short_name:
                self.send_json({"error": "?line_ref= or ?short_name= required"}, status=400)
                return
            try:
                import requests as _rq
                from concurrent.futures import (ThreadPoolExecutor as _TPX,
                                                as_completed as _fasc)
                STRIDE = HASADNA_URL
                _OP2 = {
                    "2":"רכבת ישראל","3":"דן","5":"אגד","6":"NTA מטרופולין",
                    "14":"מטרופולין","15":"אגד תעבורה","16":"תנופה",
                    "18":"סופרבוס","21":"קווים","25":"נתיב אקספרס",
                    "32":"V-Line","42":"אפיקים"
                }

                # Step 1: active vehicles for this line — only ACTIVE (velocity > 0)
                # Also constrain by operator_ref so we don't accidentally pull a different
                # operator's line that happens to share the same line_ref.
                _veh_params = {"siri_routes__line_ref": line_ref,
                               "limit": 30, "order_by": "id desc"}
                if operator_ref:
                    _veh_params["siri_routes__operator_ref"] = operator_ref
                vehs = _rq.get(
                    f"{STRIDE}/siri_vehicle_locations/list",
                    params=_veh_params,
                    timeout=10
                ).json()
                if not isinstance(vehs, list):
                    vehs = []

                # Filter to only moving vehicles (real "active" — speed > 0)
                vehs_active = [v for v in vehs if (v.get("velocity") or 0) > 0]
                # Use active list for vehicle markers; fall back to all if none moving
                vehs_for_display = vehs_active if vehs_active else vehs

                op_id = str((vehs[0] if vehs else {}).get("siri_route__operator_ref","")) or operator_ref
                operator = _OP2.get(op_id, f"מפעיל {op_id}" if op_id else "לא ידוע")

                vehicles = []
                for v in vehs_for_display:
                    try:
                        vlat = float(v.get("lat") or v.get("calculated_lat") or 0)
                        vlon = float(v.get("lon") or v.get("calculated_lon") or 0)
                        vel  = int(v.get("velocity") or 0)
                        if vlat and vlon and vel > 0:  # only render moving vehicles
                            vehicles.append({
                                "lat": vlat, "lon": vlon, "vel": vel,
                                "ride_id": v.get("siri_ride__id"),
                            })
                    except Exception:
                        pass

                # ── Step 2: Get authoritative stop list from Stride GTFS ──────────
                # Find gtfs_route by route_short_name + operator_ref (today),
                # then a gtfs_ride for that route, then ride_stops for that ride.
                # This gives proper stop names + lat/lon (matches Google Maps/Moovit).
                import datetime as _dt
                today = _dt.date.today().isoformat()

                def _gtfs_route_stops(direction_filter=None):
                    """Returns list of stop dicts for the route in the requested direction (1 or 2)."""
                    if not short_name:
                        return [], {}
                    rt_params = {
                        "route_short_name": short_name,
                        "date_from": today,
                        "date_to":   today,
                        "limit":     20,
                    }
                    if op_id:
                        rt_params["operator_ref"] = op_id
                    try:
                        rts = _rq.get(f"{STRIDE}/gtfs_routes/list",
                                      params=rt_params, timeout=8).json()
                    except Exception:
                        rts = []
                    if not isinstance(rts, list) or not rts:
                        return [], {}

                    # Pick route matching the requested direction if present,
                    # else first route returned
                    chosen = None
                    if direction_filter is not None:
                        for r in rts:
                            if str(r.get("route_direction") or "") == str(direction_filter):
                                chosen = r
                                break
                    chosen = chosen or rts[0]
                    rid_route = chosen.get("id")
                    if not rid_route:
                        return [], chosen

                    # Find a gtfs_ride for this route (any time today)
                    try:
                        rides = _rq.get(f"{STRIDE}/gtfs_rides/list", params={
                            "gtfs_route_id":     rid_route,
                            "start_time_from":   f"{today}T00:00:00Z",
                            "start_time_to":     f"{today}T23:59:59Z",
                            "limit":             1,
                            "order_by":          "id asc",
                        }, timeout=8).json()
                    except Exception:
                        rides = []
                    if not isinstance(rides, list) or not rides:
                        return [], chosen
                    ride_id = rides[0].get("id")

                    # Fetch ride stops in proper order
                    try:
                        rs = _rq.get(f"{STRIDE}/gtfs_ride_stops/list", params={
                            "gtfs_ride_ids": ride_id,
                            "limit":         120,
                            "order_by":      "stop_sequence asc",
                        }, timeout=10).json()
                    except Exception:
                        rs = []
                    if not isinstance(rs, list):
                        return [], chosen

                    # Dedupe by stop_sequence (Stride sometimes returns duplicates)
                    seen_seq = set()
                    out = []
                    for s in rs:
                        seq = s.get("stop_sequence")
                        if seq in seen_seq:
                            continue
                        seen_seq.add(seq)
                        nm = (s.get("gtfs_stop__name") or "").strip()
                        ct = (s.get("gtfs_stop__city") or "").strip()
                        if not nm and not ct:
                            continue
                        full_name = f"{nm}, {ct}" if nm and ct and ct not in nm else (nm or ct)
                        out.append({
                            "name":    full_name,
                            "stop_id": str(s.get("gtfs_stop_id") or ""),
                            "lat":     s.get("gtfs_stop__lat"),
                            "lon":     s.get("gtfs_stop__lon"),
                            "order":   seq,
                            "scheduled_dep": s.get("departure_time", "")[:19] if s.get("departure_time") else "",
                        })
                    return out, chosen

                # Try direction 1 and 2 in parallel
                dirs = []
                gtfs_route_meta = {}
                with _TPX(max_workers=2) as _pool:
                    futs = {_pool.submit(_gtfs_route_stops, d): d for d in (1, 2)}
                    results_by_dir = {}
                    for fut in _fasc(futs, timeout=15):
                        try:
                            stops, route_meta = fut.result(timeout=0.2)
                        except Exception:
                            continue
                        d = futs[fut]
                        if stops:
                            results_by_dir[d] = (stops, route_meta)
                            if not gtfs_route_meta and route_meta:
                                gtfs_route_meta = route_meta

                # Order: dir 1 then dir 2
                for d in (1, 2):
                    if d in results_by_dir:
                        dirs.append(results_by_dir[d][0])

                # Fallback: try without direction filter if both attempts failed
                if not dirs:
                    fallback_stops, fallback_meta = _gtfs_route_stops(None)
                    if fallback_stops:
                        dirs.append(fallback_stops)
                        gtfs_route_meta = gtfs_route_meta or fallback_meta

                directions = [
                    {
                        "first_stop": d[0]["name"],
                        "last_stop":  d[-1]["name"],
                        "stops":      d,
                    }
                    for d in dirs if d
                ]

                # Long name from GTFS route metadata (e.g. "Petach Tikva → Bat Yam")
                long_name = gtfs_route_meta.get("route_long_name", "") if gtfs_route_meta else ""
                # Strip trailing "-1#" / "-2#" direction suffix
                if long_name:
                    import re as _re
                    long_name = _re.sub(r"-[12]#?$", "", long_name).replace("<->", " ↔ ")

                self.send_json({
                    "line_ref":        line_ref,
                    "short_name":      short_name or (gtfs_route_meta.get("route_short_name", "") if gtfs_route_meta else ""),
                    "long_name":       long_name,
                    "operator":        operator or (gtfs_route_meta.get("agency_name", "") if gtfs_route_meta else ""),
                    "directions":      directions,
                    "vehicles":        vehicles[:20],
                    "active_count":    len(vehs_active),
                    "total_count":     len(vehs),
                    "source":          "gtfs" if dirs else "none",
                    "has_schedule":    bool(dirs),
                })
            except Exception as e:
                self.send_json({"error": str(e), "directions": [], "vehicles": []}, status=500)

        # ── Pipeline status — comprehensive health of all components ────────────
        # GET /api/pipeline_status
        elif path == "/api/pipeline_status":
            try:
                result = {}

                # 1. ES — latest document timestamp per index
                es_indices = {
                    "bus": "transit-bus-positions",
                    "train": "transit-train-positions",
                    "traffic": "transit-traffic",
                }
                es_status = {}
                for key, idx in es_indices.items():
                    try:
                        time_field = "fetched_at" if key == "bus" else "recorded_at"
                        # Filter to reasonable date range to exclude bogus synthetic records (e.g. 2038)
                        ts_filter = {"range": {time_field: {"gte": "2025-01-01", "lte": "2027-12-31"}}}
                        req = urllib.request.Request(
                            f"http://localhost:9200/{idx}/_search",
                            data=json.dumps({
                                "size": 1,
                                "query": ts_filter,
                                "sort": [{time_field: {"order": "desc"}}],
                                "_source": [time_field, "operator_name", "route_short_name"],
                            }).encode(),
                            headers={"Content-Type": "application/json"},
                        )
                        with urllib.request.urlopen(req, timeout=5) as r:
                            d = json.loads(r.read())
                            hits = d.get("hits", {})
                            total = hits.get("total", {}).get("value", 0)
                            latest = None
                            if hits.get("hits"):
                                latest = hits["hits"][0]["_source"].get(time_field)
                            es_status[key] = {"total_docs": total, "latest": latest, "index": idx}
                    except Exception as ex:
                        es_status[key] = {"error": str(ex), "index": idx}
                result["elasticsearch"] = es_status

                # 2. MinIO — latest file in bucket
                try:
                    import boto3
                    # When running on VM host, MINIO_ENDPOINT may be "http://minio:9000" (Docker hostname).
                    # Fall back to localhost if the Docker hostname doesn't resolve.
                    _minio_ep = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
                    if "minio:9000" in _minio_ep or "minio:9001" in _minio_ep:
                        _minio_ep = "http://localhost:9000"  # VM host can reach MinIO at localhost
                    s3 = boto3.client(
                        "s3",
                        endpoint_url=_minio_ep,
                        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
                    )
                    paginator = s3.get_paginator("list_objects_v2")
                    newest = None
                    newest_key = None
                    total_objs = 0
                    for page in paginator.paginate(Bucket="israel-transit-lake"):
                        for obj in page.get("Contents", []):
                            total_objs += 1
                            if newest is None or obj["LastModified"] > newest:
                                newest = obj["LastModified"]
                                newest_key = obj["Key"]
                    result["minio"] = {
                        "total_objects": total_objs,
                        "latest_file": newest_key,
                        "latest_modified": newest.isoformat() if newest else None,
                        "status": "ok" if total_objs > 0 else "empty",
                    }
                except Exception as ex:
                    result["minio"] = {"error": str(ex)}

                # 3. Airflow — latest DAG runs from Airflow REST API
                try:
                    req = urllib.request.Request(
                        "http://localhost:8081/api/v1/dags?only_active=true",
                        headers={"Authorization": "Basic YWRtaW46YWRtaW4="},  # admin:admin
                    )
                    with urllib.request.urlopen(req, timeout=5) as r:
                        dag_data = json.loads(r.read())
                    dag_ids = [d["dag_id"] for d in dag_data.get("dags", [])]
                    dag_runs = {}
                    for dag_id in dag_ids[:8]:
                        try:
                            req2 = urllib.request.Request(
                                f"http://localhost:8081/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-start_date",
                                headers={"Authorization": "Basic YWRtaW46YWRtaW4="},
                            )
                            with urllib.request.urlopen(req2, timeout=4) as r2:
                                run_data = json.loads(r2.read())
                                runs = run_data.get("dag_runs", [])
                                if runs:
                                    dag_runs[dag_id] = {
                                        "state": runs[0].get("state"),
                                        "start": runs[0].get("start_date"),
                                        "end": runs[0].get("end_date"),
                                    }
                        except Exception:
                            pass
                    result["airflow"] = {"dags": dag_runs}
                except Exception as ex:
                    result["airflow"] = {"error": str(ex)}

                # 4. Kafka — live vehicle count from Stride API
                try:
                    req = urllib.request.Request(
                        f"{HASADNA_URL}/siri_vehicle_locations/list?"
                        f"lat__greater_or_equal={GUSH_DAN['lat_min']}"
                        f"&lat__lower_or_equal={GUSH_DAN['lat_max']}"
                        f"&lon__greater_or_equal={GUSH_DAN['lon_min']}"
                        f"&lon__lower_or_equal={GUSH_DAN['lon_max']}"
                        f"&limit=1&order_by=id+desc",
                        headers={"User-Agent": "IsraelTransitBot/1.0"},
                    )
                    with urllib.request.urlopen(req, timeout=8) as r:
                        stride_data = json.loads(r.read())
                    result["stride_api"] = {
                        "reachable": True,
                        "sample_count": len(stride_data) if isinstance(stride_data, list) else 0,
                    }
                except Exception as ex:
                    result["stride_api"] = {"reachable": False, "error": str(ex)}

                self.send_json(result)
            except Exception as e:
                self.send_json({"error": str(e)}, status=500)

        # ── ES live summary — aggregated stats from Elasticsearch ────────────
        # GET /api/es_summary?hours=6
        elif path == "/api/es_summary":
            hours = int((qs.get("hours") or ["6"])[0])
            try:
                since = f"now-{hours}h"
                summary = {}

                # Bus: vehicles by operator
                req = urllib.request.Request(
                    "http://localhost:9200/transit-bus-positions/_search",
                    data=json.dumps({
                        "size": 0,
                        "query": {"range": {"fetched_at": {"gte": since}}},
                        "aggs": {
                            "by_operator": {
                                "terms": {"field": "operator_name.keyword", "size": 15}
                            },
                            "unique_lines": {
                                "cardinality": {"field": "route_short_name.keyword"}
                            },
                        },
                    }).encode(),
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=8) as r:
                    d = json.loads(r.read())
                    aggs = d.get("aggregations", {})
                    summary["buses"] = {
                        "total_records": d["hits"]["total"]["value"],
                        "unique_lines": aggs.get("unique_lines", {}).get("value", 0),
                        "by_operator": {
                            b["key"]: b["doc_count"]
                            for b in aggs.get("by_operator", {}).get("buckets", [])
                        },
                        "window_hours": hours,
                    }
            except Exception as ex:
                summary["buses"] = {"error": str(ex)}

            try:
                # Traffic: congestion stats
                req2 = urllib.request.Request(
                    "http://localhost:9200/transit-traffic/_search",
                    data=json.dumps({
                        "size": 0,
                        "query": {"range": {"recorded_at": {"gte": since}}},
                        "aggs": {
                            "avg_speed": {"avg": {"field": "speed_kmh"}},
                            "avg_jam":   {"avg": {"field": "jam_factor"}},
                            "congested": {
                                "filter": {"term": {"is_congested": True}},
                            },
                            "top_roads": {
                                "terms": {"field": "road_name.keyword", "size": 5,
                                          "order": {"avg_jam": "desc"}},
                                "aggs": {"avg_jam": {"avg": {"field": "jam_factor"}}},
                            },
                        },
                    }).encode(),
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req2, timeout=8) as r2:
                    d2 = json.loads(r2.read())
                    aggs2 = d2.get("aggregations", {})
                    summary["traffic"] = {
                        "total_records": d2["hits"]["total"]["value"],
                        "avg_speed_kmh": round(aggs2.get("avg_speed", {}).get("value") or 0, 1),
                        "avg_jam_factor": round(aggs2.get("avg_jam", {}).get("value") or 0, 2),
                        "congested_segments": aggs2.get("congested", {}).get("doc_count", 0),
                        "worst_roads": [
                            {"road": b["key"], "jam_factor": round(b["avg_jam"]["value"], 2)}
                            for b in aggs2.get("top_roads", {}).get("buckets", [])
                        ],
                        "window_hours": hours,
                    }
            except Exception as ex2:
                summary["traffic"] = {"error": str(ex2)}

            try:
                # Trains: delays
                req3 = urllib.request.Request(
                    "http://localhost:9200/transit-train-positions/_search",
                    data=json.dumps({
                        "size": 0,
                        "query": {"range": {"recorded_at": {"gte": since}}},
                        "aggs": {
                            "delayed": {"filter": {"term": {"is_delayed": True}}},
                            "avg_delay": {"avg": {"field": "delay_minutes"}},
                        },
                    }).encode(),
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req3, timeout=8) as r3:
                    d3 = json.loads(r3.read())
                    aggs3 = d3.get("aggregations", {})
                    summary["trains"] = {
                        "total_records": d3["hits"]["total"]["value"],
                        "delayed_count": aggs3.get("delayed", {}).get("doc_count", 0),
                        "avg_delay_minutes": round(aggs3.get("avg_delay", {}).get("value") or 0, 1),
                        "window_hours": hours,
                    }
            except Exception as ex3:
                summary["trains"] = {"error": str(ex3)}

            self.send_json(summary)

        else:
            self.send_json({"error": "Not found", "path": self.path}, status=404)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    threading.Thread(
        target=_refresh_buses_loop, args=(120,),
        daemon=True, name="bus-refresh"
    ).start()
    print("🔄 Background bus refresh started (every 2 min)")

    # טוען GTFS LineRef cache בsBackground (לא חוסם את ה-server)
    threading.Thread(
        target=_build_line_cache,
        daemon=True, name="linecache-build"
    ).start()
    print("📚 GTFS LineRef cache loading in background...")

    server = ThreadingHTTPServer(("0.0.0.0", BOT_PORT), BotHandler)
    print(f"🤖 Transit Bot API → http://0.0.0.0:{BOT_PORT}")
    print(f"   /gushdan                  → 🚌 Gush Dan Transit App (Moovit-style)")
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