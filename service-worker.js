/* eslint-env serviceworker */
/* global self, caches, fetch, URL */

/**
 * service-worker.js — Israel Transit PWA
 *
 * Strategy:
 *   - Static shell (HTML, manifest, icons, Leaflet CDN, Heebo font):
 *       cache-first, fall back to network.
 *   - API calls (/api/*, /proxy/*, /line_info, /gtfs/*, /geocode, /streets,
 *     /buses, /stops, /ask):
 *       network-first with a 6-second timeout, fall back to last-cached JSON.
 *   - Health endpoints (/health, /status): always network, never cached.
 *   - Offline fallback for navigations: a Hebrew "no internet" page.
 *
 * Bump CACHE_VERSION whenever the precache list changes — old caches are
 * cleaned up on `activate`.
 */

const CACHE_VERSION = 'transit-v2';
const STATIC_CACHE  = `static-${CACHE_VERSION}`;
const RUNTIME_CACHE = `runtime-${CACHE_VERSION}`;
const API_CACHE     = `api-${CACHE_VERSION}`;

// Files we want available immediately offline.
const PRECACHE_URLS = [
  '/',
  '/moovit',
  '/manifest.json',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
  '/icons/icon-maskable-512.png',
  '/icons/apple-touch-icon.png',
];

// CDN assets used by moovit.html — cache opportunistically (opaque is fine).
const RUNTIME_HOSTS = [
  'unpkg.com',
  'fonts.googleapis.com',
  'fonts.gstatic.com',
  'cdnjs.cloudflare.com',
  'tile.openstreetmap.org',
  'a.tile.openstreetmap.org',
  'b.tile.openstreetmap.org',
  'c.tile.openstreetmap.org',
  'cartodb-basemaps-a.global.ssl.fastly.net',
  'cartodb-basemaps-b.global.ssl.fastly.net',
  'cartodb-basemaps-c.global.ssl.fastly.net',
];

// API path prefixes that should use network-first.
const API_PREFIXES = [
  '/api/',
  '/proxy/',
  '/line_info',
  '/gtfs/',
  '/geocode',
  '/streets',
  '/buses',
  '/stops',
  '/ask',
];

// Endpoints we must never cache (live status only).
const NEVER_CACHE = ['/health', '/status'];

// ── Install ───────────────────────────────────────────────────────────
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) =>
      cache.addAll(PRECACHE_URLS.map((u) => new Request(u, { cache: 'reload' })))
        .catch((err) => {
          // Don't fail install if one resource is unreachable — log and continue.
          console.warn('[SW] precache partial failure:', err);
        })
    )
  );
  self.skipWaiting();
});

// ── Activate (clean old caches) ───────────────────────────────────────
self.addEventListener('activate', (event) => {
  const expected = new Set([STATIC_CACHE, RUNTIME_CACHE, API_CACHE]);
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((k) => !expected.has(k)).map((k) => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

// ── Fetch routing ─────────────────────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const req = event.request;

  // Only handle GET. Bypass POST (e.g. /ask) entirely.
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // Never cache health/status — let the network speak.
  if (NEVER_CACHE.some((p) => url.pathname.startsWith(p))) {
    return; // default browser fetch
  }

  // API calls — network-first.
  if (sameOrigin(url) && API_PREFIXES.some((p) => url.pathname.startsWith(p))) {
    event.respondWith(networkFirst(req));
    return;
  }

  // Navigations — cache-first on the shell, with offline fallback.
  if (req.mode === 'navigate') {
    event.respondWith(navigationHandler(req));
    return;
  }

  // Same-origin static assets (icons, etc.) — cache-first.
  if (sameOrigin(url)) {
    event.respondWith(cacheFirst(req, STATIC_CACHE));
    return;
  }

  // Cross-origin runtime assets (Leaflet, fonts, map tiles).
  if (RUNTIME_HOSTS.includes(url.hostname)) {
    event.respondWith(cacheFirst(req, RUNTIME_CACHE));
    return;
  }

  // Everything else — pass through.
});

function sameOrigin(url) {
  return url.origin === self.location.origin;
}

// ── Strategies ────────────────────────────────────────────────────────

async function cacheFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(req);
  if (cached) return cached;
  try {
    const resp = await fetch(req);
    if (resp && (resp.ok || resp.type === 'opaque')) {
      cache.put(req, resp.clone()).catch(() => {});
    }
    return resp;
  } catch (err) {
    // Last-ditch fallback for navigation-ish HTML
    if (req.headers.get('accept')?.includes('text/html')) {
      return offlineResponse();
    }
    throw err;
  }
}

async function networkFirst(req) {
  const cache = await caches.open(API_CACHE);
  const timeoutMs = 6000;

  try {
    const fresh = await fetchWithTimeout(req, timeoutMs);
    if (fresh && fresh.ok) {
      cache.put(req, fresh.clone()).catch(() => {});
    }
    return fresh;
  } catch (_err) {
    const cached = await cache.match(req);
    if (cached) {
      // Mark the response so the UI can show "stale data" if it wants.
      const headers = new Headers(cached.headers);
      headers.set('X-Served-From', 'sw-cache-stale');
      return new Response(await cached.clone().blob(), {
        status: cached.status,
        statusText: cached.statusText,
        headers,
      });
    }
    return new Response(
      JSON.stringify({
        error: 'offline',
        message: 'אין חיבור לאינטרנט — נסי שוב',
      }),
      {
        status: 503,
        headers: { 'Content-Type': 'application/json; charset=utf-8' },
      }
    );
  }
}

async function navigationHandler(req) {
  // Try network first for the shell (so updates land), but fall back to cache
  // and finally to the offline page.
  try {
    const fresh = await fetchWithTimeout(req, 4000);
    if (fresh && fresh.ok) {
      const cache = await caches.open(STATIC_CACHE);
      cache.put('/', fresh.clone()).catch(() => {});
    }
    return fresh;
  } catch (_err) {
    const cache = await caches.open(STATIC_CACHE);
    const cached = (await cache.match(req)) || (await cache.match('/'));
    return cached || offlineResponse();
  }
}

function fetchWithTimeout(req, ms) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout')), ms);
    fetch(req).then(
      (resp) => { clearTimeout(t); resolve(resp); },
      (err)  => { clearTimeout(t); reject(err); }
    );
  });
}

// ── Offline fallback page (Hebrew, RTL) ───────────────────────────────
function offlineResponse() {
  const html = `<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>לא מקוון — Israel Transit</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Heebo', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    background: #EAF8FB;
    color: #1A1A2E;
    min-height: 100dvh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px;
    text-align: center;
  }
  .card {
    background: #fff;
    border-radius: 20px;
    padding: 32px 24px;
    box-shadow: 0 8px 32px rgba(0,177,210,0.18);
    max-width: 360px;
    width: 100%;
  }
  .icon { font-size: 4rem; margin-bottom: 12px; }
  h1 { font-size: 1.3rem; color: #0090B0; margin-bottom: 8px; }
  p { color: #6B7280; font-size: 0.95rem; line-height: 1.5; margin-bottom: 16px; }
  button {
    background: #00B1D2; color: #fff; border: none; border-radius: 12px;
    padding: 12px 24px; font-size: 1rem; font-weight: 600; cursor: pointer;
    font-family: inherit; min-height: 44px; min-width: 120px;
  }
  button:active { background: #0090B0; }
</style>
</head>
<body>
  <div class="card">
    <div class="icon">📡</div>
    <h1>אין חיבור לאינטרנט</h1>
    <p>אין חיבור לאינטרנט — נסי שוב</p>
    <button onclick="location.reload()">נסה שוב</button>
  </div>
</body>
</html>`;
  return new Response(html, {
    status: 200,
    headers: { 'Content-Type': 'text/html; charset=utf-8' },
  });
}

// ── Skip waiting on demand (so the install button can hot-update) ─────
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
