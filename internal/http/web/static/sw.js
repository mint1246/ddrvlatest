/**
 * DDrv Service Worker
 * Caches static assets for offline use and faster loading.
 */

const CACHE_VERSION = 'ddrv-cache-v1';
const STATIC_ASSETS = [
  '/',
  '/style.css',
  '/app.js',
  '/logo.svg',
  '/icon-192.svg',
  '/icon-512.svg',
  '/manifest.json',
];

// Install: pre-cache static assets
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_VERSION).then(cache => cache.addAll(STATIC_ASSETS))
      .then(() => self.skipWaiting())
  );
});

// Activate: clean up old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_VERSION).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

// Fetch: network-first for API, cache-first for static
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // Always go to network for API and file downloads
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/files/')) {
    return; // let browser handle it
  }

  // For Google Fonts, use stale-while-revalidate
  if (url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com') {
    event.respondWith(
      caches.open(CACHE_VERSION + '-fonts').then(async cache => {
        const cached = await cache.match(request);
        const fetchPromise = fetch(request).then(res => {
          cache.put(request, res.clone());
          return res;
        }).catch(() => null);
        return cached || fetchPromise;
      })
    );
    return;
  }

  // Cache-first for static assets, network-first fallback
  event.respondWith(
    caches.match(request).then(cached => {
      const networkFetch = fetch(request).then(res => {
        if (res.ok && request.method === 'GET') {
          caches.open(CACHE_VERSION).then(cache => cache.put(request, res.clone()));
        }
        return res;
      });
      return cached || networkFetch;
    }).catch(() => caches.match('/'))
  );
});
