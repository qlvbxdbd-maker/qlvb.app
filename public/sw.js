/* Service Worker cho PartyDocs */
const VERSION = 'pd-v1.0.0';
const STATIC_PRECACHE = ['/', '/offline.html', '/manifest.webmanifest', '/icons/icon-192.png', '/icons/icon-512.png'];
const STATIC_CACHE = `${VERSION}-static`;
const RUNTIME_CACHE = `${VERSION}-runtime`;
const DOCS_CACHE = `${VERSION}-docs`;

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(STATIC_CACHE).then(c => c.addAll(STATIC_PRECACHE)));
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(
      keys
        .filter(k => ![STATIC_CACHE, RUNTIME_CACHE, DOCS_CACHE].includes(k))
        .map(k => caches.delete(k))
    );
    await self.clients.claim();
  })());
});

function isHTML(req) {
  return req.destination === 'document' || (req.headers.get('accept') || '').includes('text/html');
}
function isRuntimeAPI(url) {
  return (
    url.pathname === '/catalogs' ||
    url.pathname === '/personal/search' ||
    url.pathname === '/documents/search' ||
    url.pathname === '/documents/latest' || // bổ sung để trang Home có thể hiển thị offline
    url.pathname.startsWith('/reports/')
  );
}
function isViewDoc(url) {
  // Bắt các file tải/xem trực tiếp từ /documents/:id/download (dù có query hay không)
  return url.pathname.endsWith('/download');
}

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  const req = event.request;
  if (req.method !== 'GET') return;

  // HTML: network-first
  if (isHTML(req)) {
    event.respondWith((async () => {
      try {
        const fresh = await fetch(req);
        const cache = await caches.open(STATIC_CACHE);
        cache.put(req, fresh.clone());
        return fresh;
      } catch {
        const cache = await caches.open(STATIC_CACHE);
        return (await cache.match(req)) || (await cache.match('/offline.html'));
      }
    })());
    return;
  }

  // API: stale-while-revalidate
  if (isRuntimeAPI(url)) {
    event.respondWith((async () => {
      const cache = await caches.open(RUNTIME_CACHE);
      const cached = await cache.match(req);
      const fetchPromise = fetch(req).then(res => {
        if (res.ok) cache.put(req, res.clone());
        return res;
      }).catch(() => null);
      return (
        cached ||
        fetchPromise ||
        new Response(
          JSON.stringify({ ok: false, offline: true, items: [] }),
          { headers: { 'Content-Type': 'application/json' }, status: 200 }
        )
      );
    })());
    return;
  }

  // File xem trực tiếp (PDF/ảnh/txt) & asset tĩnh: cache-first
  if (isViewDoc(url) || req.destination === 'image' || req.destination === 'font') {
    event.respondWith((async () => {
      const cache = await caches.open(DOCS_CACHE);
      const cached = await cache.match(req);
      if (cached) return cached;
      try {
        const res = await fetch(req);
        if (res.ok) cache.put(req, res.clone());
        return res;
      } catch {
        return new Response('Offline', { status: 503 });
      }
    })());
    return;
  }
});
