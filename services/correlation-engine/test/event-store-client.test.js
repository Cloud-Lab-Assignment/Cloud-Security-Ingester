import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createEventStoreClient } from '../src/event-store-client.js';
import { silentLogger } from './fixtures.js';

const FROM = '2026-11-16T09:55:00.000Z';
const TO = '2026-11-16T10:00:00.000Z';

function ev(i, { userId = null, userName = null, outcome = 'failure' } = {}) {
  return { eventId: `e-${i}`, entityKey: '203.0.113.7', outcome, actor: { userId, userName, userType: 'user' } };
}

// Fake fetch serving `pages` in order; records each requested URL.
function fakeFetch(pages) {
  const urls = [];
  const fn = async (url, opts) => {
    urls.push(new URL(url));
    assert.ok(opts?.signal instanceof AbortSignal, 'fetch gets an abort signal');
    const page = pages[urls.length - 1];
    if (page instanceof Error) throw page;
    if (typeof page === 'number') return new Response('{"error":"x"}', { status: page });
    return Response.json(page);
  };
  fn.urls = urls;
  return fn;
}

const client = (fetch, extra = {}) =>
  createEventStoreClient({ baseUrl: 'http://es:8081/', timeoutMs: 500, fetch, logger: silentLogger, ...extra });

test('summarises events across pages via nextCursor', async () => {
  const fetch = fakeFetch([
    { items: [ev(1, { userId: 'u1' }), ev(2, { userId: 'u1' }), ev(3, { userName: 'bob', outcome: 'success' })], nextCursor: 'abc' },
    { items: [ev(4), ev(5, { userId: 'u2', outcome: 'unknown' })], nextCursor: null },
  ]);
  const res = await client(fetch).fetchEnrichment('203.0.113.7', FROM, TO);
  assert.deepEqual(res, { storedEventCount: 5, distinctUsers: 3, failures: 3 });
  assert.equal(fetch.urls.length, 2);
  const [first, second] = fetch.urls;
  assert.equal(first.origin + first.pathname, 'http://es:8081/events');
  assert.equal(first.searchParams.get('entityKey'), '203.0.113.7');
  assert.equal(first.searchParams.get('from'), FROM);
  assert.equal(first.searchParams.get('to'), TO);
  assert.equal(first.searchParams.get('limit'), '500');
  assert.equal(first.searchParams.get('cursor'), null);
  assert.equal(second.searchParams.get('cursor'), 'abc');
  assert.equal(second.searchParams.get('limit'), '497');
});

test('caps at 500 items in total', async () => {
  const page = (start, n, cursor) => ({ items: Array.from({ length: n }, (_, i) => ev(start + i)), nextCursor: cursor });
  const fetch = fakeFetch([page(0, 300, 'c1'), page(300, 300, 'c2'), page(600, 300, null)]);
  const res = await client(fetch).fetchEnrichment('203.0.113.7', FROM, TO);
  assert.equal(res.storedEventCount, 500);
  assert.equal(res.failures, 500);
  assert.equal(fetch.urls.length, 2);
  assert.equal(fetch.urls[1].searchParams.get('limit'), '200');
});

test('IPv6 entityKey is URL-encoded', async () => {
  const fetch = fakeFetch([{ items: [], nextCursor: null }]);
  const res = await client(fetch).fetchEnrichment('2001:db8::1', FROM, TO);
  assert.deepEqual(res, { storedEventCount: 0, distinctUsers: 0, failures: 0 });
  const url = fetch.urls[0];
  assert.match(url.search, /entityKey=2001%3Adb8%3A%3A1(&|$)/);
  assert.match(url.search, /from=2026-11-16T09%3A55%3A00.000Z/);
  assert.equal(url.searchParams.get('entityKey'), '2001:db8::1');
});

test('returns null on network error, non-2xx or bad body', async () => {
  assert.equal(await client(fakeFetch([new TypeError('fetch failed')])).fetchEnrichment('x', FROM, TO), null);
  assert.equal(await client(fakeFetch([500])).fetchEnrichment('x', FROM, TO), null);
  assert.equal(await client(fakeFetch([400])).fetchEnrichment('x', FROM, TO), null);
  assert.equal(await client(fakeFetch([{ nope: true }])).fetchEnrichment('x', FROM, TO), null);
  // a failure on a later page also yields null rather than a partial count
  const fetch = fakeFetch([{ items: [ev(1)], nextCursor: 'c' }, 503]);
  assert.equal(await client(fetch).fetchEnrichment('x', FROM, TO), null);
});

test('returns null when the overall deadline passes', async (t) => {
  // AbortSignal.timeout uses an unref'd timer; keep the loop alive for the test.
  const keepAlive = setInterval(() => {}, 1000);
  t.after(() => clearInterval(keepAlive));
  const hanging = async (url, { signal }) =>
    new Promise((_, reject) => {
      signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    });
  const started = Date.now();
  const res = await client(hanging, { timeoutMs: 50 }).fetchEnrichment('x', FROM, TO);
  assert.equal(res, null);
  assert.ok(Date.now() - started < 1000);
});
