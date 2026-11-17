// REST client for the Event Store's GET /events, used to enrich a candidate.
// Any failure (timeout, network error, non-2xx, bad body) yields null so the
// engine upserts with `enrichment: null` instead of retrying forever.

const MAX_ITEMS = 500;

export function createEventStoreClient({ baseUrl, timeoutMs, fetch = globalThis.fetch, logger }) {
  const base = baseUrl.replace(/\/+$/, '');

  async function fetchEnrichment(entityKey, windowStart, windowEnd) {
    // One deadline for the whole paging loop, not per request.
    const signal = AbortSignal.timeout(timeoutMs);
    const items = [];
    let cursor = null;
    try {
      do {
        // URLSearchParams percent-encodes ':' (IPv6) and '|' safely.
        const params = new URLSearchParams({
          entityKey,
          from: windowStart,
          to: windowEnd,
          limit: String(MAX_ITEMS - items.length),
        });
        if (cursor) params.set('cursor', cursor);
        const res = await fetch(`${base}/events?${params}`, { signal, headers: { accept: 'application/json' } });
        if (!res.ok) {
          await res.body?.cancel().catch(() => {});
          logger?.warn({ status: res.status, entityKey }, 'event-store enrichment request failed');
          return null;
        }
        const body = await res.json();
        if (!body || !Array.isArray(body.items)) {
          logger?.warn({ entityKey }, 'event-store enrichment response has no items array');
          return null;
        }
        items.push(...body.items.slice(0, MAX_ITEMS - items.length));
        cursor = typeof body.nextCursor === 'string' && body.nextCursor ? body.nextCursor : null;
      } while (cursor && items.length < MAX_ITEMS);
    } catch (err) {
      logger?.warn({ err, entityKey, timeoutMs }, 'event-store enrichment unavailable');
      return null;
    }
    return summarize(items);
  }

  return { fetchEnrichment };
}

function summarize(items) {
  const users = new Set();
  let failures = 0;
  for (const item of items) {
    const user = item?.actor?.userId ?? item?.actor?.userName;
    if (user !== null && user !== undefined && user !== '') users.add(String(user));
    if (item?.outcome === 'failure') failures += 1;
  }
  return { storedEventCount: items.length, distinctUsers: users.size, failures };
}
