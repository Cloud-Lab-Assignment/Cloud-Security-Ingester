// Read-only REST client for Event Store and Incident Management, with a tiny
// TTL cache (read-optimised: analysts re-run the same lookups) and one
// deadline per call. Failures surface as UpstreamError -> 502/504 upstream.

export class UpstreamError extends Error {
  constructor(message, { status = 502, upstream } = {}) {
    super(message);
    this.name = 'UpstreamError';
    this.status = status;
    this.upstream = upstream;
  }
}

export function createUpstream({ baseUrl, name, timeoutMs, cacheTtlMs, fetch = globalThis.fetch, now = Date.now }) {
  const base = baseUrl.replace(/\/+$/, '');
  const cache = new Map(); // url -> {at, value}
  const MAX_ENTRIES = 500;

  async function get(path, query = {}) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null && v !== '') params.set(k, String(v));
    }
    const qs = params.toString();
    const url = `${base}${path}${qs ? `?${qs}` : ''}`;

    const hit = cache.get(url);
    if (hit && now() - hit.at < cacheTtlMs) return hit.value;

    let res;
    try {
      res = await fetch(url, { signal: AbortSignal.timeout(timeoutMs), headers: { accept: 'application/json' } });
    } catch (err) {
      const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
      throw new UpstreamError(`${name} ${timedOut ? 'timed out' : 'unreachable'}`, {
        status: timedOut ? 504 : 502,
        upstream: name,
      });
    }

    if (res.status === 404) {
      await res.body?.cancel().catch(() => {});
      return null;
    }
    if (res.status === 400) {
      const body = await res.json().catch(() => ({}));
      throw new UpstreamError(body.message ?? body.error ?? 'bad request', { status: 400, upstream: name });
    }
    if (!res.ok) {
      await res.body?.cancel().catch(() => {});
      throw new UpstreamError(`${name} returned ${res.status}`, { status: 502, upstream: name });
    }

    const value = await res.json();
    if (cache.size >= MAX_ENTRIES) cache.delete(cache.keys().next().value);
    cache.set(url, { at: now(), value });
    return value;
  }

  return { get };
}
