// Asks the 3PC coordinator (incident-management) for its decision on a
// transaction, so the participant's sweeper can follow it instead of guessing
// from its own timeout. GET /tpc/transactions/:txId returns {txId, state, ...}
// with the coordinator log state WAITING | PRECOMMITTED | COMMITTED | ABORTED.
const DECISION = {
  COMMITTED: 'COMMITTED',
  ABORTED: 'ABORTED',
  WAITING: 'PENDING',
  PRECOMMITTED: 'PENDING',
};

// Returns resolve(txId) -> 'COMMITTED' | 'ABORTED' | 'PENDING' | null, or
// undefined when `url` is empty (lookup disabled: pure timeout rule).
// null means "no decision available" (unknown txId, error, timeout).
export function createCoordinatorClient({ url, timeoutMs = 2000, fetch = globalThis.fetch, logger }) {
  if (!url) return undefined;
  const base = url.replace(/\/+$/, '');
  return async function resolve(txId) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error(`coordinator lookup timed out after ${timeoutMs} ms`)), timeoutMs);
    try {
      const res = await fetch(`${base}/tpc/transactions/${encodeURIComponent(txId)}`, {
        method: 'GET',
        headers: { accept: 'application/json' },
        signal: controller.signal,
      });
      if (res.status === 404) return null;
      if (!res.ok) {
        logger?.warn({ txId, status: res.status }, '3pc coordinator lookup failed');
        return null;
      }
      const body = await res.json();
      return DECISION[body?.state] ?? null;
    } catch (err) {
      logger?.warn({ txId, err: err?.message }, '3pc coordinator unreachable; using the timeout rule');
      return null;
    } finally {
      clearTimeout(timer);
    }
  };
}
