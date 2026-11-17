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

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

// Returns resolve(txId) -> 'COMMITTED' | 'ABORTED' | 'PENDING' | 'UNKNOWN' | null
// (R2-3), or undefined when `url` is empty (lookup disabled: pure timeout rule).
//
//   200 COMMITTED / ABORTED            -> that decision
//   200 WAITING / PRECOMMITTED         -> 'PENDING' (still being decided)
//   404                                -> 'UNKNOWN': the coordinator logs WAITING
//                                         before any canCommit, so it never
//                                         started this transaction
//   5xx, other statuses, malformed body or an unknown state
//                                      -> 'PENDING': the coordinator is alive
//                                         but gave no usable answer; keep the
//                                         state and ask again on the next sweep
//   transport failure (connection refused, DNS, socket error)
//                                      -> null: unreachable, timeout rule
//   no complete answer within timeoutMs -> null as well. Known limit: a
//                                         coordinator that is alive but slower
//                                         than this can still lead to a split
//                                         (the participant self-commits or
//                                         self-aborts while the coordinator
//                                         decides otherwise); only a termination
//                                         protocol or consensus removes that.
export function createCoordinatorClient({ url, timeoutMs = 2000, fetch = globalThis.fetch, logger }) {
  if (!url) return undefined;
  const base = url.replace(/\/+$/, '');
  return async function resolve(txId) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error(`coordinator lookup timed out after ${timeoutMs} ms`)), timeoutMs);
    const timedOut = () => controller.signal.aborted;
    try {
      let res;
      try {
        res = await fetch(`${base}/tpc/transactions/${encodeURIComponent(txId)}`, {
          method: 'GET',
          headers: { accept: 'application/json' },
          signal: controller.signal,
        });
      } catch (err) {
        if (timedOut()) logger?.warn({ txId, timeoutMs }, '3pc coordinator lookup timed out; using the timeout rule');
        else logger?.warn({ txId, err: err?.message, code: err?.cause?.code }, '3pc coordinator unreachable; using the timeout rule');
        return null;
      }
      if (res.status === 404) {
        await res.body?.cancel().catch(() => {});
        return 'UNKNOWN';
      }
      if (!res.ok) {
        await res.body?.cancel().catch(() => {});
        logger?.warn({ txId, status: res.status }, '3pc coordinator lookup failed; retrying on the next sweep');
        return 'PENDING';
      }
      let body;
      try {
        body = await res.json();
      } catch (err) {
        if (timedOut()) {
          logger?.warn({ txId, timeoutMs }, '3pc coordinator lookup timed out; using the timeout rule');
          return null;
        }
        logger?.warn({ txId, err: err?.message }, '3pc coordinator sent a malformed body; retrying on the next sweep');
        return 'PENDING';
      }
      const decision = isObject(body) && Object.hasOwn(DECISION, body.state) ? DECISION[body.state] : undefined;
      if (decision) return decision;
      logger?.warn({ txId, state: isObject(body) ? body.state : undefined }, '3pc coordinator sent an unknown state; retrying on the next sweep');
      return 'PENDING';
    } finally {
      clearTimeout(timer);
    }
  };
}
