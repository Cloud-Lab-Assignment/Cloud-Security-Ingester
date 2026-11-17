import { TpcStateError, StoreConflict } from './errors.js';

export const UNCERTAIN = 'UNCERTAIN';
export const PRECOMMITTED = 'PRECOMMITTED';
export const COMMITTED = 'COMMITTED';
export const ABORTED = 'ABORTED';

// Three-phase commit participant. Pure state machine: storage, the business
// check (validate), the business effect (apply) and the clock are injected.
//
//   canCommit(new)            -> UNCERTAIN (yes) | ABORTED (no)
//   preCommit  UNCERTAIN      -> PRECOMMITTED
//   doCommit   PRECOMMITTED   -> apply + COMMITTED
//   abort      UNCERTAIN|PRECOMMITTED|unknown -> ABORTED
//   sweep: for each UNCERTAIN/PRECOMMITTED record past timeoutMs, first ask
//          the coordinator (optional `resolve(txId)`) for its decision:
//            'COMMITTED' -> apply + COMMITTED  'ABORTED' -> ABORTED
//            'PENDING'   -> untouched (the coordinator is alive and will finish it)
//            'UNKNOWN'   -> the coordinator never started the tx (it logs
//                           WAITING before any canCommit):
//                           UNCERTAIN -> ABORTED; PRECOMMITTED -> untouched,
//                           reported as pending and logged at error level
//                           (impossible with an intact coordinator log)
//            null / error / no answer within resolveTimeoutMs (coordinator
//            unreachable) -> timeout rule:
//              UNCERTAIN -> ABORTED; PRECOMMITTED -> apply + COMMITTED
//          Any other answer counts as null.
//          A record whose resolution throws (store error, or a commit the
//          store refuses for good) is logged at error level, reported as
//          pending and left as it was; the rest of the pass goes on.
//
// Store contract: get(txId), listPending(), put(record, expected), and
// optionally commit(record, expected). Every write passes the state this
// participant read ({state: null} = "must not exist"); the store throws
// StoreConflict when the record changed in between (another replica sharing
// the store wrote it). The operation is then re-read and re-evaluated, up to
// CONFLICT_ATTEMPTS times.
//
// "apply + COMMITTED" (R2-2): when the store has commit(record, expected),
// with record = {txId, state: 'COMMITTED', payload, updatedAt} and
// expected = {state: 'PRECOMMITTED' | 'UNCERTAIN'}, it is the only commit path:
// the store performs its business write for `payload` and the move to
// COMMITTED atomically, or writes nothing and throws StoreConflict. `apply`
// is then unused (and optional). Stores without commit get apply() followed by
// a conditional put; there a lost race can leave the business write behind on
// an ABORTED record, and `apply` must be idempotent because two replicas can
// both apply before one of them loses the write.
const CONFLICT_ATTEMPTS = 3;

const DECISIONS = new Set([COMMITTED, ABORTED, 'PENDING', 'UNKNOWN']);

export function createParticipant({
  store,
  validate,
  apply,
  timeoutMs,
  now = Date.now,
  forceVote,
  resolve,
  resolveTimeoutMs = 5000,
  logger,
}) {
  // Serialise work per txId inside this process so concurrent messages
  // (e.g. a doCommit racing the sweeper) cannot apply twice.
  const locks = new Map();
  function withLock(txId, fn) {
    const prev = locks.get(txId) ?? Promise.resolve();
    const run = prev.then(fn, fn);
    const tail = run.catch(() => {});
    locks.set(txId, tail);
    tail.then(() => {
      if (locks.get(txId) === tail) locks.delete(txId);
    });
    return run;
  }

  // Serialised per txId, and re-run from a fresh read on StoreConflict.
  function operation(txId, fn) {
    return withLock(txId, async () => {
      for (let attempt = 1; ; attempt += 1) {
        try {
          return await fn();
        } catch (err) {
          if (!(err instanceof StoreConflict) || attempt >= CONFLICT_ATTEMPTS) throw err;
        }
      }
    });
  }

  // `from` is the state that was read (null when there was no record).
  const record = (txId, state, payload, from) =>
    store.put({ txId, state, payload, updatedAt: now() }, { state: from ?? null });

  // rec is the record as read: PRECOMMITTED, or UNCERTAIN when the
  // coordinator reported COMMITTED.
  const atomicCommit = typeof store.commit === 'function';
  async function commitRecord(rec) {
    if (atomicCommit) {
      await store.commit({ txId: rec.txId, state: COMMITTED, payload: rec.payload, updatedAt: now() }, { state: rec.state });
      return;
    }
    await apply(rec.payload);
    await record(rec.txId, COMMITTED, rec.payload, rec.state);
  }

  function canCommit(txId, payload) {
    return operation(txId, async () => {
      const existing = await store.get(txId);
      if (existing) return existing.state === ABORTED ? 'no' : 'yes';
      let ok;
      if (forceVote === 'no') ok = false;
      else if (forceVote === 'yes') ok = true;
      else {
        try {
          ok = Boolean(await validate(txId, payload));
        } catch {
          ok = false;
        }
      }
      await record(txId, ok ? UNCERTAIN : ABORTED, payload, null);
      return ok ? 'yes' : 'no';
    });
  }

  function preCommit(txId) {
    return operation(txId, async () => {
      const rec = await store.get(txId);
      if (rec?.state === PRECOMMITTED) return;
      if (rec?.state !== UNCERTAIN) throw new TpcStateError(txId, rec?.state, 'preCommit');
      await record(txId, PRECOMMITTED, rec.payload, UNCERTAIN);
    });
  }

  function doCommit(txId) {
    return operation(txId, async () => {
      const rec = await store.get(txId);
      if (rec?.state === COMMITTED) return;
      if (rec?.state !== PRECOMMITTED) throw new TpcStateError(txId, rec?.state, 'doCommit');
      await commitRecord(rec);
    });
  }

  function abort(txId) {
    return operation(txId, async () => {
      const rec = await store.get(txId);
      if (rec?.state === ABORTED) return;
      if (rec?.state === COMMITTED) throw new TpcStateError(txId, COMMITTED, 'abort');
      // Unknown txId: leave a tombstone so a late canCommit votes no.
      await record(txId, ABORTED, rec?.payload ?? null, rec?.state ?? null);
    });
  }

  // The coordinator's decision for txId, or null when it cannot be had.
  async function decisionFor(txId) {
    if (!resolve) return null;
    let timer;
    try {
      const answer = await Promise.race([
        Promise.resolve().then(() => resolve(txId)),
        new Promise((r) => {
          timer = setTimeout(() => r(null), resolveTimeoutMs);
        }),
      ]);
      return DECISIONS.has(answer) ? answer : null;
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  const expired = (rec) => now() - rec.updatedAt > timeoutMs;
  const undecided = (rec) => rec?.state === UNCERTAIN || rec?.state === PRECOMMITTED;

  async function sweep() {
    const committed = [];
    const aborted = [];
    const pending = [];
    const candidates = await store.listPending();
    for (const candidate of candidates) {
      if (!expired(candidate)) continue;
      // Asked outside the per-tx lock so doCommit/abort messages are not held
      // up. A COMMITTED/ABORTED decision is final, so it stays valid below.
      const decision = await decisionFor(candidate.txId);
      if (decision === 'PENDING') {
        pending.push(candidate.txId);
        continue;
      }
      // One record failing (e.g. a commit the store refuses, R3-1) must not
      // stop the rest of the pass: it is logged, reported as pending and
      // stays undecided, so the next sweep (and the coordinator) retry it.
      await operation(candidate.txId, async () => {
        const rec = await store.get(candidate.txId);
        if (!undecided(rec) || !expired(rec)) return;
        if (decision === 'UNKNOWN' && rec.state === PRECOMMITTED) {
          // Someone sent preCommit for a tx the coordinator has no entry for:
          // neither commit nor abort is known to be safe. Leave it to an operator.
          logger?.error(
            { txId: rec.txId, state: rec.state },
            '3pc coordinator has no record of a PRECOMMITTED transaction; leaving it pending',
          );
          pending.push(rec.txId);
          return;
        }
        const commit = decision === COMMITTED || (decision === null && rec.state === PRECOMMITTED);
        if (commit) {
          await commitRecord(rec);
          committed.push(rec.txId);
        } else {
          await record(rec.txId, ABORTED, rec.payload, rec.state);
          aborted.push(rec.txId);
        }
      }).catch((err) => {
        logger?.error({ txId: candidate.txId, err }, '3pc sweep could not resolve the transaction; leaving it pending');
        pending.push(candidate.txId);
      });
    }
    return { committed, aborted, pending };
  }

  return { canCommit, preCommit, doCommit, abort, sweep };
}
