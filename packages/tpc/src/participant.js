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
//   doCommit   PRECOMMITTED   -> apply, COMMITTED
//   abort      UNCERTAIN|PRECOMMITTED|unknown -> ABORTED
//   sweep: UNCERTAIN past timeout -> ABORTED; PRECOMMITTED past timeout -> apply, COMMITTED
//
// Store contract: get(txId), listPending(), put(record, expected). Every put
// passes the state this participant read ({state: null} = "must not exist");
// the store throws StoreConflict when the record changed in between (another
// replica sharing the store wrote it). The operation is then re-read and
// re-evaluated, up to CONFLICT_ATTEMPTS times. `apply` must be idempotent:
// two replicas can both apply a commit before one of them loses the write.
const CONFLICT_ATTEMPTS = 3;

export function createParticipant({ store, validate, apply, timeoutMs, now = Date.now, forceVote }) {
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

  async function commitRecord(rec) {
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

  async function sweep() {
    const committed = [];
    const aborted = [];
    const pending = await store.listPending();
    for (const candidate of pending) {
      if (now() - candidate.updatedAt <= timeoutMs) continue;
      await operation(candidate.txId, async () => {
        const rec = await store.get(candidate.txId);
        if (!rec || now() - rec.updatedAt <= timeoutMs) return;
        if (rec.state === UNCERTAIN) {
          await record(rec.txId, ABORTED, rec.payload, UNCERTAIN);
          aborted.push(rec.txId);
        } else if (rec.state === PRECOMMITTED) {
          await commitRecord(rec);
          committed.push(rec.txId);
        }
      });
    }
    return { committed, aborted };
  }

  return { canCommit, preCommit, doCommit, abort, sweep };
}
