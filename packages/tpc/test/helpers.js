import { StoreConflict } from '../src/index.js';

// In-memory stores and a fake clock for the 3PC tests.
// put(record, expected) honours the conditional-write contract: when
// `expected` is given, the current state must equal expected.state (null =
// the record must not exist), otherwise StoreConflict is thrown.
//
// memoryStore({businessWrites}) also implements the atomic commit contract
// (R2-2): commit(record, expected) records the business write for
// record.payload in `businessWrites` (an array of {txId, payload}) and sets the
// state to COMMITTED in one step, or does neither and throws StoreConflict.
// A business write already present for the txId is not repeated (like the
// stores' unique key), so a retry after a crash only moves the state.
export function memoryStore({ businessWrites } = {}) {
  const map = new Map();
  const atomic = businessWrites
    ? {
        async commit(record, expected) {
          const current = map.get(record.txId)?.state ?? null;
          if (current !== expected.state) throw new StoreConflict(record.txId, expected.state, current);
          if (!businessWrites.some((w) => w.txId === record.txId)) {
            businessWrites.push({ txId: record.txId, payload: structuredClone(record.payload) });
          }
          map.set(record.txId, structuredClone(record));
        },
      }
    : {};
  return {
    map,
    ...atomic,
    async get(txId) {
      const r = map.get(txId);
      return r ? structuredClone(r) : null;
    },
    async put(record, expected) {
      if (expected !== undefined) {
        const current = map.get(record.txId)?.state ?? null;
        if (current !== expected.state) throw new StoreConflict(record.txId, expected.state, current);
      }
      map.set(record.txId, structuredClone(record));
    },
    async listPending() {
      return [...map.values()]
        .filter((r) => r.state === 'UNCERTAIN' || r.state === 'PRECOMMITTED')
        .map((r) => structuredClone(r));
    },
  };
}

export function memoryLog() {
  const map = new Map();
  const history = [];
  return {
    map,
    history,
    async put(entry) {
      map.set(entry.txId, structuredClone(entry));
      history.push(`${entry.txId}:${entry.state}`);
    },
    async listUnfinished() {
      return [...map.values()]
        .filter((e) => e.state === 'WAITING' || e.state === 'PRECOMMITTED')
        .map((e) => structuredClone(e));
    },
    async get(txId) {
      const e = map.get(txId);
      return e ? structuredClone(e) : null;
    },
  };
}

export function fakeClock(start = 1_000_000) {
  let t = start;
  return {
    now: () => t,
    advance: (ms) => {
      t += ms;
    },
  };
}
