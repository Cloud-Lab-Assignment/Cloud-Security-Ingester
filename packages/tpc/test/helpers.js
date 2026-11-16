import { StoreConflict } from '../src/index.js';

// In-memory stores and a fake clock for the 3PC tests.
// put(record, expected) honours the conditional-write contract: when
// `expected` is given, the current state must equal expected.state (null =
// the record must not exist), otherwise StoreConflict is thrown.
export function memoryStore() {
  const map = new Map();
  return {
    map,
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
