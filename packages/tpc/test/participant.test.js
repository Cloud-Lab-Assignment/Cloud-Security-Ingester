import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createParticipant, TpcStateError, StoreConflict } from '../src/index.js';
import { memoryStore, fakeClock } from './helpers.js';

function setup(overrides = {}) {
  const store = memoryStore();
  const clock = fakeClock();
  const applied = [];
  const p = createParticipant({
    store,
    validate: async () => true,
    apply: async (payload) => {
      applied.push(payload);
    },
    timeoutMs: 1000,
    now: clock.now,
    ...overrides,
  });
  return { p, store, clock, applied };
}

test('yes vote moves to UNCERTAIN', async () => {
  const { p, store } = setup();
  assert.equal(await p.canCommit('t1', { a: 1 }), 'yes');
  const rec = await store.get('t1');
  assert.equal(rec.state, 'UNCERTAIN');
  assert.deepEqual(rec.payload, { a: 1 });
});

test('validate false votes no and records ABORTED', async () => {
  const { p, store } = setup({ validate: async () => false });
  assert.equal(await p.canCommit('t1', {}), 'no');
  assert.equal((await store.get('t1')).state, 'ABORTED');
});

test('validate throwing votes no', async () => {
  const { p, store } = setup({ validate: async () => { throw new Error('bad'); } });
  assert.equal(await p.canCommit('t1', {}), 'no');
  assert.equal((await store.get('t1')).state, 'ABORTED');
});

test('forceVote no overrides validate', async () => {
  let called = false;
  const { p, store } = setup({ forceVote: 'no', validate: async () => { called = true; return true; } });
  assert.equal(await p.canCommit('t1', {}), 'no');
  assert.equal(called, false);
  assert.equal((await store.get('t1')).state, 'ABORTED');
});

test('repeat canCommit returns recorded vote', async () => {
  let calls = 0;
  const { p } = setup({ validate: async () => { calls += 1; return calls === 1; } });
  assert.equal(await p.canCommit('t1', {}), 'yes');
  assert.equal(await p.canCommit('t1', {}), 'yes');
  assert.equal(calls, 1);
  const { p: q } = setup({ validate: async () => false });
  assert.equal(await q.canCommit('t2', {}), 'no');
  assert.equal(await q.canCommit('t2', {}), 'no');
});

test('preCommit moves UNCERTAIN to PRECOMMITTED and repeats are no-ops', async () => {
  const { p, store } = setup();
  await p.canCommit('t1', {});
  await p.preCommit('t1');
  assert.equal((await store.get('t1')).state, 'PRECOMMITTED');
  await p.preCommit('t1');
  assert.equal((await store.get('t1')).state, 'PRECOMMITTED');
});

test('doCommit applies once even if repeated', async () => {
  const { p, store, applied } = setup();
  await p.canCommit('t1', { x: 1 });
  await p.preCommit('t1');
  await p.doCommit('t1');
  await p.doCommit('t1');
  assert.deepEqual(applied, [{ x: 1 }]);
  assert.equal((await store.get('t1')).state, 'COMMITTED');
});

test('doCommit from UNCERTAIN throws TpcStateError', async () => {
  const { p } = setup();
  await p.canCommit('t1', {});
  await assert.rejects(p.doCommit('t1'), TpcStateError);
});

test('doCommit on ABORTED throws TpcStateError', async () => {
  const { p, applied } = setup();
  await p.canCommit('t1', {});
  await p.abort('t1');
  await assert.rejects(p.doCommit('t1'), TpcStateError);
  assert.equal(applied.length, 0);
});

test('doCommit on unknown tx throws TpcStateError', async () => {
  const { p } = setup();
  await assert.rejects(p.doCommit('nope'), TpcStateError);
});

test('preCommit on unknown tx throws TpcStateError', async () => {
  const { p } = setup();
  await assert.rejects(p.preCommit('nope'), TpcStateError);
});

test('preCommit on ABORTED throws TpcStateError', async () => {
  const { p } = setup();
  await p.canCommit('t1', {});
  await p.abort('t1');
  await assert.rejects(p.preCommit('t1'), TpcStateError);
});

test('abort from UNCERTAIN and PRECOMMITTED, repeat is a no-op', async () => {
  const { p, store } = setup();
  await p.canCommit('t1', {});
  await p.abort('t1');
  await p.abort('t1');
  assert.equal((await store.get('t1')).state, 'ABORTED');
  await p.canCommit('t2', {});
  await p.preCommit('t2');
  await p.abort('t2');
  assert.equal((await store.get('t2')).state, 'ABORTED');
});

test('abort on COMMITTED throws TpcStateError', async () => {
  const { p } = setup();
  await p.canCommit('t1', {});
  await p.preCommit('t1');
  await p.doCommit('t1');
  await assert.rejects(p.abort('t1'), TpcStateError);
});

test('abort on unknown tx records ABORTED so a late canCommit votes no', async () => {
  const { p, store } = setup();
  await p.abort('late');
  assert.equal((await store.get('late')).state, 'ABORTED');
  assert.equal(await p.canCommit('late', {}), 'no');
});

test('sweep aborts UNCERTAIN older than timeoutMs', async () => {
  const { p, store, clock, applied } = setup();
  await p.canCommit('t1', {});
  clock.advance(1001);
  const res = await p.sweep();
  assert.deepEqual(res, { committed: [], aborted: ['t1'], pending: [] });
  assert.equal((await store.get('t1')).state, 'ABORTED');
  assert.equal(applied.length, 0);
});

test('sweep commits PRECOMMITTED older than timeoutMs', async () => {
  const { p, store, clock, applied } = setup();
  await p.canCommit('t1', { y: 2 });
  await p.preCommit('t1');
  clock.advance(1001);
  const res = await p.sweep();
  assert.deepEqual(res, { committed: ['t1'], aborted: [], pending: [] });
  assert.equal((await store.get('t1')).state, 'COMMITTED');
  assert.deepEqual(applied, [{ y: 2 }]);
});

test('sweep leaves fresh records alone', async () => {
  const { p, store, clock } = setup();
  await p.canCommit('t1', {});
  await p.canCommit('t2', {});
  await p.preCommit('t2');
  clock.advance(999);
  assert.deepEqual(await p.sweep(), { committed: [], aborted: [], pending: [] });
  assert.equal((await store.get('t1')).state, 'UNCERTAIN');
  assert.equal((await store.get('t2')).state, 'PRECOMMITTED');
});

test('preCommit refreshes the timeout clock', async () => {
  const { p, store, clock } = setup();
  await p.canCommit('t1', {});
  clock.advance(900);
  await p.preCommit('t1');
  clock.advance(900);
  assert.deepEqual(await p.sweep(), { committed: [], aborted: [], pending: [] });
  assert.equal((await store.get('t1')).state, 'PRECOMMITTED');
});

test('failed apply leaves PRECOMMITTED so doCommit can be retried', async () => {
  let fail = true;
  const applied = [];
  const { p, store } = setup({
    apply: async (payload) => {
      if (fail) throw new Error('db down');
      applied.push(payload);
    },
  });
  await p.canCommit('t1', { z: 3 });
  await p.preCommit('t1');
  await assert.rejects(p.doCommit('t1'), /db down/);
  assert.equal((await store.get('t1')).state, 'PRECOMMITTED');
  fail = false;
  await p.doCommit('t1');
  assert.deepEqual(applied, [{ z: 3 }]);
});

test('concurrent doCommit calls apply once', async () => {
  const applied = [];
  const { p } = setup({
    apply: async (payload) => {
      await new Promise((r) => setTimeout(r, 10));
      applied.push(payload);
    },
  });
  await p.canCommit('t1', { c: 1 });
  await p.preCommit('t1');
  await Promise.all([p.doCommit('t1'), p.doCommit('t1'), p.sweep()]);
  assert.equal(applied.length, 1);
});

// --- Conditional writes (P0-3): two replicas sharing one store ---

// A view of `shared` whose next put/commit first runs `hook` (once), simulating
// another replica writing between this replica's read and its write.
function interleaved(shared) {
  let hook = null;
  return {
    store: {
      get: (txId) => shared.get(txId),
      listPending: () => shared.listPending(),
      async put(record, expected) {
        const h = hook;
        hook = null;
        if (h) await h();
        return shared.put(record, expected);
      },
      // Only when the shared store has the atomic commit (R2-2).
      ...(shared.commit && {
        async commit(record, expected) {
          const h = hook;
          hook = null;
          if (h) await h();
          return shared.commit(record, expected);
        },
      }),
    },
    beforeNextPut(fn) {
      hook = fn;
    },
  };
}

function replicas({ validateA = async () => true, validateB = async () => true } = {}) {
  const shared = memoryStore();
  const clock = fakeClock();
  const view = interleaved(shared);
  const applied = [];
  const make = (store, validate, name) =>
    createParticipant({
      store,
      validate,
      apply: async (payload) => {
        applied.push({ name, payload });
      },
      timeoutMs: 1000,
      now: clock.now,
    });
  return {
    shared,
    clock,
    view,
    applied,
    a: make(view.store, validateA, 'a'),
    b: make(shared, validateB, 'b'),
  };
}

test('memory store: conditional put rejects a mismatched expected state', async () => {
  const s = memoryStore();
  await s.put({ txId: 't', state: 'UNCERTAIN' }, { state: null });
  await assert.rejects(s.put({ txId: 't', state: 'ABORTED' }, { state: null }), (e) => {
    assert.ok(e instanceof StoreConflict);
    assert.equal(e.name, 'StoreConflict');
    return true;
  });
  await assert.rejects(s.put({ txId: 't', state: 'COMMITTED' }, { state: 'PRECOMMITTED' }), StoreConflict);
  await assert.rejects(s.put({ txId: 'missing', state: 'PRECOMMITTED' }, { state: 'UNCERTAIN' }), StoreConflict);
  await s.put({ txId: 't', state: 'PRECOMMITTED' }, { state: 'UNCERTAIN' });
  assert.equal((await s.get('t')).state, 'PRECOMMITTED');
  // No expected state: unconditional (backwards compatible).
  await s.put({ txId: 't', state: 'ABORTED' });
  assert.equal((await s.get('t')).state, 'ABORTED');
});

test('participant passes the state it read on every put', async () => {
  const calls = [];
  const inner = memoryStore();
  const store = {
    get: (id) => inner.get(id),
    listPending: () => inner.listPending(),
    put: (rec, expected) => {
      calls.push([rec.state, expected]);
      return inner.put(rec, expected);
    },
  };
  const clock = fakeClock();
  const p = createParticipant({ store, validate: async () => true, apply: async () => {}, timeoutMs: 1000, now: clock.now });
  await p.canCommit('t1', {});
  await p.preCommit('t1');
  await p.doCommit('t1');
  await p.abort('t2');
  await p.canCommit('t3', {});
  await p.abort('t3');
  await p.canCommit('t4', {});
  clock.advance(1001);
  await p.sweep();
  assert.deepEqual(calls, [
    ['UNCERTAIN', { state: null }],
    ['PRECOMMITTED', { state: 'UNCERTAIN' }],
    ['COMMITTED', { state: 'PRECOMMITTED' }],
    ['ABORTED', { state: null }],
    ['UNCERTAIN', { state: null }],
    ['ABORTED', { state: 'UNCERTAIN' }],
    ['UNCERTAIN', { state: null }],
    ['ABORTED', { state: 'UNCERTAIN' }],
  ]);
});

test('race: sweep-abort on another replica beats preCommit → TpcStateError ABORTED', async () => {
  const { a, b, shared, clock, view } = replicas();
  await a.canCommit('t1', {});
  clock.advance(1001);
  view.beforeNextPut(async () => {
    assert.deepEqual(await b.sweep(), { committed: [], aborted: ['t1'], pending: [] });
  });
  await assert.rejects(a.preCommit('t1'), (e) => e instanceof TpcStateError && e.state === 'ABORTED');
  assert.equal((await shared.get('t1')).state, 'ABORTED');
});

test('race: preCommit on another replica beats a sweep-abort → record stays PRECOMMITTED', async () => {
  const { a, b, shared, clock, view } = replicas();
  await b.canCommit('t1', {});
  clock.advance(1001);
  view.beforeNextPut(() => b.preCommit('t1'));
  assert.deepEqual(await a.sweep(), { committed: [], aborted: [], pending: [] });
  assert.equal((await shared.get('t1')).state, 'PRECOMMITTED');
});

test('race: doCommit losing to another replica\'s commit is a no-op', async () => {
  const { a, b, shared, view, applied } = replicas();
  await a.canCommit('t1', { x: 1 });
  await a.preCommit('t1');
  view.beforeNextPut(() => b.doCommit('t1'));
  await a.doCommit('t1');
  assert.equal((await shared.get('t1')).state, 'COMMITTED');
  // apply must be idempotent: both replicas may have run it.
  assert.ok(applied.length >= 1 && applied.every((x) => x.payload.x === 1));
});

test('race: canCommit losing a create race returns the recorded vote', async () => {
  const { a, b, shared, view } = replicas({ validateB: async () => false });
  view.beforeNextPut(async () => {
    assert.equal(await b.canCommit('t1', {}), 'no');
  });
  assert.equal(await a.canCommit('t1', {}), 'no');
  assert.equal((await shared.get('t1')).state, 'ABORTED');
});

test('race: abort losing to another replica\'s commit → TpcStateError COMMITTED', async () => {
  const { a, b, shared, view } = replicas();
  await a.canCommit('t1', {});
  await a.preCommit('t1');
  view.beforeNextPut(() => b.doCommit('t1'));
  await assert.rejects(a.abort('t1'), (e) => e instanceof TpcStateError && e.state === 'COMMITTED');
  assert.equal((await shared.get('t1')).state, 'COMMITTED');
});

test('persistent conflicts are retried a bounded number of times, then rethrown', async () => {
  const inner = memoryStore();
  let puts = 0;
  const store = {
    get: (id) => inner.get(id),
    listPending: () => inner.listPending(),
    async put(rec) {
      puts += 1;
      throw new StoreConflict(rec.txId, null, 'UNCERTAIN');
    },
  };
  const p = createParticipant({ store, validate: async () => true, apply: async () => {}, timeoutMs: 1000 });
  await assert.rejects(p.canCommit('t1', {}), StoreConflict);
  assert.equal(puts, 3);
});

// --- Coordinator-decision lookup before acting on a timeout (P0-4) ---

// t1 is left UNCERTAIN and t2 PRECOMMITTED, both past the timeout.
async function expired(resolve, extra = {}) {
  const ctx = setup({ resolve, ...extra });
  await ctx.p.canCommit('t1', { n: 1 });
  await ctx.p.canCommit('t2', { n: 2 });
  await ctx.p.preCommit('t2');
  ctx.clock.advance(1001);
  return ctx;
}

const stateOf = async (store, txId) => (await store.get(txId)).state;

test('sweep: coordinator says COMMITTED → commit from PRECOMMITTED and from UNCERTAIN', async () => {
  const asked = [];
  const { p, store, applied } = await expired(async (txId) => {
    asked.push(txId);
    return 'COMMITTED';
  });
  const res = await p.sweep();
  assert.deepEqual(res, { committed: ['t1', 't2'], aborted: [], pending: [] });
  assert.deepEqual(asked.sort(), ['t1', 't2']);
  assert.equal(await stateOf(store, 't1'), 'COMMITTED');
  assert.equal(await stateOf(store, 't2'), 'COMMITTED');
  assert.deepEqual(applied, [{ n: 1 }, { n: 2 }]);
});

test('sweep: coordinator says ABORTED → abort, even a PRECOMMITTED record', async () => {
  const { p, store, applied } = await expired(async () => 'ABORTED');
  assert.deepEqual(await p.sweep(), { committed: [], aborted: ['t1', 't2'], pending: [] });
  assert.equal(await stateOf(store, 't1'), 'ABORTED');
  assert.equal(await stateOf(store, 't2'), 'ABORTED');
  assert.equal(applied.length, 0);
});

test('sweep: coordinator says PENDING → records left untouched', async () => {
  const { p, store, applied } = await expired(async () => 'PENDING');
  const before = [await store.get('t1'), await store.get('t2')];
  assert.deepEqual(await p.sweep(), { committed: [], aborted: [], pending: ['t1', 't2'] });
  assert.deepEqual([await store.get('t1'), await store.get('t2')], before);
  assert.equal(applied.length, 0);
});

test('sweep: unknown / failing / odd coordinator answers fall back to the timeout rule', async () => {
  const answers = [
    async () => null,
    async () => {
      throw new Error('ECONNREFUSED');
    },
    async () => 'WAITING',
    () => {
      throw new Error('sync throw');
    },
  ];
  for (const resolve of answers) {
    const { p, store, applied } = await expired(resolve);
    assert.deepEqual(await p.sweep(), { committed: ['t2'], aborted: ['t1'], pending: [] });
    assert.equal(await stateOf(store, 't1'), 'ABORTED');
    assert.equal(await stateOf(store, 't2'), 'COMMITTED');
    assert.deepEqual(applied, [{ n: 2 }]);
  }
});

test('sweep: a coordinator lookup that hangs counts as unreachable', async () => {
  const { p, store } = await expired(() => new Promise(() => {}), { resolveTimeoutMs: 20 });
  assert.deepEqual(await p.sweep(), { committed: ['t2'], aborted: ['t1'], pending: [] });
  assert.equal(await stateOf(store, 't1'), 'ABORTED');
});

test('sweep: the coordinator is not asked about fresh records', async () => {
  const asked = [];
  const { p, clock } = setup({
    resolve: async (txId) => {
      asked.push(txId);
      return 'ABORTED';
    },
  });
  await p.canCommit('t1', {});
  clock.advance(999);
  assert.deepEqual(await p.sweep(), { committed: [], aborted: [], pending: [] });
  assert.deepEqual(asked, []);
});

test('sweep: a record decided while the coordinator was being asked is left alone', async () => {
  let p;
  const ctx = setup({
    resolve: async (txId) => {
      await p.abort(txId); // the coordinator's abort arrives meanwhile
      return 'COMMITTED';
    },
  });
  p = ctx.p;
  await p.canCommit('t1', {});
  await p.preCommit('t1');
  ctx.clock.advance(1001);
  assert.deepEqual(await p.sweep(), { committed: [], aborted: [], pending: [] });
  assert.equal(await stateOf(ctx.store, 't1'), 'ABORTED');
  assert.equal(ctx.applied.length, 0);
});

// --- Atomic business write + COMMITTED (R2-2) ---

// "No business write without COMMITTED, and no COMMITTED without a business write."
async function assertAtomic(store, writes, txIds) {
  for (const txId of txIds) {
    const committed = (await store.get(txId))?.state === 'COMMITTED';
    const written = writes.some((w) => w.txId === txId);
    assert.equal(written, committed, `${txId}: business write ${written}, COMMITTED ${committed}`);
  }
}

// Two replicas on one atomic store. `apply` records into the same list, so a
// participant that still applied outside store.commit would show up.
function atomicReplicas() {
  const writes = [];
  const shared = memoryStore({ businessWrites: writes });
  const clock = fakeClock();
  const view = interleaved(shared);
  const make = (store) =>
    createParticipant({
      store,
      validate: async () => true,
      apply: async (payload) => {
        writes.push({ txId: payload.txId, payload, via: 'apply' });
      },
      timeoutMs: 1000,
      now: clock.now,
    });
  return { writes, shared, clock, view, a: make(view.store), b: make(shared) };
}

test('race: doCommit on one replica while another aborts → no business write, ABORTED', async () => {
  const { a, b, shared, view, writes } = atomicReplicas();
  await a.canCommit('t1', { txId: 't1', x: 1 });
  await a.preCommit('t1');
  view.beforeNextPut(() => b.abort('t1'));
  await assert.rejects(a.doCommit('t1'), (e) => e instanceof TpcStateError && e.state === 'ABORTED');
  assert.equal((await shared.get('t1')).state, 'ABORTED');
  assert.deepEqual(writes, []);
  await assertAtomic(shared, writes, ['t1']);
});

test('race: sweep timeout-commit on one replica while another aborts → no business write', async () => {
  const { a, b, shared, clock, view, writes } = atomicReplicas();
  await a.canCommit('t1', { txId: 't1' });
  await a.preCommit('t1');
  clock.advance(1001);
  view.beforeNextPut(() => b.abort('t1'));
  assert.deepEqual(await a.sweep(), { committed: [], aborted: [], pending: [] });
  assert.equal((await shared.get('t1')).state, 'ABORTED');
  await assertAtomic(shared, writes, ['t1']);
});

test('race: abort on one replica while another commits → one business write, COMMITTED', async () => {
  const { a, b, shared, view, writes } = atomicReplicas();
  await a.canCommit('t1', { txId: 't1' });
  await a.preCommit('t1');
  view.beforeNextPut(() => b.doCommit('t1'));
  await assert.rejects(a.abort('t1'), (e) => e instanceof TpcStateError && e.state === 'COMMITTED');
  assert.equal(writes.length, 1);
  await assertAtomic(shared, writes, ['t1']);
});

test('every commit path uses store.commit (expected state = the one read), never apply + put', async () => {
  const writes = [];
  const inner = memoryStore({ businessWrites: writes });
  const commits = [];
  const puts = [];
  const store = {
    get: (id) => inner.get(id),
    listPending: () => inner.listPending(),
    put: (rec, expected) => {
      puts.push(rec.state);
      return inner.put(rec, expected);
    },
    commit: (rec, expected) => {
      commits.push([rec.txId, rec.state, expected]);
      return inner.commit(rec, expected);
    },
  };
  const clock = fakeClock();
  let applied = 0;
  const decisions = { t3: 'COMMITTED', t4: 'COMMITTED' };
  const p = createParticipant({
    store,
    validate: async () => true,
    apply: async () => {
      applied += 1;
    },
    timeoutMs: 1000,
    now: clock.now,
    resolve: async (txId) => decisions[txId] ?? null,
  });
  await p.canCommit('t1', { n: 1 }); // doCommit
  await p.preCommit('t1');
  await p.doCommit('t1');
  await p.canCommit('t2', { n: 2 }); // sweep timeout rule, PRECOMMITTED
  await p.preCommit('t2');
  await p.canCommit('t3', { n: 3 }); // coordinator says COMMITTED, from UNCERTAIN
  await p.canCommit('t4', { n: 4 }); // coordinator says COMMITTED, from PRECOMMITTED
  await p.preCommit('t4');
  clock.advance(1001);
  assert.deepEqual(await p.sweep(), { committed: ['t2', 't3', 't4'], aborted: [], pending: [] });
  assert.equal(applied, 0);
  assert.ok(!puts.includes('COMMITTED'));
  assert.deepEqual(commits, [
    ['t1', 'COMMITTED', { state: 'PRECOMMITTED' }],
    ['t2', 'COMMITTED', { state: 'PRECOMMITTED' }],
    ['t3', 'COMMITTED', { state: 'UNCERTAIN' }],
    ['t4', 'COMMITTED', { state: 'PRECOMMITTED' }],
  ]);
  assert.deepEqual(
    writes.map((w) => [w.txId, w.payload]),
    [['t1', { n: 1 }], ['t2', { n: 2 }], ['t3', { n: 3 }], ['t4', { n: 4 }]],
  );
  // A repeated doCommit is a no-op.
  await p.doCommit('t1');
  assert.equal(commits.length, 4);
  await assertAtomic(inner, writes, ['t1', 't2', 't3', 't4']);
});

test('store.commit failing (store down) leaves PRECOMMITTED and nothing written; retry commits', async () => {
  const writes = [];
  const inner = memoryStore({ businessWrites: writes });
  let down = true;
  const store = {
    ...inner,
    get: (id) => inner.get(id),
    listPending: () => inner.listPending(),
    put: (r, e) => inner.put(r, e),
    commit: (r, e) => (down ? Promise.reject(new Error('db down')) : inner.commit(r, e)),
  };
  const p = createParticipant({ store, validate: async () => true, timeoutMs: 1000 });
  await p.canCommit('t1', { z: 3 });
  await p.preCommit('t1');
  await assert.rejects(p.doCommit('t1'), /db down/);
  assert.equal((await inner.get('t1')).state, 'PRECOMMITTED');
  assert.deepEqual(writes, []);
  down = false;
  await p.doCommit('t1');
  assert.deepEqual(writes, [{ txId: 't1', payload: { z: 3 } }]);
  await assertAtomic(inner, writes, ['t1']);
});

test('memory store commit: conditional, atomic and idempotent', async () => {
  const writes = [];
  const s = memoryStore({ businessWrites: writes });
  const rec = { txId: 't', state: 'COMMITTED', payload: { a: 1 }, updatedAt: 2 };
  await s.put({ txId: 't', state: 'UNCERTAIN', payload: { a: 1 }, updatedAt: 1 }, { state: null });
  await assert.rejects(s.commit(rec, { state: 'PRECOMMITTED' }), StoreConflict);
  assert.deepEqual(writes, []);
  await s.commit(rec, { state: 'UNCERTAIN' });
  assert.equal((await s.get('t')).state, 'COMMITTED');
  // Retry after a crash: already COMMITTED → conflict, nothing written twice.
  await assert.rejects(s.commit(rec, { state: 'UNCERTAIN' }), StoreConflict);
  assert.equal(writes.length, 1);
  // A business write left by pre-R2-2 code (apply ran, COMMITTED was not
  // recorded): commit only moves the state.
  writes.push({ txId: 'legacy', payload: {} });
  await s.put({ txId: 'legacy', state: 'PRECOMMITTED', payload: {}, updatedAt: 1 }, { state: null });
  await s.commit({ txId: 'legacy', state: 'COMMITTED', payload: {}, updatedAt: 2 }, { state: 'PRECOMMITTED' });
  assert.equal((await s.get('legacy')).state, 'COMMITTED');
  assert.equal(writes.filter((w) => w.txId === 'legacy').length, 1);
  // Without businessWrites there is no commit (apply + put path).
  assert.equal(memoryStore().commit, undefined);
});

// --- R2-3: the coordinator never started the tx ('UNKNOWN') ---

function recordingLogger() {
  const lines = [];
  const at = (level) => (obj, msg) => lines.push({ level, obj, msg });
  return { lines, error: at('error'), warn: at('warn'), info: at('info'), debug: at('debug') };
}

test("sweep: coordinator says UNKNOWN → abort UNCERTAIN, keep PRECOMMITTED pending and log an error", async () => {
  const logger = recordingLogger();
  const { p, store, applied } = await expired(async () => 'UNKNOWN', { logger });
  const before = await store.get('t2');
  assert.deepEqual(await p.sweep(), { committed: [], aborted: ['t1'], pending: ['t2'] });
  assert.equal(await stateOf(store, 't1'), 'ABORTED');
  assert.deepEqual(await store.get('t2'), before);
  assert.equal(applied.length, 0);
  const errors = logger.lines.filter((l) => l.level === 'error');
  assert.equal(errors.length, 1);
  assert.equal(errors[0].obj.txId, 't2');
  // Still there on the next sweep, still not committed.
  assert.deepEqual(await p.sweep(), { committed: [], aborted: [], pending: ['t2'] });
  assert.equal(await stateOf(store, 't2'), 'PRECOMMITTED');
});

test('sweep: UNKNOWN without a logger still keeps PRECOMMITTED pending', async () => {
  const { p, store } = await expired(async () => 'UNKNOWN');
  assert.deepEqual(await p.sweep(), { committed: [], aborted: ['t1'], pending: ['t2'] });
  assert.equal(await stateOf(store, 't2'), 'PRECOMMITTED');
});

// --- R3-1: a commit the store refuses for good must not stop the sweep ---

test('sweep: a commit that fails for one record leaves it PRECOMMITTED (pending, logged) and the sweep goes on', async () => {
  const logger = recordingLogger();
  const writes = [];
  const inner = memoryStore({ businessWrites: writes });
  const store = {
    ...inner,
    get: (id) => inner.get(id),
    listPending: () => inner.listPending(),
    put: (r, e) => inner.put(r, e),
    commit: (r, e) => (r.txId === 'bad' ? Promise.reject(new Error('history row belongs to another tx')) : inner.commit(r, e)),
  };
  const clock = fakeClock();
  const p = createParticipant({ store, validate: async () => true, timeoutMs: 1000, now: clock.now, logger });
  for (const id of ['bad', 'good']) {
    await p.canCommit(id, { id });
    await p.preCommit(id);
  }
  await assert.rejects(p.doCommit('bad'), /another tx/);
  clock.advance(1001);
  assert.deepEqual(await p.sweep(), { committed: ['good'], aborted: [], pending: ['bad'] });
  assert.equal((await inner.get('bad')).state, 'PRECOMMITTED');
  assert.equal((await inner.get('good')).state, 'COMMITTED');
  assert.deepEqual(writes.map((w) => w.txId), ['good']);
  const errors = logger.lines.filter((l) => l.level === 'error');
  assert.equal(errors.length, 1);
  assert.equal(errors[0].obj.txId, 'bad');
});
