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

// A view of `shared` whose next put first runs `hook` (once), simulating
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
