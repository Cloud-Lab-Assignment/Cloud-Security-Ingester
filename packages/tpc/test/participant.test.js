import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createParticipant, TpcStateError } from '../src/index.js';
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
  assert.deepEqual(res, { committed: [], aborted: ['t1'] });
  assert.equal((await store.get('t1')).state, 'ABORTED');
  assert.equal(applied.length, 0);
});

test('sweep commits PRECOMMITTED older than timeoutMs', async () => {
  const { p, store, clock, applied } = setup();
  await p.canCommit('t1', { y: 2 });
  await p.preCommit('t1');
  clock.advance(1001);
  const res = await p.sweep();
  assert.deepEqual(res, { committed: ['t1'], aborted: [] });
  assert.equal((await store.get('t1')).state, 'COMMITTED');
  assert.deepEqual(applied, [{ y: 2 }]);
});

test('sweep leaves fresh records alone', async () => {
  const { p, store, clock } = setup();
  await p.canCommit('t1', {});
  await p.canCommit('t2', {});
  await p.preCommit('t2');
  clock.advance(999);
  assert.deepEqual(await p.sweep(), { committed: [], aborted: [] });
  assert.equal((await store.get('t1')).state, 'UNCERTAIN');
  assert.equal((await store.get('t2')).state, 'PRECOMMITTED');
});

test('preCommit refreshes the timeout clock', async () => {
  const { p, store, clock } = setup();
  await p.canCommit('t1', {});
  clock.advance(900);
  await p.preCommit('t1');
  clock.advance(900);
  assert.deepEqual(await p.sweep(), { committed: [], aborted: [] });
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
