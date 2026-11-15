import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createCoordinator, createParticipant, CoordinatorCrash, TpcStateError } from '../src/index.js';
import { memoryLog, memoryStore } from './helpers.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// A real participant state machine wrapped as a coordinator-facing adapter,
// with hooks to inject faults and a shared trace of calls.
function makeParticipant(name, trace, { vote = 'yes', hooks = {}, timeoutMs = 60_000 } = {}) {
  const store = memoryStore();
  const applied = [];
  const sm = createParticipant({
    store,
    validate: async () => vote === 'yes',
    apply: async (payload) => {
      applied.push(payload);
    },
    timeoutMs,
    now: Date.now,
  });
  const wrap = (phase) => async (...args) => {
    trace.push(`${name}:${phase}`);
    if (hooks[phase]) {
      const r = await hooks[phase](...args);
      if (r !== undefined) return r;
    }
    return sm[phase](...args);
  };
  return {
    adapter: {
      name,
      canCommit: wrap('canCommit'),
      preCommit: wrap('preCommit'),
      doCommit: wrap('doCommit'),
      abort: wrap('abort'),
    },
    sm,
    store,
    applied,
    state: async (txId) => (await store.get(txId))?.state ?? null,
  };
}

function coordinator(participants, extra = {}) {
  const log = memoryLog();
  const committed = [];
  const c = createCoordinator({
    log,
    participants: participants.map((p) => p.adapter),
    timeoutMs: 40,
    retryDelayMs: 1,
    now: Date.now,
    onCommitted: async (txId, payloads) => {
      committed.push({ txId, payloads });
    },
    ...extra,
  });
  return { c, log, committed };
}

const payloads = { a: { pa: 1 }, b: { pb: 2 } };

test('all yes → committed', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const b = makeParticipant('b', trace);
  const { c, log, committed } = coordinator([a, b]);
  const res = await c.run('tx1', payloads);
  assert.deepEqual(res, { outcome: 'committed' });
  // Phases strictly in order across participants.
  const phases = trace.map((t) => t.split(':')[1]);
  assert.deepEqual(phases, ['canCommit', 'canCommit', 'preCommit', 'preCommit', 'doCommit', 'doCommit']);
  assert.deepEqual(log.history, ['tx1:WAITING', 'tx1:PRECOMMITTED', 'tx1:COMMITTED']);
  assert.equal(committed.length, 1);
  assert.deepEqual(committed[0], { txId: 'tx1', payloads });
  assert.equal(await a.state('tx1'), 'COMMITTED');
  assert.equal(await b.state('tx1'), 'COMMITTED');
  assert.deepEqual(a.applied, [{ pa: 1 }]);
  assert.deepEqual(b.applied, [{ pb: 2 }]);
});

test('each participant receives only its own payload', async () => {
  const trace = [];
  const seen = {};
  const a = makeParticipant('a', trace, { hooks: { canCommit: async (txId, p) => { seen.a = p; } } });
  const b = makeParticipant('b', trace, { hooks: { canCommit: async (txId, p) => { seen.b = p; } } });
  const { c } = coordinator([a, b]);
  await c.run('tx1', payloads);
  assert.deepEqual(seen, { a: { pa: 1 }, b: { pb: 2 } });
});

test('one no → aborted everywhere', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const b = makeParticipant('b', trace, { vote: 'no' });
  const { c, log, committed } = coordinator([a, b]);
  const res = await c.run('tx1', payloads);
  assert.deepEqual(res, { outcome: 'aborted', reason: 'vote_no' });
  assert.equal(log.map.get('tx1').state, 'ABORTED');
  assert.equal(await a.state('tx1'), 'ABORTED');
  assert.equal(await b.state('tx1'), 'ABORTED');
  assert.ok(!trace.some((t) => t.endsWith(':preCommit')));
  assert.equal(committed.length, 0);
  assert.equal(a.applied.length + b.applied.length, 0);
});

test('canCommit error counts as a no vote', async () => {
  const trace = [];
  const a = makeParticipant('a', trace, { hooks: { canCommit: async () => { throw new Error('ECONNREFUSED'); } } });
  const b = makeParticipant('b', trace);
  const { c } = coordinator([a, b]);
  const res = await c.run('tx1', payloads);
  assert.deepEqual(res, { outcome: 'aborted', reason: 'vote_no' });
  assert.equal(await b.state('tx1'), 'ABORTED');
});

test('canCommit timeout → aborted', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const b = makeParticipant('b', trace, { hooks: { canCommit: async () => { await sleep(200); return 'yes'; } } });
  const { c, log } = coordinator([a, b]);
  const res = await c.run('tx1', payloads);
  assert.deepEqual(res, { outcome: 'aborted', reason: 'vote_timeout' });
  assert.equal(log.map.get('tx1').state, 'ABORTED');
  assert.equal(await a.state('tx1'), 'ABORTED');
  // b never recorded a vote, but the abort leaves a tombstone so a late canCommit votes no.
  assert.equal(await b.state('tx1'), 'ABORTED');
  assert.equal(await b.sm.canCommit('tx1', {}), 'no');
});

test('preCommit failure → aborted', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const b = makeParticipant('b', trace, { hooks: { preCommit: async () => { throw new Error('disk full'); } } });
  const { c, log, committed } = coordinator([a, b]);
  const res = await c.run('tx1', payloads);
  assert.deepEqual(res, { outcome: 'aborted', reason: 'precommit_failed' });
  assert.deepEqual(log.history, ['tx1:WAITING', 'tx1:PRECOMMITTED', 'tx1:ABORTED']);
  assert.equal(await a.state('tx1'), 'ABORTED');
  assert.equal(await b.state('tx1'), 'ABORTED');
  assert.ok(!trace.some((t) => t.endsWith(':doCommit')));
  assert.equal(committed.length, 0);
});

test('preCommit timeout → aborted', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const b = makeParticipant('b', trace, { hooks: { preCommit: async () => { await sleep(200); } } });
  const { c } = coordinator([a, b]);
  const res = await c.run('tx1', payloads);
  assert.deepEqual(res, { outcome: 'aborted', reason: 'precommit_failed' });
  assert.equal(await a.state('tx1'), 'ABORTED');
});

test('crash after precommit then recover commits', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const b = makeParticipant('b', trace);
  const crashing = coordinator([a, b], { fault: 'crash-after-precommit' });
  await assert.rejects(crashing.c.run('tx1', payloads), CoordinatorCrash);
  assert.equal(await a.state('tx1'), 'PRECOMMITTED');
  assert.equal(await b.state('tx1'), 'PRECOMMITTED');
  assert.equal(crashing.committed.length, 0);

  // A restarted coordinator shares the durable log but has no fault.
  const committed = [];
  const restarted = createCoordinator({
    log: crashing.log,
    participants: [a.adapter, b.adapter],
    timeoutMs: 40,
    retryDelayMs: 1,
    now: Date.now,
    onCommitted: async (txId, p) => committed.push({ txId, p }),
  });
  assert.deepEqual(await restarted.recover(), ['tx1']);
  assert.equal(crashing.log.map.get('tx1').state, 'COMMITTED');
  assert.equal(await a.state('tx1'), 'COMMITTED');
  assert.equal(await b.state('tx1'), 'COMMITTED');
  assert.deepEqual(committed, [{ txId: 'tx1', p: payloads }]);
  assert.equal(a.applied.length, 1);
  assert.equal(b.applied.length, 1);
});

test('recover commits when a participant already self-committed', async () => {
  const trace = [];
  const a = makeParticipant('a', trace, { timeoutMs: 1 });
  const b = makeParticipant('b', trace);
  const { c, log } = coordinator([a, b], { fault: 'crash-after-precommit' });
  await assert.rejects(c.run('tx1', payloads), CoordinatorCrash);
  await sleep(5);
  await a.sm.sweep(); // a times out in PRECOMMITTED and commits on its own
  assert.equal(await a.state('tx1'), 'COMMITTED');
  const r = createCoordinator({ log, participants: [a.adapter, b.adapter], timeoutMs: 40, retryDelayMs: 1, now: Date.now });
  assert.deepEqual(await r.recover(), ['tx1']);
  assert.equal(log.map.get('tx1').state, 'COMMITTED');
  assert.equal(await b.state('tx1'), 'COMMITTED');
  assert.equal(a.applied.length, 1);
});

test('recover aborts when a participant already self-aborted', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const b = makeParticipant('b', trace);
  // Simulate a crash after logging PRECOMMITTED but before b got preCommit:
  // b stayed UNCERTAIN and self-aborted on its timeout.
  const { log } = coordinator([a, b]);
  await a.sm.canCommit('tx1', payloads.a);
  await a.sm.preCommit('tx1');
  await b.sm.canCommit('tx1', payloads.b);
  await b.sm.abort('tx1');
  await log.put({ txId: 'tx1', state: 'PRECOMMITTED', payloads, updatedAt: Date.now() });

  const committed = [];
  const r = createCoordinator({
    log,
    participants: [a.adapter, b.adapter],
    timeoutMs: 40,
    retryDelayMs: 1,
    now: Date.now,
    onCommitted: async (txId) => committed.push(txId),
  });
  assert.deepEqual(await r.recover(), ['tx1']);
  assert.equal(log.map.get('tx1').state, 'ABORTED');
  assert.equal(await a.state('tx1'), 'ABORTED');
  assert.equal(await b.state('tx1'), 'ABORTED');
  assert.ok(trace.includes('a:abort') && trace.includes('b:abort'));
  assert.equal(committed.length, 0);
  assert.equal(a.applied.length, 0);
});

test('recover aborts WAITING entries', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const b = makeParticipant('b', trace);
  const { log } = coordinator([a, b]);
  await a.sm.canCommit('tx1', payloads.a);
  await log.put({ txId: 'tx1', state: 'WAITING', payloads, updatedAt: Date.now() });
  const r = createCoordinator({ log, participants: [a.adapter, b.adapter], timeoutMs: 40, retryDelayMs: 1, now: Date.now });
  assert.deepEqual(await r.recover(), ['tx1']);
  assert.equal(log.map.get('tx1').state, 'ABORTED');
  assert.equal(await a.state('tx1'), 'ABORTED');
  assert.equal(await b.state('tx1'), 'ABORTED');
});

test('recover ignores finished entries', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const { c } = coordinator([a]);
  await c.run('tx1', { a: {} });
  assert.deepEqual(await c.recover(), []);
});

test('abort is retried until acked', async () => {
  const trace = [];
  let failures = 0;
  const a = makeParticipant('a', trace, {
    hooks: {
      abort: async () => {
        if (failures < 2) {
          failures += 1;
          throw new Error('flaky network');
        }
      },
    },
  });
  const b = makeParticipant('b', trace, { vote: 'no' });
  const { c } = coordinator([a, b]);
  const res = await c.run('tx1', payloads);
  assert.equal(res.outcome, 'aborted');
  assert.equal(trace.filter((t) => t === 'a:abort').length, 3);
  assert.equal(await a.state('tx1'), 'ABORTED');
});

test('doCommit is retried until acked', async () => {
  const trace = [];
  let failures = 0;
  const a = makeParticipant('a', trace, {
    hooks: {
      doCommit: async () => {
        if (failures < 3) {
          failures += 1;
          throw new Error('flaky network');
        }
      },
    },
  });
  const { c } = coordinator([a]);
  const res = await c.run('tx1', { a: {} });
  assert.equal(res.outcome, 'committed');
  assert.equal(trace.filter((t) => t === 'a:doCommit').length, 4);
  assert.equal(await a.state('tx1'), 'COMMITTED');
});

test('retries give up after 5 attempts without throwing', async () => {
  const trace = [];
  const a = makeParticipant('a', trace, { hooks: { doCommit: async () => { throw new Error('down'); } } });
  const { c, log } = coordinator([a]);
  const res = await c.run('tx1', { a: {} });
  // The decision is final once every participant pre-committed; a missed
  // doCommit is covered by the participant's own PRECOMMITTED timeout.
  assert.equal(res.outcome, 'committed');
  assert.equal(trace.filter((t) => t === 'a:doCommit').length, 5);
  assert.equal(log.map.get('tx1').state, 'COMMITTED');
});

test('TpcStateError carries the participant state', () => {
  const e = new TpcStateError('tx', 'COMMITTED', 'abort');
  assert.equal(e.state, 'COMMITTED');
  assert.equal(e.name, 'TpcStateError');
  assert.ok(e instanceof Error);
});
