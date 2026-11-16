import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createCoordinator, createParticipant, CoordinatorCrash, CoordinatorBusy, TpcStateError } from '../src/index.js';
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
  const { c, log, committed } = coordinator([a]);
  const res = await c.run('tx1', { a: {} });
  // The decision is final once every participant pre-committed, so the caller
  // still sees "committed". But the unacked doCommit keeps the log entry
  // PRECOMMITTED and onCommitted is not called until recover() finishes it.
  assert.deepEqual(res, { outcome: 'committed', pending: ['a'] });
  assert.equal(trace.filter((t) => t === 'a:doCommit').length, 5);
  assert.equal(log.map.get('tx1').state, 'PRECOMMITTED');
  assert.equal(committed.length, 0);
  assert.equal(await a.state('tx1'), 'PRECOMMITTED');
});

test('a later recover() finishes a commit whose doCommit was unacked', async () => {
  const trace = [];
  let down = true;
  const a = makeParticipant('a', trace, {
    hooks: {
      doCommit: async () => {
        if (down) throw new Error('down');
      },
    },
  });
  const b = makeParticipant('b', trace);
  const { c, log, committed } = coordinator([a, b]);
  const res = await c.run('tx1', payloads);
  assert.deepEqual(res, { outcome: 'committed', pending: ['a'] });
  assert.equal(await b.state('tx1'), 'COMMITTED');
  assert.equal(committed.length, 0);

  // Still unreachable: recovery leaves it unfinished.
  assert.deepEqual(await c.recover(), []);
  assert.equal(log.map.get('tx1').state, 'PRECOMMITTED');
  assert.equal(committed.length, 0);

  down = false;
  assert.deepEqual(await c.recover(), ['tx1']);
  assert.equal(log.map.get('tx1').state, 'COMMITTED');
  assert.equal(await a.state('tx1'), 'COMMITTED');
  assert.deepEqual(committed, [{ txId: 'tx1', payloads }]);
  assert.equal(a.applied.length, 1);
  assert.equal(b.applied.length, 1);
  assert.deepEqual(await c.recover(), []);
});

test('TpcStateError carries the participant state', () => {
  const e = new TpcStateError('tx', 'COMMITTED', 'abort');
  assert.equal(e.state, 'COMMITTED');
  assert.equal(e.name, 'TpcStateError');
  assert.ok(e instanceof Error);
});

test('recover() skips a transaction that run() is still driving', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const b = makeParticipant('b', trace, {
    hooks: {
      canCommit: async () => {
        await sleep(20); // slow vote, still inside the coordinator timeout
      },
    },
  });
  const { c, log, committed } = coordinator([a, b]);
  const running = c.run('tx1', payloads);
  await sleep(5);
  assert.equal(log.map.get('tx1').state, 'WAITING');
  assert.deepEqual(c.inFlight(), ['tx1']);
  assert.deepEqual(await c.recover(), []);
  assert.equal(log.map.get('tx1').state, 'WAITING');
  assert.ok(!trace.some((t) => t.endsWith(':abort') || t.endsWith(':preCommit')));
  assert.deepEqual(await running, { outcome: 'committed' });
  assert.deepEqual(log.history, ['tx1:WAITING', 'tx1:PRECOMMITTED', 'tx1:COMMITTED']);
  assert.equal(committed.length, 1);
  assert.equal(await a.state('tx1'), 'COMMITTED');
  assert.equal(await b.state('tx1'), 'COMMITTED');
  assert.deepEqual(c.inFlight(), []);
});

test('a run() that threw is no longer in flight, so recover() finishes it', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const { c, log } = coordinator([a], { fault: 'crash-after-precommit' });
  await assert.rejects(c.run('tx1', { a: {} }), CoordinatorCrash);
  assert.deepEqual(await c.recover(), ['tx1']);
  assert.equal(log.map.get('tx1').state, 'COMMITTED');
});

test('concurrent recover() calls do not overlap: the second joins the first', async () => {
  const trace = [];
  const a = makeParticipant('a', trace, {
    hooks: {
      preCommit: async () => {
        await sleep(5);
      },
    },
  });
  const { log } = coordinator([a]);
  await a.sm.canCommit('tx1', {});
  await log.put({ txId: 'tx1', state: 'PRECOMMITTED', payloads: { a: {} }, updatedAt: Date.now() });
  const committed = [];
  const r = createCoordinator({
    log,
    participants: [a.adapter],
    timeoutMs: 40,
    retryDelayMs: 1,
    onCommitted: async (txId) => committed.push(txId),
  });
  const [first, second] = await Promise.all([r.recover(), r.recover()]);
  assert.deepEqual(first, ['tx1']);
  assert.deepEqual(second, ['tx1']);
  assert.equal(trace.filter((t) => t === 'a:preCommit').length, 1);
  assert.equal(trace.filter((t) => t === 'a:doCommit').length, 1);
  assert.deepEqual(committed, ['tx1']);
  // Once finished, a new call runs again.
  assert.deepEqual(await r.recover(), []);
});

test('participant PRECOMMITTED but crashed before acking; coordinator aborted → sweep asks and aborts (case a)', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  // b persists PRECOMMITTED, then "crashes": the ack is lost and b stays
  // unreachable for the coordinator's abort.
  let bDown = false;
  const bStore = memoryStore();
  const bApplied = [];
  const { log } = coordinator([]);
  const decision = async (txId) => {
    const s = log.map.get(txId)?.state;
    if (s === 'COMMITTED' || s === 'ABORTED') return s;
    return s ? 'PENDING' : null;
  };
  let t = Date.now();
  const bSm = createParticipant({
    store: bStore,
    validate: async () => true,
    apply: async (p) => bApplied.push(p),
    timeoutMs: 1000,
    now: () => t,
    resolve: decision,
  });
  const b = {
    name: 'b',
    canCommit: (...args) => bSm.canCommit(...args),
    preCommit: async (txId) => {
      await bSm.preCommit(txId);
      bDown = true;
      throw new Error('crashed before ack');
    },
    doCommit: (txId) => (bDown ? Promise.reject(new Error('down')) : bSm.doCommit(txId)),
    abort: (txId) => (bDown ? Promise.reject(new Error('down')) : bSm.abort(txId)),
  };
  const c = createCoordinator({ log, participants: [a.adapter, b], timeoutMs: 40, retryDelayMs: 1, now: Date.now });
  assert.deepEqual(await c.run('tx1', payloads), { outcome: 'aborted', reason: 'precommit_failed' });
  assert.equal(log.map.get('tx1').state, 'ABORTED');
  assert.equal(await a.state('tx1'), 'ABORTED');
  assert.equal((await bStore.get('tx1')).state, 'PRECOMMITTED');

  // b restarts; its PRECOMMITTED timeout expires. Without the lookup it would
  // self-commit (a split). With it, it follows the coordinator's abort.
  t += 5000;
  assert.deepEqual(await bSm.sweep(), { committed: [], aborted: ['tx1'], pending: [] });
  assert.equal((await bStore.get('tx1')).state, 'ABORTED');
  assert.equal(bApplied.length, 0);
});

// --- R2-1: recovery must act on the current log state, not its snapshot ---

test('recovery does not abort a tx that run() committed (with pending) after the snapshot', async () => {
  const trace = [];
  let gateOpen;
  const gate = new Promise((r) => {
    gateOpen = r;
  });
  let runB;
  let bDown = true;
  const a = makeParticipant('a', trace, {
    hooks: {
      // B's vote waits until recovery has taken its snapshot.
      canCommit: async (txId) => {
        if (txId === 'B') await gate;
      },
      // While recovery aborts the earlier entry A, B runs to completion.
      abort: async (txId) => {
        if (txId === 'A') {
          gateOpen();
          await runB;
        }
      },
    },
  });
  const b = makeParticipant('b', trace, {
    hooks: {
      doCommit: async (txId) => {
        if (txId === 'B' && bDown) throw new Error('down');
      },
    },
  });
  const { c, log, committed } = coordinator([a, b]);
  // A: a dead run left WAITING, logged before B.
  await log.put({ txId: 'A', state: 'WAITING', payloads, updatedAt: Date.now() });
  runB = c.run('B', payloads);
  await sleep(1);
  assert.equal(log.map.get('B').state, 'WAITING');

  const finished = await c.recover();
  assert.deepEqual(await runB, { outcome: 'committed', pending: ['b'] });
  assert.deepEqual(finished, ['A']);
  // B was not aborted: the decision stays commit and is still unfinished.
  assert.ok(!log.history.includes('B:ABORTED'), log.history.join(','));
  assert.equal(log.map.get('B').state, 'PRECOMMITTED');
  assert.equal(await a.state('B'), 'COMMITTED');
  assert.equal(await b.state('B'), 'PRECOMMITTED');
  // Only A's abort was sent (once per participant).
  assert.equal(trace.filter((t) => t === 'a:abort').length, 1);
  assert.equal(trace.filter((t) => t === 'b:abort').length, 1);
  assert.equal(committed.length, 0);

  // The next recovery finishes the commit.
  bDown = false;
  assert.deepEqual(await c.recover(), ['B']);
  assert.equal(log.map.get('B').state, 'COMMITTED');
  assert.equal(await b.state('B'), 'COMMITTED');
  assert.deepEqual(committed.map((x) => x.txId), ['B']);
  assert.equal(a.applied.length, 1);
  assert.equal(b.applied.length, 1);
});

test('recovery skips an entry that became terminal after the snapshot', async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const { log } = coordinator([a]);
  await log.put({ txId: 'tx1', state: 'WAITING', payloads: { a: {} }, updatedAt: Date.now() });
  // listUnfinished returns the stale WAITING; by the time recovery reads it
  // again, it is COMMITTED.
  const stale = {
    ...log,
    async listUnfinished() {
      const entries = await log.listUnfinished();
      await log.put({ txId: 'tx1', state: 'COMMITTED', payloads: { a: {} }, updatedAt: Date.now() });
      return entries;
    },
  };
  const r = createCoordinator({ log: stale, participants: [a.adapter], timeoutMs: 40, retryDelayMs: 1 });
  assert.deepEqual(await r.recover(), []);
  assert.equal(log.map.get('tx1').state, 'COMMITTED');
  assert.ok(!trace.some((t) => t.endsWith(':abort') || t.endsWith(':preCommit')));
});

test('run() waits for a recovery that holds the same txId', async () => {
  const trace = [];
  let release;
  const held = new Promise((r) => {
    release = r;
  });
  const a = makeParticipant('a', trace, {
    hooks: {
      abort: async (txId) => {
        if (txId === 'tx1') await held;
      },
    },
  });
  const { c, log } = coordinator([a]);
  await log.put({ txId: 'tx1', state: 'WAITING', payloads: { a: {} }, updatedAt: Date.now() });
  const recovering = c.recover();
  await sleep(1);
  // A (misbehaving) run() for the same txId must not interleave with recovery.
  const running = c.run('tx1', { a: {} });
  await sleep(5);
  assert.ok(!trace.includes('a:canCommit'));
  release();
  assert.deepEqual(await recovering, ['tx1']);
  await running;
  assert.ok(trace.indexOf('a:abort') < trace.indexOf('a:canCommit'));
});

// --- R3-2: no unbounded waits ---
// coordinator() uses timeoutMs 40, retries 5, retryDelayMs 1, so the default
// lockWaitMs is 2 * 40 * 5 = 400 ms. Each test also has a hard timeout, so a
// regression fails instead of hanging the suite.

const never = () => new Promise(() => {});
const BOUNDED = { timeout: 3000 };

async function timed(promise) {
  const start = Date.now();
  const value = await promise;
  return { value, ms: Date.now() - start };
}

test('a doCommit that never settles counts as a failed attempt; run() still returns (R3-2)', BOUNDED, async () => {
  const trace = [];
  const a = makeParticipant('a', trace, { hooks: { doCommit: never } });
  const b = makeParticipant('b', trace);
  const { c, log, committed } = coordinator([a, b]);
  const { value, ms } = await timed(c.run('tx1', payloads));
  assert.deepEqual(value, { outcome: 'committed', pending: ['a'] });
  assert.equal(trace.filter((t) => t === 'a:doCommit').length, 5);
  assert.ok(ms < 1000, `${ms} ms`);
  assert.equal(log.map.get('tx1').state, 'PRECOMMITTED');
  assert.equal(committed.length, 0);
  assert.equal(await b.state('tx1'), 'COMMITTED');
});

test('recover() with an abort that never resolves finishes in bounded time (R3-2)', BOUNDED, async () => {
  const trace = [];
  const a = makeParticipant('a', trace, { hooks: { abort: never } });
  const { c, log } = coordinator([a]);
  await a.sm.canCommit('tx1', {});
  await log.put({ txId: 'tx1', state: 'WAITING', payloads: { a: {} }, updatedAt: Date.now() });
  const { value, ms } = await timed(c.recover());
  assert.ok(ms < 1000, `${ms} ms`);
  // The decision (abort) is logged before it is sent; the unacked participant
  // learns it from the coordinator when its own sweep asks.
  assert.deepEqual(value, ['tx1']);
  assert.equal(log.map.get('tx1').state, 'ABORTED');
  assert.equal(trace.filter((t) => t === 'a:abort').length, 5);
  assert.equal(await a.state('tx1'), 'UNCERTAIN');
  assert.deepEqual(await c.recover(), []);
});

test('recover() with a doCommit that never resolves leaves the entry PRECOMMITTED for the next pass (R3-2)', BOUNDED, async () => {
  const trace = [];
  let hung = true;
  const a = makeParticipant('a', trace, {
    hooks: {
      doCommit: async () => {
        if (hung) await never();
      },
    },
  });
  const { c, log, committed } = coordinator([a]);
  await a.sm.canCommit('tx1', {});
  await log.put({ txId: 'tx1', state: 'PRECOMMITTED', payloads: { a: {} }, updatedAt: Date.now() });
  const { value, ms } = await timed(c.recover());
  assert.ok(ms < 1000, `${ms} ms`);
  assert.deepEqual(value, []);
  assert.equal(log.map.get('tx1').state, 'PRECOMMITTED');
  assert.equal(committed.length, 0);
  hung = false;
  assert.deepEqual(await c.recover(), ['tx1']);
  assert.equal(log.map.get('tx1').state, 'COMMITTED');
  assert.equal(await a.state('tx1'), 'COMMITTED');
});

test('a queued run() gives up waiting for the txId lock with CoordinatorBusy and writes nothing (R3-2)', BOUNDED, async () => {
  const trace = [];
  const a = makeParticipant('a', trace, { hooks: { abort: never } });
  // Recovery holds tx1 for about 5 * 40 ms of abort timeouts; run() waits 60 ms at most.
  const { c, log } = coordinator([a], { lockWaitMs: 60 });
  await log.put({ txId: 'tx1', state: 'WAITING', payloads: { a: {} }, updatedAt: Date.now() });
  const recovering = c.recover();
  await sleep(1);
  const start = Date.now();
  await assert.rejects(c.run('tx1', { a: {} }), (err) => {
    assert.ok(err instanceof CoordinatorBusy);
    assert.equal(err.name, 'CoordinatorBusy');
    assert.equal(err.txId, 'tx1');
    return true;
  });
  const ms = Date.now() - start;
  assert.ok(ms >= 50 && ms < 500, `${ms} ms`);
  assert.deepEqual(c.inFlight(), []);
  await recovering;
  // Let recovery's abort attempts run out; the abandoned run() must never start.
  await sleep(400);
  assert.ok(!trace.includes('a:canCommit'), trace.join());
  assert.deepEqual(log.history, ['tx1:WAITING', 'tx1:ABORTED']);
});

test('the default lockWaitMs is 2 * timeoutMs * retries (R3-2)', BOUNDED, async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const { log } = coordinator([a]);
  let release;
  const held = new Promise((r) => {
    release = r;
  });
  // A coordinator log whose ABORTED write hangs: recovery holds tx1's lock.
  const hangingLog = {
    ...log,
    async put(entry) {
      if (entry.state === 'ABORTED') await held;
      return log.put(entry);
    },
  };
  const c = createCoordinator({ log: hangingLog, participants: [a.adapter], timeoutMs: 20, retries: 3, retryDelayMs: 1 });
  await log.put({ txId: 'tx1', state: 'WAITING', payloads: { a: {} }, updatedAt: Date.now() });
  const recovering = c.recover();
  await sleep(1);
  const start = Date.now();
  await assert.rejects(c.run('tx1', { a: {} }), CoordinatorBusy);
  const ms = Date.now() - start;
  assert.ok(ms >= 110 && ms < 400, `${ms} ms (expected about 120)`);
  release();
  await recovering;
});

test('a coordinator-log write that hangs in recovery: the pass ends, the lock is kept until the write settles (R3-2)', BOUNDED, async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const { log } = coordinator([a]);
  let release;
  const held = new Promise((r) => {
    release = r;
  });
  const hangingLog = {
    ...log,
    async put(entry) {
      if (entry.txId === 'tx1' && entry.state === 'ABORTED') await held;
      return log.put(entry);
    },
  };
  const c = createCoordinator({ log: hangingLog, participants: [a.adapter], timeoutMs: 40, retryDelayMs: 1 });
  await log.put({ txId: 'tx1', state: 'WAITING', payloads: { a: {} }, updatedAt: Date.now() });
  await log.put({ txId: 'tx2', state: 'WAITING', payloads: { a: {} }, updatedAt: Date.now() });
  // The pass gives up on tx1 after lockWaitMs (400 ms) and still finishes tx2.
  const first = await timed(c.recover());
  assert.ok(first.ms < 1000, `${first.ms} ms`);
  assert.deepEqual(first.value, ['tx2']);
  assert.equal(log.map.get('tx1').state, 'WAITING');
  // tx1's write may still land, so its lock is still held: the next pass
  // skips tx1 at once instead of acting on it again, and run() gets CoordinatorBusy.
  const second = await timed(c.recover());
  assert.ok(second.ms < 100, `${second.ms} ms`);
  assert.deepEqual(second.value, []);
  await assert.rejects(c.run('tx1', { a: {} }), CoordinatorBusy);
  assert.equal(await a.state('tx1'), null);
  // Once the write settles, the held-up recovery sends its abort and the lock is free.
  release();
  await sleep(20);
  assert.equal(log.map.get('tx1').state, 'ABORTED');
  assert.equal(await a.state('tx1'), 'ABORTED');
  assert.deepEqual(await c.recover(), []);
});

test('recover() rejects in bounded time when listUnfinished hangs (R3-2)', BOUNDED, async () => {
  const trace = [];
  const a = makeParticipant('a', trace);
  const { log } = coordinator([a]);
  const c = createCoordinator({ log: { ...log, listUnfinished: never }, participants: [a.adapter], timeoutMs: 40, retryDelayMs: 1, lockWaitMs: 50 });
  const start = Date.now();
  await assert.rejects(c.recover(), /listUnfinished/);
  assert.ok(Date.now() - start < 500);
});

test('CoordinatorBusy carries the txId and how long it waited', () => {
  const e = new CoordinatorBusy('tx', 123);
  assert.equal(e.name, 'CoordinatorBusy');
  assert.equal(e.txId, 'tx');
  assert.equal(e.waitedMs, 123);
  assert.ok(e instanceof Error);
});
