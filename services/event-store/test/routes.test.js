import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildServer } from '../src/routes.js';
import { createCorrelationParticipant, validateCorrelationPayload } from '../src/participant.js';
import { encodeCursor, InvalidCursorError, storageKey } from '../src/dynamo.js';
import { fakeRepo, normalizedEvent, correlationPayload } from './helpers.js';

const NOW = Date.parse('2026-11-16T06:00:00.000Z');

function setup({ forceVote, repo = fakeRepo(), ready } = {}) {
  const participant = createCorrelationParticipant({
    store: repo.tpcStore,
    putCorrelation: (rec) => repo.putCorrelation(rec),
    timeoutMs: 10_000,
    forceVote,
    now: () => NOW,
  });
  const app = buildServer({ repo, participant, logger: false, now: () => NOW, ready });
  return { app, repo, participant };
}

const post = (app, url, payload) =>
  app.inject({ method: 'POST', url, ...(payload === undefined ? {} : { payload }) });

test('GET /events/:id 404', async () => {
  const { app, repo } = setup();
  const res = await app.inject(`/events/${'a'.repeat(150)}`);
  assert.equal(res.statusCode, 404);
  assert.deepEqual(res.json(), { error: 'not_found' });
  assert.deepEqual(repo.calls.getEvent, ['a'.repeat(150)]);
});

test('GET /events/:id returns the stored doc', async () => {
  const { app, repo } = setup();
  const ev = normalizedEvent();
  repo.events.set(ev.eventId, ev);
  const res = await app.inject(`/events/${ev.eventId}`);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), ev);
});

test('GET /events requires entityKey (400)', async () => {
  const { app, repo } = setup();
  const res = await app.inject('/events?from=2026-11-16T05:00:00.000Z');
  assert.equal(res.statusCode, 400);
  assert.equal(res.json().error, 'bad_request');
  assert.equal(repo.calls.queryEvents.length, 0);
});

test('GET /events by IPv6 entityKey', async () => {
  const { app, repo } = setup();
  repo.queryEvents = async (q) => {
    repo.calls.queryEvents.push(q);
    return { items: [{ eventId: 'e1' }], nextCursor: 'abc' };
  };
  const res = await app.inject(
    '/events?entityKey=2001%3Adb8%3A%3A1&from=2026-11-16T05%3A00%3A00.000Z&to=2026-11-16T05:30:00Z&limit=10',
  );
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), { items: [{ eventId: 'e1' }], nextCursor: 'abc' });
  assert.deepEqual(repo.calls.queryEvents, [
    {
      entityKey: '2001:db8::1',
      from: '2026-11-16T05:00:00.000Z',
      // Normalised to millisecond precision so it sorts correctly against stored keys.
      to: '2026-11-16T05:30:00.000Z',
      limit: 10,
      cursor: undefined,
    },
  ]);
});

test('GET /events defaults to the last hour and limit 100', async () => {
  const { app, repo } = setup();
  const res = await app.inject('/events?entityKey=10.0.0.5');
  assert.equal(res.statusCode, 200);
  assert.deepEqual(repo.calls.queryEvents[0], {
    entityKey: '10.0.0.5',
    from: '2026-11-16T05:00:00.000Z',
    to: '2026-11-16T06:00:00.000Z',
    limit: 100,
    cursor: undefined,
  });
});

test('GET /events from defaults to one hour before an explicit to', async () => {
  const { app, repo } = setup();
  await app.inject('/events?entityKey=x&to=2026-11-15T10:00:00.000Z');
  assert.equal(repo.calls.queryEvents[0].from, '2026-11-15T09:00:00.000Z');
});

test('GET /events rejects bad from/to/limit/cursor (400)', async () => {
  const { app, repo } = setup();
  const bad = [
    'entityKey=x&from=yesterday',
    'entityKey=x&to=2026-11-16',
    'entityKey=x&from=2026-11-16T07:00:00.000Z&to=2026-11-16T06:00:00.000Z',
    'entityKey=x&limit=0',
    'entityKey=x&limit=501',
    'entityKey=x&limit=abc',
    'entityKey=x&limit=1.5',
    'entityKey=x&cursor=%21%21%21',
    'entityKey=',
    'entityKey=a&entityKey=b',
  ];
  for (const qs of bad) {
    const res = await app.inject(`/events?${qs}`);
    assert.equal(res.statusCode, 400, qs);
    assert.equal(res.json().error, 'bad_request', qs);
    assert.equal(typeof res.json().message, 'string', qs);
  }
  assert.equal(repo.calls.queryEvents.length, 0);
});

test('GET /events rejects cursors that are not exactly {entityKey, sk} for this entity (400)', async () => {
  const { app, repo } = setup();
  const sk = '2026-11-16T05:10:00.000Z#e';
  const cursors = [
    { a: 1 },
    { entityKey: 'x' },
    { sk },
    { entityKey: 'x', sk: 5 },
    { entityKey: 1, sk },
    { entityKey: 'x', sk, extra: 'z' },
    { entityKey: 'y', sk }, // another entity's cursor
    {},
  ];
  for (const key of cursors) {
    const res = await app.inject(`/events?entityKey=x&cursor=${encodeCursor(key)}`);
    assert.equal(res.statusCode, 400, JSON.stringify(key));
    assert.equal(res.json().error, 'bad_request');
  }
  assert.equal(repo.calls.queryEvents.length, 0);
});

test('GET /events with an oversized entityKey accepts cursors for its hashed storage key', async () => {
  const { app, repo } = setup();
  const entityKey = 'u'.repeat(3000);
  const cursor = encodeCursor({ entityKey: storageKey(entityKey), sk: '2026-11-16T05:10:00.000Z#e' });
  const res = await app.inject(`/events?entityKey=${entityKey}&cursor=${cursor}`);
  assert.equal(res.statusCode, 200);
  assert.equal(repo.calls.queryEvents[0].entityKey, entityKey);
  const raw = encodeCursor({ entityKey, sk: '2026-11-16T05:10:00.000Z#e' });
  assert.equal((await app.inject(`/events?entityKey=${entityKey}&cursor=${raw}`)).statusCode, 400);
});

test('GET /events maps an InvalidCursorError from the repo to 400', async () => {
  const { app, repo } = setup();
  repo.queryEvents = async () => {
    throw new InvalidCursorError();
  };
  const cursor = encodeCursor({ entityKey: 'x', sk: '2026-11-16T05:10:00.000Z#e' });
  const res = await app.inject(`/events?entityKey=x&cursor=${cursor}`);
  assert.equal(res.statusCode, 400);
});

test('GET /events passes a valid cursor through', async () => {
  const { app, repo } = setup();
  const cursor = encodeCursor({ entityKey: 'x', sk: '2026-11-16T05:10:00.000Z#e' });
  const res = await app.inject(`/events?entityKey=x&limit=500&cursor=${cursor}`);
  assert.equal(res.statusCode, 200);
  assert.equal(repo.calls.queryEvents[0].cursor, cursor);
  assert.equal(repo.calls.queryEvents[0].limit, 500);
});

test('GET /correlations with encoded id', async () => {
  const { app, repo } = setup();
  const res = await app.inject('/correlations/cor-%7Cx');
  assert.equal(res.statusCode, 404);
  assert.deepEqual(res.json(), { error: 'not_found' });
  assert.deepEqual(repo.calls.getCorrelation, ['cor-|x']);
});

test('GET /correlations returns versions', async () => {
  const { app, repo } = setup();
  const rec = { ...correlationPayload({ correlationId: 'cor-|x' }), txId: 't', recordedAt: 'r' };
  await repo.putCorrelation(rec);
  const res = await app.inject('/correlations/cor-%7Cx');
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), { correlationId: 'cor-|x', versions: [rec] });
});

test('can-commit, pre-commit, do-commit stores correlation once', async () => {
  const { app, repo } = setup();
  const payload = correlationPayload();
  let res = await post(app, '/tpc/tx-1/can-commit', { payload });
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), { vote: 'yes' });
  res = await post(app, '/tpc/tx-1/pre-commit');
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), { ack: true });
  res = await post(app, '/tpc/tx-1/do-commit', {});
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), { ack: true });
  // Repeats are idempotent and do not re-apply.
  assert.equal((await post(app, '/tpc/tx-1/do-commit')).statusCode, 200);
  assert.equal((await post(app, '/tpc/tx-1/pre-commit')).statusCode, 409);
  assert.equal(repo.calls.putCorrelation.length, 1);
  const stored = repo.calls.putCorrelation[0];
  assert.equal(stored.txId, 'tx-1');
  assert.equal(stored.recordedAt, '2026-11-16T06:00:00.000Z');
  assert.deepEqual({ ...stored, txId: undefined, recordedAt: undefined }, {
    ...payload,
    txId: undefined,
    recordedAt: undefined,
  });
  assert.equal((await repo.tpcStore.get('tx-1')).state, 'COMMITTED');
});

test('pre-commit unknown tx → 409', async () => {
  const { app } = setup();
  const res = await post(app, '/tpc/nope/pre-commit');
  assert.equal(res.statusCode, 409);
  assert.deepEqual(res.json(), { error: 'bad_state', state: null });
});

test('do-commit after abort → 409', async () => {
  const { app, repo } = setup();
  await post(app, '/tpc/tx-2/can-commit', { payload: correlationPayload() });
  const ab = await post(app, '/tpc/tx-2/abort');
  assert.equal(ab.statusCode, 200);
  assert.deepEqual(ab.json(), { ack: true });
  const res = await post(app, '/tpc/tx-2/do-commit');
  assert.equal(res.statusCode, 409);
  assert.deepEqual(res.json(), { error: 'bad_state', state: 'ABORTED' });
  assert.equal(repo.calls.putCorrelation.length, 0);
});

test('abort after commit → 409 with state COMMITTED', async () => {
  const { app } = setup();
  await post(app, '/tpc/tx-3/can-commit', { payload: correlationPayload() });
  await post(app, '/tpc/tx-3/pre-commit');
  await post(app, '/tpc/tx-3/do-commit');
  const res = await post(app, '/tpc/tx-3/abort');
  assert.equal(res.statusCode, 409);
  assert.deepEqual(res.json(), { error: 'bad_state', state: 'COMMITTED' });
});

test('abort on unknown tx acks and leaves a tombstone', async () => {
  const { app, repo } = setup();
  assert.equal((await post(app, '/tpc/late/abort')).statusCode, 200);
  assert.equal((await repo.tpcStore.get('late')).state, 'ABORTED');
  const res = await post(app, '/tpc/late/can-commit', { payload: correlationPayload() });
  assert.deepEqual(res.json(), { vote: 'no' });
});

test('TPC_FORCE_VOTE=no → vote no', async () => {
  const { app, repo } = setup({ forceVote: 'no' });
  const res = await post(app, '/tpc/tx-4/can-commit', { payload: correlationPayload() });
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), { vote: 'no' });
  assert.equal((await repo.tpcStore.get('tx-4')).state, 'ABORTED');
});

test('invalid payload votes no', async () => {
  const { app } = setup();
  const cases = [
    undefined,
    {},
    correlationPayload({ version: 0 }),
    correlationPayload({ version: 1.5 }),
    correlationPayload({ correlationId: '' }),
    correlationPayload({ alertIds: 'a' }),
    correlationPayload({ decision: { kind: 'delete' } }),
  ];
  let i = 0;
  for (const payload of cases) {
    const res = await post(app, `/tpc/bad-${i++}/can-commit`, payload === undefined ? {} : { payload });
    assert.equal(res.statusCode, 200);
    assert.deepEqual(res.json(), { vote: 'no' }, JSON.stringify(payload));
  }
});

test('validateCorrelationPayload accepts the coordinator payload', () => {
  assert.equal(validateCorrelationPayload(correlationPayload()), true);
  assert.equal(
    validateCorrelationPayload(correlationPayload({ decision: { ...correlationPayload().decision, kind: 'update' } })),
    true,
  );
  assert.equal(validateCorrelationPayload(correlationPayload({ incidentId: 5 })), false);
  assert.equal(
    validateCorrelationPayload(correlationPayload({ decision: { ...correlationPayload().decision, newAlertIds: null } })),
    false,
  );
  assert.equal(validateCorrelationPayload(null), false);
});

test('participant store errors return 500 and count as error', async () => {
  const repo = fakeRepo();
  repo.tpcStore.get = async () => {
    throw new Error('dynamo down');
  };
  const { app } = setup({ repo });
  const res = await post(app, '/tpc/tx-5/pre-commit');
  assert.equal(res.statusCode, 500);
  assert.equal(res.json().error, 'internal');
  const metrics = await (await app.inject('/metrics')).body;
  assert.match(metrics, /tpc_participant_messages_total\{phase="pre_commit",result="error"[^}]*\} 1/);
});

test('tpc_participant_messages_total counts phases and results', async () => {
  const { app } = setup();
  await post(app, '/tpc/m1/can-commit', { payload: correlationPayload() });
  await post(app, '/tpc/m2/can-commit', { payload: {} });
  await post(app, '/tpc/m1/pre-commit');
  await post(app, '/tpc/m1/do-commit');
  await post(app, '/tpc/m3/do-commit');
  await post(app, '/tpc/m3/abort');
  const text = (await app.inject('/metrics')).body;
  const has = (phase, result) =>
    assert.match(text, new RegExp(`tpc_participant_messages_total\\{phase="${phase}",result="${result}"[^}]*\\} 1`));
  has('can_commit', 'yes');
  has('can_commit', 'no');
  has('pre_commit', 'ack');
  has('do_commit', 'ack');
  has('do_commit', 'bad_state');
  has('abort', 'ack');
});

test('/ready uses the injected readiness check', async () => {
  let ok = false;
  const { app } = setup({ ready: async () => ok });
  assert.equal((await app.inject('/ready')).statusCode, 503);
  ok = true;
  assert.equal((await app.inject('/ready')).statusCode, 200);
  assert.deepEqual((await app.inject('/health')).json(), { status: 'ok', service: 'event-store' });
});
