import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { DeleteTableCommand } from '@aws-sdk/client-dynamodb';
import {
  createDynamoClients,
  createDynamoRepo,
  encodeCursor,
  decodeCursor,
  tableNames,
} from '../src/dynamo.js';
import { ensureLocalResources } from '../src/local-setup.js';
import { createCorrelationParticipant } from '../src/participant.js';
import { normalizedEvent, correlationPayload, integrationEnv, silentLogger } from './helpers.js';

test('cursor round-trips a LastEvaluatedKey', () => {
  const key = { entityKey: '2001:db8::1', sk: '2026-11-16T05:00:00.000Z#abc' };
  const c = encodeCursor(key);
  assert.match(c, /^[A-Za-z0-9_-]+$/);
  assert.deepEqual(decodeCursor(c), key);
});

test('invalid cursor throws', () => {
  assert.throws(() => decodeCursor('not-a-cursor!'), /cursor/);
  assert.throws(() => decodeCursor(Buffer.from('[1,2]').toString('base64url')), /cursor/);
  assert.throws(() => decodeCursor(Buffer.from('"x"').toString('base64url')), /cursor/);
});

test('tableNames uses the prefix', () => {
  assert.deepEqual(tableNames('cs-'), {
    events: 'cs-events',
    correlations: 'cs-correlations',
    tpc: 'cs-tpc',
  });
});

describe('dynamodb repo (integration)', { skip: !process.env.INTEGRATION }, () => {
  let env;
  let clients;
  let repo;

  before(async () => {
    env = integrationEnv();
    clients = createDynamoClients({ region: env.region, endpoint: env.endpoint });
    const cfg = { dynamodbEndpoint: env.endpoint, tablePrefix: env.tablePrefix };
    await ensureLocalResources({ config: cfg, ddb: clients.ddb, logger: silentLogger() });
    // Second run must be a no-op (tables already exist).
    await ensureLocalResources({ config: cfg, ddb: clients.ddb, logger: silentLogger() });
    repo = createDynamoRepo({ doc: clients.doc, tablePrefix: env.tablePrefix });
  });

  after(async () => {
    for (const TableName of Object.values(tableNames(env.tablePrefix))) {
      await clients.ddb.send(new DeleteTableCommand({ TableName })).catch(() => {});
    }
    clients.ddb.destroy();
  });

  test('ping describes the events table', async () => {
    assert.equal(await repo.ping(), true);
  });

  test('putEvent then getEvent round-trips doc', async () => {
    const ev = normalizedEvent({ entityKey: `k-${randomUUID()}` });
    assert.equal(await repo.putEvent(ev), 'stored');
    assert.deepEqual(await repo.getEvent(ev.eventId), ev);
  });

  test('getEvent unknown id returns null', async () => {
    assert.equal(await repo.getEvent(randomUUID()), null);
  });

  test('putEvent same event twice → "duplicate"', async () => {
    const ev = normalizedEvent({ entityKey: `k-${randomUUID()}` });
    assert.equal(await repo.putEvent(ev), 'stored');
    assert.equal(await repo.putEvent(ev), 'duplicate');
  });

  test('putEvent keeps large integers and odd values intact', async () => {
    const ev = normalizedEvent({
      entityKey: `k-${randomUUID()}`,
      attributes: { big: 2 ** 60, empty: '', list: [], nested: { a: [{ b: null }] }, f: 1.5e300 },
    });
    assert.equal(await repo.putEvent(ev), 'stored');
    assert.deepEqual(await repo.getEvent(ev.eventId), ev);
  });

  test('queryEvents includes events exactly at "to"', async () => {
    const entityKey = `2001:db8::${randomUUID().slice(0, 4)}`;
    const atTo = normalizedEvent({ entityKey, eventTime: '2026-11-16T05:30:00.000Z' });
    const atFrom = normalizedEvent({ entityKey, eventTime: '2026-11-16T05:00:00.000Z' });
    const after = normalizedEvent({ entityKey, eventTime: '2026-11-16T05:30:00.001Z' });
    const before = normalizedEvent({ entityKey, eventTime: '2026-11-16T04:59:59.999Z' });
    for (const ev of [atTo, atFrom, after, before]) await repo.putEvent(ev);
    const res = await repo.queryEvents({
      entityKey,
      from: '2026-11-16T05:00:00.000Z',
      to: '2026-11-16T05:30:00.000Z',
    });
    assert.deepEqual(
      res.items.map((e) => e.eventId),
      [atFrom.eventId, atTo.eventId],
    );
    assert.equal(res.nextCursor, null);
  });

  test('queryEvents paginates', async () => {
    const entityKey = `k-${randomUUID()}`;
    for (let i = 0; i < 3; i++) {
      await repo.putEvent(normalizedEvent({ entityKey, eventTime: `2026-11-16T05:0${i}:00.000Z` }));
    }
    const q = { entityKey, from: '2026-11-16T05:00:00.000Z', to: '2026-11-16T06:00:00.000Z', limit: 2 };
    const page1 = await repo.queryEvents(q);
    assert.equal(page1.items.length, 2);
    assert.ok(page1.nextCursor);
    const page2 = await repo.queryEvents({ ...q, cursor: page1.nextCursor });
    assert.equal(page2.items.length, 1);
    assert.equal(page2.nextCursor, null);
    assert.equal(page2.items[0].eventTime, '2026-11-16T05:02:00.000Z');
  });

  test('putCorrelation stores versions once, getCorrelation orders by version', async () => {
    const correlationId = `cor-${randomUUID()}|x`;
    const rec = (version) => ({
      correlationId,
      version,
      incidentId: 'i1',
      alertIds: ['a'],
      decision: { kind: version === 1 ? 'create' : 'update', severity: 'high', newAlertIds: ['a'] },
      txId: `tx-${version}`,
      recordedAt: '2026-11-16T05:00:00.000Z',
    });
    assert.equal(await repo.putCorrelation(rec(2)), 'stored');
    assert.equal(await repo.putCorrelation(rec(1)), 'stored');
    assert.equal(await repo.putCorrelation(rec(1)), 'duplicate');
    const versions = await repo.getCorrelation(correlationId);
    assert.deepEqual(versions.map((v) => v.version), [1, 2]);
    assert.deepEqual(versions[0], rec(1));
    assert.deepEqual(await repo.getCorrelation(`cor-${randomUUID()}`), []);
  });

  test('tpcStore get/put/listPending', async () => {
    const ids = [randomUUID(), randomUUID(), randomUUID(), randomUUID()];
    await repo.tpcStore.put({ txId: ids[0], state: 'UNCERTAIN', payload: { a: 1 }, updatedAt: 1 });
    await repo.tpcStore.put({ txId: ids[1], state: 'PRECOMMITTED', payload: { a: 2 }, updatedAt: 2 });
    await repo.tpcStore.put({ txId: ids[2], state: 'COMMITTED', payload: { a: 3 }, updatedAt: 3 });
    await repo.tpcStore.put({ txId: ids[3], state: 'ABORTED', payload: null, updatedAt: 4 });
    assert.deepEqual(await repo.tpcStore.get(ids[3]), {
      txId: ids[3],
      state: 'ABORTED',
      payload: null,
      updatedAt: 4,
    });
    assert.equal(await repo.tpcStore.get(randomUUID()), null);
    const pending = (await repo.tpcStore.listPending()).filter((r) => ids.includes(r.txId));
    assert.deepEqual(pending.map((r) => r.txId).sort(), [ids[0], ids[1]].sort());
    assert.deepEqual(pending.find((r) => r.txId === ids[1]).payload, { a: 2 });
    // Resolve them so the sweep test below does not pick up these dummy payloads.
    await repo.tpcStore.put({ txId: ids[0], state: 'ABORTED', payload: { a: 1 }, updatedAt: 5 });
    await repo.tpcStore.put({ txId: ids[1], state: 'ABORTED', payload: { a: 2 }, updatedAt: 6 });
  });

  test('3pc participant commits through dynamodb; sweep resolves timed-out records', async () => {
    let t = 1_000_000;
    const participant = createCorrelationParticipant({
      store: repo.tpcStore,
      putCorrelation: repo.putCorrelation,
      timeoutMs: 1000,
      now: () => t,
    });
    const correlationId = `cor-${randomUUID()}`;
    const [a, b, c] = [randomUUID(), randomUUID(), randomUUID()];
    assert.equal(await participant.canCommit(a, correlationPayload({ correlationId, version: 1 })), 'yes');
    await participant.preCommit(a);
    await participant.doCommit(a);
    await participant.doCommit(a);
    // b is left PRECOMMITTED and c UNCERTAIN, then time passes.
    assert.equal(await participant.canCommit(b, correlationPayload({ correlationId, version: 2 })), 'yes');
    await participant.preCommit(b);
    assert.equal(await participant.canCommit(c, correlationPayload({ correlationId, version: 3 })), 'yes');
    t += 5000;
    const { committed, aborted } = await participant.sweep();
    assert.ok(committed.includes(b));
    assert.ok(aborted.includes(c));
    const versions = await repo.getCorrelation(correlationId);
    assert.deepEqual(versions.map((v) => [v.version, v.txId]), [[1, a], [2, b]]);
    assert.equal(versions[0].recordedAt, new Date(1_000_000).toISOString());
    assert.equal((await repo.tpcStore.get(c)).state, 'ABORTED');
  });
});
