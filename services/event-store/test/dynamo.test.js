import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID, createHash } from 'node:crypto';
import { DeleteTableCommand } from '@aws-sdk/client-dynamodb';
import {
  createDynamoClients,
  createDynamoRepo,
  encodeCursor,
  decodeCursor,
  tableNames,
  InvalidCursorError,
  storageKey,
  MAX_ENTITY_KEY_BYTES,
  correlationItem,
} from '../src/dynamo.js';
import { ensureLocalResources } from '../src/local-setup.js';
import { StoreConflict, TpcStateError } from '@core/tpc';
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

test('cursor must be exactly string {entityKey, sk}, for the expected entity', () => {
  const sk = '2026-11-16T05:00:00.000Z#abc';
  for (const key of [{ a: 1 }, { entityKey: 'x' }, { entityKey: 'x', sk: 1 }, { entityKey: 'x', sk, more: 'y' }]) {
    assert.throws(() => decodeCursor(encodeCursor(key)), InvalidCursorError, JSON.stringify(key));
  }
  assert.throws(() => decodeCursor(encodeCursor({ entityKey: 'y', sk }), 'x'), InvalidCursorError);
  assert.deepEqual(decodeCursor(encodeCursor({ sk, entityKey: 'x' }), 'x'), { entityKey: 'x', sk });
});

test('cursor keys must be non-empty, well-formed and within DynamoDB key byte limits (R2-8)', () => {
  const sk = '2026-11-16T05:00:00.000Z#abc';
  const ok = (key) => assert.deepEqual(decodeCursor(encodeCursor(key)), key, JSON.stringify(key).slice(0, 60));
  const bad = (key) => assert.throws(() => decodeCursor(encodeCursor(key)), InvalidCursorError, JSON.stringify(key).slice(0, 60));
  bad({ entityKey: '', sk });
  bad({ entityKey: 'x', sk: '' });
  bad({ entityKey: '', sk: '' });
  // Sort key: at most 1024 bytes of UTF-8.
  ok({ entityKey: 'x', sk: 's'.repeat(1024) });
  bad({ entityKey: 'x', sk: 's'.repeat(1025) });
  ok({ entityKey: 'x', sk: 'é'.repeat(512) }); // 1024 bytes
  bad({ entityKey: 'x', sk: 'é'.repeat(513) }); // 1026 bytes, 513 characters
  // Partition key: at most 2048 bytes of UTF-8.
  ok({ entityKey: 'e'.repeat(2048), sk });
  bad({ entityKey: 'e'.repeat(2049), sk });
  bad({ entityKey: '€'.repeat(683), sk }); // 2049 bytes, 683 characters
  // Lone surrogates cannot be encoded as UTF-8.
  bad({ entityKey: 'x', sk: `${sk}\ud800` });
  bad({ entityKey: '\udc00', sk });
});

test('cursor sort key must lie inside the queried time range (R2-8)', () => {
  const range = { from: '2026-11-16T05:00:00.000Z', to: '2026-11-16T06:00:00.000Z' };
  const at = (sk) => encodeCursor({ entityKey: 'x', sk });
  for (const sk of ['2026-11-16T05:00:00.000Z', '2026-11-16T05:30:00.000Z#e', '2026-11-16T06:00:00.000Z#zzz']) {
    assert.deepEqual(decodeCursor(at(sk), 'x', range), { entityKey: 'x', sk }, sk);
  }
  for (const sk of ['2026-11-16T04:59:59.999Z#e', '2026-11-16T06:00:00.001Z#e', '2020', '~']) {
    assert.throws(() => decodeCursor(at(sk), 'x', range), InvalidCursorError, sk);
  }
});

test('storageKey keeps keys up to 1024 UTF-8 bytes and hashes longer ones', () => {
  assert.equal(storageKey('10.0.0.5'), '10.0.0.5');
  const max = 'a'.repeat(MAX_ENTITY_KEY_BYTES);
  assert.equal(storageKey(max), max);
  const long = 'a'.repeat(MAX_ENTITY_KEY_BYTES + 1);
  assert.equal(storageKey(long), `h:${createHash('sha256').update(long, 'utf8').digest('hex')}`);
  // Bytes, not characters: 600 two-byte characters is 1200 bytes.
  assert.match(storageKey('é'.repeat(600)), /^h:[0-9a-f]{64}$/);
});

test('storageKey also hashes literal keys starting with "h:", so they cannot collide with a hashed key (R2-6)', () => {
  const sha = (k) => `h:${createHash('sha256').update(k, 'utf8').digest('hex')}`;
  const long = 'a'.repeat(MAX_ENTITY_KEY_BYTES + 1);
  // A literal key equal to another key's hashed form gets its own partition.
  assert.equal(storageKey(sha(long)), sha(sha(long)));
  assert.notEqual(storageKey(sha(long)), storageKey(long));
  for (const k of ['h:', 'h:x', 'h:10.0.0.5']) assert.equal(storageKey(k), sha(k), k);
  // Only the exact lower-case "h:" prefix is reserved.
  for (const k of ['H:x', 'hx', 'host:h:1', ' h:x']) assert.equal(storageKey(k), k, k);
});

test('tableNames uses the prefix', () => {
  assert.deepEqual(tableNames('cs-'), {
    events: 'cs-events',
    correlations: 'cs-correlations',
    tpc: 'cs-tpc',
  });
});

// --- tpcStore.commit (R2-2): mapping of TransactWriteItems outcomes, no DynamoDB ---

function fakeDoc(handler) {
  const sent = [];
  return {
    sent,
    async send(cmd) {
      sent.push({ type: cmd.constructor.name, input: cmd.input });
      return handler(cmd, sent.length);
    },
  };
}

function cancelled(...codes) {
  const err = new Error('Transaction cancelled');
  err.name = 'TransactionCanceledException';
  err.CancellationReasons = codes.map((Code) => ({ Code }));
  return err;
}

const commitRecord = (overrides = {}) => ({
  txId: 'tx-1',
  state: 'COMMITTED',
  payload: { ...correlationPayload({ correlationId: 'cor-1', version: 3 }), txId: 'tx-1' },
  updatedAt: Date.parse('2026-11-16T06:00:00.000Z'),
  ...overrides,
});

test('correlationItem builds the history row as apply did: {...payload, txId, recordedAt}', () => {
  const rec = commitRecord();
  const { txId, ...rest } = rec.payload;
  assert.equal(txId, 'tx-1');
  assert.deepEqual(correlationItem(rec), { ...rest, txId: 'tx-1', recordedAt: '2026-11-16T06:00:00.000Z' });
});

test('tpcStore.commit writes the history row and COMMITTED in one TransactWriteItems', async () => {
  const doc = fakeDoc(async () => ({}));
  const repo = createDynamoRepo({ doc, tablePrefix: 'p-' });
  const rec = commitRecord();
  await repo.tpcStore.commit(rec, { state: 'PRECOMMITTED' });
  assert.equal(doc.sent.length, 1);
  assert.equal(doc.sent[0].type, 'TransactWriteCommand');
  const [corr, tpc] = doc.sent[0].input.TransactItems;
  assert.deepEqual(corr.Put, {
    TableName: 'p-correlations',
    Item: correlationItem(rec),
    ConditionExpression: 'attribute_not_exists(correlationId)',
  });
  assert.deepEqual(tpc.Put, {
    TableName: 'p-tpc',
    Item: rec,
    ConditionExpression: '#s = :expected',
    ExpressionAttributeNames: { '#s': 'state' },
    ExpressionAttributeValues: { ':expected': 'PRECOMMITTED' },
  });
});

test('tpcStore.commit: tpc condition failed (or a concurrent write) → StoreConflict, nothing else sent', async () => {
  for (const codes of [
    ['None', 'ConditionalCheckFailed'],
    ['ConditionalCheckFailed', 'ConditionalCheckFailed'],
    ['None', 'TransactionConflict'],
  ]) {
    const doc = fakeDoc(async () => {
      throw cancelled(...codes);
    });
    const repo = createDynamoRepo({ doc, tablePrefix: 'p-' });
    await assert.rejects(repo.tpcStore.commit(commitRecord(), { state: 'UNCERTAIN' }), StoreConflict, codes.join());
    assert.equal(doc.sent.length, 1, codes.join());
  }
});

test('tpcStore.commit: only the history row exists → conditional state move alone', async () => {
  const doc = fakeDoc(async (cmd, n) => {
    if (n === 1) throw cancelled('ConditionalCheckFailed', 'None');
    return {};
  });
  const repo = createDynamoRepo({ doc, tablePrefix: 'p-' });
  const rec = commitRecord();
  await repo.tpcStore.commit(rec, { state: 'PRECOMMITTED' });
  assert.equal(doc.sent.length, 2);
  assert.equal(doc.sent[1].type, 'PutCommand');
  assert.deepEqual(doc.sent[1].input, {
    TableName: 'p-tpc',
    Item: rec,
    ConditionExpression: '#s = :expected',
    ExpressionAttributeNames: { '#s': 'state' },
    ExpressionAttributeValues: { ':expected': 'PRECOMMITTED' },
  });
  // ...and that move is still conditional.
  const lost = fakeDoc(async (cmd, n) => {
    if (n === 1) throw cancelled('ConditionalCheckFailed', 'None');
    const err = new Error('failed');
    err.name = 'ConditionalCheckFailedException';
    throw err;
  });
  await assert.rejects(createDynamoRepo({ doc: lost, tablePrefix: 'p-' }).tpcStore.commit(rec, { state: 'PRECOMMITTED' }), StoreConflict);
});

test('tpcStore.commit: other failures are rethrown', async () => {
  for (const err of [cancelled('ValidationError', 'None'), Object.assign(new Error('x'), { name: 'ProvisionedThroughputExceededException' })]) {
    const doc = fakeDoc(async () => {
      throw err;
    });
    const repo = createDynamoRepo({ doc, tablePrefix: 'p-' });
    await assert.rejects(repo.tpcStore.commit(commitRecord(), { state: 'PRECOMMITTED' }), (e) => e === err);
  }
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
    // Another entity's cursor is rejected before it reaches DynamoDB.
    await assert.rejects(repo.queryEvents({ ...q, entityKey: `k-${randomUUID()}`, cursor: page1.nextCursor }), InvalidCursorError);
  });

  test('queryEvents rejects edge-case cursors as InvalidCursorError, not a DynamoDB error (R2-8)', async () => {
    const entityKey = `k-${randomUUID()}`;
    const q = { entityKey, from: '2026-11-16T05:00:00.000Z', to: '2026-11-16T06:00:00.000Z', limit: 1 };
    for (const sk of ['', 's'.repeat(1025), '2020-01-01T00:00:00.000Z#e', '2030-01-01T00:00:00.000Z#e']) {
      await assert.rejects(repo.queryEvents({ ...q, cursor: encodeCursor({ entityKey, sk }) }), InvalidCursorError, sk.slice(0, 30));
    }
    // An in-range cursor still works (no events after it).
    const res = await repo.queryEvents({ ...q, cursor: encodeCursor({ entityKey, sk: '2026-11-16T05:30:00.000Z#e' }) });
    assert.deepEqual(res, { items: [], nextCursor: null });
  });

  test('an entityKey over the DynamoDB key limit is stored under its hash and queryable by the original', async () => {
    const entityKey = `${'é'.repeat(1400)}-${randomUUID()}`; // ~2.8 KB of UTF-8
    const evs = [0, 1, 2].map((i) => normalizedEvent({ entityKey, eventTime: `2026-11-16T05:0${i}:00.000Z` }));
    for (const ev of evs) assert.equal(await repo.putEvent(ev), 'stored');
    assert.equal(await repo.putEvent(evs[0]), 'duplicate');
    assert.deepEqual(await repo.getEvent(evs[1].eventId), evs[1]);
    const q = { entityKey, from: '2026-11-16T05:00:00.000Z', to: '2026-11-16T06:00:00.000Z', limit: 2 };
    const page1 = await repo.queryEvents(q);
    assert.deepEqual(page1.items, evs.slice(0, 2));
    const page2 = await repo.queryEvents({ ...q, cursor: page1.nextCursor });
    assert.deepEqual(page2.items, evs.slice(2));
    // A different long key with the same prefix is a different partition.
    assert.deepEqual((await repo.queryEvents({ ...q, entityKey: `${entityKey}x` })).items, []);
  });

  test('a literal "h:<sha256>" entityKey and the long key it hashes to stay separate (R2-6)', async () => {
    const long = `${'z'.repeat(MAX_ENTITY_KEY_BYTES)}-${randomUUID()}`;
    const literal = `h:${createHash('sha256').update(long, 'utf8').digest('hex')}`;
    const a = normalizedEvent({ entityKey: long, eventTime: '2026-11-16T05:00:00.000Z' });
    const b = normalizedEvent({ entityKey: literal, eventTime: '2026-11-16T05:00:00.000Z' });
    assert.equal(await repo.putEvent(a), 'stored');
    assert.equal(await repo.putEvent(b), 'stored');
    const q = { from: '2026-11-16T05:00:00.000Z', to: '2026-11-16T06:00:00.000Z' };
    assert.deepEqual((await repo.queryEvents({ ...q, entityKey: long })).items, [a]);
    assert.deepEqual((await repo.queryEvents({ ...q, entityKey: literal })).items, [b]);
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

  test('tpcStore.put is conditional on the expected state (StoreConflict on mismatch)', async () => {
    const txId = randomUUID();
    const rec = (state, updatedAt) => ({ txId, state, payload: { a: 1 }, updatedAt });
    await repo.tpcStore.put(rec('UNCERTAIN', 1), { state: null });
    await assert.rejects(repo.tpcStore.put(rec('ABORTED', 2), { state: null }), (e) => {
      assert.ok(e instanceof StoreConflict);
      assert.equal(e.name, 'StoreConflict');
      return true;
    });
    await assert.rejects(repo.tpcStore.put(rec('COMMITTED', 2), { state: 'PRECOMMITTED' }), StoreConflict);
    // A missing record does not match an expected state.
    await assert.rejects(
      repo.tpcStore.put({ txId: randomUUID(), state: 'PRECOMMITTED', payload: null, updatedAt: 1 }, { state: 'UNCERTAIN' }),
      StoreConflict,
    );
    assert.equal((await repo.tpcStore.get(txId)).state, 'UNCERTAIN');
    await repo.tpcStore.put(rec('PRECOMMITTED', 3), { state: 'UNCERTAIN' });
    assert.equal((await repo.tpcStore.get(txId)).state, 'PRECOMMITTED');
    // Without `expected` the write stays unconditional.
    await repo.tpcStore.put(rec('ABORTED', 4));
    assert.equal((await repo.tpcStore.get(txId)).state, 'ABORTED');
  });

  test('two replicas on one table: a stale sweep-abort cannot overwrite PRECOMMITTED', async () => {
    let t = 2_000_000;
    const now = () => t;
    const make = (store) =>
      createCorrelationParticipant({ store, putCorrelation: repo.putCorrelation, timeoutMs: 1000, now });
    const b = make(repo.tpcStore);
    // Replica a's view of the table runs replica b's preCommit between a's read and a's write.
    let hook = null;
    const racy = {
      ...repo.tpcStore,
      async put(record, expected) {
        const h = hook;
        hook = null;
        if (h) await h();
        return repo.tpcStore.put(record, expected);
      },
    };
    const a = make(racy);
    const txId = randomUUID();
    assert.equal(await b.canCommit(txId, correlationPayload({ correlationId: `cor-${randomUUID()}` })), 'yes');
    t += 5000;
    hook = () => b.preCommit(txId);
    const res = await a.sweep();
    assert.ok(!res.aborted.includes(txId));
    assert.equal((await repo.tpcStore.get(txId)).state, 'PRECOMMITTED');
    // And the other way round: preCommit losing to a sweep-abort is rejected.
    const tx2 = randomUUID();
    assert.equal(await a.canCommit(tx2, correlationPayload({ correlationId: `cor-${randomUUID()}` })), 'yes');
    t += 5000;
    hook = async () => {
      await b.sweep();
    };
    await assert.rejects(a.preCommit(tx2), (e) => e instanceof TpcStateError && e.state === 'ABORTED');
    assert.equal((await repo.tpcStore.get(tx2)).state, 'ABORTED');
    // Resolve the first one so later sweeps in this suite do not commit it.
    await b.doCommit(txId);
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

  // --- R2-2: atomic history row + COMMITTED on DynamoDB ---

  // Every txId: a history row exists for its (correlationId, version) iff its tpc record is COMMITTED.
  async function assertAtomic(cases) {
    for (const { txId, correlationId } of cases) {
      const committed = (await repo.tpcStore.get(txId))?.state === 'COMMITTED';
      const rows = (await repo.getCorrelation(correlationId)).filter((r) => r.txId === txId);
      assert.equal(rows.length, committed ? 1 : 0, `${txId}: ${rows.length} rows, COMMITTED ${committed}`);
    }
  }

  function twoReplicas() {
    let t = 3_000_000;
    let hook = null;
    const racy = {
      ...repo.tpcStore,
      async put(record, expected) {
        const h = hook;
        hook = null;
        if (h) await h();
        return repo.tpcStore.put(record, expected);
      },
      async commit(record, expected) {
        const h = hook;
        hook = null;
        if (h) await h();
        return repo.tpcStore.commit(record, expected);
      },
    };
    const make = (store) =>
      createCorrelationParticipant({ store, putCorrelation: repo.putCorrelation, timeoutMs: 1000, now: () => t });
    return {
      a: make(racy),
      b: make(repo.tpcStore),
      before: (fn) => {
        hook = fn;
      },
      advance: (ms) => {
        t += ms;
      },
    };
  }

  async function prepared(participant) {
    const txId = randomUUID();
    const correlationId = `cor-${randomUUID()}`;
    assert.equal(await participant.canCommit(txId, correlationPayload({ correlationId })), 'yes');
    await participant.preCommit(txId);
    return { txId, correlationId };
  }

  test('race: one replica applies while another aborts → no history row, ABORTED', async () => {
    const { a, b, before } = twoReplicas();
    const tx = await prepared(a);
    before(() => b.abort(tx.txId));
    await assert.rejects(a.doCommit(tx.txId), (e) => e instanceof TpcStateError && e.state === 'ABORTED');
    assert.equal((await repo.tpcStore.get(tx.txId)).state, 'ABORTED');
    assert.deepEqual(await repo.getCorrelation(tx.correlationId), []);
    await assertAtomic([tx]);
  });

  test('race: a timed-out sweep commit while another replica aborts → no history row', async () => {
    const { a, b, before, advance } = twoReplicas();
    const tx = await prepared(a);
    advance(5000);
    before(() => b.abort(tx.txId));
    const res = await a.sweep();
    assert.ok(!res.committed.includes(tx.txId));
    await assertAtomic([tx]);
    assert.equal((await repo.tpcStore.get(tx.txId)).state, 'ABORTED');
  });

  test('race: abort while another replica commits → one history row, COMMITTED', async () => {
    const { a, b, before } = twoReplicas();
    const tx = await prepared(a);
    before(() => b.doCommit(tx.txId));
    await assert.rejects(a.abort(tx.txId), (e) => e instanceof TpcStateError && e.state === 'COMMITTED');
    await assertAtomic([tx]);
    assert.equal((await repo.getCorrelation(tx.correlationId)).length, 1);
  });

  test('race: concurrent doCommit and abort on two replicas never split', async () => {
    const { a, b } = twoReplicas();
    const cases = [];
    for (let i = 0; i < 15; i += 1) cases.push(await prepared(a));
    await Promise.all(
      cases.flatMap(({ txId }, i) => {
        const [commit, abort] = i % 2 ? [a, b] : [b, a];
        return [commit.doCommit(txId).catch(() => {}), abort.abort(txId).catch(() => {})];
      }),
    );
    await assertAtomic(cases);
    for (const { txId } of cases) assert.ok(['COMMITTED', 'ABORTED'].includes((await repo.tpcStore.get(txId)).state));
  });

  test('tpcStore.commit is conditional and idempotent; a legacy history row only moves the state', async () => {
    const correlationId = `cor-${randomUUID()}`;
    const txId = randomUUID();
    const payload = { ...correlationPayload({ correlationId, version: 1 }), txId };
    const rec = { txId, state: 'COMMITTED', payload, updatedAt: 4_000_000 };
    await repo.tpcStore.put({ txId, state: 'UNCERTAIN', payload, updatedAt: 1 }, { state: null });
    await assert.rejects(repo.tpcStore.commit(rec, { state: 'PRECOMMITTED' }), StoreConflict);
    assert.deepEqual(await repo.getCorrelation(correlationId), []);
    // UNCERTAIN -> COMMITTED (the coordinator reported COMMITTED).
    await repo.tpcStore.commit(rec, { state: 'UNCERTAIN' });
    assert.equal((await repo.tpcStore.get(txId)).state, 'COMMITTED');
    assert.deepEqual(await repo.getCorrelation(correlationId), [correlationItem(rec)]);
    // A retry after a crash: already COMMITTED → StoreConflict; the caller re-reads COMMITTED.
    await assert.rejects(repo.tpcStore.commit(rec, { state: 'UNCERTAIN' }), StoreConflict);
    assert.equal((await repo.getCorrelation(correlationId)).length, 1);

    // Pre-R2-2 data: apply wrote the row, COMMITTED was never recorded.
    const legacyId = randomUUID();
    const legacyCor = `cor-${randomUUID()}`;
    const legacy = { txId: legacyId, state: 'COMMITTED', payload: { ...correlationPayload({ correlationId: legacyCor }), txId: legacyId }, updatedAt: 5 };
    await repo.tpcStore.put({ ...legacy, state: 'PRECOMMITTED' }, { state: null });
    assert.equal(await repo.putCorrelation(correlationItem(legacy)), 'stored');
    await repo.tpcStore.commit(legacy, { state: 'PRECOMMITTED' });
    assert.equal((await repo.tpcStore.get(legacyId)).state, 'COMMITTED');
    assert.equal((await repo.getCorrelation(legacyCor)).length, 1);
  });
});
