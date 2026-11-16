import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { migrate } from '../src/migrate.js';
import { createIncidentRepo } from '../src/incidents.js';
import { createIncidentService } from '../src/tpc-wiring.js';
import { createOutboxRelay } from '../src/outbox.js';
import { createTestDatabase, fakeEventStore, fakeProducer, silentLogger, candidate } from './helpers.js';

const skip = !process.env.INTEGRATION;
let db;
let repo;
let svc;

before(async () => {
  if (skip) return;
  db = await createTestDatabase();
  await migrate(db.pool);
  repo = createIncidentRepo({ pool: db.pool, correlationWindowMs: 900_000 });
  svc = createIncidentService({
    pool: db.pool,
    repo,
    eventStore: fakeEventStore(),
    config: { tpcTimeoutMs: 500, tpcParticipantTimeoutMs: 10_000, correlationWindowMs: 900_000 },
    logger: silentLogger,
  });
});

after(async () => {
  if (db) await db.drop();
});

const unpublished = async () =>
  Number((await db.pool.query('SELECT count(*) FROM outbox WHERE published_at IS NULL')).rows[0].count);

test('relay publishes outbox rows in id order, keyed by incidentId, and marks them published (P1-3)', { skip }, async () => {
  const c = candidate();
  const { incident } = await svc.upsertIncident(c);
  await svc.upsertIncident({ ...c, alertIds: [...c.alertIds, 'second'] });
  await repo.updateStatus(incident.incidentId, { status: 'investigating', version: 2 });

  const producer = fakeProducer();
  const relay = createOutboxRelay({ pool: db.pool, producer, topic: 'incident-updates', logger: silentLogger });
  const n = await relay.publishPending();
  assert.equal(n, 3);
  const messages = producer.sent.flatMap((r) => r.messages.map((m) => ({ topic: r.topic, acks: r.acks, ...m })));
  assert.deepEqual(messages.map((m) => m.key), [incident.incidentId, incident.incidentId, incident.incidentId]);
  assert.ok(messages.every((m) => m.topic === 'incident-updates' && m.acks === -1));
  assert.deepEqual(
    messages.map((m) => JSON.parse(m.value).type),
    ['incident.created', 'incident.updated', 'incident.status_changed'],
  );
  assert.deepEqual(messages.map((m) => JSON.parse(m.value).incident.version), [1, 2, 3]);
  assert.equal(await unpublished(), 0);
  assert.equal(await relay.publishPending(), 0, 'nothing is sent twice');
});

test('relay keeps rows when Kafka fails and sends them on the next tick (P1-3)', { skip }, async () => {
  const { incident } = await svc.upsertIncident(candidate());
  const producer = fakeProducer();
  producer.failNext = 1;
  const relay = createOutboxRelay({ pool: db.pool, producer, topic: 'incident-updates', logger: silentLogger });
  await assert.rejects(relay.publishPending(), /kafka unavailable/);
  assert.equal(await unpublished(), 1, 'the row survives a failed send');
  assert.equal(await relay.publishPending(), 1);
  assert.equal(JSON.parse(producer.sent[0].messages[0].value).incident.incidentId, incident.incidentId);
  assert.equal(await unpublished(), 0);
});

test('relay start/stop runs the loop on an interval (P1-3)', { skip }, async () => {
  await svc.upsertIncident(candidate());
  const producer = fakeProducer();
  const relay = createOutboxRelay({ pool: db.pool, producer, topic: 'incident-updates', logger: silentLogger, intervalMs: 20 });
  relay.start();
  const deadline = Date.now() + 2000;
  while ((await unpublished()) > 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
  await relay.stop();
  assert.equal(await unpublished(), 0);
});

test('two overlapping relays keep per-incident order (R2-5a)', { skip }, async () => {
  // Drain anything earlier tests left behind.
  await createOutboxRelay({ pool: db.pool, producer: fakeProducer(), topic: 't', logger: silentLogger }).publishPending();
  const c = candidate();
  const { incident } = await svc.upsertIncident(c);
  await svc.upsertIncident({ ...c, alertIds: [...c.alertIds, 'r2-5a'] });

  const order = [];
  let releaseFirst;
  let firstSending;
  const sending = new Promise((r) => {
    firstSending = r;
  });
  const paused = {
    async send(record) {
      firstSending();
      await new Promise((r) => {
        releaseFirst = r;
      });
      order.push(...record.messages.map((m) => JSON.parse(m.value).incident.version));
    },
  };
  const normal = {
    async send(record) {
      order.push(...record.messages.map((m) => JSON.parse(m.value).incident.version));
    },
  };
  const relay1 = createOutboxRelay({ pool: db.pool, producer: paused, topic: 't', logger: silentLogger, batchSize: 1 });
  const relay2 = createOutboxRelay({ pool: db.pool, producer: normal, topic: 't', logger: silentLogger, batchSize: 1 });
  const p1 = relay1.publishPending();
  await sending; // relay1 holds version 1 and is stuck in send
  const p2 = relay2.publishPending();
  await new Promise((r) => setTimeout(r, 300));
  releaseFirst();
  await Promise.all([p1, p2]);
  await relay2.publishPending();
  assert.deepEqual(order, [1, 2], `incident ${incident.incidentId} published out of order`);
});

test('a 3PC outbox row waits until the coordinator logs COMMITTED (R2-5b)', { skip }, async () => {
  const relay = createOutboxRelay({ pool: db.pool, producer: fakeProducer(), topic: 't', logger: silentLogger });
  await relay.publishPending();
  const eventStore = fakeEventStore();
  const realDoCommit = eventStore.doCommit;
  eventStore.doCommit = async () => {
    throw new Error('event-store down');
  };
  const gated = createIncidentService({
    pool: db.pool,
    repo,
    eventStore,
    config: { tpcTimeoutMs: 500, tpcParticipantTimeoutMs: 60_000, correlationWindowMs: 900_000 },
    logger: silentLogger,
  });
  const c = candidate();
  const { incident } = await gated.upsertIncident(c); // committed, but the event-store never acked doCommit
  const outboxRows = await db.pool.query('SELECT 1 FROM outbox WHERE incident_id = $1', [incident.incidentId]);
  assert.equal(outboxRows.rowCount, 1, 'the Postgres side committed and queued its message');
  assert.equal(await relay.publishPending(), 0, 'not published while the coordinator is PRECOMMITTED');

  eventStore.doCommit = realDoCommit;
  await gated.recover();
  const producer = fakeProducer();
  const after = createOutboxRelay({ pool: db.pool, producer, topic: 't', logger: silentLogger });
  assert.equal(await after.publishPending(), 1);
  assert.equal(JSON.parse(producer.sent[0].messages[0].value).incident.incidentId, incident.incidentId);
});

test('a status change queued behind a held-back 3PC row keeps its place (R2-5)', { skip }, async () => {
  await createOutboxRelay({ pool: db.pool, producer: fakeProducer(), topic: 't', logger: silentLogger }).publishPending();
  const eventStore = fakeEventStore();
  const realDoCommit = eventStore.doCommit;
  eventStore.doCommit = async () => {
    throw new Error('event-store down');
  };
  const gated = createIncidentService({
    pool: db.pool,
    repo,
    eventStore,
    config: { tpcTimeoutMs: 500, tpcParticipantTimeoutMs: 60_000, correlationWindowMs: 900_000 },
    logger: silentLogger,
  });
  const { incident } = await gated.upsertIncident(candidate());
  // The Postgres side has committed, so an analyst may already change the status.
  await repo.updateStatus(incident.incidentId, { status: 'investigating', version: 1 });
  const producer = fakeProducer();
  const relay = createOutboxRelay({ pool: db.pool, producer, topic: 't', logger: silentLogger });
  assert.equal(await relay.publishPending(), 0, 'the status change must not overtake incident.created');

  eventStore.doCommit = realDoCommit;
  await gated.recover();
  assert.equal(await relay.publishPending(), 2);
  assert.deepEqual(
    producer.sent.flatMap((r) => r.messages.map((m) => JSON.parse(m.value).type)),
    ['incident.created', 'incident.status_changed'],
  );
});
