// End-to-end test against the Docker Compose stack.
//   docker compose up -d --build && npm run test:integration
// Publishes raw-events, normalized-events and security-alerts for a fresh
// entity and follows them through all three services.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { Kafka, Partitioners, logLevel } from 'kafkajs';

const skip = !process.env.INTEGRATION;
const BROKER = process.env.E2E_KAFKA_BROKER ?? 'localhost:29092';
const EVENT_STORE = process.env.E2E_EVENT_STORE_URL ?? 'http://localhost:8081';
const INCIDENTS = process.env.E2E_INCIDENTS_URL ?? 'http://localhost:8082';
const S3 = process.env.E2E_S3_URL ?? 'http://localhost:9000';
const RAW_BUCKET = process.env.E2E_RAW_BUCKET ?? 'cs-raw-events';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const iso = (ms) => new Date(ms).toISOString();

async function getJson(url) {
  const res = await fetch(url);
  return { status: res.status, body: res.status === 200 ? await res.json() : null };
}

// Poll fn until it returns a truthy value or the deadline passes.
async function eventually(fn, { timeoutMs = 30_000, intervalMs = 500, what = 'condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      last = await fn();
      if (last) return last;
    } catch (err) {
      last = err;
    }
    await sleep(intervalMs);
  }
  throw new Error(`timed out waiting for ${what} (last: ${last instanceof Error ? last.message : JSON.stringify(last)})`);
}

async function freshEntityKey() {
  for (let i = 0; i < 20; i += 1) {
    const key = `198.51.100.${1 + Math.floor(Math.random() * 254)}`;
    const { body } = await getJson(`${INCIDENTS}/incidents?entityKey=${encodeURIComponent(key)}&limit=500`);
    if (body && body.items.length === 0) return key;
  }
  throw new Error('no unused 198.51.100.x entity key left; reset the stack with docker compose down');
}

let kafka;
let producer;
let consumer;
const incidentUpdates = [];

before(async () => {
  if (skip) return;
  kafka = new Kafka({ clientId: 'e2e-test', brokers: [BROKER], logLevel: logLevel.NOTHING });
  producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner, allowAutoTopicCreation: false });
  await producer.connect();
  consumer = kafka.consumer({ groupId: `e2e-${randomUUID()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: 'incident-updates', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      incidentUpdates.push({ key: message.key?.toString(), value: JSON.parse(message.value.toString()) });
    },
  });
});

after(async () => {
  await consumer?.disconnect();
  await producer?.disconnect();
});

const send = (topic, key, value) => producer.send({ topic, acks: -1, messages: [{ key, value: JSON.stringify(value) }] });

function rawEvent(eventId, entityKey, t) {
  return {
    schemaVersion: '1.0',
    eventId,
    receivedAt: iso(t),
    eventTime: iso(t),
    source: { type: 'auth', name: 'e2e-sshd', host: 'bastion-1', region: 'ap-south-1' },
    format: 'json',
    payload: { msg: `Failed password for alice from ${entityKey}` },
    ingestedBy: { name: 'e2e-test', version: '1.0.0' },
  };
}

function normalizedEvent(eventId, entityKey, t) {
  return {
    schemaVersion: '1.0',
    eventId,
    eventTime: iso(t),
    receivedAt: iso(t),
    normalizedAt: iso(t + 10),
    entityKey,
    category: 'authentication',
    action: 'login',
    outcome: 'failure',
    actor: { userId: 'alice', userName: 'alice', userType: 'human' },
    network: { srcIp: entityKey, srcPort: 50000, dstIp: '10.0.0.5', dstPort: 22, protocol: 'tcp' },
    target: { resourceType: 'host', resourceId: 'bastion-1' },
    source: { type: 'auth', name: 'e2e-sshd', host: 'bastion-1' },
    message: `Failed password for alice from ${entityKey}`,
    attributes: {},
    origin: { topic: 'raw-events', partition: 0, offset: 0 },
    normalizedBy: { name: 'e2e-test', version: '1.0.0' },
  };
}

function alert(entityKey, triggerEventId, t, relatedEventIds) {
  return {
    schemaVersion: '1.0',
    alertId: `RULE-AUTH-001|${entityKey}|${triggerEventId}`,
    alertTime: iso(t),
    ruleId: 'RULE-AUTH-001',
    ruleName: 'Brute-force login attempts',
    ruleVersion: '1.0.0',
    severity: 'high',
    alertType: 'brute_force',
    title: `Possible brute force from ${entityKey}`,
    description: 'e2e test alert',
    entityKey,
    entity: { srcIp: entityKey, userId: null, host: null },
    triggerEventId,
    relatedEventIds,
    eventCount: relatedEventIds.length,
    window: { start: iso(t - 60_000), end: iso(t), durationSeconds: 60 },
    evidence: { threshold: 5, observedFailures: relatedEventIds.length, usernamesTried: ['alice'] },
    status: 'open',
    raisedBy: { name: 'detection-rules-service', version: '1.0.0' },
  };
}

let ctx;

test('alerts flow into one incident committed by 3PC across Postgres and the Event Store', { skip, timeout: 90_000 }, async () => {
  const entityKey = await freshEntityKey();
  const t0 = Date.now() - 60_000;
  const rawId = randomUUID();
  const ev1 = randomUUID();
  const ev2 = randomUUID();

  await send('raw-events', 'e2e-sshd', rawEvent(rawId, entityKey, t0));
  await send('normalized-events', entityKey, normalizedEvent(ev1, entityKey, t0 + 1_000));
  await send('normalized-events', entityKey, normalizedEvent(ev2, entityKey, t0 + 31_000));

  // Let the Event Store store the normalized events so enrichment can see them.
  await eventually(async () => (await getJson(`${EVENT_STORE}/events/${ev2}`)).status === 200, { what: 'normalized event stored' });

  const alert1 = alert(entityKey, ev1, t0 + 2_000, [ev1]);
  const alert2 = alert(entityKey, ev2, t0 + 32_000, [ev1, ev2]);
  await send('security-alerts', entityKey, alert1);
  await sleep(1_000); // keep the two upserts in order in the demo logs
  await send('security-alerts', entityKey, alert2);

  // (a) exactly one open incident for the entity, with both alerts.
  const incident = await eventually(
    async () => {
      const { body } = await getJson(`${INCIDENTS}/incidents?status=open&limit=500`);
      const mine = body.items.filter((i) => i.entityKey === entityKey);
      return mine.length === 1 && mine[0].alertCount === 2 ? mine[0] : null;
    },
    { what: 'incident with alertCount 2' },
  );
  const all = (await getJson(`${INCIDENTS}/incidents?entityKey=${encodeURIComponent(entityKey)}&limit=500`)).body.items;
  assert.equal(all.length, 1, 'exactly one incident for the entity');
  assert.equal(incident.version, 2);
  assert.ok(incident.enrichment, 'incident was enriched from the Event Store');
  assert.ok(incident.enrichment.storedEventCount >= 1);

  // (b) the Event Store holds both correlation versions.
  const correlation = await eventually(
    async () => {
      const { body } = await getJson(`${EVENT_STORE}/correlations/${encodeURIComponent(incident.correlationId)}`);
      return body?.versions?.length === 2 ? body : null;
    },
    { what: 'two correlation versions' },
  );
  assert.deepEqual(correlation.versions.map((v) => v.version), [1, 2]);
  assert.deepEqual(correlation.versions.map((v) => v.decision.kind), ['create', 'update']);
  assert.equal(correlation.versions[1].incidentId, incident.incidentId);

  // (c) the raw event was archived to S3 under raw/yyyy/mm/dd/hh/<eventId>.json.
  const d = new Date(t0);
  const p = (n) => String(n).padStart(2, '0');
  const key = `raw/${d.getUTCFullYear()}/${p(d.getUTCMonth() + 1)}/${p(d.getUTCDate())}/${p(d.getUTCHours())}/${rawId}.json`;
  await eventually(async () => (await fetch(`${S3}/${RAW_BUCKET}/${key}`, { method: 'HEAD' })).status === 200, {
    what: `S3 object ${key}`,
  });

  // (d) incident-updates shows created then updated for this incident.
  const updates = await eventually(
    async () => {
      const mine = incidentUpdates.filter((m) => m.key === incident.incidentId).map((m) => m.value.type);
      return mine.length >= 2 ? mine : null;
    },
    { what: 'incident-updates messages' },
  );
  assert.deepEqual(updates.slice(0, 2), ['incident.created', 'incident.updated']);
  const created = incidentUpdates.find((m) => m.key === incident.incidentId).value;
  assert.equal(created.schemaVersion, '1.0');
  assert.deepEqual(created.producedBy, { name: 'incident-management', version: '1.0.0' });

  // (e) the coordinator log shows two COMMITTED transactions.
  const { body: txs } = await getJson(`${INCIDENTS}/tpc/transactions?correlationId=${encodeURIComponent(incident.correlationId)}`);
  assert.deepEqual(txs.items.map((t) => t.state), ['COMMITTED', 'COMMITTED']);

  ctx = { entityKey, incident, alert1 };
});

test('redelivered alert is a no-op', { skip, timeout: 30_000 }, async () => {
  assert.ok(ctx, 'previous test must pass first');
  await send('security-alerts', ctx.entityKey, ctx.alert1);
  await sleep(5_000);
  const { body } = await getJson(`${INCIDENTS}/incidents/${ctx.incident.incidentId}`);
  assert.equal(body.version, ctx.incident.version);
  assert.equal(body.alertCount, 2);
  const { body: txs } = await getJson(
    `${INCIDENTS}/tpc/transactions?correlationId=${encodeURIComponent(ctx.incident.correlationId)}`,
  );
  assert.equal(txs.items.length, 2, 'no new 3PC transaction');
});
