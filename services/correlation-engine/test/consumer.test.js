import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import client from 'prom-client';
import { status as grpcStatus } from '@grpc/grpc-js';
import { createCorrelator } from '../src/correlator.js';
import { handleAlert, createMetrics, startConsumer } from '../src/consumer.js';
import { makeAlert, toMessage, silentLogger } from './fixtures.js';

function grpcError(code, message = 'boom') {
  return Object.assign(new Error(`${code} ${message}`), { code, details: message });
}

function setup({ fetchEnrichment, upsertIncident } = {}) {
  const registry = new client.Registry();
  const calls = { enrich: [], upsert: [] };
  const deps = {
    correlator: createCorrelator({ windowMs: 15 * 60_000, escalateCount: 3 }),
    logger: silentLogger,
    metrics: createMetrics(registry),
    fetchEnrichment: async (...args) => {
      calls.enrich.push(args);
      return fetchEnrichment ? fetchEnrichment(...args) : { storedEventCount: 3, distinctUsers: 1, failures: 3 };
    },
    upsertIncident: async (candidate, enrichment) => {
      calls.upsert.push({ candidate, enrichment });
      if (upsertIncident) return upsertIncident(candidate, enrichment, calls.upsert.length);
      return { incidentId: 'i-1', correlationId: candidate.correlationId, outcome: 'created' };
    },
  };
  return { deps, calls, registry };
}

async function metricValue(registry, name, labels) {
  const metric = await registry.getSingleMetric(name).get();
  const match = metric.values.find((v) => Object.entries(labels).every(([k, val]) => v.labels[k] === val));
  return match?.value ?? 0;
}

test('happy path enriches and upserts the candidate', async () => {
  const { deps, calls, registry } = setup();
  const alert = makeAlert({ entityKey: '2001:db8::1' });
  await handleAlert(toMessage(alert), deps);
  assert.equal(calls.enrich.length, 1);
  assert.deepEqual(calls.enrich[0], ['2001:db8::1', alert.window.start, alert.window.end]);
  assert.equal(calls.upsert.length, 1);
  assert.deepEqual(calls.upsert[0].candidate.alertIds, [alert.alertId]);
  assert.deepEqual(calls.upsert[0].enrichment, { storedEventCount: 3, distinctUsers: 1, failures: 3 });
  assert.equal(await metricValue(registry, 'incidents_upserted_total', { outcome: 'created' }), 1);
  assert.equal(await metricValue(registry, 'alerts_consumed_total', { result: 'upserted' }), 1);
});

test('enrichment failure yields null enrichment', async () => {
  // both a client that returns null and one that throws must end in enrichment: null
  for (const fetchEnrichment of [async () => null, async () => { throw new Error('ECONNREFUSED'); }]) {
    const { deps, calls } = setup({ fetchEnrichment });
    await handleAlert(toMessage(makeAlert()), deps);
    assert.equal(calls.upsert.length, 1);
    assert.equal(calls.upsert[0].enrichment, null);
  }
});

test('failed upsert forgets alert so retry re-sends', async () => {
  const { deps, calls, registry } = setup({
    upsertIncident: async (candidate, _e, n) => {
      if (n === 1) throw grpcError(grpcStatus.UNAVAILABLE, 'connect failed');
      return { correlationId: candidate.correlationId, outcome: 'created' };
    },
  });
  const message = toMessage(makeAlert());
  await assert.rejects(handleAlert(message, deps), (err) => err.code === grpcStatus.UNAVAILABLE);
  await handleAlert(message, deps);
  assert.equal(calls.upsert.length, 2);
  assert.deepEqual(calls.upsert[1].candidate, calls.upsert[0].candidate);
  // a third delivery after success is a duplicate and is not re-sent
  await handleAlert(message, deps);
  assert.equal(calls.upsert.length, 2);
  assert.equal(await metricValue(registry, 'alerts_consumed_total', { result: 'failed' }), 1);
  assert.equal(await metricValue(registry, 'alerts_consumed_total', { result: 'duplicate' }), 1);
});

test('ABORTED upsert is rethrown so kafkajs retries', async () => {
  const { deps, calls } = setup({
    upsertIncident: async () => { throw grpcError(grpcStatus.ABORTED, 'vote_no'); },
  });
  const message = toMessage(makeAlert());
  await assert.rejects(handleAlert(message, deps), (err) => err.code === grpcStatus.ABORTED);
  await assert.rejects(handleAlert(message, deps), (err) => err.code === grpcStatus.ABORTED);
  assert.equal(calls.upsert.length, 2);
});

test('INVALID_ARGUMENT is skipped without throw', async () => {
  const { deps, calls, registry } = setup({
    upsertIncident: async () => { throw grpcError(grpcStatus.INVALID_ARGUMENT, 'bad severity'); },
  });
  await handleAlert(toMessage(makeAlert()), deps);
  assert.equal(calls.upsert.length, 1);
  assert.equal(await metricValue(registry, 'alerts_consumed_total', { result: 'rejected' }), 1);
});

test('invalid json skipped without throw', async () => {
  const { deps, calls, registry } = setup();
  await handleAlert({ key: null, value: Buffer.from('not json') }, deps);
  await handleAlert({ key: null, value: null }, deps);
  assert.equal(calls.enrich.length, 0);
  assert.equal(calls.upsert.length, 0);
  assert.equal(await metricValue(registry, 'alerts_consumed_total', { result: 'invalid' }), 2);
});

test('alert missing required fields is skipped without throw', async () => {
  const { deps, calls } = setup();
  for (const field of ['alertId', 'entityKey', 'alertTime']) {
    const alert = makeAlert();
    delete alert[field];
    await handleAlert({ value: Buffer.from(JSON.stringify(alert)) }, deps);
  }
  await handleAlert(toMessage({ ...makeAlert(), alertTime: 'yesterday' }), deps);
  await handleAlert({ value: Buffer.from('[1,2]') }, deps);
  await handleAlert({ value: Buffer.from('null') }, deps);
  assert.equal(calls.enrich.length, 0);
  assert.equal(calls.upsert.length, 0);
});

test('wrong-typed alert fields are skipped without throw and leave the correlator unchanged', async () => {
  const { deps, calls, registry } = setup();
  const base = makeAlert({ entityKey: '192.0.2.77' });
  const badFields = [
    { alertId: 42 },
    { alertId: '' },
    { entityKey: { ip: '192.0.2.77' } },
    { entityKey: '' },
    { severity: 3 },
    { alertTime: 123 },
    { alertTime: 'yesterday' },
    { window: 'x' },
    { window: [] },
    { window: { start: 'nope', end: base.window.end } },
    { window: { start: base.window.start, end: 5 } },
    { eventCount: '5' },
    { eventCount: -1 },
    { eventCount: 1.5 },
    { relatedEventIds: 'a' },
    { relatedEventIds: [1, 2] },
    { ruleId: 7 },
    { entity: 'x' },
    { entity: [] },
  ];
  for (const bad of badFields) {
    const message = { key: Buffer.from(base.entityKey), value: Buffer.from(JSON.stringify({ ...base, ...bad })) };
    await handleAlert(message, deps, { topic: 'security-alerts', partition: 0 });
  }
  assert.equal(calls.upsert.length, 0);
  assert.equal(calls.enrich.length, 0);
  assert.equal(await metricValue(registry, 'alerts_consumed_total', { result: 'invalid' }), badFields.length);

  // the correlator never saw them: the valid alert with the same alertId is
  // new (not a duplicate) and opens a one-alert group
  await handleAlert(toMessage(base), deps);
  assert.equal(calls.upsert.length, 1);
  assert.deepEqual(calls.upsert[0].candidate.alertIds, [base.alertId]);
});

test('optional alert fields may be absent or null', async () => {
  const { deps, calls } = setup();
  const alert = makeAlert({ entityKey: '192.0.2.78' });
  for (const field of ['window', 'eventCount', 'relatedEventIds', 'ruleId', 'entity']) delete alert[field];
  await handleAlert(toMessage(alert), deps);
  const nulls = { ...makeAlert({ entityKey: '192.0.2.78', minutes: 1 }), window: null, eventCount: null, relatedEventIds: null, ruleId: null, entity: null };
  await handleAlert(toMessage(nulls), deps);
  // unknown severity strings still map to medium
  await handleAlert(toMessage(makeAlert({ entityKey: '192.0.2.79', severity: 'urgent' })), deps);
  assert.equal(calls.upsert.length, 3);
  assert.equal(calls.upsert[1].candidate.alertIds.length, 2);
  assert.equal(calls.upsert[2].candidate.severity, 'medium');
});

test('an alert whose ingest throws is skipped as invalid', async () => {
  const { deps, calls, registry } = setup();
  deps.correlator = {
    ingest() { throw new TypeError('unexpected'); },
    forget() {},
  };
  await handleAlert(toMessage(makeAlert()), deps);
  assert.equal(calls.upsert.length, 0);
  assert.equal(await metricValue(registry, 'alerts_consumed_total', { result: 'invalid' }), 1);
});

test('invalid alerts are logged at warn with topic, partition and offset', async () => {
  const { deps } = setup();
  const warnings = [];
  deps.logger = { ...silentLogger, warn: (obj, msg) => warnings.push({ obj, msg }) };
  await handleAlert({ ...toMessage({ ...makeAlert(), window: 'x' }), offset: '17' }, deps, { topic: 'security-alerts', partition: 2 });
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].obj.topic, 'security-alerts');
  assert.equal(warnings[0].obj.partition, 2);
  assert.equal(warnings[0].obj.offset, '17');
  assert.match(warnings[0].obj.reason, /window/);
});

// A kafkajs-like consumer that lets the test emit instrumentation events.
function fakeKafka() {
  const emitter = new EventEmitter();
  const consumer = {
    events: {
      CONNECT: 'consumer.connect',
      GROUP_JOIN: 'consumer.group_join',
      CRASH: 'consumer.crash',
      STOP: 'consumer.stop',
      DISCONNECT: 'consumer.disconnect',
    },
    on: (event, listener) => {
      emitter.on(event, listener);
      return () => emitter.off(event, listener);
    },
    connect: async () => emitter.emit('consumer.connect', { type: 'consumer.connect', payload: null }),
    subscribe: async () => {},
    run: async () => {},
    disconnect: async () => emitter.emit('consumer.disconnect', { type: 'consumer.disconnect', payload: null }),
  };
  const emit = (name, payload = {}) => emitter.emit(consumer.events[name], { type: consumer.events[name], payload });
  return { kafka: { consumer: () => consumer }, emit };
}

test('readiness follows the kafkajs consumer group state', async () => {
  const { kafka, emit } = fakeKafka();
  const handle = await startConsumer({ kafka, logger: silentLogger, topic: 'security-alerts', groupId: 'g', handle: async () => {} });
  // connected but not yet in the group
  assert.equal(handle.isReady(), false);
  emit('GROUP_JOIN');
  assert.equal(handle.isReady(), true);
  emit('CRASH', { error: new Error('boom'), restart: true });
  assert.equal(handle.isReady(), false);
  // kafkajs restarts the consumer, which rejoins the group
  emit('GROUP_JOIN');
  assert.equal(handle.isReady(), true);
  emit('STOP');
  assert.equal(handle.isReady(), false);
  emit('GROUP_JOIN');
  emit('DISCONNECT');
  assert.equal(handle.isReady(), false);
  emit('GROUP_JOIN');
  assert.equal(handle.isReady(), true);
  // after stop() a late GROUP_JOIN does not make it ready again
  await handle.stop();
  assert.equal(handle.isReady(), false);
  emit('GROUP_JOIN');
  assert.equal(handle.isReady(), false);
});
