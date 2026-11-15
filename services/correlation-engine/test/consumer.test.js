import { test } from 'node:test';
import assert from 'node:assert/strict';
import client from 'prom-client';
import { status as grpcStatus } from '@grpc/grpc-js';
import { createCorrelator } from '../src/correlator.js';
import { handleAlert, createMetrics } from '../src/consumer.js';
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
