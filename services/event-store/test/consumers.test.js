import { test } from 'node:test';
import assert from 'node:assert/strict';
import client from 'prom-client';
import { startConsumers } from '../src/consumers.js';
import { normalizedEvent, rawEvent, silentLogger } from './helpers.js';

const EVENTS = {
  GROUP_JOIN: 'consumer.group_join',
  CRASH: 'consumer.crash',
  DISCONNECT: 'consumer.disconnect',
  STOP: 'consumer.stop',
};

function fakeKafka() {
  const state = { groupId: null, subscriptions: [], eachMessage: null, connected: false, disconnected: false };
  const listeners = new Map();
  const consumer = {
    events: EVENTS,
    on(event, cb) {
      listeners.set(event, cb);
      return () => listeners.delete(event);
    },
    async connect() {
      state.connected = true;
    },
    async subscribe(s) {
      state.subscriptions.push(s);
    },
    async run({ eachMessage }) {
      state.eachMessage = eachMessage;
    },
    async disconnect() {
      state.disconnected = true;
    },
  };
  return {
    state,
    emit: (event, payload = {}) => listeners.get(event)?.({ type: event, payload }),
    consumer(opts) {
      state.groupId = opts.groupId;
      return consumer;
    },
  };
}

async function setup(overrides = {}) {
  const kafka = fakeKafka();
  const archived = [];
  const stored = [];
  const registry = new client.Registry();
  const consumers = await startConsumers({
    kafka,
    logger: silentLogger(),
    groupId: 'event-store-cg',
    rawTopic: 'raw-events',
    normalizedTopic: 'normalized-events',
    archiveRaw: async (e) => {
      archived.push(e);
      return `raw/${e.eventId}.json`;
    },
    putEvent: async (e) => {
      stored.push(e);
      return 'stored';
    },
    registry,
    ...overrides,
  });
  const deliver = (topic, value) =>
    kafka.state.eachMessage({
      topic,
      partition: 0,
      message: { offset: '1', key: null, value: value === null ? null : Buffer.from(value) },
    });
  return { kafka, consumers, archived, stored, registry, deliver };
}

test('subscribes one group to both topics from the beginning', async () => {
  const { kafka } = await setup();
  assert.equal(kafka.state.groupId, 'event-store-cg');
  assert.equal(kafka.state.connected, true);
  const topics = kafka.state.subscriptions.flatMap((s) => s.topics ?? [s.topic]);
  assert.deepEqual(topics.sort(), ['normalized-events', 'raw-events']);
  assert.ok(kafka.state.subscriptions.every((s) => s.fromBeginning === true));
});

test('consumer routes by topic', async () => {
  const { archived, stored, deliver } = await setup();
  const raw = rawEvent();
  const norm = normalizedEvent();
  await deliver('raw-events', JSON.stringify(raw));
  await deliver('normalized-events', JSON.stringify(norm));
  assert.deepEqual(archived, [raw]);
  assert.deepEqual(stored, [norm]);
  await deliver('raw-events', 'not json');
  await deliver('normalized-events', 'not json');
  await deliver('normalized-events', null);
  assert.equal(archived.length, 1);
  assert.equal(stored.length, 1);
});

test('events missing required fields are skipped', async () => {
  const { archived, stored, deliver } = await setup();
  await deliver('normalized-events', JSON.stringify(normalizedEvent({ entityKey: undefined })));
  await deliver('normalized-events', JSON.stringify(normalizedEvent({ eventTime: 'yesterday' })));
  await deliver('normalized-events', JSON.stringify([1, 2]));
  await deliver('raw-events', JSON.stringify(rawEvent({ eventId: '../x' })));
  await deliver('raw-events', JSON.stringify(rawEvent({ receivedAt: null })));
  assert.equal(archived.length, 0);
  assert.equal(stored.length, 0);
});

test('storage errors are thrown so kafkajs retries', async () => {
  const boom = new Error('dynamo down');
  const { deliver } = await setup({
    putEvent: async () => {
      throw boom;
    },
    archiveRaw: async () => {
      throw new Error('s3 down');
    },
  });
  await assert.rejects(deliver('normalized-events', JSON.stringify(normalizedEvent())), /dynamo down/);
  await assert.rejects(deliver('raw-events', JSON.stringify(rawEvent())), /s3 down/);
});

test('a DynamoDB ValidationException (e.g. oversized item) is skipped, not retried, but never silently', async () => {
  const err = Object.assign(new Error('Item size has exceeded the maximum allowed size'), {
    name: 'ValidationException',
  });
  const errors = [];
  const logger = { ...silentLogger(), error: (obj, msg) => errors.push({ obj, msg }) };
  const { deliver, registry } = await setup({
    logger,
    putEvent: async () => {
      throw err;
    },
  });
  const ev = normalizedEvent();
  await deliver('normalized-events', JSON.stringify(ev));
  assert.equal(errors.length, 1);
  assert.deepEqual(
    { topic: errors[0].obj.topic, partition: errors[0].obj.partition, offset: errors[0].obj.offset, eventId: errors[0].obj.eventId },
    { topic: 'normalized-events', partition: 0, offset: '1', eventId: ev.eventId },
  );
  assert.match(await registry.metrics(), /events_consumed_total\{topic="normalized-events",result="invalid"\} 1/);
});

test('events_consumed_total counts results', async () => {
  const { deliver, registry } = await setup({ putEvent: async () => 'duplicate' });
  await deliver('normalized-events', JSON.stringify(normalizedEvent()));
  await deliver('raw-events', JSON.stringify(rawEvent()));
  await deliver('raw-events', 'nope');
  const text = await registry.metrics();
  assert.match(text, /events_consumed_total\{topic="normalized-events",result="duplicate"\} 1/);
  assert.match(text, /events_consumed_total\{topic="raw-events",result="archived"\} 1/);
  assert.match(text, /events_consumed_total\{topic="raw-events",result="invalid"\} 1/);
});

test('isReady follows group join, crash and stop', async () => {
  const { kafka, consumers } = await setup();
  assert.equal(consumers.isReady(), false);
  kafka.emit(EVENTS.GROUP_JOIN);
  assert.equal(consumers.isReady(), true);
  kafka.emit(EVENTS.CRASH, { error: new Error('x'), restart: true });
  assert.equal(consumers.isReady(), false);
  kafka.emit(EVENTS.GROUP_JOIN);
  assert.equal(consumers.isReady(), true);
  await consumers.stop();
  assert.equal(kafka.state.disconnected, true);
  assert.equal(consumers.isReady(), false);
});
