import test from 'node:test';
import assert from 'node:assert/strict';
import { createHandler } from '../src/handler.js';

const rawEvent = {
  schemaVersion: '1.0',
  eventId: '3b1f7a52-8c4e-4d19-9a63-5f20c8d1e7b4',
  receivedAt: '2026-11-06T07:40:12.903Z',
  eventTime: null,
  source: {
    type: 'auth_log',
    name: 'web-01',
    host: 'web-01',
    region: null,
    accountId: null
  },
  format: 'text',
  payload: 'Nov 6 07:40:12 web-01 sshd[2211]: Failed password for invalid user admin from 203.0.113.45 port 51234 ssh2',
  ingestedBy: {
    name: 'log-ingestion-service',
    version: '1.0.0'
  }
};

function makeRecord(value, offset) {
  return {
    offset: String(offset),
    value: Buffer.from(
      typeof value === 'string' ? value : JSON.stringify(value)
    ).toString('base64')
  };
}

function makeEvent(value) {
  return {
    records: {
      'raw-events-1': [makeRecord(value, 10)]
    }
  };
}

function makeBatch(values) {
  return {
    records: {
      'raw-events-1': values.map((value, index) =>
        makeRecord(value, 10 + index)
      )
    }
  };
}

function makeTestHandler(send) {
  const calls = [];

  const handler = createHandler({
    kafkaProducer: {
      send: async (message) => {
        calls.push(message);
        return send ? send(message) : [];
      }
    },
    connect: async () => {},
    disconnect: async () => {}
  });

  return { handler, calls };
}

test('publishes a valid event to normalized-events', async () => {
  const { handler, calls } = makeTestHandler();

  const result = await handler(makeEvent(rawEvent));

  assert.deepEqual(result, { processed: 1, failed: 0 });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].topic, 'normalized-events');

  const published = JSON.parse(calls[0].messages[0].value);
  assert.equal(published.eventId, rawEvent.eventId);
});

test('sends an invalid event to the raw-events-dlq', async () => {
  const invalidEvent = { ...rawEvent, eventId: 'invalid-id' };
  const { handler, calls } = makeTestHandler();

  const result = await handler(makeEvent(invalidEvent));

  assert.deepEqual(result, { processed: 0, failed: 1 });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].topic, 'raw-events-dlq');
});

test('sends unsupported schema versions to the DLQ', async () => {
  const unsupportedEvent = { ...rawEvent, schemaVersion: '2.0' };
  const { handler, calls } = makeTestHandler();

  const result = await handler(makeEvent(unsupportedEvent));

  assert.deepEqual(result, { processed: 0, failed: 1 });
  assert.equal(calls[0].topic, 'raw-events-dlq');

  const message = JSON.parse(calls[0].messages[0].value);
  assert.equal(message.errorCode, 'UNSUPPORTED_SCHEMA_VERSION');
});

test('propagates normalized-events publishing failures', async () => {
  const { handler, calls } = makeTestHandler(async () => {
    throw new Error('Kafka publish failed');
  });

  await assert.rejects(
    handler(makeEvent(rawEvent)),
    /Kafka publish failed/
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].topic, 'normalized-events');
});

test('propagates DLQ publishing failures', async () => {
  const invalidEvent = { ...rawEvent, eventId: 'invalid-id' };
  const { handler, calls } = makeTestHandler(async () => {
    throw new Error('DLQ publish failed');
  });

  await assert.rejects(
    handler(makeEvent(invalidEvent)),
    /DLQ publish failed/
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].topic, 'raw-events-dlq');
});

test('processes multiple records in one batch', async () => {
  const secondEvent = {
    ...rawEvent,
    eventId: '4b1f7a52-8c4e-4d19-9a63-5f20c8d1e7b5'
  };

  const { handler, calls } = makeTestHandler();

  const result = await handler(makeBatch([rawEvent, secondEvent]));

  assert.deepEqual(result, { processed: 2, failed: 0 });
  assert.equal(calls.length, 2);
  assert.equal(calls[0].topic, 'normalized-events');
  assert.equal(calls[1].topic, 'normalized-events');
});

test('preserves eventId when the same batch is delivered again', async () => {
  const { handler, calls } = makeTestHandler();

  await handler(makeEvent(rawEvent));
  await handler(makeEvent(rawEvent));

  const publishedIds = calls.map((call) =>
    JSON.parse(call.messages[0].value).eventId
  );

  assert.deepEqual(publishedIds, [
    rawEvent.eventId,
    rawEvent.eventId
  ]);
});
