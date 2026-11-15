import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeEvent } from '../src/services/normalizer.js';

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

const origin = {
  topic: 'raw-events',
  partition: 1,
  offset: 48212
};

test('normalizes a failed SSH login correctly', () => {
  const event = normalizeEvent(rawEvent, origin);

  assert.equal(event.eventId, rawEvent.eventId);
  assert.equal(event.category, 'authentication');
  assert.equal(event.action, 'login');
  assert.equal(event.outcome, 'failure');
  assert.equal(event.actor.userName, 'admin');
  assert.equal(event.network.srcIp, '203.0.113.45');
  assert.equal(event.network.srcPort, 51234);
  assert.equal(event.entityKey, '203.0.113.45');
  assert.equal(event.target.resourceId, 'web-01');
  assert.equal(event.attributes.invalidUser, true);
});

test('uses receivedAt when eventTime is unavailable', () => {
  const event = normalizeEvent(rawEvent, origin);

  assert.equal(event.eventTime, rawEvent.receivedAt);
});

test('rejects an invalid raw event ID', () => {
  const invalidEvent = {
    ...rawEvent,
    eventId: 'invalid-id'
  };

  assert.throws(
    () => normalizeEvent(invalidEvent, origin),
    /Raw event validation failed/
  );
});

test('rejects invalid Kafka origin information', () => {
  assert.throws(
    () => normalizeEvent(rawEvent, { topic: 'raw-events' }),
    /Invalid Kafka origin information/
  );
});
test('extracts a best-effort action from an unknown source', () => {
  const event = normalizeEvent(
    {
      ...rawEvent,
      source: { ...rawEvent.source, type: 'aws_cloudtrail' },
      format: 'json',
      payload: { eventName: 'ConsoleLogin' }
    },
    origin
  );

  assert.equal(event.category, 'other');
  assert.equal(event.action, 'console_login');
  assert.equal(event.outcome, 'unknown');
});

test('uses unknown when an unrecognized payload has no action', () => {
  const event = normalizeEvent(
    {
      ...rawEvent,
      source: { ...rawEvent.source, type: 'unknown_source' },
      format: 'json',
      payload: { someField: 'someValue' }
    },
    origin
  );

  assert.equal(event.action, 'unknown');
  assert.equal(event.category, 'other');
});
test('prefers payload timestamp over raw eventTime', () => {
  const payloadTime = '2026-11-06T08:00:00.000Z';

  const event = normalizeEvent(
    {
      ...rawEvent,
      eventTime: '2026-11-06T07:00:00.000Z',
      format: 'json',
      payload: { timestamp: payloadTime }
    },
    origin
  );

  assert.equal(event.eventTime, payloadTime);
});
test('supports eventTime as a payload timestamp key', () => {
  const payloadTime = '2026-11-06T08:30:00.000Z';

  const event = normalizeEvent(
    {
      ...rawEvent,
      format: 'json',
      payload: { eventTime: payloadTime }
    },
    origin
  );

  assert.equal(event.eventTime, payloadTime);
});

test('supports time as a payload timestamp key', () => {
  const payloadTime = '2026-11-06T09:00:00.000Z';

  const event = normalizeEvent(
    {
      ...rawEvent,
      format: 'json',
      payload: { time: payloadTime }
    },
    origin
  );

  assert.equal(event.eventTime, payloadTime);
});

test('ignores an impossible calendar date in the payload', () => {
  const event = normalizeEvent(
    {
      ...rawEvent,
      format: 'json',
      payload: { timestamp: '2026-02-30T08:00:00.000Z' }
    },
    origin
  );

  assert.equal(event.eventTime, rawEvent.receivedAt);
});

test('uses source name as entityKey when no network IP is available', () => {
  const event = normalizeEvent(
    {
      ...rawEvent,
      source: { ...rawEvent.source, type: 'unknown_source' },
      format: 'json',
      payload: { eventName: 'ConsoleLogin' }
    },
    origin
  );

  assert.equal(event.entityKey, rawEvent.source.name);
});

test('preserves nullable source metadata', () => {
  const event = normalizeEvent(
    {
      ...rawEvent,
      source: {
        type: 'auth_log',
        name: 'web-01',
        host: null,
        region: null,
        accountId: null
      }
    },
    origin
  );

  assert.equal(event.source.host, null);
  assert.equal(event.source.region, null);
  assert.equal(event.source.accountId, null);
});

test('generates a valid normalizedAt timestamp', () => {
  const event = normalizeEvent(rawEvent, origin);

  assert.match(
    event.normalizedAt,
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/
  );

  assert.ok(!Number.isNaN(Date.parse(event.normalizedAt)));
});
test('uses raw eventTime when payload has no valid timestamp', () => {
  const rawTime = '2026-11-06T07:00:00.000Z';

  const event = normalizeEvent(
    {
      ...rawEvent,
      eventTime: rawTime,
      format: 'json',
      payload: { timestamp: 'invalid-timestamp' }
    },
    origin
  );

  assert.equal(event.eventTime, rawTime);
});

test('handles an unrecognized SSH log without rejecting it', () => {
  const event = normalizeEvent(
    {
      ...rawEvent,
      payload: 'Unrecognized SSH log message'
    },
    origin
  );

  assert.equal(event.category, 'other');
  assert.equal(event.action, 'unknown');
  assert.equal(event.outcome, 'unknown');
  assert.equal(event.network.srcIp, null);
});
