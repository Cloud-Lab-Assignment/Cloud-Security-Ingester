import { test } from 'node:test';
import assert from 'node:assert/strict';
import { isoTimestamp, SERVICE_VERSION } from '../src/index.js';

test('isoTimestamp formats ms', () => {
  assert.equal(isoTimestamp(0), '1970-01-01T00:00:00.000Z');
  assert.equal(isoTimestamp(Date.UTC(2026, 10, 16, 1, 2, 3, 4)), '2026-11-16T01:02:03.004Z');
});

test('SERVICE_VERSION is 1.0.0', () => {
  assert.equal(SERVICE_VERSION, '1.0.0');
});

import { isIsoTimestamp, kafkaConfigFromEnv } from '../src/index.js';

test('isIsoTimestamp accepts UTC ISO and rejects others', () => {
  assert.equal(isIsoTimestamp('2026-11-16T01:02:03.004Z'), true);
  assert.equal(isIsoTimestamp('2026-11-16T01:02:03Z'), true);
  assert.equal(isIsoTimestamp('2026-11-16'), false);
  assert.equal(isIsoTimestamp('2026-11-16T01:02:03+05:30'), false);
  assert.equal(isIsoTimestamp('nope'), false);
});

test('kafkaConfigFromEnv reads group env vars', () => {
  const c = kafkaConfigFromEnv({ KAFKA_BROKERS: 'a:1, b:2', KAFKA_SSL: 'true', KAFKA_USERNAME: 'u', KAFKA_PASSWORD: 'p' });
  assert.deepEqual(c.brokers, ['a:1', 'b:2']);
  assert.equal(c.ssl, true);
  assert.deepEqual(c.sasl, { mechanism: 'plain', username: 'u', password: 'p' });
  assert.equal(kafkaConfigFromEnv({ KAFKA_BROKERS: 'k:9092' }).sasl, undefined);
});
