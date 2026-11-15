import test from 'node:test';
import assert from 'node:assert/strict';
import { createDetectionEngine } from '../src/services/detection-engine.js';

function makeEvent(number, overrides = {}) {
  const eventTime = new Date(
    Date.UTC(2026, 10, 6, 7, 40, number)
  ).toISOString();

  return {
    schemaVersion: '1.0',
    eventId: `00000000-0000-4000-8000-${String(number).padStart(12, '0')}`,
    eventTime,
    receivedAt: eventTime,
    normalizedAt: eventTime,
    entityKey: '203.0.113.45',
    category: 'authentication',
    action: 'login',
    outcome: 'failure',
    actor: {
      userId: null,
      userName: 'admin',
      userType: 'unknown'
    },
    network: {
      srcIp: '203.0.113.45',
      srcPort: 51234,
      dstIp: null,
      dstPort: null,
      protocol: null
    },
    target: {
      resourceType: 'host',
      resourceId: 'web-01'
    },
    source: {
      type: 'auth_log',
      name: 'web-01',
      host: 'web-01',
      region: null,
      accountId: null
    },
    message: 'Failed SSH password login for invalid user admin from 203.0.113.45',
    attributes: {
      authMethod: 'password',
      invalidUser: true,
      program: 'sshd'
    },
    origin: {
      topic: 'raw-events',
      partition: 0,
      offset: number
    },
    normalizedBy: {
      name: 'event-normalizer',
      version: '1.0.0'
    },
    ...overrides
  };
}

test('does not alert before five failures', () => {
  const engine = createDetectionEngine();

  for (let i = 1; i < 5; i++) {
    assert.deepEqual(engine.evaluate(makeEvent(i)), []);
  }
});

test('creates alert on fifth failure', () => {
  const engine = createDetectionEngine({
    now: () => Date.UTC(2026, 10, 6, 7, 41, 0)
  });

  for (let i = 1; i < 5; i++) {
    engine.evaluate(makeEvent(i));
  }

  const [alert] = engine.evaluate(makeEvent(5));

  assert.equal(alert.ruleId, 'RULE-AUTH-001');
  assert.equal(alert.severity, 'high');
  assert.equal(alert.alertType, 'brute_force');
  assert.equal(alert.eventCount, 5);
  assert.equal(alert.relatedEventIds.length, 5);
  assert.equal(
    alert.alertId,
    `${alert.ruleId}|${alert.entityKey}|${alert.triggerEventId}`
  );
  assert.equal(alert.status, 'open');
});

test('ignores duplicate event IDs', () => {
  const engine = createDetectionEngine();
  const item = makeEvent(1);

  engine.evaluate(item);

  assert.deepEqual(engine.evaluate(item), []);
});

test('ignores successful authentication events', () => {
  const engine = createDetectionEngine();

  assert.deepEqual(
    engine.evaluate(makeEvent(1, { outcome: 'success' })),
    []
  );
});

test('suppresses repeat alerts during the quiet period', () => {
  const engine = createDetectionEngine();

  for (let i = 1; i <= 5; i++) {
    engine.evaluate(makeEvent(i));
  }

  for (let i = 6; i <= 10; i++) {
    assert.deepEqual(engine.evaluate(makeEvent(i)), []);
  }
});
