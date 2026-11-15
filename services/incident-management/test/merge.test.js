import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mergeCandidate } from '../src/incidents.js';

const NOW = Date.UTC(2026, 10, 16, 10, 0, 0);
const opts = { now: () => NOW, newId: () => '11111111-1111-4111-8111-111111111111' };

function candidate(over = {}) {
  return {
    correlationId: 'cor-abc',
    entityKey: '203.0.113.7',
    title: '1 correlated alert(s) for 203.0.113.7',
    severity: 'high',
    alertIds: ['RULE-AUTH-001|203.0.113.7|e1'],
    ruleIds: ['RULE-AUTH-001'],
    firstSeen: '2026-11-16T09:00:00.000Z',
    lastSeen: '2026-11-16T09:01:00.000Z',
    eventCount: 5,
    relatedEventIds: ['e1', 'e2'],
    entity: { srcIp: '203.0.113.7', userId: null, host: null },
    enrichment: null,
    ...over,
  };
}

function existingIncident(over = {}) {
  return {
    incidentId: '22222222-2222-4222-8222-222222222222',
    correlationId: 'cor-abc',
    entityKey: '203.0.113.7',
    title: '1 correlated alert(s) for 203.0.113.7',
    severity: 'high',
    status: 'investigating',
    alertIds: ['RULE-AUTH-001|203.0.113.7|e1'],
    alertCount: 1,
    eventCount: 5,
    ruleIds: ['RULE-AUTH-001'],
    relatedEventIds: ['e1', 'e2'],
    entity: { srcIp: '203.0.113.7', userId: null, host: null },
    enrichment: { storedEventCount: 3, distinctUsers: 1, failures: 3 },
    firstSeen: '2026-11-16T09:00:00.000Z',
    lastSeen: '2026-11-16T09:01:00.000Z',
    version: 3,
    createdAt: '2026-11-16T09:01:01.000Z',
    updatedAt: '2026-11-16T09:05:00.000Z',
    ...over,
  };
}

test('create when no existing', () => {
  const r = mergeCandidate(null, new Set(), candidate(), opts);
  assert.equal(r.kind, 'create');
  assert.equal(r.incident.incidentId, '11111111-1111-4111-8111-111111111111');
  assert.equal(r.incident.version, 1);
  assert.equal(r.incident.status, 'open');
  assert.equal(r.incident.correlationId, 'cor-abc');
  assert.equal(r.incident.alertCount, 1);
  assert.equal(r.incident.createdAt, '2026-11-16T10:00:00.000Z');
  assert.deepEqual(r.newAlertIds, ['RULE-AUTH-001|203.0.113.7|e1']);
});

test('create uses a random UUID v4 by default', () => {
  const r = mergeCandidate(null, new Set(), candidate());
  assert.match(r.incident.incidentId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
});

test('update unions alerts and bumps version', () => {
  const ex = existingIncident();
  const c = candidate({
    alertIds: ['RULE-AUTH-001|203.0.113.7|e1', 'RULE-AUTH-001|203.0.113.7|e9'],
    ruleIds: ['RULE-AUTH-001', 'RULE-NET-002'],
    firstSeen: '2026-11-16T08:59:00.000Z',
    lastSeen: '2026-11-16T09:03:00.000Z',
    eventCount: 4,
    relatedEventIds: ['e2', 'e9'],
  });
  const r = mergeCandidate(ex, new Set(ex.alertIds), c, opts);
  assert.equal(r.kind, 'update');
  assert.equal(r.incident.incidentId, ex.incidentId);
  assert.equal(r.incident.correlationId, 'cor-abc');
  assert.equal(r.incident.version, 4);
  assert.deepEqual(r.newAlertIds, ['RULE-AUTH-001|203.0.113.7|e9']);
  assert.deepEqual(r.incident.alertIds, ['RULE-AUTH-001|203.0.113.7|e1', 'RULE-AUTH-001|203.0.113.7|e9']);
  assert.equal(r.incident.alertCount, 2);
  assert.deepEqual(r.incident.ruleIds, ['RULE-AUTH-001', 'RULE-NET-002']);
  assert.equal(r.incident.firstSeen, '2026-11-16T08:59:00.000Z');
  assert.equal(r.incident.lastSeen, '2026-11-16T09:03:00.000Z');
  assert.equal(r.incident.eventCount, 5); // max(existing 5, candidate 4)
  assert.deepEqual(r.incident.relatedEventIds, ['e1', 'e2', 'e9']);
  assert.equal(r.incident.title, '2 correlated alert(s) for 203.0.113.7');
  assert.equal(r.incident.createdAt, ex.createdAt);
  assert.equal(r.incident.updatedAt, '2026-11-16T10:00:00.000Z');
});

test('update keeps existing enrichment when the candidate has none', () => {
  const ex = existingIncident();
  const r = mergeCandidate(ex, new Set(ex.alertIds), candidate({ alertIds: ['x2'], enrichment: null }), opts);
  assert.deepEqual(r.incident.enrichment, ex.enrichment);
  const fresh = { storedEventCount: 9, distinctUsers: 2, failures: 7 };
  const r2 = mergeCandidate(ex, new Set(ex.alertIds), candidate({ alertIds: ['x2'], enrichment: fresh }), opts);
  assert.deepEqual(r2.incident.enrichment, fresh);
});

test('update never changes status', () => {
  const ex = existingIncident({ status: 'investigating' });
  const r = mergeCandidate(ex, new Set(ex.alertIds), candidate({ alertIds: ['new-1'] }), opts);
  assert.equal(r.incident.status, 'investigating');
});

test('severity takes max', () => {
  const ex = existingIncident({ severity: 'high' });
  const low = mergeCandidate(ex, new Set(ex.alertIds), candidate({ alertIds: ['n1'], severity: 'low' }), opts);
  assert.equal(low.incident.severity, 'high');
  const crit = mergeCandidate(ex, new Set(ex.alertIds), candidate({ alertIds: ['n1'], severity: 'critical' }), opts);
  assert.equal(crit.incident.severity, 'critical');
});

test('relatedEventIds union is capped at 200', () => {
  const ex = existingIncident({ relatedEventIds: Array.from({ length: 150 }, (_, i) => `a${i}`) });
  const c = candidate({ alertIds: ['n1'], relatedEventIds: Array.from({ length: 150 }, (_, i) => `b${i}`) });
  const r = mergeCandidate(ex, new Set(ex.alertIds), c, opts);
  assert.equal(r.incident.relatedEventIds.length, 200);
});

test('upsert with only known alertIds is a no-op', () => {
  const ex = existingIncident();
  const r = mergeCandidate(ex, new Set(ex.alertIds), candidate({ alertIds: [...ex.alertIds] }), opts);
  assert.equal(r.kind, 'noop');
  assert.equal(r.incident, ex);
  assert.deepEqual(r.newAlertIds, []);
});

test('fallback merge into an incident with a different correlationId keeps the stored one', () => {
  const ex = existingIncident({ correlationId: 'cor-old' });
  const r = mergeCandidate(ex, new Set(ex.alertIds), candidate({ correlationId: 'cor-new', alertIds: ['n1'] }), opts);
  assert.equal(r.kind, 'update');
  assert.equal(r.incident.correlationId, 'cor-old');
});
