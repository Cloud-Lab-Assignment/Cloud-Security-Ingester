import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createCorrelator } from '../src/correlator.js';
import { makeAlert, at, eventId } from './fixtures.js';

const WINDOW = 15 * 60_000;
const newCorrelator = () => createCorrelator({ windowMs: WINDOW, escalateCount: 3 });

test('two alerts same entity inside window share correlationId', () => {
  const c = newCorrelator();
  const a1 = makeAlert({ minutes: 0 });
  const a2 = makeAlert({ minutes: 5 });
  const c1 = c.ingest(a1);
  const c2 = c.ingest(a2);
  assert.ok(c1 && c2);
  assert.equal(c1.correlationId, c2.correlationId);
  const expected = 'cor-' + createHash('sha256').update(`${a1.entityKey}|${a1.alertId}`).digest('hex').slice(0, 24);
  assert.equal(c1.correlationId, expected);
  assert.deepEqual(c2.alertIds, [a1.alertId, a2.alertId]);
  assert.equal(c2.title, `2 correlated alert(s) for ${a1.entityKey}`);
  assert.equal(c2.firstSeen, a1.alertTime);
  assert.equal(c2.lastSeen, a2.alertTime);
  assert.equal(c2.entityKey, a1.entityKey);
  assert.deepEqual(c2.entity, a2.entity);
});

test('alert after window starts new correlationId', () => {
  const c = newCorrelator();
  const c1 = c.ingest(makeAlert({ minutes: 0 }));
  const c2 = c.ingest(makeAlert({ minutes: 16 }));
  assert.notEqual(c1.correlationId, c2.correlationId);
  assert.equal(c2.alertIds.length, 1);
  // exactly at lastSeen + window still joins
  const c3 = c.ingest(makeAlert({ minutes: 31 }));
  assert.equal(c3.correlationId, c2.correlationId);
});

test('correlator ignores duplicate alertId', () => {
  const c = newCorrelator();
  const a = makeAlert({ minutes: 0 });
  assert.ok(c.ingest(a));
  assert.equal(c.ingest(a), null);
  assert.equal(c.ingest(structuredClone(a)), null);
  const next = c.ingest(makeAlert({ minutes: 1 }));
  assert.equal(next.alertIds.length, 2);
});

test('forget lets the same alert be ingested again', () => {
  const c = newCorrelator();
  const a1 = makeAlert({ minutes: 0 });
  const a2 = makeAlert({ minutes: 2 });
  const first = c.ingest(a1);
  c.ingest(a2);
  c.forget(a2.alertId);
  const again = c.ingest(a2);
  assert.ok(again);
  assert.equal(again.correlationId, first.correlationId);
  assert.deepEqual(again.alertIds, [a1.alertId, a2.alertId]);

  // forgetting the only alert removes the group; re-ingest recreates the same id
  const c2 = newCorrelator();
  const solo = makeAlert({ entityKey: '198.51.100.9' });
  const before = c2.ingest(solo);
  c2.forget(solo.alertId);
  const after = c2.ingest(solo);
  assert.deepEqual(after, before);
  // forgetting an unknown alert is a no-op
  c2.forget('RULE-AUTH-001|nope|nope');
});

test('out-of-order alert inside window lowers firstSeen', () => {
  const c = newCorrelator();
  const late = makeAlert({ minutes: 10 });
  const early = makeAlert({ minutes: 0 });
  const c1 = c.ingest(late);
  const c2 = c.ingest(early);
  assert.equal(c2.correlationId, c1.correlationId);
  assert.equal(c2.firstSeen, early.alertTime);
  assert.equal(c2.lastSeen, late.alertTime);
  assert.deepEqual(c2.alertIds, [early.alertId, late.alertId]);
  // an alert older than firstSeen by more than the window belongs to another group
  const ancient = c.ingest(makeAlert({ minutes: -20 }));
  assert.notEqual(ancient.correlationId, c1.correlationId);
});

test('window spans alert.window start/end', () => {
  const c = newCorrelator();
  c.ingest(makeAlert({ minutes: 0, windowStart: at(-7), windowEnd: at(0), eventCount: 5 }));
  const cand = c.ingest(makeAlert({ minutes: 4, windowStart: at(-1), windowEnd: at(4), eventCount: 7 }));
  assert.equal(cand.windowStart, at(-7));
  assert.equal(cand.windowEnd, at(4));
  assert.equal(cand.eventCount, 12);
});

test('relatedEventIds are a union capped at 200', () => {
  const c = newCorrelator();
  const shared = eventId();
  const a1 = makeAlert({ minutes: 0, relatedEventIds: [shared, eventId()] });
  const a2 = makeAlert({ minutes: 1, relatedEventIds: [shared, eventId()] });
  c.ingest(a1);
  const cand = c.ingest(a2);
  assert.equal(cand.relatedEventIds.length, 3);
  assert.equal(new Set(cand.relatedEventIds).size, 3);

  const big = createCorrelator({ windowMs: WINDOW, escalateCount: 1000 });
  let last;
  for (let i = 0; i < 12; i += 1) {
    const ids = Array.from({ length: 20 }, () => eventId());
    last = big.ingest(makeAlert({ entityKey: '192.0.2.50', minutes: i, relatedEventIds: ids }));
  }
  assert.equal(last.relatedEventIds.length, 200);
});

test('escalates to critical at escalateCount alerts', () => {
  const c = newCorrelator();
  assert.equal(c.ingest(makeAlert({ minutes: 0, severity: 'medium' })).severity, 'medium');
  assert.equal(c.ingest(makeAlert({ minutes: 1, severity: 'high' })).severity, 'high');
  assert.equal(c.ingest(makeAlert({ minutes: 2, severity: 'low' })).severity, 'critical');
});

test('escalates to critical with two distinct ruleIds', () => {
  const c = newCorrelator();
  const c1 = c.ingest(makeAlert({ minutes: 0, ruleId: 'RULE-AUTH-001' }));
  assert.equal(c1.severity, 'high');
  const c2 = c.ingest(makeAlert({ minutes: 1, ruleId: 'RULE-NET-002', severity: 'low' }));
  assert.equal(c2.severity, 'critical');
  assert.deepEqual(c2.ruleIds, ['RULE-AUTH-001', 'RULE-NET-002']);
});

test('different entities never share a group', () => {
  const c = newCorrelator();
  const a = c.ingest(makeAlert({ entityKey: '203.0.113.7', minutes: 0 }));
  const b = c.ingest(makeAlert({ entityKey: '2001:db8::1', minutes: 0 }));
  const d = c.ingest(makeAlert({ entityKey: 'alice', minutes: 1 }));
  assert.notEqual(a.correlationId, b.correlationId);
  assert.notEqual(b.correlationId, d.correlationId);
  assert.equal(b.alertIds.length, 1);
  assert.equal(d.alertIds.length, 1);
  assert.equal(b.entityKey, '2001:db8::1');
});
