// Test fixtures in the real `security-alerts` shape (see docs/project-brief.md).
let seq = 0;

// Deterministic lowercase UUID v4-shaped ids, unique per call.
export function eventId() {
  seq += 1;
  return `00000000-0000-4000-8000-${String(seq).padStart(12, '0')}`;
}

const T0 = Date.UTC(2026, 10, 16, 10, 0, 0, 0);

// minutes offset from a fixed base time, as a UTC ISO timestamp
export function at(minutes) {
  return new Date(T0 + minutes * 60_000).toISOString();
}

export function makeAlert({
  entityKey = '203.0.113.7',
  minutes = 0,
  ruleId = 'RULE-AUTH-001',
  severity = 'high',
  eventCount = 5,
  windowStart,
  windowEnd,
  relatedEventIds,
  triggerEventId,
  entity,
} = {}) {
  const trigger = triggerEventId ?? eventId();
  const related = relatedEventIds ?? [eventId(), eventId(), trigger];
  return {
    schemaVersion: '1.0',
    alertId: `${ruleId}|${entityKey}|${trigger}`,
    alertTime: at(minutes),
    ruleId,
    ruleName: 'Brute-force authentication',
    ruleVersion: '1.0.0',
    severity,
    alertType: 'brute_force',
    title: `Possible brute-force login from ${entityKey}`,
    description: 'Repeated failed logins from one source inside the detection window',
    entityKey,
    entity: entity ?? { srcIp: entityKey, userId: null, host: null },
    triggerEventId: trigger,
    relatedEventIds: related,
    eventCount,
    window: {
      start: windowStart ?? at(minutes - 5),
      end: windowEnd ?? at(minutes),
      durationSeconds: 300,
    },
    evidence: { threshold: 5, observedFailures: eventCount, usernamesTried: ['root', 'admin'] },
    status: 'open',
    raisedBy: { name: 'detection-rules-service', version: '1.0.0' },
  };
}

// A kafkajs-style message carrying the alert as JSON.
export function toMessage(alert) {
  return { key: Buffer.from(alert.entityKey), value: Buffer.from(JSON.stringify(alert)) };
}

export const silentLogger = {
  trace() {}, debug() {}, info() {}, warn() {}, error() {}, fatal() {},
  child() { return silentLogger; },
};
