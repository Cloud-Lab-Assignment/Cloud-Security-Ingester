import { randomUUID } from 'node:crypto';

// Shared fixtures and fakes for the event-store tests.

export function normalizedEvent(overrides = {}) {
  return {
    schemaVersion: '1.0',
    eventId: randomUUID(),
    eventTime: '2026-11-16T05:00:00.000Z',
    receivedAt: '2026-11-16T05:00:01.000Z',
    normalizedAt: '2026-11-16T05:00:02.000Z',
    entityKey: '10.0.0.5',
    category: 'authentication',
    action: 'login',
    outcome: 'failure',
    actor: { userId: 'u1', userName: 'alice', userType: 'human' },
    network: { srcIp: '10.0.0.5', srcPort: 51515, dstIp: '10.0.0.1', dstPort: 22, protocol: 'tcp' },
    target: { resourceType: 'host', resourceId: 'bastion-1' },
    source: { type: 'syslog', name: 'sshd', host: 'bastion-1' },
    message: 'Failed password for alice',
    attributes: { attempt: 1 },
    origin: { topic: 'raw-events', partition: 0, offset: '42' },
    normalizedBy: { name: 'event-normalizer', version: '1.0.0' },
    ...overrides,
  };
}

export function rawEvent(overrides = {}) {
  return {
    schemaVersion: '1.0',
    eventId: randomUUID(),
    receivedAt: '2026-11-16T05:07:00.000Z',
    eventTime: null,
    source: { type: 'syslog', name: 'sshd', host: 'bastion-1' },
    format: 'text',
    payload: 'Failed password for alice from 10.0.0.5',
    ingestedBy: { name: 'log-ingestion', version: '1.0.0' },
    ...overrides,
  };
}

export function correlationPayload(overrides = {}) {
  return {
    correlationId: 'cor-0123456789abcdef01234567',
    version: 1,
    incidentId: randomUUID(),
    alertIds: ['RULE-AUTH-001|10.0.0.5|e1'],
    decision: {
      kind: 'create',
      severity: 'high',
      newAlertIds: ['RULE-AUTH-001|10.0.0.5|e1'],
      firstSeen: '2026-11-16T05:00:00.000Z',
      lastSeen: '2026-11-16T05:00:00.000Z',
    },
    ...overrides,
  };
}

// In-memory stand-in for the DynamoDB repo, recording the calls the routes make.
export function fakeRepo() {
  const calls = { getEvent: [], queryEvents: [], getCorrelation: [], putCorrelation: [] };
  const events = new Map();
  const correlations = new Map();
  const tpc = new Map();
  return {
    calls,
    events,
    correlations,
    async getEvent(eventId) {
      calls.getEvent.push(eventId);
      return events.get(eventId) ?? null;
    },
    async queryEvents(q) {
      calls.queryEvents.push(q);
      return { items: [], nextCursor: null };
    },
    async getCorrelation(correlationId) {
      calls.getCorrelation.push(correlationId);
      return correlations.get(correlationId) ?? [];
    },
    async putCorrelation(rec) {
      calls.putCorrelation.push(structuredClone(rec));
      const list = correlations.get(rec.correlationId) ?? [];
      if (list.some((r) => r.version === rec.version)) return 'duplicate';
      list.push(structuredClone(rec));
      correlations.set(rec.correlationId, list);
      return 'stored';
    },
    tpcStore: {
      async get(txId) {
        const r = tpc.get(txId);
        return r ? structuredClone(r) : null;
      },
      async put(rec) {
        tpc.set(rec.txId, structuredClone(rec));
      },
      async listPending() {
        return [...tpc.values()]
          .filter((r) => r.state === 'UNCERTAIN' || r.state === 'PRECOMMITTED')
          .map((r) => structuredClone(r));
      },
    },
  };
}

// Silent logger with the pino surface the modules use.
export function silentLogger() {
  const noop = () => {};
  const l = { info: noop, warn: noop, error: noop, debug: noop, fatal: noop, trace: noop };
  l.child = () => l;
  return l;
}

// Integration settings: DynamoDB Local with dummy credentials and a unique table prefix.
export function integrationEnv() {
  process.env.DYNAMODB_ENDPOINT ??= 'http://localhost:8000';
  process.env.AWS_ACCESS_KEY_ID ??= 'local';
  process.env.AWS_SECRET_ACCESS_KEY ??= 'local';
  process.env.AWS_REGION ??= 'ap-south-1';
  return {
    endpoint: process.env.DYNAMODB_ENDPOINT,
    region: process.env.AWS_REGION,
    tablePrefix: `t${randomUUID().slice(0, 8)}-`,
  };
}
