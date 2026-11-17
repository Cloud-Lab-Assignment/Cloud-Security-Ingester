import { test } from 'node:test';
import assert from 'node:assert/strict';
import grpc from '@grpc/grpc-js';
import { loadIncidentProto, startGrpc } from '../src/grpc.js';
import { IncidentAborted } from '../src/tpc-wiring.js';
import { silentLogger } from './helpers.js';

const INCIDENT = {
  incidentId: '3f0c9e3e-5b8a-4c5e-9a57-3a8e2b9f1c11',
  correlationId: 'cor-0123456789abcdef01234567',
  entityKey: '2001:db8::1',
  title: '2 correlated alert(s) for 2001:db8::1',
  severity: 'critical',
  status: 'open',
  alertCount: 2,
  eventCount: 12,
  firstSeen: '2026-11-16T09:00:00.000Z',
  lastSeen: '2026-11-16T09:00:30.000Z',
  version: 2,
  createdAt: '2026-11-16T09:00:01.000Z',
  updatedAt: '2026-11-16T09:00:31.000Z',
};

function request(over = {}) {
  return {
    correlationId: INCIDENT.correlationId,
    entityKey: '2001:db8::1',
    title: '2 correlated alert(s) for 2001:db8::1',
    severity: 'critical',
    alertIds: ['RULE-AUTH-001|2001:db8::1|e1', 'RULE-AUTH-001|2001:db8::1|e2'],
    ruleIds: ['RULE-AUTH-001'],
    firstSeen: '2026-11-16T09:00:00.000Z',
    lastSeen: '2026-11-16T09:00:30.000Z',
    eventCount: 12,
    relatedEventIds: ['e1', 'e2'],
    entityJson: JSON.stringify({ srcIp: '2001:db8::1', userId: null, host: null }),
    enrichmentJson: JSON.stringify({ storedEventCount: 12, distinctUsers: 3, failures: 12 }),
    ...over,
  };
}

async function withServer(handlers, fn) {
  const { server, port } = await startGrpc({ port: 0, host: '127.0.0.1', logger: silentLogger, ...handlers });
  const { IncidentService } = loadIncidentProto().incident.v1;
  const client = new IncidentService(`127.0.0.1:${port}`, grpc.credentials.createInsecure());
  const call = (method, req) =>
    new Promise((resolve, reject) => client[method](req, (err, res) => (err ? reject(err) : resolve(res))));
  try {
    await fn(call);
  } finally {
    client.close();
    server.forceShutdown();
  }
}

test('grpc UpsertIncident round-trip', async () => {
  let received;
  await withServer(
    {
      upsertIncident: async (candidate) => {
        received = candidate;
        return { incident: INCIDENT, outcome: 'updated' };
      },
      getIncident: async () => null,
    },
    async (call) => {
      const res = await call('UpsertIncident', request());
      assert.equal(res.incidentId, INCIDENT.incidentId);
      assert.equal(res.correlationId, INCIDENT.correlationId);
      assert.equal(res.entityKey, '2001:db8::1');
      assert.equal(res.alertCount, 2);
      assert.equal(res.version, 2);
      assert.equal(res.firstSeen, INCIDENT.firstSeen);
      assert.equal(res.outcome, 'updated');
    },
  );
  assert.deepEqual(received.alertIds, request().alertIds);
  assert.deepEqual(received.entity, { srcIp: '2001:db8::1', userId: null, host: null });
  assert.deepEqual(received.enrichment, { storedEventCount: 12, distinctUsers: 3, failures: 12 });
  assert.equal(received.eventCount, 12);
});

test('grpc empty enrichmentJson becomes null enrichment', async () => {
  let received;
  await withServer(
    {
      upsertIncident: async (c) => {
        received = c;
        return { incident: INCIDENT, outcome: 'created' };
      },
      getIncident: async () => null,
    },
    async (call) => {
      await call('UpsertIncident', request({ enrichmentJson: '' }));
    },
  );
  assert.equal(received.enrichment, null);
});

test('grpc maps IncidentAborted to ABORTED', async () => {
  await withServer(
    {
      upsertIncident: async () => {
        throw new IncidentAborted('vote_no');
      },
      getIncident: async () => null,
    },
    async (call) => {
      await assert.rejects(call('UpsertIncident', request()), (err) => {
        assert.equal(err.code, grpc.status.ABORTED);
        assert.match(err.details, /vote_no/);
        return true;
      });
    },
  );
});

test('grpc rejects missing correlationId with INVALID_ARGUMENT', async () => {
  let called = false;
  await withServer(
    {
      upsertIncident: async () => {
        called = true;
      },
      getIncident: async () => null,
    },
    async (call) => {
      await assert.rejects(call('UpsertIncident', request({ correlationId: '' })), (err) => err.code === grpc.status.INVALID_ARGUMENT);
      await assert.rejects(call('UpsertIncident', request({ severity: 'urgent' })), (err) => err.code === grpc.status.INVALID_ARGUMENT);
      await assert.rejects(call('UpsertIncident', request({ alertIds: [] })), (err) => err.code === grpc.status.INVALID_ARGUMENT);
      await assert.rejects(call('UpsertIncident', request({ entityJson: '{nope' })), (err) => err.code === grpc.status.INVALID_ARGUMENT);
      await assert.rejects(call('UpsertIncident', request({ firstSeen: 'yesterday' })), (err) => err.code === grpc.status.INVALID_ARGUMENT);
    },
  );
  assert.equal(called, false);
});

test('grpc maps unexpected errors to INTERNAL', async () => {
  await withServer(
    {
      upsertIncident: async () => {
        throw new Error('db exploded');
      },
      getIncident: async () => null,
    },
    async (call) => {
      await assert.rejects(call('UpsertIncident', request()), (err) => err.code === grpc.status.INTERNAL);
    },
  );
});

test('grpc GetIncident returns NOT_FOUND for unknown ids', async () => {
  await withServer(
    {
      upsertIncident: async () => null,
      getIncident: async (id) => (id === INCIDENT.incidentId ? INCIDENT : null),
    },
    async (call) => {
      const res = await call('GetIncident', { incidentId: INCIDENT.incidentId });
      assert.equal(res.status, 'open');
      assert.equal(res.outcome, '');
      await assert.rejects(
        call('GetIncident', { incidentId: '00000000-0000-4000-8000-000000000000' }),
        (err) => err.code === grpc.status.NOT_FOUND,
      );
      await assert.rejects(call('GetIncident', { incidentId: 'not-a-uuid' }), (err) => err.code === grpc.status.NOT_FOUND);
    },
  );
});

test('grpc maps CoordinatorBusy to UNAVAILABLE so the caller retries (R3-2)', async () => {
  const { CoordinatorBusy } = await import('@core/tpc');
  await withServer(
    {
      upsertIncident: async () => {
        throw new CoordinatorBusy('tx-busy', 30000);
      },
      getIncident: async () => null,
    },
    async (call) => {
      await assert.rejects(call('UpsertIncident', request()), (err) => err.code === grpc.status.UNAVAILABLE);
    },
  );
});
