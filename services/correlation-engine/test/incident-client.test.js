import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { createIncidentClient, DEFAULT_PROTO_PATH } from '../src/incident-client.js';

function loadService() {
  const def = protoLoader.loadSync(DEFAULT_PROTO_PATH, { keepCase: false, longs: String, defaults: true });
  return grpc.loadPackageDefinition(def).incident.v1.IncidentService;
}

// In-process IncidentService on an ephemeral port with a pluggable UpsertIncident.
async function startServer(upsert) {
  const server = new grpc.Server();
  server.addService(loadService().service, {
    UpsertIncident: (call, cb) => upsert(call, cb),
    GetIncident: (_call, cb) => cb({ code: grpc.status.UNIMPLEMENTED }),
  });
  const port = await new Promise((resolve, reject) =>
    server.bindAsync('127.0.0.1:0', grpc.ServerCredentials.createInsecure(), (err, p) => (err ? reject(err) : resolve(p))),
  );
  return { server, addr: `127.0.0.1:${port}` };
}

const candidate = {
  correlationId: 'cor-0123456789abcdef01234567',
  entityKey: '2001:db8::1',
  title: '2 correlated alert(s) for 2001:db8::1',
  severity: 'high',
  alertIds: ['RULE-AUTH-001|2001:db8::1|e1', 'RULE-AUTH-001|2001:db8::1|e2'],
  ruleIds: ['RULE-AUTH-001'],
  firstSeen: '2026-11-16T10:00:00.000Z',
  lastSeen: '2026-11-16T10:05:00.000Z',
  windowStart: '2026-11-16T09:55:00.000Z',
  windowEnd: '2026-11-16T10:05:00.000Z',
  eventCount: 10,
  relatedEventIds: ['e1', 'e2', 'e3'],
  entity: { srcIp: '2001:db8::1', userId: null, host: null },
};

test('UpsertIncident sends camelCase fields and returns the incident with outcome', async (t) => {
  let received;
  const { server, addr } = await startServer((call, cb) => {
    received = call.request;
    cb(null, {
      incidentId: 'b3c1f0f4-9a7e-4d43-9b8f-2a6f3d1e5c77',
      correlationId: call.request.correlationId,
      entityKey: call.request.entityKey,
      title: call.request.title,
      severity: call.request.severity,
      status: 'open',
      alertCount: call.request.alertIds.length,
      eventCount: call.request.eventCount,
      firstSeen: call.request.firstSeen,
      lastSeen: call.request.lastSeen,
      version: 1,
      createdAt: '2026-11-16T10:05:01.000Z',
      updatedAt: '2026-11-16T10:05:01.000Z',
      outcome: 'created',
    });
  });
  const client = createIncidentClient({ addr, deadlineMs: 2000 });
  t.after(() => { client.close(); server.forceShutdown(); });

  const enrichment = { storedEventCount: 4, distinctUsers: 2, failures: 4 };
  const incident = await client.upsertIncident(candidate, enrichment);

  assert.equal(received.correlationId, candidate.correlationId);
  assert.equal(received.entityKey, candidate.entityKey);
  assert.equal(received.title, candidate.title);
  assert.equal(received.severity, 'high');
  assert.deepEqual(received.alertIds, candidate.alertIds);
  assert.deepEqual(received.ruleIds, candidate.ruleIds);
  assert.equal(received.firstSeen, candidate.firstSeen);
  assert.equal(received.lastSeen, candidate.lastSeen);
  assert.equal(received.eventCount, 10);
  assert.deepEqual(received.relatedEventIds, candidate.relatedEventIds);
  assert.deepEqual(JSON.parse(received.entityJson), candidate.entity);
  assert.deepEqual(JSON.parse(received.enrichmentJson), enrichment);

  assert.equal(incident.outcome, 'created');
  assert.equal(incident.incidentId, 'b3c1f0f4-9a7e-4d43-9b8f-2a6f3d1e5c77');
  assert.equal(incident.alertCount, 2);
  assert.equal(incident.version, 1);

  // null enrichment is sent as an empty string
  await client.upsertIncident({ ...candidate, entity: undefined }, null);
  assert.equal(received.enrichmentJson, '');
  assert.equal(received.entityJson, '{}');
});

test('server errors surface with their grpc status code', async (t) => {
  const { server, addr } = await startServer((_call, cb) => cb({ code: grpc.status.ABORTED, details: 'vote_no' }));
  const client = createIncidentClient({ addr, deadlineMs: 2000 });
  t.after(() => { client.close(); server.forceShutdown(); });
  await assert.rejects(client.upsertIncident(candidate, null), (err) => err.code === grpc.status.ABORTED);
});

test('call past the deadline fails with DEADLINE_EXCEEDED', async (t) => {
  const { server, addr } = await startServer(() => {}); // never answers
  const client = createIncidentClient({ addr, deadlineMs: 100 });
  t.after(() => { client.close(); server.forceShutdown(); });
  await assert.rejects(client.upsertIncident(candidate, null), (err) => err.code === grpc.status.DEADLINE_EXCEEDED);
});
