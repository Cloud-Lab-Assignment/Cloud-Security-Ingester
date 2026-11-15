import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '@core/common';
import { registerRoutes } from '../src/routes.js';
import { VersionConflict, TransactionPending } from '../src/incidents.js';

const ID = '3f0c9e3e-5b8a-4c5e-9a57-3a8e2b9f1c11';
const incident = (over = {}) => ({ incidentId: ID, status: 'open', severity: 'high', version: 1, ...over });

function setup(repoOver = {}) {
  const calls = { list: [], update: [], published: [], tx: [] };
  const repo = {
    async listIncidents(q) {
      calls.list.push(q);
      return [incident({ status: q.status ?? 'open' })];
    },
    async getIncident(id) {
      return id === ID ? incident() : null;
    },
    async getHistory() {
      return [];
    },
    async updateStatus(id, body) {
      calls.update.push({ id, body });
      return incident({ status: body.status, version: body.version + 1 });
    },
    ...repoOver,
  };
  const publisher = {
    async publishIncidentUpdate(type, inc) {
      calls.published.push({ type, inc });
    },
  };
  const coordinatorLog = {
    async list(q) {
      calls.tx.push(q);
      return [{ txId: 't1', state: 'COMMITTED' }];
    },
  };
  const app = buildApp({ service: 'incident-management', logger: false });
  registerRoutes(app, { repo, publisher, coordinatorLog });
  return { app, calls };
}

test('GET /incidents filters by status', async (t) => {
  const { app, calls } = setup();
  t.after(() => app.close());
  const res = await app.inject({ method: 'GET', url: '/incidents?status=investigating&severity=high&limit=5' });
  assert.equal(res.statusCode, 200);
  assert.equal(res.json().items[0].status, 'investigating');
  assert.deepEqual(calls.list[0], { status: 'investigating', severity: 'high', limit: 5 });
});

test('GET /incidents rejects unknown status with 400', async (t) => {
  const { app } = setup();
  t.after(() => app.close());
  const res = await app.inject({ method: 'GET', url: '/incidents?status=closed' });
  assert.equal(res.statusCode, 400);
  assert.equal(res.json().error, 'bad_request');
});

test('GET /incidents/:id returns 200 or 404', async (t) => {
  const { app } = setup();
  t.after(() => app.close());
  assert.equal((await app.inject({ method: 'GET', url: `/incidents/${ID}` })).statusCode, 200);
  const missing = await app.inject({ method: 'GET', url: '/incidents/00000000-0000-4000-8000-000000000000' });
  assert.equal(missing.statusCode, 404);
  assert.deepEqual(missing.json(), { error: 'not_found' });
  assert.equal((await app.inject({ method: 'GET', url: '/incidents/not-a-uuid' })).statusCode, 404);
});

test('PATCH updates status and publishes incident.status_changed', async (t) => {
  const { app, calls } = setup();
  t.after(() => app.close());
  const res = await app.inject({
    method: 'PATCH',
    url: `/incidents/${ID}`,
    payload: { status: 'investigating', version: 1, actor: 'alice', note: 'on it' },
  });
  assert.equal(res.statusCode, 200);
  assert.equal(res.json().status, 'investigating');
  assert.equal(res.json().version, 2);
  assert.deepEqual(calls.update[0], { id: ID, body: { status: 'investigating', version: 1, actor: 'alice', note: 'on it' } });
  assert.equal(calls.published[0].type, 'incident.status_changed');
});

test('PATCH with stale version returns 409', async (t) => {
  const { app, calls } = setup({
    async updateStatus() {
      throw new VersionConflict(ID, 1, 3);
    },
  });
  t.after(() => app.close());
  const res = await app.inject({ method: 'PATCH', url: `/incidents/${ID}`, payload: { status: 'resolved', version: 1 } });
  assert.equal(res.statusCode, 409);
  assert.deepEqual(res.json(), { error: 'version_conflict', currentVersion: 3 });
  assert.equal(calls.published.length, 0);
});

test('PATCH during a pending 3PC transaction returns 409', async (t) => {
  const { app } = setup({
    async updateStatus() {
      throw new TransactionPending(ID);
    },
  });
  t.after(() => app.close());
  const res = await app.inject({ method: 'PATCH', url: `/incidents/${ID}`, payload: { status: 'resolved', version: 1 } });
  assert.equal(res.statusCode, 409);
  assert.equal(res.json().error, 'transaction_pending');
});

test('PATCH invalid status returns 400', async (t) => {
  const { app, calls } = setup();
  t.after(() => app.close());
  const bad = await app.inject({ method: 'PATCH', url: `/incidents/${ID}`, payload: { status: 'closed', version: 1 } });
  assert.equal(bad.statusCode, 400);
  assert.equal(bad.json().error, 'bad_request');
  const noVersion = await app.inject({ method: 'PATCH', url: `/incidents/${ID}`, payload: { status: 'resolved' } });
  assert.equal(noVersion.statusCode, 400);
  assert.equal(calls.update.length, 0);
});

test('GET /tpc/transactions passes limit and correlationId', async (t) => {
  const { app, calls } = setup();
  t.after(() => app.close());
  const res = await app.inject({ method: 'GET', url: '/tpc/transactions?limit=5&correlationId=cor-%7Cx' });
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), { items: [{ txId: 't1', state: 'COMMITTED' }] });
  assert.deepEqual(calls.tx[0], { limit: 5, correlationId: 'cor-|x' });
});
