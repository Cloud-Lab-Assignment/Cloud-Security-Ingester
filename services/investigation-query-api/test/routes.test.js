import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '@core/common';
import { registerRoutes } from '../src/routes.js';
import { createUpstream } from '../src/upstream.js';

function fakeFetch(routes) {
  const calls = [];
  const fn = async (url) => {
    calls.push(url);
    const path = new URL(url).pathname;
    const handler = routes[path];
    if (!handler) return new Response('{}', { status: 404 });
    const { status = 200, body } = handler;
    return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
  };
  fn.calls = calls;
  return fn;
}

function setup(esRoutes, imRoutes, cacheTtlMs = 0) {
  const esFetch = fakeFetch(esRoutes);
  const imFetch = fakeFetch(imRoutes);
  const app = buildApp({ service: 'test', logger: false });
  registerRoutes(app, {
    eventStore: createUpstream({ baseUrl: 'http://es', name: 'event-store', timeoutMs: 500, cacheTtlMs, fetch: esFetch }),
    incidents: createUpstream({ baseUrl: 'http://im', name: 'incident-management', timeoutMs: 500, cacheTtlMs, fetch: imFetch }),
    defaultWindowMs: 60_000,
  });
  return { app, esFetch, imFetch };
}

const incident = {
  incidentId: 'a1',
  correlationId: 'cor-1',
  entityKey: '198.18.0.1',
  firstSeen: '2026-11-16T10:00:00.000Z',
  lastSeen: '2026-11-16T10:05:00.000Z',
};

test('GET /incidents/:id aggregates incident, history, correlation and events', async () => {
  const { app } = setup(
    { '/correlations/cor-1': { body: { items: [{ version: 1 }] } }, '/events': { body: { items: [{ eventId: 'e1' }], nextCursor: null } } },
    { '/incidents/a1': { body: incident }, '/incidents/a1/history': { body: { items: [{ version: 1 }] } } },
  );
  const res = await app.inject('/incidents/a1');
  assert.equal(res.statusCode, 200);
  const body = res.json();
  assert.equal(body.incident.incidentId, 'a1');
  assert.deepEqual(body.history, [{ version: 1 }]);
  assert.equal(body.events[0].eventId, 'e1');
  assert.deepEqual(body.correlationHistory, { items: [{ version: 1 }] });
});

test('enrichment failure degrades to null, not an error', async () => {
  const { app } = setup({}, { '/incidents/a1': { body: incident } });
  const res = await app.inject('/incidents/a1');
  assert.equal(res.statusCode, 200);
  assert.equal(res.json().events, null);
});

test('unknown incident is 404', async () => {
  const { app } = setup({}, {});
  assert.equal((await app.inject('/incidents/nope')).statusCode, 404);
});

test('upstream 5xx maps to 502', async () => {
  const { app } = setup({}, { '/incidents': { status: 500, body: {} } });
  const res = await app.inject('/incidents');
  assert.equal(res.statusCode, 502);
  assert.equal(res.json().upstream, 'incident-management');
});

test('GET /events requires entityKey', async () => {
  const { app } = setup({}, {});
  assert.equal((await app.inject('/events')).statusCode, 400);
});

test('responses are cached for the TTL', async () => {
  const { app, imFetch } = setup({}, { '/incidents': { body: { items: [] } } }, 60_000);
  await app.inject('/incidents?limit=5');
  await app.inject('/incidents?limit=5');
  assert.equal(imFetch.calls.length, 1);
});
