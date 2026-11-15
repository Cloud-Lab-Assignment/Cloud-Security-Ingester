import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../src/index.js';

test('health ok', async (t) => {
  const app = buildApp({ service: 't', ready: async () => true, logger: false });
  t.after(() => app.close());
  const res = await app.inject({ method: 'GET', url: '/health' });
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), { status: 'ok', service: 't' });
});

test('ready 200 when ready', async (t) => {
  const app = buildApp({ service: 't', ready: async () => true, logger: false });
  t.after(() => app.close());
  const res = await app.inject({ method: 'GET', url: '/ready' });
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), { status: 'ready' });
});

test('ready 503 when not ready', async (t) => {
  const app = buildApp({ service: 't', ready: async () => false, logger: false });
  t.after(() => app.close());
  const res = await app.inject({ method: 'GET', url: '/ready' });
  assert.equal(res.statusCode, 503);
  assert.deepEqual(res.json(), { status: 'not_ready' });
});

test('ready 503 when the check throws', async (t) => {
  const app = buildApp({ service: 't', ready: async () => { throw new Error('boom'); }, logger: false });
  t.after(() => app.close());
  const res = await app.inject({ method: 'GET', url: '/ready' });
  assert.equal(res.statusCode, 503);
});

test('metrics exposes request counter', async (t) => {
  const app = buildApp({ service: 't', ready: async () => true, logger: false });
  t.after(() => app.close());
  await app.inject({ method: 'GET', url: '/health' });
  const res = await app.inject({ method: 'GET', url: '/metrics' });
  assert.equal(res.statusCode, 200);
  assert.match(res.headers['content-type'], /text\/plain/);
  assert.match(res.body, /http_requests_total\{[^}]*route="\/health"/);
  assert.match(res.body, /http_request_duration_seconds/);
});

test('each app has its own registry', async (t) => {
  const a = buildApp({ service: 'a', ready: async () => true, logger: false });
  const b = buildApp({ service: 'b', ready: async () => true, logger: false });
  t.after(() => Promise.all([a.close(), b.close()]));
  assert.notEqual(a.metrics, b.metrics);
});
