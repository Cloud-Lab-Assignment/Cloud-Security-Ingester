import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createCoordinatorClient } from '../src/coordinator-client.js';
import { loadConfig } from '../src/config.js';
import { createCorrelationParticipant } from '../src/participant.js';
import { fakeRepo, correlationPayload, silentLogger } from './helpers.js';

function fakeFetch(handler) {
  const calls = [];
  const fetch = async (url, opts = {}) => {
    calls.push({ url, opts });
    return handler(url, opts);
  };
  return { fetch, calls };
}

const json = (status, body) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

test('maps the coordinator log state to a decision', async () => {
  const cases = [
    ['COMMITTED', 'COMMITTED'],
    ['ABORTED', 'ABORTED'],
    ['WAITING', 'PENDING'],
    ['PRECOMMITTED', 'PENDING'],
    ['SOMETHING_ELSE', null],
  ];
  for (const [state, expected] of cases) {
    const { fetch } = fakeFetch(async () => json(200, { txId: 't', state, updatedAt: 1 }));
    const resolve = createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() });
    assert.equal(await resolve('t'), expected, state);
  }
});

test('GETs /tpc/transactions/:txId with the id URL-encoded and a timeout signal', async () => {
  const { fetch, calls } = fakeFetch(async () => json(200, { state: 'ABORTED' }));
  const resolve = createCoordinatorClient({ url: 'http://im:8082/', fetch, timeoutMs: 1234, logger: silentLogger() });
  await resolve('a/b c|d');
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, 'http://im:8082/tpc/transactions/a%2Fb%20c%7Cd');
  assert.ok(calls[0].opts.signal instanceof AbortSignal);
  assert.equal(calls[0].opts.method ?? 'GET', 'GET');
});

test('404, other statuses, bad bodies and network errors → null', async () => {
  const handlers = [
    async () => json(404, { error: 'not_found' }),
    async () => json(500, { error: 'internal' }),
    async () => json(503, { state: 'COMMITTED' }),
    async () => new Response('not json', { status: 200 }),
    async () => json(200, null),
    async () => {
      throw new TypeError('fetch failed');
    },
  ];
  for (const h of handlers) {
    const { fetch } = fakeFetch(h);
    const resolve = createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() });
    assert.equal(await resolve('t'), null);
  }
});

test('a coordinator that does not answer in time → null', async () => {
  const { fetch } = fakeFetch(
    (url, { signal }) =>
      new Promise((resolve, reject) => {
        signal.addEventListener('abort', () => reject(signal.reason));
      }),
  );
  const resolve = createCoordinatorClient({ url: 'http://im:8082', fetch, timeoutMs: 20, logger: silentLogger() });
  const started = Date.now();
  assert.equal(await resolve('t'), null);
  assert.ok(Date.now() - started < 1000);
});

test('an empty COORDINATOR_URL disables the lookup', () => {
  assert.equal(createCoordinatorClient({ url: '', fetch: async () => json(200, {}) }), undefined);
  assert.equal(createCoordinatorClient({ url: undefined }), undefined);
});

test('config: COORDINATOR_URL defaults to localhost:8082, empty string disables', () => {
  const saved = { url: process.env.COORDINATOR_URL, t: process.env.COORDINATOR_TIMEOUT_MS };
  try {
    delete process.env.COORDINATOR_URL;
    delete process.env.COORDINATOR_TIMEOUT_MS;
    let c = loadConfig();
    assert.equal(c.coordinatorUrl, 'http://localhost:8082');
    assert.equal(c.coordinatorTimeoutMs, 2000);
    process.env.COORDINATOR_URL = '';
    process.env.COORDINATOR_TIMEOUT_MS = '500';
    c = loadConfig();
    assert.equal(c.coordinatorUrl, '');
    assert.equal(c.coordinatorTimeoutMs, 500);
    process.env.COORDINATOR_URL = 'http://incident-management:8082';
    assert.equal(loadConfig().coordinatorUrl, 'http://incident-management:8082');
  } finally {
    for (const [k, v] of [['COORDINATOR_URL', saved.url], ['COORDINATOR_TIMEOUT_MS', saved.t]]) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
});

test('the correlation participant asks the coordinator before acting on a timeout', async () => {
  let t = 1_000_000;
  const repo = fakeRepo();
  const { fetch, calls } = fakeFetch(async () => json(200, { state: 'ABORTED' }));
  const participant = createCorrelationParticipant({
    store: repo.tpcStore,
    putCorrelation: (rec) => repo.putCorrelation(rec),
    timeoutMs: 1000,
    now: () => t,
    resolve: createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() }),
  });
  assert.equal(await participant.canCommit('tx-a', correlationPayload()), 'yes');
  await participant.preCommit('tx-a');
  t += 5000;
  // PRECOMMITTED past its timeout, but the coordinator logged ABORTED.
  assert.deepEqual(await participant.sweep(), { committed: [], aborted: ['tx-a'], pending: [] });
  assert.equal(calls[0].url, 'http://im:8082/tpc/transactions/tx-a');
  assert.equal((await repo.tpcStore.get('tx-a')).state, 'ABORTED');
  assert.equal(repo.calls.putCorrelation.length, 0);
});
