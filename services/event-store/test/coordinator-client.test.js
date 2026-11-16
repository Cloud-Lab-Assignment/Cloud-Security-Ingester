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

test('200: maps the coordinator log state to a decision (unknown states → PENDING)', async () => {
  const cases = [
    ['COMMITTED', 'COMMITTED'],
    ['ABORTED', 'ABORTED'],
    ['WAITING', 'PENDING'],
    ['PRECOMMITTED', 'PENDING'],
    ['SOMETHING_ELSE', 'PENDING'],
    [undefined, 'PENDING'],
    [42, 'PENDING'],
    ['constructor', 'PENDING'],
  ];
  for (const [state, expected] of cases) {
    const { fetch } = fakeFetch(async () => json(200, { txId: 't', state, updatedAt: 1 }));
    const resolve = createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() });
    assert.equal(await resolve('t'), expected, String(state));
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

test('404 → UNKNOWN (the coordinator never started the transaction)', async () => {
  const { fetch } = fakeFetch(async () => json(404, { error: 'not_found' }));
  const resolve = createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() });
  assert.equal(await resolve('t'), 'UNKNOWN');
});

test('5xx and other error statuses → PENDING (alive, no answer yet)', async () => {
  for (const res of [json(500, { error: 'internal' }), json(503, { state: 'COMMITTED' }), json(502, null), json(400, {})]) {
    const { fetch } = fakeFetch(async () => res);
    const resolve = createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() });
    assert.equal(await resolve('t'), 'PENDING', String(res.status));
  }
});

test('malformed 200 bodies → PENDING', async () => {
  const bodies = [
    () => new Response('not json', { status: 200 }),
    () => json(200, null),
    () => json(200, ['COMMITTED']),
    () => json(200, 'COMMITTED'),
    () => new Response('', { status: 200 }),
  ];
  for (const body of bodies) {
    const { fetch } = fakeFetch(async () => body());
    const resolve = createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() });
    assert.equal(await resolve('t'), 'PENDING');
  }
});

test('transport failures (refused, DNS, socket) → null (unreachable: timeout rule)', async () => {
  const errors = [
    Object.assign(new TypeError('fetch failed'), { cause: Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' }) }),
    Object.assign(new TypeError('fetch failed'), { cause: Object.assign(new Error('getaddrinfo ENOTFOUND im'), { code: 'ENOTFOUND' }) }),
    Object.assign(new TypeError('fetch failed'), { cause: Object.assign(new Error('other side closed'), { code: 'UND_ERR_SOCKET' }) }),
  ];
  for (const err of errors) {
    const { fetch } = fakeFetch(async () => {
      throw err;
    });
    const resolve = createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() });
    assert.equal(await resolve('t'), null, err.cause.code);
  }
});

test('a real connection refused → null', async () => {
  const net = await import('node:net');
  const server = net.createServer();
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const { port } = server.address();
  await new Promise((r) => server.close(r));
  const resolve = createCoordinatorClient({ url: `http://127.0.0.1:${port}`, logger: silentLogger() });
  assert.equal(await resolve('t'), null);
});

test('a body that stalls past the lookup timeout → null', async () => {
  const { fetch } = fakeFetch(
    async (url, { signal }) =>
      new Response(
        new ReadableStream({
          start(controller) {
            signal.addEventListener('abort', () => controller.error(signal.reason));
          },
        }),
        { status: 200 },
      ),
  );
  const resolve = createCoordinatorClient({ url: 'http://im:8082', fetch, timeoutMs: 20, logger: silentLogger() });
  assert.equal(await resolve('t'), null);
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

test('404 from the coordinator: UNCERTAIN aborts, PRECOMMITTED stays pending with an error log (R2-3)', async () => {
  let t = 1_000_000;
  const repo = fakeRepo();
  const errors = [];
  const logger = { ...silentLogger(), error: (obj, msg) => errors.push({ obj, msg }) };
  const { fetch } = fakeFetch(async () => json(404, { error: 'not_found' }));
  const participant = createCorrelationParticipant({
    store: repo.tpcStore,
    putCorrelation: (rec) => repo.putCorrelation(rec),
    timeoutMs: 1000,
    now: () => t,
    logger,
    resolve: createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() }),
  });
  assert.equal(await participant.canCommit('tx-u', correlationPayload()), 'yes');
  assert.equal(await participant.canCommit('tx-p', correlationPayload({ version: 2 })), 'yes');
  await participant.preCommit('tx-p');
  t += 5000;
  assert.deepEqual(await participant.sweep(), { committed: [], aborted: ['tx-u'], pending: ['tx-p'] });
  assert.equal((await repo.tpcStore.get('tx-p')).state, 'PRECOMMITTED');
  assert.equal(repo.calls.putCorrelation.length, 0);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].obj.txId, 'tx-p');
});

test('5xx from the coordinator keeps both records for the next sweep', async () => {
  let t = 1_000_000;
  const repo = fakeRepo();
  const { fetch } = fakeFetch(async () => json(503, {}));
  const participant = createCorrelationParticipant({
    store: repo.tpcStore,
    putCorrelation: (rec) => repo.putCorrelation(rec),
    timeoutMs: 1000,
    now: () => t,
    resolve: createCoordinatorClient({ url: 'http://im:8082', fetch, logger: silentLogger() }),
  });
  assert.equal(await participant.canCommit('tx-u', correlationPayload()), 'yes');
  assert.equal(await participant.canCommit('tx-p', correlationPayload({ version: 2 })), 'yes');
  await participant.preCommit('tx-p');
  t += 5000;
  assert.deepEqual(await participant.sweep(), { committed: [], aborted: [], pending: ['tx-u', 'tx-p'] });
});
