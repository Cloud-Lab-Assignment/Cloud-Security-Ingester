import { test } from 'node:test';
import assert from 'node:assert/strict';
import { toKafkaEvent, createHttpFunction } from '../src/gcf.js';

const raw = { schemaVersion: '1.0', eventId: 'e1' };

function fakeRes() {
  return {
    statusCode: null,
    body: null,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; }
  };
}

test('array of values becomes one partition with synthetic increasing offsets', () => {
  const event = toKafkaEvent([raw, { ...raw, eventId: 'e2' }], 1000);
  const records = event.records['raw-events-0'];
  assert.equal(records.length, 2);
  assert.deepEqual(records.map((r) => r.offset), [1000, 1001]);
  assert.deepEqual(JSON.parse(Buffer.from(records[0].value, 'base64').toString('utf8')), raw);
});

test('a single object body is one record', () => {
  assert.equal(toKafkaEvent(raw, 5).records['raw-events-0'].length, 1);
});

test('string values pass through untouched', () => {
  const event = toKafkaEvent(['not json'], 5);
  assert.equal(Buffer.from(event.records['raw-events-0'][0].value, 'base64').toString('utf8'), 'not json');
});

test('an envelope with Kafka metadata keeps its real coordinates', () => {
  const event = toKafkaEvent([{ topic: 'raw-events', partition: 2, offset: 41, value: raw }]);
  assert.deepEqual(event.records['raw-events-2'].map((r) => r.offset), [41]);
});

test('POST invokes the handler and returns its counts', async () => {
  let seen;
  const fn = createHttpFunction(async (event) => { seen = event; return { processed: 1, failed: 0 }; });
  const res = fakeRes();
  await fn({ method: 'POST', body: [raw] }, res);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.body, { processed: 1, failed: 0 });
  assert.equal(Object.keys(seen.records).length, 1);
});

test('handler failure is a 500 so the connector retries', async () => {
  const fn = createHttpFunction(async () => { throw new Error('kafka down'); });
  const res = fakeRes();
  await fn({ method: 'POST', body: [raw] }, res);
  assert.equal(res.statusCode, 500);
});

test('empty body and non-POST are handled without invoking', async () => {
  const fn = createHttpFunction(async () => { throw new Error('must not run'); });
  const empty = fakeRes();
  await fn({ method: 'POST', body: [] }, empty);
  assert.equal(empty.statusCode, 200);
  const get = fakeRes();
  await fn({ method: 'GET' }, get);
  assert.equal(get.statusCode, 405);
});
