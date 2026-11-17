import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { rawKey, createS3Archive, createS3Client } from '../src/s3-archive.js';
import { rawEvent } from './helpers.js';

test('rawKey layout', () => {
  const ev = rawEvent({ eventId: '7f1c2d3e-4b5a-4c6d-8e9f-0a1b2c3d4e5f', receivedAt: '2026-11-16T05:07:00.000Z' });
  assert.equal(rawKey(ev), 'raw/2026/11/16/05/7f1c2d3e-4b5a-4c6d-8e9f-0a1b2c3d4e5f.json');
});

test('rawKey uses UTC and zero-pads', () => {
  const ev = rawEvent({ eventId: 'abc', receivedAt: '2027-01-02T03:04:05.006Z' });
  assert.equal(rawKey(ev), 'raw/2027/01/02/03/abc.json');
});

test('rawKey rejects missing receivedAt or unsafe eventId', () => {
  assert.throws(() => rawKey(rawEvent({ receivedAt: undefined })), /receivedAt/);
  assert.throws(() => rawKey(rawEvent({ eventId: '../etc/passwd' })), /eventId/);
  assert.throws(() => rawKey(rawEvent({ eventId: '' })), /eventId/);
});

test('archiveRaw puts the event as JSON and returns the key', async () => {
  const sent = [];
  const s3 = { send: async (cmd) => { sent.push(cmd); return {}; } };
  const archive = createS3Archive({ s3, bucket: 'b1' });
  const ev = rawEvent();
  const key = await archive.archiveRaw(ev);
  assert.equal(key, rawKey(ev));
  assert.equal(sent.length, 1);
  assert.ok(sent[0] instanceof PutObjectCommand);
  assert.equal(sent[0].input.Bucket, 'b1');
  assert.equal(sent[0].input.Key, key);
  assert.equal(sent[0].input.ContentType, 'application/json');
  assert.deepEqual(JSON.parse(sent[0].input.Body), ev);
});

test('createS3Client uses path-style addressing with a custom endpoint', async () => {
  const local = createS3Client({ region: 'ap-south-1', endpoint: 'http://localhost:9000' });
  assert.equal(local.config.forcePathStyle, true);
  const aws = createS3Client({ region: 'ap-south-1' });
  assert.notEqual(aws.config.forcePathStyle, true);
  local.destroy();
  aws.destroy();
});
