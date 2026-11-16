import { test } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { createDynamoClients } from '../src/dynamo.js';
import { createS3Client } from '../src/s3-archive.js';
import { loadConfig } from '../src/config.js';

process.env.AWS_ACCESS_KEY_ID ??= 'local';
process.env.AWS_SECRET_ACCESS_KEY ??= 'local';

// Accepts connections and never answers: a stuck storage endpoint.
async function blackhole() {
  const sockets = new Set();
  let connections = 0;
  const server = net.createServer((s) => {
    connections += 1;
    sockets.add(s);
    s.on('close', () => sockets.delete(s));
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  return {
    endpoint: `http://127.0.0.1:${server.address().port}`,
    connections: () => connections,
    close: () => {
      for (const s of sockets) s.destroy();
      return new Promise((r) => server.close(r));
    },
  };
}

const opts = { requestTimeoutMs: 150, maxAttempts: 2 };

test('a stuck DynamoDB call fails after maxAttempts request timeouts instead of hanging', { timeout: 5000 }, async () => {
  const hole = await blackhole();
  const { ddb } = createDynamoClients({ region: 'ap-south-1', endpoint: hole.endpoint, ...opts });
  try {
    const started = Date.now();
    await assert.rejects(ddb.send(new DescribeTableCommand({ TableName: 't' })));
    assert.ok(Date.now() - started < 3000);
    // Retried (maxAttempts), but bounded; exact counts vary with timing.
    assert.ok(hole.connections() >= 2 && hole.connections() <= 4);
  } finally {
    ddb.destroy();
    await hole.close();
  }
});

test('a stuck S3 call fails after maxAttempts request timeouts instead of hanging', { timeout: 5000 }, async () => {
  const hole = await blackhole();
  const s3 = createS3Client({ region: 'ap-south-1', endpoint: hole.endpoint, ...opts });
  try {
    const started = Date.now();
    await assert.rejects(s3.send(new PutObjectCommand({ Bucket: 'b', Key: 'k', Body: '{}' })));
    assert.ok(Date.now() - started < 3000);
    // S3's own middleware may add a connection; the bound is what matters.
    assert.ok(hole.connections() >= 2 && hole.connections() <= 4);
  } finally {
    s3.destroy();
    await hole.close();
  }
});

test('config: AWS deadlines default well below the 30 s Kafka session timeout', () => {
  const saved = { t: process.env.AWS_REQUEST_TIMEOUT_MS, a: process.env.AWS_MAX_ATTEMPTS };
  try {
    delete process.env.AWS_REQUEST_TIMEOUT_MS;
    delete process.env.AWS_MAX_ATTEMPTS;
    const c = loadConfig();
    assert.equal(c.awsRequestTimeoutMs, 5000);
    assert.equal(c.awsMaxAttempts, 3);
    // Worst case per storage call: every attempt connects and then times out.
    assert.ok(c.awsMaxAttempts * (c.awsRequestTimeoutMs + 1000) < 30_000);
    process.env.AWS_REQUEST_TIMEOUT_MS = '2000';
    process.env.AWS_MAX_ATTEMPTS = '2';
    assert.deepEqual([loadConfig().awsRequestTimeoutMs, loadConfig().awsMaxAttempts], [2000, 2]);
  } finally {
    for (const [k, v] of [['AWS_REQUEST_TIMEOUT_MS', saved.t], ['AWS_MAX_ATTEMPTS', saved.a]]) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
});
