import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { migrate, MIGRATIONS_DIR } from '../src/migrate.js';
import { createTestDatabase, fakeProducer, silentLogger } from './helpers.js';
import { createOutboxRelay } from '../src/outbox.js';

// A fresh database migrated only up to (and including) `upTo`.
async function databaseAt(upTo) {
  const d = await createTestDatabase();
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'im-mig-'));
  for (const f of (await fs.readdir(MIGRATIONS_DIR)).filter((f) => f.endsWith('.sql') && f <= upTo)) {
    await fs.copyFile(path.join(MIGRATIONS_DIR, f), path.join(dir, f));
  }
  await migrate(d.pool, { dir });
  await fs.rm(dir, { recursive: true, force: true });
  return d;
}

const skip = !process.env.INTEGRATION;
let db;
let partialDir;

before(async () => {
  if (skip) return;
  db = await createTestDatabase();
  // A database as it was before 003: only 001 and 002 applied.
  partialDir = await fs.mkdtemp(path.join(os.tmpdir(), 'im-mig-'));
  for (const f of ['001_init.sql', '002_outbox.sql']) {
    await fs.copyFile(path.join(MIGRATIONS_DIR, f), path.join(partialDir, f));
  }
});

after(async () => {
  if (db) await db.drop();
  if (partialDir) await fs.rm(partialDir, { recursive: true, force: true });
});

test('003 upgrades a database that already links one alert to two incidents (R2-4)', { skip }, async () => {
  await migrate(db.pool, { dir: partialDir });
  const mk = (id, corr, createdAt) =>
    db.pool.query(
      `INSERT INTO incidents (incident_id, correlation_id, entity_key, severity, version, created_at, updated_at)
       VALUES ($1, $2, 'e', 'high', 1, $3, $3)`,
      [id, corr, createdAt],
    );
  const older = '00000000-0000-4000-8000-00000000000a';
  const newer = '00000000-0000-4000-8000-00000000000b';
  await mk(older, 'cor-older', '2026-11-15T10:00:00Z');
  await mk(newer, 'cor-newer', '2026-11-16T10:00:00Z');
  // a1 is owned twice; a2 only by the newer incident.
  for (const [inc, alert] of [[newer, 'a1'], [older, 'a1'], [newer, 'a2']]) {
    await db.pool.query('INSERT INTO incident_alerts (incident_id, alert_id) VALUES ($1, $2)', [inc, alert]);
  }

  const applied = await migrate(db.pool); // the full set: applies 003 (and anything later)
  assert.ok(applied.includes('003_alert_unique.sql'));

  const { rows } = await db.pool.query('SELECT alert_id, incident_id FROM incident_alerts ORDER BY alert_id');
  assert.deepEqual(rows, [
    { alert_id: 'a1', incident_id: older }, // the earliest incident keeps the alert
    { alert_id: 'a2', incident_id: newer },
  ]);
  const idx = await db.pool.query(
    `SELECT 1 FROM pg_indexes WHERE tablename = 'incident_alerts' AND indexname = 'incident_alerts_alert_id_unique'`,
  );
  assert.equal(idx.rowCount, 1);
  await assert.rejects(
    db.pool.query('INSERT INTO incident_alerts (incident_id, alert_id) VALUES ($1, $2)', [newer, 'a1']),
    /duplicate key/,
  );
});

test('005 backfills tx_id on pre-004 correlation outbox rows so they wait for COMMITTED (R3-3)', { skip }, async () => {
  const d = await databaseAt('004_outbox_tx.sql');
  try {
    const inc = '00000000-0000-4000-8000-0000000000c1';
    await d.pool.query(
      `INSERT INTO incidents (incident_id, correlation_id, entity_key, severity, version, created_at, updated_at)
       VALUES ($1, 'cor-r33', 'e', 'high', 2, now(), now())`,
      [inc],
    );
    // History of a create (tx-a, committed) and an update (tx-b, still PRECOMMITTED at the coordinator).
    for (const [tx, kind, version] of [['tx-a', 'create', 1], ['tx-b', 'update', 2]]) {
      await d.pool.query(
        `INSERT INTO incident_history (incident_id, tx_id, change, actor, at) VALUES ($1, $2, $3, 'correlation-engine', now())`,
        [inc, tx, JSON.stringify({ kind, version, severity: 'high', newAlertIds: [] })],
      );
    }
    await d.pool.query(
      `INSERT INTO tpc_coordinator (tx_id, state, payloads, updated_at) VALUES ('tx-a', 'COMMITTED', '{}', now()), ('tx-b', 'PRECOMMITTED', '{}', now())`,
    );
    // Outbox rows written by the pre-004 code: no tx_id, unpublished.
    const row = (type, version) => ({ schemaVersion: '1.0', type, incident: { incidentId: inc, version } });
    for (const [type, version] of [['incident.created', 1], ['incident.updated', 2], ['incident.status_changed', 3]]) {
      await d.pool.query('INSERT INTO outbox (type, incident_id, payload) VALUES ($1, $2, $3)', [
        type,
        inc,
        JSON.stringify(row(type, version)),
      ]);
    }

    await migrate(d.pool);

    const { rows } = await d.pool.query('SELECT type, tx_id FROM outbox ORDER BY id');
    assert.deepEqual(rows, [
      { type: 'incident.created', tx_id: 'tx-a' },
      { type: 'incident.updated', tx_id: 'tx-b' },
      { type: 'incident.status_changed', tx_id: null },
    ]);
    // Only the committed create goes out; the update (and the status change behind it) wait for tx-b.
    const producer = fakeProducer();
    const relay = createOutboxRelay({ pool: d.pool, producer, topic: 't', logger: silentLogger });
    assert.equal(await relay.publishPending(), 1);
    assert.equal(JSON.parse(producer.sent[0].messages[0].value).type, 'incident.created');
  } finally {
    await d.drop();
  }
});
