import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { CoordinatorCrash, StoreConflict } from '@core/tpc';
import { migrate } from '../src/migrate.js';
import { createIncidentRepo, VersionConflict, TransactionPending, mergeCandidate } from '../src/incidents.js';

const mergeFor = (c) => mergeCandidate(null, new Set(), c).incident;
import {
  createIncidentService,
  createPgCoordinatorLog,
  createPgParticipantStore,
  applyDecision,
  IncidentAborted,
  StaleVersion,
} from '../src/tpc-wiring.js';
import { createTestDatabase, fakeEventStore, silentLogger, candidate } from './helpers.js';

const skip = !process.env.INTEGRATION;
const config = {
  tpcTimeoutMs: 500,
  tpcParticipantTimeoutMs: 10_000,
  correlationWindowMs: 900_000,
};

let db;
let repo;

// incident-updates messages waiting in the transactional outbox for an incident.
async function outboxTypes(incidentId) {
  const { rows } = await db.pool.query('SELECT type FROM outbox WHERE incident_id = $1 ORDER BY id', [incidentId]);
  return rows.map((r) => r.type);
}

async function outboxForCorrelation(correlationId) {
  const { rows } = await db.pool.query(
    `SELECT o.type FROM outbox o JOIN incidents i ON i.incident_id = o.incident_id WHERE i.correlation_id = $1 ORDER BY o.id`,
    [correlationId],
  );
  return rows.map((r) => r.type);
}

function service(over = {}) {
  const eventStore = over.eventStore ?? fakeEventStore();
  const svc = createIncidentService({
    pool: db.pool,
    repo,
    eventStore,
    config: { ...config, ...(over.config ?? {}) },
    logger: silentLogger,
    onCrash: over.onCrash,
    participantHooks: over.participantHooks,
  });
  return { svc, eventStore };
}

before(async () => {
  if (skip) return;
  db = await createTestDatabase();
  await migrate(db.pool);
  repo = createIncidentRepo({ pool: db.pool, correlationWindowMs: config.correlationWindowMs });
});

after(async () => {
  if (db) await db.drop();
});

test('migrate is idempotent', { skip }, async () => {
  assert.deepEqual(await migrate(db.pool), []);
  const { rows } = await db.pool.query('SELECT name FROM schema_migrations');
  assert.deepEqual(rows.map((r) => r.name).sort(), ['001_init.sql', '002_outbox.sql']);
});

test('upsertIncident commits to postgres and event-store and queues incident.created', { skip }, async () => {
  const { svc, eventStore } = service();
  const c = candidate();
  const res = await svc.upsertIncident(c);
  assert.equal(res.outcome, 'created');
  assert.equal(res.incident.version, 1);
  assert.equal(res.incident.status, 'open');
  assert.deepEqual(res.incident.alertIds, c.alertIds);

  const stored = await repo.getIncident(res.incident.incidentId);
  assert.equal(stored.correlationId, c.correlationId);
  assert.deepEqual(stored.enrichment, c.enrichment);

  assert.equal(eventStore.applied.length, 1);
  assert.equal(eventStore.applied[0].correlationId, c.correlationId);
  assert.equal(eventStore.applied[0].version, 1);
  assert.equal(eventStore.applied[0].decision.kind, 'create');

  assert.deepEqual(await outboxTypes(res.incident.incidentId), ['incident.created']);
  const { rows: [msg] } = await db.pool.query('SELECT payload FROM outbox WHERE incident_id = $1', [res.incident.incidentId]);
  assert.equal(msg.payload.schemaVersion, '1.0');
  assert.equal(msg.payload.type, 'incident.created');
  assert.equal(msg.payload.incident.version, 1);
  assert.deepEqual(msg.payload.incident.alertIds, c.alertIds);
  assert.deepEqual(msg.payload.producedBy, { name: 'incident-management', version: '1.0.0' });

  const [tx] = await createPgCoordinatorLog(db.pool).list({ correlationId: c.correlationId });
  assert.equal(tx.state, 'COMMITTED');

  const history = await repo.getHistory(res.incident.incidentId);
  assert.equal(history.length, 1);
  assert.equal(history[0].txId, tx.txId);
});

test('second candidate with a new alert updates and queues incident.updated', { skip }, async () => {
  const { svc, eventStore } = service();
  const c1 = candidate();
  const first = await svc.upsertIncident(c1);
  const newAlert = `${c1.alertIds[0]}-2`;
  const res = await svc.upsertIncident({ ...c1, alertIds: [...c1.alertIds, newAlert], severity: 'critical' });
  assert.equal(res.outcome, 'updated');
  assert.equal(res.incident.version, 2);
  assert.equal(res.incident.alertCount, 2);
  assert.equal(res.incident.severity, 'critical');
  assert.equal(res.incident.incidentId, first.incident.incidentId);
  assert.deepEqual(eventStore.applied.map((p) => p.version), [1, 2]);
  assert.deepEqual(eventStore.applied[1].decision.newAlertIds, [newAlert]);
  assert.deepEqual(await outboxTypes(res.incident.incidentId), ['incident.created', 'incident.updated']);
});

test('redelivered candidate with only known alerts is a noop without 3PC', { skip }, async () => {
  const { svc, eventStore } = service();
  const c = candidate();
  await svc.upsertIncident(c);
  const res = await svc.upsertIncident(c);
  assert.equal(res.outcome, 'noop');
  assert.equal(res.incident.version, 1);
  assert.equal(eventStore.applied.length, 1);
});

test('event-store votes no → no row, IncidentAborted("vote_no")', { skip }, async () => {
  const { svc, eventStore } = service({ eventStore: fakeEventStore({ vote: 'no' }) });
  const c = candidate();
  await assert.rejects(svc.upsertIncident(c), (err) => err instanceof IncidentAborted && err.reason === 'vote_no');
  const { rowCount } = await db.pool.query('SELECT 1 FROM incidents WHERE correlation_id = $1', [c.correlationId]);
  assert.equal(rowCount, 0);
  assert.equal(eventStore.applied.length, 0);
  assert.deepEqual(await outboxForCorrelation(c.correlationId), []);
  const [tx] = await createPgCoordinatorLog(db.pool).list({ correlationId: c.correlationId });
  assert.equal(tx.state, 'ABORTED');
  assert.equal(tx.reason, 'vote_no');
  const local = await db.pool.query('SELECT state FROM tpc_participant WHERE tx_id = $1', [tx.txId]);
  assert.equal(local.rows[0].state, 'ABORTED');
});

test('TPC_FAULT crash then recover() commits and queues the update', { skip }, async () => {
  const eventStore = fakeEventStore();
  let crashed = null;
  const { svc } = service({
    eventStore,
    config: { tpcFault: 'crash-after-precommit' },
    onCrash: async (err) => {
      crashed = err;
    },
  });
  const c = candidate();
  await assert.rejects(svc.upsertIncident(c), CoordinatorCrash);
  assert.ok(crashed instanceof CoordinatorCrash);
  let { rowCount } = await db.pool.query('SELECT 1 FROM incidents WHERE correlation_id = $1', [c.correlationId]);
  assert.equal(rowCount, 0, 'nothing applied before doCommit');

  // "Restart": a new coordinator without the fault over the same database.
  assert.deepEqual(await outboxForCorrelation(c.correlationId), [], 'nothing queued before the commit');
  const { svc: restarted } = service({ eventStore });
  const recovered = await restarted.recover();
  assert.equal(recovered.length, 1);
  ({ rowCount } = await db.pool.query('SELECT 1 FROM incidents WHERE correlation_id = $1', [c.correlationId]));
  assert.equal(rowCount, 1);
  assert.equal(eventStore.applied.length, 1);
  assert.deepEqual(await outboxForCorrelation(c.correlationId), ['incident.created']);
  const [tx] = await createPgCoordinatorLog(db.pool).list({ correlationId: c.correlationId });
  assert.equal(tx.state, 'COMMITTED');
});

test('concurrent upsert for same correlationId votes no', { skip }, async () => {
  const { svc, eventStore } = service();
  const c = candidate();
  // Hold transaction A in PRECOMMITTED on the local participant.
  const payloadA = {
    correlationId: c.correlationId,
    kind: 'create',
    version: 1,
    incident: { incidentId: '00000000-0000-4000-8000-000000000001', correlationId: c.correlationId },
    newAlertIds: [],
  };
  assert.equal(await svc.participant.canCommit('tx-A-' + c.correlationId, payloadA), 'yes');
  await svc.participant.preCommit('tx-A-' + c.correlationId);

  await assert.rejects(svc.upsertIncident(c), (err) => err instanceof IncidentAborted && err.reason === 'vote_no');
  assert.equal(eventStore.applied.length, 0);
  await svc.participant.abort('tx-A-' + c.correlationId);
});

test('stale apply version rejected', { skip }, async () => {
  const { svc } = service();
  const c = candidate();
  const { incident } = await svc.upsertIncident(c);
  const staleUpdate = {
    txId: `stale-${c.correlationId}`,
    kind: 'update',
    version: 5, // expects the row at version 4, but it is at 1
    incident: { ...incident, alertCount: 99 },
    newAlertIds: ['x'],
  };
  await assert.rejects(applyDecision(db.pool, staleUpdate), StaleVersion);
  const after = await repo.getIncident(incident.incidentId);
  assert.equal(after.version, 1);
  assert.equal(after.alertCount, 1);
  // ...and the participant votes no on such a payload before it ever gets that far.
  assert.equal(
    await svc.participant.canCommit(`stale2-${c.correlationId}`, { ...staleUpdate, correlationId: incident.correlationId }),
    'no',
  );
});

test('applyDecision is idempotent per txId', { skip }, async () => {
  const { svc } = service();
  const c = candidate();
  const { incident, txId } = await svc.upsertIncident(c);
  await applyDecision(db.pool, { txId, kind: 'create', version: 1, incident, newAlertIds: incident.alertIds });
  const { rowCount } = await db.pool.query('SELECT 1 FROM incident_history WHERE tx_id = $1', [txId]);
  assert.equal(rowCount, 1);
});

test('findTarget falls back to open incident for same entity inside window', { skip }, async () => {
  const { svc } = service();
  const c = candidate();
  const { incident } = await svc.upsertIncident(c);

  const inside = await repo.findTarget({ ...c, correlationId: 'cor-other', firstSeen: '2026-11-16T09:10:00.000Z' });
  assert.equal(inside.existing?.incidentId, incident.incidentId);
  assert.ok(inside.knownAlertIds.has(c.alertIds[0]));

  const outside = await repo.findTarget({ ...c, correlationId: 'cor-other', firstSeen: '2026-11-16T09:20:00.001Z' });
  assert.equal(outside.existing, null);

  // A rebalanced engine's candidate merges into the stored incident and keeps its correlationId.
  const res = await svc.upsertIncident({ ...c, correlationId: 'cor-rebalanced', alertIds: ['late-alert'] });
  assert.equal(res.outcome, 'updated');
  assert.equal(res.incident.correlationId, c.correlationId);

  await repo.updateStatus(incident.incidentId, { status: 'resolved', version: res.incident.version });
  const resolved = await repo.findTarget({ ...c, correlationId: 'cor-other', firstSeen: '2026-11-16T09:10:00.000Z' });
  assert.equal(resolved.existing, null, 'resolved incidents are not reopened by fallback');
});

test('updateStatus bumps version and records history', { skip }, async () => {
  const { svc } = service();
  const { incident } = await svc.upsertIncident(candidate());
  const updated = await repo.updateStatus(incident.incidentId, { status: 'investigating', version: 1, actor: 'alice', note: 'looking' });
  assert.equal(updated.status, 'investigating');
  assert.equal(updated.version, 2);
  const history = await repo.getHistory(incident.incidentId);
  assert.deepEqual(history.at(-1).change, { kind: 'status', from: 'open', to: 'investigating', note: 'looking' });
  assert.equal(history.at(-1).actor, 'alice');
  // The status change is queued for incident-updates in the same transaction (P1-3).
  assert.deepEqual(await outboxTypes(incident.incidentId), ['incident.created', 'incident.status_changed']);
  const { rows } = await db.pool.query(
    `SELECT payload FROM outbox WHERE incident_id = $1 AND type = 'incident.status_changed'`,
    [incident.incidentId],
  );
  assert.equal(rows[0].payload.incident.status, 'investigating');
  assert.equal(rows[0].payload.incident.version, 2);
});

test('updateStatus stale version throws VersionConflict', { skip }, async () => {
  const { svc } = service();
  const { incident } = await svc.upsertIncident(candidate());
  await repo.updateStatus(incident.incidentId, { status: 'investigating', version: 1 });
  await assert.rejects(repo.updateStatus(incident.incidentId, { status: 'resolved', version: 1 }), VersionConflict);
  assert.deepEqual(await outboxTypes(incident.incidentId), ['incident.created', 'incident.status_changed'], 'a refused PATCH queues nothing');
});

test('updateStatus refuses while a 3PC transaction is pending', { skip }, async () => {
  const { svc } = service();
  const c = candidate();
  const { incident } = await svc.upsertIncident(c);
  const txId = `pending-${c.correlationId}`;
  await svc.participant.canCommit(txId, {
    correlationId: c.correlationId,
    kind: 'update',
    version: 2,
    incident: { ...incident, version: 2 },
    newAlertIds: ['n'],
  });
  await assert.rejects(repo.updateStatus(incident.incidentId, { status: 'resolved', version: 1 }), TransactionPending);
  await svc.participant.abort(txId);
  const ok = await repo.updateStatus(incident.incidentId, { status: 'resolved', version: 1 });
  assert.equal(ok.status, 'resolved');
});

test('listIncidents filters by status and severity', { skip }, async () => {
  const { svc } = service();
  const a = await svc.upsertIncident(candidate({ severity: 'low' }));
  await repo.updateStatus(a.incident.incidentId, { status: 'false_positive', version: 1 });
  const fp = await repo.listIncidents({ status: 'false_positive' });
  assert.ok(fp.some((i) => i.incidentId === a.incident.incidentId));
  assert.ok(fp.every((i) => i.status === 'false_positive'));
  const low = await repo.listIncidents({ severity: 'low', limit: 500 });
  assert.ok(low.every((i) => i.severity === 'low'));
});

test('PATCH racing a 3PC update never leaves a decided commit unapplied (P0-2)', { skip }, async () => {
  const { svc: base } = service();
  const c = candidate();
  const { incident } = await base.upsertIncident(c);

  // Start an analyst PATCH between the participant's validate step and its
  // UNCERTAIN record, and give it time to finish if nothing blocks it.
  let patch;
  const { svc } = service({
    participantHooks: {
      afterValidate: async () => {
        if (patch) return;
        patch = repo.updateStatus(incident.incidentId, { status: 'investigating', version: 1 }).then(
          () => 'ok',
          (err) => err.name,
        );
        await new Promise((r) => setTimeout(r, 300));
      },
    },
  });
  const upsert = await svc
    .upsertIncident({ ...c, alertIds: [...c.alertIds, `${c.alertIds[0]}-race`] })
    .then((ok) => ({ ok }), (err) => ({ err }));
  const patchResult = await patch;

  // Nothing may be left in flight on the Postgres participant.
  const { rows: stuck } = await db.pool.query(
    `SELECT tx_id, state FROM tpc_participant
      WHERE payload->>'correlationId' = $1 AND state IN ('UNCERTAIN', 'PRECOMMITTED')`,
    [c.correlationId],
  );
  assert.deepEqual(stuck, [], 'no transaction stuck after a decided commit');

  const stored = await repo.getIncident(incident.incidentId);
  if (upsert.ok) {
    // The 3PC update won: the PATCH must have been refused, the update applied.
    assert.equal(patchResult, 'TransactionPending');
    assert.equal(stored.alertCount, 2);
    assert.equal(stored.version, 2);
    assert.equal(stored.status, 'open');
  } else {
    // The PATCH won: the 3PC transaction must have voted no.
    assert.ok(upsert.err instanceof IncidentAborted, String(upsert.err));
    assert.equal(patchResult, 'ok');
    assert.equal(stored.alertCount, 1);
    assert.equal(stored.status, 'investigating');
  }
});

test('findTarget fallback ignores an incident from a different window (P1-6)', { skip }, async () => {
  const { svc } = service();
  const c = candidate(); // firstSeen 09:00, lastSeen 09:01 on 16 Nov
  const { incident } = await svc.upsertIncident(c);
  // A late, redelivered alert for the same entity from the previous day.
  const yesterday = {
    ...c,
    correlationId: 'cor-yesterday',
    alertIds: ['old-alert'],
    firstSeen: '2026-11-15T08:00:00.000Z',
    lastSeen: '2026-11-15T08:05:00.000Z',
  };
  const target = await repo.findTarget(yesterday);
  assert.equal(target.existing, null, 'yesterday’s alert must not merge into today’s incident');
  // Inside the window just before the incident still matches.
  const justBefore = { ...yesterday, firstSeen: '2026-11-16T08:46:00.000Z', lastSeen: '2026-11-16T08:46:00.000Z' };
  assert.equal((await repo.findTarget(justBefore)).existing?.incidentId, incident.incidentId);
});

test('postgres participant store honours expected-state writes (P0-3)', { skip }, async () => {
  const store = createPgParticipantStore(db.pool);
  const txId = `store-${Date.now()}-${Math.random()}`;
  const rec = (state) => ({ txId, state, payload: { correlationId: 'x' }, updatedAt: Date.now() });
  await store.put(rec('UNCERTAIN'), { state: null });
  await assert.rejects(store.put(rec('UNCERTAIN'), { state: null }), StoreConflict, 'create only if absent');
  await assert.rejects(store.put(rec('ABORTED'), { state: 'PRECOMMITTED' }), StoreConflict, 'wrong expected state');
  assert.equal((await store.get(txId)).state, 'UNCERTAIN');
  await store.put(rec('PRECOMMITTED'), { state: 'UNCERTAIN' });
  assert.equal((await store.get(txId)).state, 'PRECOMMITTED');
  await store.put(rec('COMMITTED')); // no expectation: unconditional
  assert.equal((await store.get(txId)).state, 'COMMITTED');
});

test('incident-db timeout sweep follows the coordinator log decision (P0-4)', { skip }, async () => {
  const { svc } = service({ config: { tpcParticipantTimeoutMs: 1 } });
  const log = createPgCoordinatorLog(db.pool);
  const prepare = async (decision) => {
    const c = candidate();
    const txId = `p04-${decision}-${c.correlationId}`;
    const payload = {
      correlationId: c.correlationId,
      kind: 'create',
      version: 1,
      incident: { ...mergeFor(c), correlationId: c.correlationId },
      newAlertIds: c.alertIds,
    };
    assert.equal(await svc.participant.canCommit(txId, payload), 'yes');
    await svc.participant.preCommit(txId);
    await log.put({ txId, state: decision, payloads: {}, updatedAt: Date.now() });
    return { txId, c };
  };
  // Case (a): the participant pre-committed, but the coordinator decided abort.
  const aborted = await prepare('ABORTED');
  const committed = await prepare('COMMITTED');
  const pending = await prepare('PRECOMMITTED');
  await new Promise((r) => setTimeout(r, 10));
  const res = await svc.participant.sweep();

  const state = async (txId) => (await db.pool.query('SELECT state FROM tpc_participant WHERE tx_id = $1', [txId])).rows[0].state;
  assert.equal(await state(aborted.txId), 'ABORTED', 'coordinator said ABORTED: no self-commit');
  assert.equal(await state(committed.txId), 'COMMITTED');
  assert.equal(await state(pending.txId), 'PRECOMMITTED', 'coordinator still deciding: leave it');
  assert.ok(res.aborted.includes(aborted.txId));
  assert.ok(res.committed.includes(committed.txId));
  const rows = async (c) => (await db.pool.query('SELECT 1 FROM incidents WHERE correlation_id = $1', [c.correlationId])).rowCount;
  assert.equal(await rows(aborted.c), 0);
  assert.equal(await rows(committed.c), 1);
  await svc.participant.abort(pending.txId);
});
