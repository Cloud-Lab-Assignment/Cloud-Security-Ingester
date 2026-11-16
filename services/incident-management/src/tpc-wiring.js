import { randomUUID } from 'node:crypto';
import client from 'prom-client';
import { createCoordinator, createParticipant, CoordinatorCrash, StoreConflict, TpcStateError } from '@core/tpc';
import { withTransaction } from './db.js';
import { mergeCandidate, rowToIncident } from './incidents.js';
import { enqueueIncidentUpdate } from './outbox.js';

export const INCIDENT_DB = 'incident-db';
export const EVENT_STORE = 'event-store';

// The 3PC transaction aborted; reason is vote_no | vote_timeout | precommit_failed.
export class IncidentAborted extends Error {
  constructor(reason) {
    super(`incident upsert aborted: ${reason}`);
    this.name = 'IncidentAborted';
    this.reason = reason;
  }
}

export class StaleVersion extends Error {
  constructor(incidentId, version) {
    super(`incident ${incidentId}: apply of version ${version} is stale`);
    this.name = 'StaleVersion';
  }
}

const iso = (d) => (d instanceof Date ? d.toISOString() : d);

// ---- Durable 3PC storage in Postgres ---------------------------------------

export function createPgParticipantStore(pool, { now = Date.now } = {}) {
  const toRecord = (r) => ({ txId: r.tx_id, state: r.state, payload: r.payload, updatedAt: Number(r.updated_ms) });
  const cols = 'tx_id, state, payload, (extract(epoch from updated_at) * 1000)::bigint AS updated_ms';
  return {
    async get(txId) {
      const { rows } = await pool.query(`SELECT ${cols} FROM tpc_participant WHERE tx_id = $1`, [txId]);
      return rows[0] ? toRecord(rows[0]) : null;
    },
    // `expected` (optional): {state: null} = must not exist, {state: S} = must
    // currently be S. A mismatch throws StoreConflict (P0-3 store contract).
    async put({ txId, state, payload, updatedAt }, expected) {
      const params = [txId, state, payload == null ? null : JSON.stringify(payload), new Date(updatedAt)];
      if (expected === undefined) {
        await pool.query(
          `INSERT INTO tpc_participant (tx_id, state, payload, updated_at) VALUES ($1, $2, $3, $4)
           ON CONFLICT (tx_id) DO UPDATE SET state = EXCLUDED.state, payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at`,
          params,
        );
        return;
      }
      const res =
        expected.state === null
          ? await pool.query(
              `INSERT INTO tpc_participant (tx_id, state, payload, updated_at) VALUES ($1, $2, $3, $4)
               ON CONFLICT (tx_id) DO NOTHING`,
              params,
            )
          : await pool.query(
              `UPDATE tpc_participant SET state = $2, payload = $3, updated_at = $4 WHERE tx_id = $1 AND state = $5`,
              [...params, expected.state],
            );
      if (res.rowCount === 0) throw new StoreConflict(txId, expected.state);
    },
    async listPending() {
      const { rows } = await pool.query(
        `SELECT ${cols} FROM tpc_participant WHERE state IN ('UNCERTAIN', 'PRECOMMITTED') ORDER BY updated_at`,
      );
      return rows.map(toRecord);
    },
    // Atomic commit (R2-2): the state transition to COMMITTED and the incident
    // write happen in ONE Postgres transaction. The conditional UPDATE runs
    // first and row-locks the record, so a concurrent abort either already won
    // (0 rows: nothing is written) or waits and then fails its own condition.
    async commit({ txId, state, payload, updatedAt }, expected) {
      await withTransaction(pool, async (db) => {
        const res = await db.query(
          `UPDATE tpc_participant SET state = $2, payload = $3, updated_at = $4 WHERE tx_id = $1 AND state = $5`,
          [txId, state, payload == null ? null : JSON.stringify(payload), new Date(updatedAt), expected.state],
        );
        if (res.rowCount === 0) throw new StoreConflict(txId, expected.state);
        await applyDecisionIn(db, { ...payload, txId }, now);
      });
    },
  };
}

export function createPgCoordinatorLog(pool) {
  const toEntry = (r) => ({
    txId: r.tx_id,
    state: r.state,
    reason: r.reason,
    payloads: r.payloads,
    correlationId: r.payloads?.[EVENT_STORE]?.correlationId ?? r.payloads?.[INCIDENT_DB]?.correlationId ?? null,
    incidentId: r.payloads?.[EVENT_STORE]?.incidentId ?? null,
    version: r.payloads?.[EVENT_STORE]?.version ?? null,
    kind: r.payloads?.[INCIDENT_DB]?.kind ?? null,
    createdAt: iso(r.created_at),
    updatedAt: iso(r.updated_at),
  });
  return {
    async put({ txId, state, payloads, updatedAt, reason = null }) {
      await pool.query(
        `INSERT INTO tpc_coordinator (tx_id, state, payloads, reason, updated_at) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (tx_id) DO UPDATE SET state = EXCLUDED.state, payloads = EXCLUDED.payloads,
           reason = EXCLUDED.reason, updated_at = EXCLUDED.updated_at`,
        [txId, state, JSON.stringify(payloads), reason, new Date(updatedAt)],
      );
    },
    async listUnfinished() {
      const { rows } = await pool.query(
        `SELECT * FROM tpc_coordinator WHERE state IN ('WAITING', 'PRECOMMITTED') ORDER BY created_at`,
      );
      return rows.map(toEntry);
    },
    async get(txId) {
      const { rows } = await pool.query('SELECT * FROM tpc_coordinator WHERE tx_id = $1', [txId]);
      return rows[0] ? toEntry(rows[0]) : null;
    },
    // Latest transactions, newest first (for GET /tpc/transactions).
    async list({ limit = 20, correlationId } = {}) {
      const params = [];
      let where = '';
      if (correlationId) {
        params.push(correlationId);
        where = `WHERE payloads->'${EVENT_STORE}'->>'correlationId' = $1 OR payloads->'${INCIDENT_DB}'->>'correlationId' = $1`;
      }
      params.push(limit);
      const { rows } = await pool.query(
        `SELECT * FROM tpc_coordinator ${where} ORDER BY created_at DESC, updated_at DESC LIMIT $${params.length}`,
        params,
      );
      return rows.map(toEntry);
    },
  };
}

// ---- Event Store participant over HTTP --------------------------------------

export function createEventStoreParticipant({ baseUrl, timeoutMs, fetch: fetchImpl = globalThis.fetch }) {
  const base = baseUrl.replace(/\/+$/, '');
  async function post(txId, route, body) {
    const res = await fetchImpl(`${base}/tpc/${encodeURIComponent(txId)}/${route}`, {
      method: 'POST',
      headers: body === undefined ? {} : { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    });
    let json = null;
    try {
      json = await res.json();
    } catch {
      // non-JSON body; handled by status below
    }
    return { status: res.status, json };
  }
  async function ack(txId, route, phase) {
    const { status, json } = await post(txId, route);
    if (status === 200) return;
    if (status === 409) throw new TpcStateError(txId, json?.state ?? null, phase);
    throw new Error(`event-store ${phase} failed with HTTP ${status}`);
  }
  return {
    name: EVENT_STORE,
    async canCommit(txId, payload) {
      const { status, json } = await post(txId, 'can-commit', { payload });
      if (status !== 200) throw new Error(`event-store canCommit failed with HTTP ${status}`);
      return json?.vote === 'yes' ? 'yes' : 'no';
    },
    preCommit: (txId) => ack(txId, 'pre-commit', 'preCommit'),
    doCommit: (txId) => ack(txId, 'do-commit', 'doCommit'),
    abort: (txId) => ack(txId, 'abort', 'abort'),
  };
}

// ---- Local incident-db participant -------------------------------------------

// Apply one decided correlation decision in a single Postgres transaction.
// Idempotent per txId (a history row with that tx_id means it already ran).
export async function applyDecision(pool, payload, now = Date.now) {
  await withTransaction(pool, (db) => applyDecisionIn(db, payload, now));
}

// The same, inside a caller's transaction (`db` is a client with BEGIN issued).
export async function applyDecisionIn(db, { txId, kind, version, incident, newAlertIds }, now = Date.now) {
  const done = await db.query('SELECT 1 FROM incident_history WHERE tx_id = $1', [txId]);
  if (done.rowCount > 0) return;
  const i = incident;
  if (kind === 'create') {
    await db.query(
      `INSERT INTO incidents (incident_id, correlation_id, entity_key, title, severity, status, alert_count,
         event_count, rule_ids, related_event_ids, entity, enrichment, first_seen, last_seen, version, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,'open',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
      [
        i.incidentId, i.correlationId, i.entityKey, i.title, i.severity, i.alertCount, i.eventCount,
        i.ruleIds, i.relatedEventIds, JSON.stringify(i.entity ?? null), JSON.stringify(i.enrichment ?? null),
        i.firstSeen, i.lastSeen, version, i.createdAt, i.updatedAt,
      ],
    );
  } else {
    // Never touches status: analyst status changes are separate local updates.
    const res = await db.query(
      `UPDATE incidents SET title = $3, severity = $4, alert_count = $5, event_count = $6, rule_ids = $7,
         related_event_ids = $8, entity = $9, enrichment = $10, first_seen = $11, last_seen = $12,
         version = $2, updated_at = $13
       WHERE incident_id = $1 AND version = $2 - 1`,
      [
        i.incidentId, version, i.title, i.severity, i.alertCount, i.eventCount, i.ruleIds, i.relatedEventIds,
        JSON.stringify(i.entity ?? null), JSON.stringify(i.enrichment ?? null), i.firstSeen, i.lastSeen, i.updatedAt,
      ],
    );
    if (res.rowCount === 0) throw new StaleVersion(i.incidentId, version);
  }
  for (const alertId of newAlertIds) {
    await db.query('INSERT INTO incident_alerts (incident_id, alert_id) VALUES ($1, $2) ON CONFLICT DO NOTHING', [
      i.incidentId,
      alertId,
    ]);
  }
  await db.query('INSERT INTO incident_history (incident_id, tx_id, change, actor, at) VALUES ($1, $2, $3, $4, $5)', [
    i.incidentId,
    txId,
    JSON.stringify({ kind, version, severity: i.severity, newAlertIds }),
    'correlation-engine',
    new Date(now()),
  ]);
  // Queue incident-updates in the same transaction (P1-3): if this commits,
  // the message is published eventually, whoever drove the commit.
  const { rows } = await db.query('SELECT * FROM incidents WHERE incident_id = $1', [i.incidentId]);
  const alerts = await db.query('SELECT alert_id FROM incident_alerts WHERE incident_id = $1 ORDER BY alert_id', [i.incidentId]);
  const snapshot = rowToIncident(rows[0], alerts.rows.map((r) => r.alert_id));
  await enqueueIncidentUpdate(db, kind === 'create' ? 'incident.created' : 'incident.updated', snapshot, now, txId);
}

// Vote yes only if the decision still applies cleanly: no other in-flight
// transaction for this correlationId, and the incident is where the decision
// expects it (absent for create, at version-1 for update). `db` is a pool or a
// client inside a transaction.
export async function validateDecision(db, txId, payload) {
  if (!payload || !payload.incident || !payload.correlationId) return false;
  const pending = await db.query(
    `SELECT 1 FROM tpc_participant
      WHERE state IN ('UNCERTAIN', 'PRECOMMITTED') AND payload->>'correlationId' = $1 AND tx_id <> $2 LIMIT 1`,
    [payload.correlationId, txId],
  );
  if (pending.rowCount > 0) return false;
  if (payload.kind === 'create') {
    const exists = await db.query('SELECT 1 FROM incidents WHERE correlation_id = $1', [payload.correlationId]);
    return exists.rowCount === 0;
  }
  const { rows } = await db.query('SELECT version FROM incidents WHERE incident_id = $1', [payload.incident.incidentId]);
  return rows.length === 1 && rows[0].version === payload.version - 1;
}

// Map a coordinator log entry to the participant's resolve() answer (P0-4,
// R2-3): a final decision, PENDING while the coordinator is still driving or
// recovering it, or UNKNOWN when there is no entry. The coordinator logs
// WAITING before any canCommit, so no entry means it never started the
// transaction (UNCERTAIN aborts; PRECOMMITTED is kept and reported). This
// participant reads the log in-process, so "unreachable" (null) only arises
// when the lookup itself throws.
export function decisionOf(entry) {
  if (!entry) return 'UNKNOWN';
  if (entry.state === 'COMMITTED' || entry.state === 'ABORTED') return entry.state;
  return 'PENDING';
}

// Per-incident serialisation shared by the 3PC vote and analyst PATCHes.
export async function lockCorrelation(db, correlationId) {
  await db.query('SELECT pg_advisory_xact_lock(hashtext($1))', [correlationId]);
}

// The incident-db participant. Its vote validates and records UNCERTAIN (or
// ABORTED) in ONE Postgres transaction under the correlation's advisory lock,
// so a PATCH can never slip in between "validated" and "reserved" (P0-2).
export function createIncidentDbParticipant({ pool, timeoutMs, now = Date.now, hooks = {}, resolve }) {
  const store = createPgParticipantStore(pool, { now });
  const sm = createParticipant({
    store,
    timeoutMs,
    now,
    // Before acting on a timeout, ask the coordinator log (same process) for
    // the decision; only fall back to the timeout rule when there is none.
    resolve,
    // Only reached when reserve() did not record a vote (malformed payload).
    validate: (txId, payload) => validateDecision(pool, txId, payload),
    apply: (payload) => applyDecision(pool, payload, now),
  });

  async function reserve(txId, payload) {
    await withTransaction(pool, async (db) => {
      await lockCorrelation(db, payload.correlationId);
      const existing = await db.query('SELECT 1 FROM tpc_participant WHERE tx_id = $1', [txId]);
      if (existing.rowCount > 0) return; // repeat canCommit: keep the recorded vote
      const ok = await validateDecision(db, txId, payload);
      await hooks.afterValidate?.(txId, payload);
      await db.query(
        `INSERT INTO tpc_participant (tx_id, state, payload, updated_at) VALUES ($1, $2, $3, $4)`,
        [txId, ok ? 'UNCERTAIN' : 'ABORTED', JSON.stringify(payload), new Date(now())],
      );
    });
  }

  return {
    name: INCIDENT_DB,
    ...sm,
    // Fold the txId into the stored payload so apply can write it to history.
    async canCommit(txId, payload) {
      const withTx = payload && { ...payload, txId };
      if (withTx && typeof withTx.correlationId === 'string' && withTx.incident) await reserve(txId, withTx);
      return sm.canCommit(txId, withTx);
    },
  };
}

// ---- The service-level upsert ------------------------------------------------

export function createTpcMetrics(registry) {
  return {
    transactions: new client.Counter({
      name: 'tpc_transactions_total',
      help: '3PC correlation-decision transactions by outcome (committed|aborted|crashed)',
      labelNames: ['outcome'],
      registers: registry ? [registry] : [],
    }),
  };
}

export function createIncidentService({
  pool,
  repo,
  eventStore,
  config,
  logger,
  metrics = createTpcMetrics(),
  onCrash = async () => {},
  now = Date.now,
  participantHooks,
}) {
  const coordinatorLog = createPgCoordinatorLog(pool);
  const local = createIncidentDbParticipant({
    pool,
    timeoutMs: config.tpcParticipantTimeoutMs,
    now,
    hooks: participantHooks,
    resolve: async (txId) => decisionOf(await coordinatorLog.get(txId)),
  });

  // incident-updates is queued by applyDecision in the outbox, so a commit
  // only needs counting here.
  async function onCommitted() {
    metrics.transactions.inc({ outcome: 'committed' });
  }

  const coordinator = createCoordinator({
    log: coordinatorLog,
    participants: [local, eventStore],
    timeoutMs: config.tpcTimeoutMs,
    now,
    fault: config.tpcFault,
    onCommitted,
    logger,
  });

  // One upsert per entity at a time inside this (single-replica) coordinator,
  // so two candidates for the same entity never race findTarget/merge.
  const locks = new Map();
  function withEntityLock(key, fn) {
    const prev = locks.get(key) ?? Promise.resolve();
    const run = prev.then(fn, fn);
    const tail = run.catch(() => {});
    locks.set(key, tail);
    tail.then(() => {
      if (locks.get(key) === tail) locks.delete(key);
    });
    return run;
  }

  async function upsertIncident(candidate) {
    return withEntityLock(candidate.entityKey, async () => {
      const { existing, knownAlertIds } = await repo.findTarget(candidate);
      const merged = mergeCandidate(existing, knownAlertIds, candidate, { now });
      if (merged.kind === 'noop') return { incident: existing, outcome: 'noop' };

      const { incident, kind, newAlertIds } = merged;
      const payloads = {
        [INCIDENT_DB]: { correlationId: incident.correlationId, kind, version: incident.version, incident, newAlertIds },
        [EVENT_STORE]: {
          correlationId: incident.correlationId,
          version: incident.version,
          incidentId: incident.incidentId,
          alertIds: incident.alertIds,
          decision: { kind, severity: incident.severity, newAlertIds, firstSeen: incident.firstSeen, lastSeen: incident.lastSeen },
        },
      };
      const txId = randomUUID();
      let result;
      try {
        result = await coordinator.run(txId, payloads);
      } catch (err) {
        if (err instanceof CoordinatorCrash) {
          metrics.transactions.inc({ outcome: 'crashed' });
          logger?.fatal({ txId, correlationId: incident.correlationId }, 'injected coordinator crash after preCommit');
          await onCrash(err);
        }
        throw err;
      }
      if (result.outcome === 'aborted') {
        metrics.transactions.inc({ outcome: 'aborted' });
        throw new IncidentAborted(result.reason);
      }
      const stored = (await repo.getIncident(incident.incidentId)) ?? incident;
      return { incident: stored, outcome: kind === 'create' ? 'created' : 'updated', txId };
    });
  }

  return { upsertIncident, recover: () => coordinator.recover(), participant: local, coordinator };
}
