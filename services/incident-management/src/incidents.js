import { randomUUID } from 'node:crypto';
import { isoTimestamp } from '@core/common';

export const SEVERITIES = ['low', 'medium', 'high', 'critical'];
export const STATUSES = ['open', 'investigating', 'resolved', 'false_positive'];
const MAX_RELATED_EVENTS = 200;

export class VersionConflict extends Error {
  constructor(incidentId, expected, actual) {
    super(`incident ${incidentId}: version ${expected} is stale (current ${actual})`);
    this.name = 'VersionConflict';
    this.currentVersion = actual;
  }
}

export class IncidentNotFound extends Error {
  constructor(incidentId) {
    super(`incident ${incidentId} not found`);
    this.name = 'IncidentNotFound';
  }
}

// A 3PC transaction for this incident is still in flight; retry shortly.
export class TransactionPending extends Error {
  constructor(incidentId) {
    super(`incident ${incidentId} has a 3PC transaction in progress`);
    this.name = 'TransactionPending';
  }
}

const sevRank = (s) => SEVERITIES.indexOf(s);
const maxSeverity = (a, b) => (sevRank(a) >= sevRank(b) ? a : b);
const union = (...lists) => [...new Set(lists.flat().filter((x) => x != null))];
const minIso = (a, b) => (Date.parse(a) <= Date.parse(b) ? a : b);
const maxIso = (a, b) => (Date.parse(a) >= Date.parse(b) ? a : b);
const titleFor = (count, entityKey) => `${count} correlated alert(s) for ${entityKey}`;

// Pure: decide what a correlation candidate does to an incident.
//   existing null              -> create (new UUID v4, version 1)
//   every alert already known  -> noop
//   otherwise                  -> update (unions, max severity, version+1; status untouched)
export function mergeCandidate(existing, knownAlertIds, candidate, { now = Date.now, newId = randomUUID } = {}) {
  const ts = isoTimestamp(now());
  const candidateAlerts = union(candidate.alertIds ?? []);

  if (!existing) {
    const incident = {
      incidentId: newId(),
      correlationId: candidate.correlationId,
      entityKey: candidate.entityKey,
      title: candidate.title || titleFor(candidateAlerts.length, candidate.entityKey),
      severity: candidate.severity,
      status: 'open',
      alertIds: candidateAlerts,
      alertCount: candidateAlerts.length,
      eventCount: candidate.eventCount ?? 0,
      ruleIds: union(candidate.ruleIds ?? []),
      relatedEventIds: union(candidate.relatedEventIds ?? []).slice(0, MAX_RELATED_EVENTS),
      entity: candidate.entity ?? null,
      enrichment: candidate.enrichment ?? null,
      firstSeen: candidate.firstSeen,
      lastSeen: candidate.lastSeen,
      version: 1,
      createdAt: ts,
      updatedAt: ts,
    };
    return { kind: 'create', incident, newAlertIds: candidateAlerts };
  }

  const newAlertIds = candidateAlerts.filter((id) => !knownAlertIds.has(id));
  if (newAlertIds.length === 0) return { kind: 'noop', incident: existing, newAlertIds: [] };

  const alertIds = union(existing.alertIds ?? [...knownAlertIds], newAlertIds);
  const incident = {
    ...existing,
    title: titleFor(alertIds.length, existing.entityKey),
    severity: maxSeverity(existing.severity, candidate.severity),
    alertIds,
    alertCount: alertIds.length,
    eventCount: Math.max(existing.eventCount ?? 0, candidate.eventCount ?? 0),
    ruleIds: union(existing.ruleIds ?? [], candidate.ruleIds ?? []),
    relatedEventIds: union(existing.relatedEventIds ?? [], candidate.relatedEventIds ?? []).slice(0, MAX_RELATED_EVENTS),
    entity: existing.entity ?? candidate.entity ?? null,
    enrichment: candidate.enrichment ?? existing.enrichment ?? null,
    firstSeen: minIso(existing.firstSeen, candidate.firstSeen),
    lastSeen: maxIso(existing.lastSeen, candidate.lastSeen),
    version: existing.version + 1,
    updatedAt: ts,
  };
  return { kind: 'update', incident, newAlertIds };
}

const iso = (d) => (d == null ? null : d instanceof Date ? d.toISOString() : new Date(d).toISOString());

export function rowToIncident(row, alertIds) {
  const incident = {
    incidentId: row.incident_id,
    correlationId: row.correlation_id,
    entityKey: row.entity_key,
    title: row.title,
    severity: row.severity,
    status: row.status,
    alertCount: row.alert_count,
    eventCount: row.event_count,
    ruleIds: row.rule_ids ?? [],
    relatedEventIds: row.related_event_ids ?? [],
    entity: row.entity,
    enrichment: row.enrichment,
    firstSeen: iso(row.first_seen),
    lastSeen: iso(row.last_seen),
    version: row.version,
    createdAt: iso(row.created_at),
    updatedAt: iso(row.updated_at),
  };
  if (alertIds) incident.alertIds = alertIds;
  return incident;
}

// Postgres-backed incident repository.
export function createIncidentRepo({ pool, correlationWindowMs, now = Date.now }) {
  async function alertIdsFor(db, incidentId) {
    const { rows } = await db.query('SELECT alert_id FROM incident_alerts WHERE incident_id = $1 ORDER BY alert_id', [
      incidentId,
    ]);
    return rows.map((r) => r.alert_id);
  }

  // The incident a candidate belongs to: same correlationId first, otherwise an
  // open/investigating incident for the same entity still inside the window
  // (covers correlation-engine rebalances, where in-memory groups are lost).
  async function findTarget(candidate, db = pool) {
    let { rows } = await db.query('SELECT * FROM incidents WHERE correlation_id = $1', [candidate.correlationId]);
    if (rows.length === 0) {
      // The incident's [first_seen, last_seen] must overlap the candidate's
      // span widened by the window on both sides (P1-6).
      const since = new Date(Date.parse(candidate.firstSeen) - correlationWindowMs);
      const until = new Date(Date.parse(candidate.lastSeen) + correlationWindowMs);
      ({ rows } = await db.query(
        `SELECT * FROM incidents
          WHERE entity_key = $1 AND status IN ('open', 'investigating')
            AND last_seen >= $2 AND first_seen <= $3
          ORDER BY last_seen DESC LIMIT 1`,
        [candidate.entityKey, since, until],
      ));
    }
    if (rows.length === 0) return { existing: null, knownAlertIds: new Set() };
    const alertIds = await alertIdsFor(db, rows[0].incident_id);
    return { existing: rowToIncident(rows[0], alertIds), knownAlertIds: new Set(alertIds) };
  }

  async function listIncidents({ status, severity, entityKey, limit = 50 } = {}) {
    const where = [];
    const params = [];
    if (status) where.push(`status = $${params.push(status)}`);
    if (severity) where.push(`severity = $${params.push(severity)}`);
    if (entityKey) where.push(`entity_key = $${params.push(entityKey)}`);
    params.push(limit);
    const { rows } = await pool.query(
      `SELECT * FROM incidents ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
        ORDER BY last_seen DESC NULLS LAST LIMIT $${params.length}`,
      params,
    );
    return rows.map((r) => rowToIncident(r));
  }

  async function getIncident(incidentId, db = pool) {
    const { rows } = await db.query('SELECT * FROM incidents WHERE incident_id = $1', [incidentId]);
    if (rows.length === 0) return null;
    return rowToIncident(rows[0], await alertIdsFor(db, incidentId));
  }

  async function getHistory(incidentId) {
    const { rows } = await pool.query(
      'SELECT id, tx_id, change, actor, at FROM incident_history WHERE incident_id = $1 ORDER BY id',
      [incidentId],
    );
    return rows.map((r) => ({ id: String(r.id), txId: r.tx_id, change: r.change, actor: r.actor, at: iso(r.at) }));
  }

  // Analyst status change: a plain local update guarded by optimistic locking.
  // Not part of 3PC; refused while a correlation transaction is in flight so it
  // cannot make an already-decided 3PC apply stale.
  async function updateStatus(incidentId, { status, version, actor = 'analyst', note = null }) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query('SELECT * FROM incidents WHERE incident_id = $1 FOR UPDATE', [incidentId]);
      if (rows.length === 0) throw new IncidentNotFound(incidentId);
      const current = rows[0];
      if (current.version !== version) throw new VersionConflict(incidentId, version, current.version);
      // Same lock the incident-db participant holds while it votes (P0-2): wait
      // for an in-progress vote, then see its pending record below.
      await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [current.correlation_id]);
      const pending = await client.query(
        `SELECT 1 FROM tpc_participant
          WHERE state IN ('UNCERTAIN', 'PRECOMMITTED') AND payload->>'correlationId' = $1 LIMIT 1`,
        [current.correlation_id],
      );
      if (pending.rowCount > 0) throw new TransactionPending(incidentId);
      const at = new Date(now());
      const updated = await client.query(
        `UPDATE incidents SET status = $2, version = version + 1, updated_at = $3
          WHERE incident_id = $1 AND version = $4 RETURNING *`,
        [incidentId, status, at, version],
      );
      await client.query('INSERT INTO incident_history (incident_id, change, actor, at) VALUES ($1, $2, $3, $4)', [
        incidentId,
        { kind: 'status', from: current.status, to: status, note },
        actor,
        at,
      ]);
      const alertIds = await alertIdsFor(client, incidentId);
      await client.query('COMMIT');
      return rowToIncident(updated.rows[0], alertIds);
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  return { findTarget, listIncidents, getIncident, getHistory, updateStatus };
}
