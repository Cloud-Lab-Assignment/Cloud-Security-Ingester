import { createHash } from 'node:crypto';

// Pure alert correlation (no I/O).
//
// Alerts are grouped per entityKey. A group covers the alert times
// [firstSeen, lastSeen]; an alert joins a group of its entity when its
// alertTime lies within windowMs of that span, i.e. in
// [firstSeen - windowMs, lastSeen + windowMs]. So:
//   - an alert more than windowMs after lastSeen starts a new group;
//   - an out-of-order alert up to windowMs before firstSeen joins and lowers
//     firstSeen;
//   - an alert more than windowMs before firstSeen does NOT join: it is an
//     older, separate burst and opens (or joins) an older group. This keeps the
//     rule symmetric, so the grouping does not depend on arrival order.
// If an alert is within reach of two groups it joins the nearest one (ties go
// to the newer group). Groups are never merged, because that would change an
// existing correlationId.
//
// Eviction (memory bound): a group's window closes at lastSeen + windowMs (no
// later alert can join it after that). A group whose window closed more than
// 2 windows before the watermark is dropped together with its alertIds, i.e.
// when lastSeen + 3 * windowMs < watermark. The watermark is event time: the
// newest alertTime ingested so far, so replaying old alerts from the start of
// the topic behaves like live traffic. A single alert's contribution is capped
// at now() + windowMs (wall clock, injectable for tests), so one far-future
// alertTime cannot evict every live group. The sweep runs on ingest whenever
// the watermark advances. After eviction, a very late alert opens a new group
// and a redelivered alertId is ingested again (with the same correlationId if
// it opened its group); incident-management's durable dedupe and merge are the
// safety net for both.
//
// correlationId = "cor-" + sha256(entityKey + "|" + firstAlertId)[0..24], where
// firstAlertId is the alert that opened the group. It does not change when
// that alert is later forgotten, as long as the group still has other alerts.

export const SEVERITIES = ['low', 'medium', 'high', 'critical'];
const RANK = new Map(SEVERITIES.map((s, i) => [s, i]));
const DEFAULT_SEVERITY = 'medium';
const MAX_RELATED_EVENT_IDS = 200;

export function correlationIdFor(entityKey, firstAlertId) {
  return 'cor-' + createHash('sha256').update(`${entityKey}|${firstAlertId}`).digest('hex').slice(0, 24);
}

function normalizeSeverity(value) {
  const s = typeof value === 'string' ? value.toLowerCase() : '';
  return RANK.has(s) ? s : DEFAULT_SEVERITY;
}

function toMs(value, fallback) {
  const ms = typeof value === 'string' ? Date.parse(value) : Number.NaN;
  return Number.isNaN(ms) ? fallback : ms;
}

const iso = (ms) => new Date(ms).toISOString();

// Keep only what the candidate needs from an alert.
function summarize(alert, seq) {
  const timeMs = toMs(alert.alertTime, Number.NaN);
  if (Number.isNaN(timeMs)) throw new TypeError(`alert ${alert.alertId} has an invalid alertTime`);
  const count = Number(alert.eventCount);
  return {
    alertId: alert.alertId,
    seq,
    timeMs,
    ruleId: alert.ruleId ?? null,
    severity: normalizeSeverity(alert.severity),
    entity: alert.entity && typeof alert.entity === 'object' ? alert.entity : null,
    windowStartMs: toMs(alert.window?.start, timeMs),
    windowEndMs: toMs(alert.window?.end, timeMs),
    eventCount: Number.isFinite(count) && count > 0 ? Math.trunc(count) : 0,
    relatedEventIds: Array.isArray(alert.relatedEventIds) ? alert.relatedEventIds : [],
  };
}

// evictEvery: besides every watermark advance, run the eviction sweep at least
// once per this many ingests, so a replay of old alerts (which never advances
// the watermark) cannot accumulate groups without bound (R2-7). One sweep is
// O(groups), so the amortised cost per ingest stays O(groups / evictEvery).
export function createCorrelator({ windowMs, escalateCount, now = Date.now, evictEvery = 256 }) {
  if (!(windowMs > 0)) throw new TypeError('windowMs must be > 0');
  if (!(escalateCount > 0)) throw new TypeError('escalateCount must be > 0');

  const groupsByEntity = new Map(); // entityKey -> Set<group>
  const groupByAlertId = new Map(); // alertId -> group
  let seq = 0;
  let watermark = -Infinity; // newest (capped) alertTime seen
  let ingestsSinceSweep = 0;

  function dropGroup(group) {
    const groups = groupsByEntity.get(group.entityKey);
    groups.delete(group);
    if (groups.size === 0) groupsByEntity.delete(group.entityKey);
  }

  function evictExpired() {
    ingestsSinceSweep = 0;
    const horizon = watermark - 3 * windowMs;
    for (const groups of [...groupsByEntity.values()]) {
      for (const group of [...groups]) {
        if (group.lastSeen >= horizon) continue;
        for (const alertId of group.alerts.keys()) groupByAlertId.delete(alertId);
        dropGroup(group);
      }
    }
  }

  function advanceWatermark(timeMs) {
    const t = Math.min(timeMs, now() + windowMs);
    if (t <= watermark) return;
    watermark = t;
    evictExpired();
  }

  function refreshSpan(group) {
    let first = Infinity;
    let last = -Infinity;
    for (const a of group.alerts.values()) {
      if (a.timeMs < first) first = a.timeMs;
      if (a.timeMs > last) last = a.timeMs;
    }
    group.firstSeen = first;
    group.lastSeen = last;
  }

  function distance(group, t) {
    if (t < group.firstSeen) return group.firstSeen - t;
    if (t > group.lastSeen) return t - group.lastSeen;
    return 0;
  }

  function findGroup(entityKey, t) {
    let best = null;
    let bestDist = Infinity;
    for (const g of groupsByEntity.get(entityKey) ?? []) {
      const d = distance(g, t);
      if (d > windowMs) continue;
      if (d < bestDist || (d === bestDist && g.lastSeen > best.lastSeen)) {
        best = g;
        bestDist = d;
      }
    }
    return best;
  }

  function toCandidate(group) {
    const alerts = [...group.alerts.values()].sort((a, b) => a.timeMs - b.timeMs || a.seq - b.seq);
    const ruleIds = [];
    const related = new Set();
    let maxRank = -1;
    let windowStart = Infinity;
    let windowEnd = -Infinity;
    let eventCount = 0;
    let entity = null;
    for (const a of alerts) {
      if (a.ruleId && !ruleIds.includes(a.ruleId)) ruleIds.push(a.ruleId);
      maxRank = Math.max(maxRank, RANK.get(a.severity));
      windowStart = Math.min(windowStart, a.windowStartMs);
      windowEnd = Math.max(windowEnd, a.windowEndMs);
      eventCount += a.eventCount;
      if (a.entity) entity = a.entity; // latest alert's entity wins
      for (const id of a.relatedEventIds) {
        if (related.size >= MAX_RELATED_EVENT_IDS) break;
        related.add(id);
      }
    }
    const escalate = alerts.length >= escalateCount || ruleIds.length >= 2;
    return {
      correlationId: group.correlationId,
      entityKey: group.entityKey,
      title: `${alerts.length} correlated alert(s) for ${group.entityKey}`,
      severity: escalate ? 'critical' : SEVERITIES[maxRank],
      alertIds: alerts.map((a) => a.alertId),
      ruleIds,
      firstSeen: iso(group.firstSeen),
      lastSeen: iso(group.lastSeen),
      windowStart: iso(windowStart),
      windowEnd: iso(windowEnd),
      eventCount,
      relatedEventIds: [...related],
      entity,
    };
  }

  // Add an alert. Returns the group's updated Candidate, or null when the
  // alertId is already known (redelivery).
  function ingest(alert) {
    if (!alert?.alertId || !alert?.entityKey) throw new TypeError('alert needs alertId and entityKey');
    if (groupByAlertId.has(alert.alertId)) return null;
    seq += 1;
    const summary = summarize(alert, seq);
    advanceWatermark(summary.timeMs);
    let group = findGroup(alert.entityKey, summary.timeMs);
    if (!group) {
      group = {
        correlationId: correlationIdFor(alert.entityKey, alert.alertId),
        entityKey: alert.entityKey,
        alerts: new Map(),
        firstSeen: summary.timeMs,
        lastSeen: summary.timeMs,
      };
      if (!groupsByEntity.has(alert.entityKey)) groupsByEntity.set(alert.entityKey, new Set());
      groupsByEntity.get(alert.entityKey).add(group);
    }
    group.alerts.set(alert.alertId, summary);
    groupByAlertId.set(alert.alertId, group);
    refreshSpan(group);
    const candidate = toCandidate(group);
    ingestsSinceSweep += 1;
    if (ingestsSinceSweep >= evictEvery) evictExpired();
    return candidate;
  }

  // Undo an ingest (used when the upsert failed, so a retry re-sends it).
  function forget(alertId) {
    const group = groupByAlertId.get(alertId);
    if (!group) return;
    groupByAlertId.delete(alertId);
    group.alerts.delete(alertId);
    if (group.alerts.size > 0) {
      refreshSpan(group);
      return;
    }
    dropGroup(group);
  }

  // Current size of the in-memory state.
  function stats() {
    let groups = 0;
    for (const set of groupsByEntity.values()) groups += set.size;
    return { groups, alerts: groupByAlertId.size };
  }

  return { ingest, forget, stats };
}
