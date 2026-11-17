import { createParticipant } from '@core/tpc';
import { correlationItem } from './dynamo.js';

const nonEmpty = (v) => typeof v === 'string' && v.length > 0;
const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const stringArray = (v) => Array.isArray(v) && v.every((x) => typeof x === 'string');

// The coordinator's payload for one correlation decision:
// {correlationId, version, incidentId, alertIds, decision:{kind, severity, newAlertIds, firstSeen, lastSeen}}
export function validateCorrelationPayload(p) {
  if (!isObject(p)) return false;
  const d = p.decision;
  return (
    nonEmpty(p.correlationId) &&
    Number.isInteger(p.version) &&
    p.version > 0 &&
    nonEmpty(p.incidentId) &&
    stringArray(p.alertIds) &&
    isObject(d) &&
    (d.kind === 'create' || d.kind === 'update') &&
    nonEmpty(d.severity) &&
    stringArray(d.newAlertIds) &&
    nonEmpty(d.firstSeen) &&
    nonEmpty(d.lastSeen)
  );
}

// 3PC participant for correlation-history writes. The txId is folded into the
// stored payload at canCommit, so the history row can record it. With the
// DynamoDB store, commits go through store.commit, which writes the row and
// COMMITTED atomically (R2-2); `apply` is only used by stores without commit.
// `resolve` (optional) asks the coordinator for its decision before the sweeper
// acts on a timed-out transaction (see coordinator-client.js). `logger`
// (optional) gets an error when the coordinator has no record of a
// PRECOMMITTED transaction (R2-3).
export function createCorrelationParticipant({
  store,
  putCorrelation,
  timeoutMs,
  forceVote,
  now = Date.now,
  resolve,
  resolveTimeoutMs,
  logger,
}) {
  const participant = createParticipant({
    store,
    timeoutMs,
    forceVote,
    now,
    resolve,
    resolveTimeoutMs,
    logger,
    validate: async (txId, payload) => validateCorrelationPayload(payload),
    apply: async (payload) => {
      await putCorrelation(correlationItem({ txId: payload.txId, payload, updatedAt: now() }));
    },
  });
  return {
    ...participant,
    canCommit: (txId, payload) =>
      participant.canCommit(txId, isObject(payload) ? { ...payload, txId } : payload),
  };
}
