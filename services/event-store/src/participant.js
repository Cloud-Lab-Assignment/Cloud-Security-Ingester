import { createParticipant } from '@core/tpc';
import { isoTimestamp } from '@core/common';

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
// stored payload at canCommit, so apply (which only sees the payload) can record it.
export function createCorrelationParticipant({ store, putCorrelation, timeoutMs, forceVote, now = Date.now }) {
  const participant = createParticipant({
    store,
    timeoutMs,
    forceVote,
    now,
    validate: async (txId, payload) => validateCorrelationPayload(payload),
    apply: async ({ txId, ...payload }) => {
      await putCorrelation({ ...payload, txId, recordedAt: isoTimestamp(now()) });
    },
  });
  return {
    ...participant,
    canCommit: (txId, payload) =>
      participant.canCommit(txId, isObject(payload) ? { ...payload, txId } : payload),
  };
}
