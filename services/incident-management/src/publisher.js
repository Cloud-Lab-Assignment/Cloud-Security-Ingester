import { SERVICE_VERSION, isoTimestamp } from '@core/common';

export const PRODUCED_BY = { name: 'incident-management', version: SERVICE_VERSION };

// The incident-updates message. Published through the outbox (outbox.js),
// keyed by incidentId.
export function incidentUpdateMessage(type, incident, now = Date.now) {
  return { schemaVersion: '1.0', type, incident, changedAt: isoTimestamp(now()), producedBy: PRODUCED_BY };
}
