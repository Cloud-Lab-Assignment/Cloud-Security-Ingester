import { SERVICE_VERSION, isoTimestamp, sendJson } from '@core/common';

export const PRODUCED_BY = { name: 'incident-management', version: SERVICE_VERSION };

export function incidentUpdateMessage(type, incident, now = Date.now) {
  return { schemaVersion: '1.0', type, incident, changedAt: isoTimestamp(now()), producedBy: PRODUCED_BY };
}

// Publishes incident.created | incident.updated | incident.status_changed to
// the incident-updates topic, keyed by incidentId.
export function createPublisher({ producer, topic, now = Date.now }) {
  return {
    async publishIncidentUpdate(type, incident) {
      const message = incidentUpdateMessage(type, incident, now);
      await sendJson(producer, topic, incident.incidentId, message);
      return message;
    },
  };
}
