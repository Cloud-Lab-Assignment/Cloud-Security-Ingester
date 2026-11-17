import { fileURLToPath } from 'node:url';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

// Repo layout and Docker image layout (/app/proto, /app/services/correlation-engine/src)
// both put the proto three levels up from this file.
export const DEFAULT_PROTO_PATH = fileURLToPath(new URL('../../../proto/incident/v1/incident.proto', import.meta.url));

const LOADER_OPTIONS = { keepCase: false, longs: String, defaults: true };

// Maps a correlator Candidate (+ enrichment) to incident.v1.UpsertIncidentRequest.
export function toUpsertRequest(candidate, enrichment) {
  return {
    correlationId: candidate.correlationId,
    entityKey: candidate.entityKey,
    title: candidate.title,
    severity: candidate.severity,
    alertIds: candidate.alertIds,
    ruleIds: candidate.ruleIds,
    firstSeen: candidate.firstSeen,
    lastSeen: candidate.lastSeen,
    eventCount: candidate.eventCount,
    relatedEventIds: candidate.relatedEventIds,
    entityJson: JSON.stringify(candidate.entity ?? {}),
    enrichmentJson: enrichment ? JSON.stringify(enrichment) : '',
  };
}

// gRPC client for Incident Management. Insecure (in-cluster) credentials.
// Errors are grpc-js ServiceErrors carrying a numeric `.code` (grpc.status).
export function createIncidentClient({ addr, deadlineMs, protoPath = DEFAULT_PROTO_PATH }) {
  const definition = protoLoader.loadSync(protoPath, LOADER_OPTIONS);
  const { IncidentService } = grpc.loadPackageDefinition(definition).incident.v1;
  const client = new IncidentService(addr, grpc.credentials.createInsecure());

  function upsertIncident(candidate, enrichment) {
    const request = toUpsertRequest(candidate, enrichment);
    return new Promise((resolve, reject) => {
      client.UpsertIncident(request, { deadline: new Date(Date.now() + deadlineMs) }, (err, incident) =>
        err ? reject(err) : resolve(incident),
      );
    });
  }

  return { upsertIncident, close: () => client.close() };
}
