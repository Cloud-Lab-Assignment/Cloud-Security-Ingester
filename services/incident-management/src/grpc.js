import { fileURLToPath } from 'node:url';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import { isIsoTimestamp } from '@core/common';
import { SEVERITIES } from './incidents.js';
import { IncidentAborted } from './tpc-wiring.js';

// Repo layout and image layout both keep proto/ three levels above src/.
export const DEFAULT_PROTO_PATH =
  process.env.PROTO_PATH || fileURLToPath(new URL('../../../proto/incident/v1/incident.proto', import.meta.url));

export function loadIncidentProto(protoPath = DEFAULT_PROTO_PATH) {
  const definition = protoLoader.loadSync(protoPath, { keepCase: false, longs: String, defaults: true });
  return grpc.loadPackageDefinition(definition);
}

export class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ValidationError';
  }
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function parseJsonField(name, text, { allowEmpty }) {
  if (text === '' || text === 'null') {
    if (allowEmpty) return null;
    return {};
  }
  try {
    const value = JSON.parse(text);
    if (value !== null && (typeof value !== 'object' || Array.isArray(value))) throw new Error('not an object');
    return value;
  } catch {
    throw new ValidationError(`${name} must be a JSON object`);
  }
}

// UpsertIncidentRequest (camelCase) -> correlation candidate. Throws ValidationError.
export function requestToCandidate(req) {
  const need = (cond, msg) => {
    if (!cond) throw new ValidationError(msg);
  };
  need(req.correlationId, 'correlationId is required');
  need(req.entityKey, 'entityKey is required');
  need(SEVERITIES.includes(req.severity), `severity must be one of ${SEVERITIES.join(', ')}`);
  need(Array.isArray(req.alertIds) && req.alertIds.length > 0, 'alertIds must not be empty');
  need(isIsoTimestamp(req.firstSeen), 'firstSeen must be a UTC ISO timestamp');
  need(isIsoTimestamp(req.lastSeen), 'lastSeen must be a UTC ISO timestamp');
  need(Number.isInteger(req.eventCount) && req.eventCount >= 0, 'eventCount must be a non-negative integer');
  return {
    correlationId: req.correlationId,
    entityKey: req.entityKey,
    title: req.title || `${req.alertIds.length} correlated alert(s) for ${req.entityKey}`,
    severity: req.severity,
    alertIds: req.alertIds,
    ruleIds: req.ruleIds ?? [],
    firstSeen: new Date(req.firstSeen).toISOString(),
    lastSeen: new Date(req.lastSeen).toISOString(),
    eventCount: req.eventCount,
    relatedEventIds: req.relatedEventIds ?? [],
    entity: parseJsonField('entityJson', req.entityJson ?? '', { allowEmpty: false }),
    enrichment: parseJsonField('enrichmentJson', req.enrichmentJson ?? '', { allowEmpty: true }),
  };
}

export function incidentToProto(incident, outcome = '') {
  return {
    incidentId: incident.incidentId,
    correlationId: incident.correlationId ?? '',
    entityKey: incident.entityKey ?? '',
    title: incident.title ?? '',
    severity: incident.severity ?? '',
    status: incident.status ?? '',
    alertCount: incident.alertCount ?? 0,
    eventCount: incident.eventCount ?? 0,
    firstSeen: incident.firstSeen ?? '',
    lastSeen: incident.lastSeen ?? '',
    version: incident.version ?? 0,
    createdAt: incident.createdAt ?? '',
    updatedAt: incident.updatedAt ?? '',
    outcome,
  };
}

function toGrpcError(err) {
  if (err instanceof ValidationError) return { code: grpc.status.INVALID_ARGUMENT, details: err.message };
  if (err instanceof IncidentAborted) return { code: grpc.status.ABORTED, details: err.message };
  if (err?.name === 'IncidentNotFound') return { code: grpc.status.NOT_FOUND, details: err.message };
  return { code: grpc.status.INTERNAL, details: 'internal error' };
}

// Start the IncidentService gRPC server. Resolves to {server, port}.
export async function startGrpc({ port, host = '0.0.0.0', upsertIncident, getIncident, logger, metrics, protoPath }) {
  const { IncidentService } = loadIncidentProto(protoPath).incident.v1;

  const instrument = (method, handler) => async (call, callback) => {
    const started = process.hrtime.bigint();
    let code = grpc.status.OK;
    try {
      callback(null, await handler(call.request));
    } catch (err) {
      const mapped = toGrpcError(err);
      code = mapped.code;
      const level = code === grpc.status.INTERNAL ? 'error' : 'warn';
      logger?.[level]({ err, method, code: grpc.status[code] }, 'grpc call failed');
      callback(mapped);
    } finally {
      metrics?.requests.inc({ method, code: grpc.status[code] });
      metrics?.duration.observe({ method }, Number(process.hrtime.bigint() - started) / 1e9);
    }
  };

  const server = new grpc.Server();
  server.addService(IncidentService.service, {
    UpsertIncident: instrument('UpsertIncident', async (req) => {
      const candidate = requestToCandidate(req);
      const { incident, outcome } = await upsertIncident(candidate);
      return incidentToProto(incident, outcome);
    }),
    GetIncident: instrument('GetIncident', async (req) => {
      const id = req.incidentId;
      const incident = UUID_RE.test(id ?? '') ? await getIncident(id) : null;
      if (!incident) {
        const e = new Error(`incident ${id} not found`);
        e.name = 'IncidentNotFound';
        throw e;
      }
      return incidentToProto(incident);
    }),
  });

  const boundPort = await new Promise((resolve, reject) => {
    server.bindAsync(`${host}:${port}`, grpc.ServerCredentials.createInsecure(), (err, p) => (err ? reject(err) : resolve(p)));
  });
  return { server, port: boundPort };
}
