import client from 'prom-client';
import { status as grpcStatus } from '@grpc/grpc-js';

export function createMetrics(registry) {
  return {
    alertsConsumed: new client.Counter({
      name: 'alerts_consumed_total',
      help: 'security-alerts messages handled, by result (upserted|duplicate|invalid|rejected|failed)',
      labelNames: ['result'],
      registers: [registry],
    }),
    incidentsUpserted: new client.Counter({
      name: 'incidents_upserted_total',
      help: 'Successful UpsertIncident calls, by outcome (created|updated|noop)',
      labelNames: ['outcome'],
      registers: [registry],
    }),
  };
}

// UTC ISO-8601 date-time with an explicit zone, as upstream's toISOString().
const ISO_TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;
const isIsoTime = (v) => typeof v === 'string' && ISO_TIME.test(v) && !Number.isNaN(Date.parse(v));
const isNonEmptyString = (v) => typeof v === 'string' && v !== '';
const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const present = (v) => v !== undefined && v !== null;

// Check the fields the correlator reads. Returns an error string or null.
// Required: alertId, entityKey (non-empty strings), alertTime (ISO time).
// severity must be a string when present; unknown values map to medium.
// Optional (absent or null is fine; upstream detection-rules always sends
// them): window {start, end} (ISO times), eventCount (integer >= 0),
// relatedEventIds (string[]), ruleId (string), entity (object).
// When window is absent the correlator uses alertTime as the window.
export function validateAlert(alert) {
  if (!isPlainObject(alert)) return 'not a JSON object';
  for (const field of ['alertId', 'entityKey']) {
    if (!isNonEmptyString(alert[field])) return `${field} must be a non-empty string`;
  }
  if (!isIsoTime(alert.alertTime)) return 'alertTime must be an ISO-8601 timestamp';
  if (present(alert.severity) && typeof alert.severity !== 'string') return 'severity must be a string';
  if (present(alert.window)) {
    if (!isPlainObject(alert.window)) return 'window must be an object';
    if (!isIsoTime(alert.window.start) || !isIsoTime(alert.window.end)) return 'window.start and window.end must be ISO-8601 timestamps';
  }
  if (present(alert.eventCount) && !(Number.isInteger(alert.eventCount) && alert.eventCount >= 0)) {
    return 'eventCount must be a non-negative integer';
  }
  if (present(alert.relatedEventIds)
    && !(Array.isArray(alert.relatedEventIds) && alert.relatedEventIds.every((id) => typeof id === 'string'))) {
    return 'relatedEventIds must be an array of strings';
  }
  if (present(alert.ruleId) && typeof alert.ruleId !== 'string') return 'ruleId must be a string';
  if (present(alert.entity) && !isPlainObject(alert.entity)) return 'entity must be an object';
  return null;
}

function parseAlert(message) {
  const text = message?.value == null ? '' : message.value.toString('utf8');
  let alert;
  try {
    alert = JSON.parse(text);
  } catch {
    return { error: 'invalid JSON' };
  }
  const error = validateAlert(alert);
  return error ? { error } : { alert };
}

// Handle one security-alerts message.
// deps: { correlator, fetchEnrichment, upsertIncident, logger, metrics }
// where: { topic, partition } of the message, for logs.
// An invalid alert (bad JSON, wrong field types, or any error thrown by
// correlator.ingest) is logged at warn, counted as result="invalid" and
// skipped, so its offset is committed and the consumer keeps going.
// Throws only when the upsert failed with a retryable gRPC error, after
// forgetting the alert, so the kafkajs retry re-ingests and re-sends it.
export async function handleAlert(message, deps, where = {}) {
  const { correlator, fetchEnrichment, upsertIncident, logger, metrics } = deps;
  const count = (result) => metrics?.alertsConsumed.inc({ result });
  const skipInvalid = (reason, extra = {}) => {
    logger.warn(
      { reason, topic: where.topic, partition: where.partition, offset: message?.offset, ...extra },
      'skipping invalid security alert',
    );
    count('invalid');
  };

  const { alert, error } = parseAlert(message);
  if (error) {
    skipInvalid(error);
    return;
  }

  let candidate;
  try {
    candidate = correlator.ingest(alert);
  } catch (err) {
    skipInvalid(`ingest failed: ${err?.message ?? err}`, { err, alertId: alert.alertId });
    return;
  }
  if (!candidate) {
    logger.debug({ alertId: alert.alertId }, 'duplicate alert ignored');
    count('duplicate');
    return;
  }

  let enrichment = null;
  try {
    enrichment = await fetchEnrichment(candidate.entityKey, candidate.windowStart, candidate.windowEnd);
  } catch (err) {
    logger.warn({ err, correlationId: candidate.correlationId }, 'enrichment failed, continuing without it');
  }

  let incident;
  try {
    incident = await upsertIncident(candidate, enrichment ?? null);
  } catch (err) {
    // Forget in both cases: a retry must re-send this alert, and a rejected
    // alert must not ride along in later candidates of the same group.
    correlator.forget(alert.alertId);
    if (err?.code === grpcStatus.INVALID_ARGUMENT) {
      logger.error({ err, alertId: alert.alertId, correlationId: candidate.correlationId }, 'incident upsert rejected, skipping alert');
      count('rejected');
      return;
    }
    logger.warn({ err, code: err?.code, alertId: alert.alertId, correlationId: candidate.correlationId }, 'incident upsert failed, will retry');
    count('failed');
    throw err;
  }

  const outcome = incident?.outcome || 'unknown';
  metrics?.incidentsUpserted.inc({ outcome });
  count('upserted');
  logger.info(
    {
      alertId: alert.alertId,
      correlationId: candidate.correlationId,
      incidentId: incident?.incidentId,
      outcome,
      severity: candidate.severity,
      alertCount: candidate.alertIds.length,
      enriched: enrichment != null,
    },
    'incident upserted',
  );
}

// Subscribe to the alert topic and feed every message through `handle`.
// kafkajs runs partitions concurrently but each partition serially; alerts are
// keyed by entityKey, so one entity's correlator state is never raced.
//
// isReady() is true only while the consumer is in its group: set on
// GROUP_JOIN, cleared on CRASH, STOP and DISCONNECT. When kafkajs restarts
// after a retriable crash it rejoins and GROUP_JOIN makes it ready again;
// after stop() it stays false.
export async function startConsumer({ kafka, logger, topic, groupId, handle }) {
  const consumer = kafka.consumer({ groupId });
  let ready = false;
  let stopped = false;
  const { GROUP_JOIN, CRASH, STOP, DISCONNECT } = consumer.events;
  consumer.on(GROUP_JOIN, () => { ready = true; });
  consumer.on(CRASH, ({ payload } = {}) => {
    ready = false;
    logger.error({ err: payload?.error, restart: payload?.restart }, 'kafka consumer crashed');
  });
  consumer.on(STOP, () => { ready = false; });
  consumer.on(DISCONNECT, () => { ready = false; });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic: t, partition, message }) => handle(message, { topic: t, partition }),
  });
  logger.info({ topic, groupId }, 'consuming security alerts');

  return {
    consumer,
    isReady: () => ready && !stopped,
    stop: async () => {
      stopped = true;
      ready = false;
      await consumer.disconnect();
    },
  };
}
