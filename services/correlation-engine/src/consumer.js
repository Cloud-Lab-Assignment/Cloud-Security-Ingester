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

function parseAlert(message) {
  const text = message?.value == null ? '' : message.value.toString('utf8');
  let alert;
  try {
    alert = JSON.parse(text);
  } catch {
    return { error: 'invalid JSON' };
  }
  if (!alert || typeof alert !== 'object' || Array.isArray(alert)) return { error: 'not a JSON object' };
  for (const field of ['alertId', 'entityKey', 'alertTime']) {
    if (typeof alert[field] !== 'string' || alert[field] === '') return { error: `missing ${field}` };
  }
  if (Number.isNaN(Date.parse(alert.alertTime))) return { error: 'invalid alertTime' };
  return { alert };
}

// Handle one security-alerts message.
// deps: { correlator, fetchEnrichment, upsertIncident, logger, metrics }
// Throws only when the upsert failed with a retryable gRPC error, after
// forgetting the alert, so the kafkajs retry re-ingests and re-sends it.
export async function handleAlert(message, deps) {
  const { correlator, fetchEnrichment, upsertIncident, logger, metrics } = deps;
  const count = (result) => metrics?.alertsConsumed.inc({ result });

  const { alert, error } = parseAlert(message);
  if (error) {
    logger.warn({ reason: error, offset: message?.offset }, 'skipping invalid security alert');
    count('invalid');
    return;
  }

  const candidate = correlator.ingest(alert);
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
export async function startConsumer({ kafka, logger, topic, groupId, handle }) {
  const consumer = kafka.consumer({ groupId });
  let connected = false;
  consumer.on(consumer.events.CONNECT, () => { connected = true; });
  consumer.on(consumer.events.DISCONNECT, () => { connected = false; });
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    connected = false;
    logger.error({ err: payload?.error, restart: payload?.restart }, 'kafka consumer crashed');
  });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message, partition }) => handle(message, { partition }),
  });
  logger.info({ topic, groupId }, 'consuming security alerts');

  return {
    consumer,
    isConnected: () => connected,
    stop: () => consumer.disconnect(),
  };
}
