import { buildApp, createKafka, createLogger } from '@core/common';
import { loadConfig } from './config.js';
import { createCorrelator } from './correlator.js';
import { createEventStoreClient } from './event-store-client.js';
import { createIncidentClient } from './incident-client.js';
import { createMetrics, handleAlert, startConsumer } from './consumer.js';

const SERVICE = 'correlation-engine';

const config = loadConfig();
const logger = createLogger(SERVICE);

let consumerHandle = null;
const app = buildApp({
  service: SERVICE,
  logger,
  ready: async () => consumerHandle?.isReady() ?? false,
});

const metrics = createMetrics(app.metrics);
const correlator = createCorrelator({ windowMs: config.correlationWindowMs, escalateCount: config.escalateAlertCount });
const eventStore = createEventStoreClient({ baseUrl: config.eventStoreUrl, timeoutMs: config.enrichTimeoutMs, logger });
const incidents = createIncidentClient({
  addr: config.incidentGrpcAddr,
  deadlineMs: config.grpcDeadlineMs,
  ...(config.protoPath ? { protoPath: config.protoPath } : {}),
});

const deps = {
  correlator,
  fetchEnrichment: eventStore.fetchEnrichment,
  upsertIncident: incidents.upsertIncident,
  logger,
  metrics,
};

let shuttingDown = false;
async function shutdown(signal, exitCode = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info({ signal }, 'shutting down');
  try {
    await consumerHandle?.stop();
  } catch (err) {
    logger.warn({ err }, 'error disconnecting kafka consumer');
  }
  incidents.close();
  try {
    await app.close();
  } catch (err) {
    logger.warn({ err }, 'error closing http server');
  }
  process.exit(exitCode);
}

process.once('SIGTERM', () => shutdown('SIGTERM'));
process.once('SIGINT', () => shutdown('SIGINT'));

try {
  await app.listen({ host: '0.0.0.0', port: config.port });
  const kafka = createKafka({ clientId: SERVICE, logger });
  consumerHandle = await startConsumer({
    kafka,
    logger,
    topic: config.alertTopic,
    groupId: config.kafkaGroupId,
    handle: (message, where) => handleAlert(message, deps, where),
  });
  logger.info(
    {
      port: config.port,
      incidentGrpcAddr: config.incidentGrpcAddr,
      eventStoreUrl: config.eventStoreUrl,
      windowMs: config.correlationWindowMs,
    },
    'correlation-engine started',
  );
} catch (err) {
  logger.fatal({ err }, 'startup failed');
  await shutdown('startup-failure', 1);
}
