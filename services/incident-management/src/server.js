import client from 'prom-client';
import { buildApp, createKafka, createLogger, createProducer } from '@core/common';
import { loadConfig } from './config.js';
import { createPool } from './db.js';
import { migrate } from './migrate.js';
import { createIncidentRepo } from './incidents.js';
import { createPublisher } from './publisher.js';
import {
  createEventStoreParticipant,
  createIncidentService,
  createPgCoordinatorLog,
  createTpcMetrics,
} from './tpc-wiring.js';
import { startGrpc } from './grpc.js';
import { registerRoutes } from './routes.js';

const SERVICE = 'incident-management';
const SWEEP_INTERVAL_MS = 2000;

async function main() {
  const config = loadConfig();
  const logger = createLogger(SERVICE);
  if (config.tpcFault) logger.warn({ tpcFault: config.tpcFault }, '3PC fault injection is ENABLED');

  const pool = createPool(config);
  pool.on('error', (err) => logger.error({ err }, 'idle postgres client error'));
  await migrate(pool, { logger });

  const kafka = createKafka({ clientId: SERVICE, logger });
  const producer = createProducer(kafka);
  let producerConnected = false;
  producer.on(producer.events.CONNECT, () => {
    producerConnected = true;
  });
  producer.on(producer.events.DISCONNECT, () => {
    producerConnected = false;
  });
  await producer.connect();

  const app = buildApp({
    service: SERVICE,
    logger,
    ready: async () => {
      if (!producerConnected) return false;
      await pool.query('SELECT 1');
      return true;
    },
  });

  const publisher = createPublisher({ producer, topic: config.incidentTopic });
  const repo = createIncidentRepo({ pool, correlationWindowMs: config.correlationWindowMs });
  const coordinatorLog = createPgCoordinatorLog(pool);
  const eventStore = createEventStoreParticipant({ baseUrl: config.eventStoreUrl, timeoutMs: config.tpcTimeoutMs });
  const service = createIncidentService({
    pool,
    repo,
    eventStore,
    publisher,
    config,
    logger,
    metrics: createTpcMetrics(app.metrics),
    // The injected crash stands in for a real process failure: exit, and the
    // restarted process finishes the transaction in recover().
    onCrash: async () => {
      logger.flush?.();
      process.exit(1);
    },
  });

  // Finish anything a previous run left in flight before taking new work.
  const recovered = await service.recover();
  logger.info({ recovered }, '3PC recovery complete');

  const grpcMetrics = {
    requests: new client.Counter({
      name: 'grpc_requests_total',
      help: 'gRPC calls handled, by method and status code',
      labelNames: ['method', 'code'],
      registers: [app.metrics],
    }),
    duration: new client.Histogram({
      name: 'grpc_request_duration_seconds',
      help: 'gRPC call latency in seconds, by method',
      labelNames: ['method'],
      buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      registers: [app.metrics],
    }),
  };
  const { server: grpcServer, port: grpcPort } = await startGrpc({
    port: config.grpcPort,
    upsertIncident: service.upsertIncident,
    getIncident: (id) => repo.getIncident(id),
    logger,
    metrics: grpcMetrics,
  });
  logger.info({ port: grpcPort }, 'gRPC listening');

  registerRoutes(app, { repo, publisher, coordinatorLog, logger });
  await app.listen({ host: '0.0.0.0', port: config.port });

  // Participant-side timeouts for the local incident-db participant.
  let sweeping = false;
  const sweeper = setInterval(async () => {
    if (sweeping) return;
    sweeping = true;
    try {
      const { committed, aborted } = await service.participant.sweep();
      if (committed.length || aborted.length) logger.warn({ committed, aborted }, 'incident-db participant timed out transactions');
    } catch (err) {
      logger.error({ err }, 'participant sweep failed');
    } finally {
      sweeping = false;
    }
  }, SWEEP_INTERVAL_MS);

  let stopping = false;
  const shutdown = async (signal) => {
    if (stopping) return;
    stopping = true;
    logger.info({ signal }, 'shutting down');
    clearInterval(sweeper);
    await new Promise((resolve) => grpcServer.tryShutdown(() => resolve()));
    await app.close();
    await producer.disconnect().catch(() => {});
    await pool.end().catch(() => {});
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(JSON.stringify({ level: 'fatal', service: SERVICE, msg: 'startup failed', err: { message: err.message, stack: err.stack } }));
  process.exit(1);
});
