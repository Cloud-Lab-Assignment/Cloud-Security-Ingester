import { createKafka, createLogger } from '@core/common';
import { loadConfig, SERVICE_NAME } from './config.js';
import { createDynamoClients, createDynamoRepo } from './dynamo.js';
import { createS3Client, createS3Archive } from './s3-archive.js';
import { ensureLocalResources } from './local-setup.js';
import { startConsumers } from './consumers.js';
import { createCorrelationParticipant } from './participant.js';
import { createCoordinatorClient } from './coordinator-client.js';
import { buildServer } from './routes.js';

const SWEEP_INTERVAL_MS = 2000;

const logger = createLogger(SERVICE_NAME);

async function main() {
  const config = loadConfig();
  logger.info({ config: { ...config } }, 'starting event-store');

  const deadlines = { requestTimeoutMs: config.awsRequestTimeoutMs, maxAttempts: config.awsMaxAttempts };
  const { ddb, doc } = createDynamoClients({ region: config.awsRegion, endpoint: config.dynamodbEndpoint, ...deadlines });
  const s3 = createS3Client({ region: config.awsRegion, endpoint: config.s3Endpoint, ...deadlines });
  const repo = createDynamoRepo({ doc, tablePrefix: config.tablePrefix });
  const archive = createS3Archive({ s3, bucket: config.rawBucket });
  const participant = createCorrelationParticipant({
    store: repo.tpcStore,
    putCorrelation: repo.putCorrelation,
    timeoutMs: config.tpcTimeoutMs,
    forceVote: config.tpcForceVote,
    resolve: createCoordinatorClient({
      url: config.coordinatorUrl,
      timeoutMs: config.coordinatorTimeoutMs,
      logger,
    }),
    resolveTimeoutMs: config.coordinatorTimeoutMs + 1000,
    logger,
  });

  let consumers = null;
  const app = buildServer({
    repo,
    participant,
    logger,
    ready: async () => Boolean(consumers?.isReady()) && (await repo.ping()),
  });

  await ensureLocalResources({ config, ddb, s3, logger });

  consumers = await startConsumers({
    kafka: createKafka({ clientId: SERVICE_NAME, logger }),
    logger,
    groupId: config.kafkaGroupId,
    rawTopic: config.rawTopic,
    normalizedTopic: config.normalizedTopic,
    archiveRaw: archive.archiveRaw,
    putEvent: repo.putEvent,
    registry: app.metrics,
  });

  await app.listen({ host: '0.0.0.0', port: config.port });

  // Participant-side 3PC timeouts. Each timed-out record first asks the
  // coordinator (COORDINATOR_URL) for its decision; only when it is
  // unreachable (or does not answer in time) does the timeout rule apply:
  // UNCERTAIN -> ABORTED, PRECOMMITTED -> COMMITTED. A coordinator that never
  // started the tx (404) aborts UNCERTAIN and leaves PRECOMMITTED pending; an
  // error answer leaves both pending. The first sweep after a restart works the same way.
  let sweeping = false;
  const sweepTimer = setInterval(async () => {
    if (sweeping) return;
    sweeping = true;
    try {
      const { committed, aborted, pending } = await participant.sweep();
      if (committed.length || aborted.length) logger.info({ committed, aborted }, '3pc sweep resolved transactions');
      if (pending.length) logger.debug({ pending }, '3pc sweep: waiting for the coordinator');
    } catch (err) {
      logger.error({ err }, '3pc sweep failed');
    } finally {
      sweeping = false;
    }
  }, SWEEP_INTERVAL_MS);

  let shuttingDown = false;
  async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutting down');
    clearInterval(sweepTimer);
    const results = await Promise.allSettled([consumers.stop(), app.close()]);
    for (const r of results) if (r.status === 'rejected') logger.error({ err: r.reason }, 'shutdown step failed');
    ddb.destroy();
    s3.destroy();
    process.exit(0);
  }
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  logger.fatal({ err }, 'event-store failed to start');
  process.exit(1);
});
