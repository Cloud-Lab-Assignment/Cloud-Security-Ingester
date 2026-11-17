import { buildApp, createLogger, envInt, envStr } from '@core/common';
import { registerRoutes } from './routes.js';
import { createUpstream } from './upstream.js';

const SERVICE = 'investigation-query-api';

const port = envInt('PORT', 8084);
const eventStoreUrl = envStr('EVENT_STORE_URL', 'http://localhost:8081');
const incidentsUrl = envStr('INCIDENT_MANAGEMENT_URL', 'http://localhost:8082');
const timeoutMs = envInt('UPSTREAM_TIMEOUT_MS', 3000);
const cacheTtlMs = envInt('CACHE_TTL_MS', 5000);
const defaultWindowMs = envInt('CORRELATION_WINDOW_MS', 900_000);

const logger = createLogger(SERVICE);
const eventStore = createUpstream({ baseUrl: eventStoreUrl, name: 'event-store', timeoutMs, cacheTtlMs });
const incidents = createUpstream({ baseUrl: incidentsUrl, name: 'incident-management', timeoutMs, cacheTtlMs });

const app = buildApp({
  service: SERVICE,
  logger,
  // Ready when both read sources answer their liveness endpoint.
  ready: async () => {
    const probe = (base) => fetch(`${base.replace(/\/+$/, '')}/health`, { signal: AbortSignal.timeout(1500) }).then((r) => r.ok);
    const results = await Promise.allSettled([probe(eventStoreUrl), probe(incidentsUrl)]);
    return results.every((r) => r.status === 'fulfilled' && r.value);
  },
});

registerRoutes(app, { eventStore, incidents, defaultWindowMs });

async function shutdown(signal) {
  logger.info({ signal }, 'shutting down');
  try {
    await app.close();
  } finally {
    process.exit(0);
  }
}
process.once('SIGTERM', () => shutdown('SIGTERM'));
process.once('SIGINT', () => shutdown('SIGINT'));

try {
  await app.listen({ host: '0.0.0.0', port });
  logger.info({ port, eventStoreUrl, incidentsUrl }, `${SERVICE} started`);
} catch (err) {
  logger.fatal({ err }, 'startup failed');
  process.exit(1);
}
