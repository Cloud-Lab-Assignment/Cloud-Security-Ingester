import Fastify from 'fastify';
import client from 'prom-client';
import { createLogger } from './logger.js';

// Base Fastify app shared by every service:
//   GET /health  -> {status:"ok", service}
//   GET /ready   -> 200 {status:"ready"} or 503 {status:"not_ready"}
//   GET /metrics -> Prometheus text format
// app.metrics is this app's own prom-client registry.
export function buildApp({ service, ready = async () => true, logger, fastifyOptions = {} }) {
  const loggerOpt = logger === false ? { logger: false } : { loggerInstance: logger ?? createLogger(service) };
  const app = Fastify({ ...loggerOpt, ...fastifyOptions });

  const registry = new client.Registry();
  registry.setDefaultLabels({ service });
  client.collectDefaultMetrics({ register: registry });

  const requests = new client.Counter({
    name: 'http_requests_total',
    help: 'HTTP requests handled, by route, method and status code',
    labelNames: ['route', 'method', 'status'],
    registers: [registry],
  });
  const duration = new client.Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request latency in seconds, by route and method',
    labelNames: ['route', 'method'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [registry],
  });

  app.decorate('metrics', registry);

  app.addHook('onRequest', async (request) => {
    request.startTime = process.hrtime.bigint();
  });
  app.addHook('onResponse', async (request, reply) => {
    const route = request.routeOptions?.url ?? 'unmatched';
    if (route === '/metrics') return;
    const seconds = Number(process.hrtime.bigint() - request.startTime) / 1e9;
    requests.inc({ route, method: request.method, status: String(reply.statusCode) });
    duration.observe({ route, method: request.method }, seconds);
  });

  app.get('/health', async () => ({ status: 'ok', service }));

  app.get('/ready', async (request, reply) => {
    let ok = false;
    try {
      ok = await ready();
    } catch (err) {
      request.log.warn({ err }, 'readiness check failed');
    }
    if (ok) return { status: 'ready' };
    return reply.code(503).send({ status: 'not_ready' });
  });

  app.get('/metrics', async (request, reply) => {
    reply.header('content-type', registry.contentType);
    return registry.metrics();
  });

  return app;
}
