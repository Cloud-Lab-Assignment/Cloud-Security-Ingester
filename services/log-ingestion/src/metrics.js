import client from 'prom-client';

// Same metric names and labels as the core services' shared buildApp()
// (packages/common/src/app.js) so one Grafana dashboard covers every service.
export function registerMetrics(app, service) {
  const registry = new client.Registry();
  registry.setDefaultLabels({ service });
  client.collectDefaultMetrics({ register: registry });

  const requests = new client.Counter({
    name: 'http_requests_total',
    help: 'HTTP requests handled, by route, method and status code',
    labelNames: ['route', 'method', 'status'],
    registers: [registry]
  });
  const duration = new client.Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request latency in seconds, by route and method',
    labelNames: ['route', 'method'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [registry]
  });

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

  app.get('/metrics', async (request, reply) => {
    reply.header('content-type', registry.contentType);
    return registry.metrics();
  });
}
