import { UpstreamError } from './upstream.js';

// Investigation Query API: the single read-only entry point for analysts.
//   GET /incidents?status=&severity=&entityKey=&limit=   -> {items}
//   GET /incidents/:id                                   -> incident + history + correlation history + events in its window
//   GET /events?entityKey=&from=&to=&limit=&cursor=      -> {items, nextCursor}
//   GET /events/:eventId                                 -> event
//   GET /entities/:entityKey/timeline?from=&to=&limit=   -> events + incidents for one entity
// Everything is a GET: this service never writes and is not part of 3PC.
export function registerRoutes(app, { eventStore, incidents, defaultWindowMs }) {
  app.setErrorHandler((err, request, reply) => {
    if (err instanceof UpstreamError) {
      request.log.warn({ upstream: err.upstream, status: err.status }, err.message);
      return reply.code(err.status).send({ error: 'upstream_error', upstream: err.upstream, message: err.message });
    }
    if (err.validation) return reply.code(400).send({ error: 'bad_request', message: err.message });
    request.log.error({ err }, 'request failed');
    return reply.code(500).send({ error: 'internal' });
  });

  const limit = { type: 'integer', minimum: 1, maximum: 500, default: 50 };

  app.get(
    '/incidents',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            status: { type: 'string' },
            severity: { type: 'string' },
            entityKey: { type: 'string', minLength: 1 },
            limit,
          },
          additionalProperties: false,
        },
      },
    },
    async (request) => (await incidents.get('/incidents', request.query)) ?? { items: [] },
  );

  app.get('/incidents/:id', async (request, reply) => {
    const { id } = request.params;
    const incident = await incidents.get(`/incidents/${encodeURIComponent(id)}`);
    if (!incident) return reply.code(404).send({ error: 'not_found' });

    // Fan out in parallel; a failing enrichment degrades to null, never to a 5xx.
    const settle = (p) => p.catch(() => null);
    const from = new Date(Date.parse(incident.firstSeen) - defaultWindowMs).toISOString();
    const to = new Date(Date.parse(incident.lastSeen) + defaultWindowMs).toISOString();

    const [history, correlation, events] = await Promise.all([
      settle(incidents.get(`/incidents/${encodeURIComponent(id)}/history`)),
      incident.correlationId
        ? settle(eventStore.get(`/correlations/${encodeURIComponent(incident.correlationId)}`))
        : null,
      settle(eventStore.get('/events', { entityKey: incident.entityKey, from, to, limit: 100 })),
    ]);

    return {
      incident,
      history: history?.items ?? null,
      correlationHistory: correlation ?? null,
      events: events?.items ?? null,
      eventsNextCursor: events?.nextCursor ?? null,
    };
  });

  app.get(
    '/events',
    {
      schema: {
        querystring: {
          type: 'object',
          required: ['entityKey'],
          properties: {
            entityKey: { type: 'string', minLength: 1 },
            from: { type: 'string' },
            to: { type: 'string' },
            limit,
            cursor: { type: 'string' },
          },
          additionalProperties: false,
        },
      },
    },
    async (request) => (await eventStore.get('/events', request.query)) ?? { items: [] },
  );

  app.get('/events/:eventId', async (request, reply) => {
    const event = await eventStore.get(`/events/${encodeURIComponent(request.params.eventId)}`);
    if (!event) return reply.code(404).send({ error: 'not_found' });
    return event;
  });

  app.get(
    '/entities/:entityKey/timeline',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: { from: { type: 'string' }, to: { type: 'string' }, limit },
          additionalProperties: false,
        },
      },
    },
    async (request) => {
      const { entityKey } = request.params;
      const { from, to, limit: lim } = request.query;
      const [events, incs] = await Promise.all([
        eventStore.get('/events', { entityKey, from, to, limit: lim }),
        incidents.get('/incidents', { entityKey, limit: 50 }),
      ]);
      return {
        entityKey,
        events: events?.items ?? [],
        eventsNextCursor: events?.nextCursor ?? null,
        incidents: incs?.items ?? [],
      };
    },
  );
}
