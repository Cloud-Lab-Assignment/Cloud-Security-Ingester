import { SEVERITIES, STATUSES, VersionConflict, TransactionPending, IncidentNotFound } from './incidents.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// REST API (camelCase JSON):
//   GET   /incidents?status=&severity=&entityKey=&limit=   -> {items}
//   GET   /incidents/:id                                   -> incident | 404
//   GET   /incidents/:id/history                           -> {items} | 404
//   PATCH /incidents/:id {status, version, actor?, note?}  -> incident | 400 | 404 | 409
//   GET   /tpc/transactions?limit=&correlationId=          -> {items} (coordinator log, newest first)
//   GET   /tpc/transactions/:txId                          -> entry | 404 (decision lookup for participants)
// Status changes are plain local updates; only correlation decisions use 3PC.
// incident-updates messages go through the transactional outbox (outbox.js).
export function registerRoutes(app, { repo, coordinatorLog }) {
  app.setErrorHandler((err, request, reply) => {
    if (err.validation) return reply.code(400).send({ error: 'bad_request', message: err.message });
    request.log.error({ err }, 'request failed');
    return reply.code(err.statusCode && err.statusCode < 500 ? err.statusCode : 500).send({ error: 'internal' });
  });

  app.get(
    '/incidents',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            status: { type: 'string', enum: STATUSES },
            severity: { type: 'string', enum: SEVERITIES },
            entityKey: { type: 'string', minLength: 1 },
            limit: { type: 'integer', minimum: 1, maximum: 500, default: 50 },
          },
          additionalProperties: false,
        },
      },
    },
    async (request) => ({ items: await repo.listIncidents({ ...request.query }) }),
  );

  app.get('/incidents/:id', async (request, reply) => {
    const { id } = request.params;
    const incident = UUID_RE.test(id) ? await repo.getIncident(id) : null;
    if (!incident) return reply.code(404).send({ error: 'not_found' });
    return incident;
  });

  app.get('/incidents/:id/history', async (request, reply) => {
    const { id } = request.params;
    const incident = UUID_RE.test(id) ? await repo.getIncident(id) : null;
    if (!incident) return reply.code(404).send({ error: 'not_found' });
    return { items: await repo.getHistory(id) };
  });

  app.patch(
    '/incidents/:id',
    {
      schema: {
        body: {
          type: 'object',
          required: ['status', 'version'],
          properties: {
            status: { type: 'string', enum: STATUSES },
            version: { type: 'integer', minimum: 1 },
            actor: { type: 'string', minLength: 1, maxLength: 200 },
            note: { type: 'string', maxLength: 2000 },
          },
          additionalProperties: false,
        },
      },
    },
    async (request, reply) => {
      const { id } = request.params;
      if (!UUID_RE.test(id)) return reply.code(404).send({ error: 'not_found' });
      let incident;
      try {
        incident = await repo.updateStatus(id, { ...request.body });
      } catch (err) {
        if (err instanceof IncidentNotFound) return reply.code(404).send({ error: 'not_found' });
        if (err instanceof VersionConflict) {
          return reply.code(409).send({ error: 'version_conflict', currentVersion: err.currentVersion });
        }
        if (err instanceof TransactionPending) return reply.code(409).send({ error: 'transaction_pending' });
        throw err;
      }
      // incident.status_changed was queued in the outbox by updateStatus.
      return incident;
    },
  );

  app.get(
    '/tpc/transactions',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            limit: { type: 'integer', minimum: 1, maximum: 200, default: 20 },
            correlationId: { type: 'string', minLength: 1 },
          },
          additionalProperties: false,
        },
      },
    },
    async (request) => ({ items: await coordinatorLog.list({ ...request.query }) }),
  );

  // The coordinator's decision for one transaction. Event Store participants
  // ask this before acting on their own timeout (P0-4).
  app.get('/tpc/transactions/:txId', async (request, reply) => {
    const entry = await coordinatorLog.get(request.params.txId);
    if (!entry) return reply.code(404).send({ error: 'not_found' });
    return entry;
  });
}
