import client from 'prom-client';
import { buildApp, isIsoTimestamp, isoTimestamp } from '@core/common';
import { TpcStateError } from '@core/tpc';
import { decodeCursor, InvalidCursorError } from './dynamo.js';
import { SERVICE_NAME } from './config.js';

const HOUR_MS = 60 * 60 * 1000;
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 500;
const MAX_ID_LENGTH = 512;

const badRequest = (reply, message) => reply.code(400).send({ error: 'bad_request', message });

// Parse and validate GET /events query params. Returns {query} or {error}.
function parseEventsQuery(q, now) {
  const { entityKey, from, to, limit, cursor } = q;
  if (typeof entityKey !== 'string' || entityKey === '') return { error: 'entityKey is required' };
  for (const [name, v] of [['from', from], ['to', to], ['limit', limit], ['cursor', cursor]]) {
    if (v !== undefined && typeof v !== 'string') return { error: `${name} must be given once` };
  }
  if (to !== undefined && !isIsoTimestamp(to)) return { error: 'to must be an ISO-8601 UTC timestamp' };
  if (from !== undefined && !isIsoTimestamp(from)) return { error: 'from must be an ISO-8601 UTC timestamp' };
  const toMs = to === undefined ? now() : Date.parse(to);
  const fromMs = from === undefined ? toMs - HOUR_MS : Date.parse(from);
  if (fromMs > toMs) return { error: 'from must not be after to' };
  let n = DEFAULT_LIMIT;
  if (limit !== undefined) {
    if (!/^\d+$/.test(limit)) return { error: `limit must be an integer 1..${MAX_LIMIT}` };
    n = Number(limit);
    if (n < 1 || n > MAX_LIMIT) return { error: `limit must be an integer 1..${MAX_LIMIT}` };
  }
  if (cursor !== undefined) {
    try {
      decodeCursor(cursor, entityKey);
    } catch {
      return { error: 'cursor is invalid' };
    }
  }
  return {
    query: { entityKey, from: isoTimestamp(fromMs), to: isoTimestamp(toMs), limit: n, cursor },
  };
}

export function registerRoutes(app, { repo, participant, now = Date.now }) {
  const tpcMessages = new client.Counter({
    name: 'tpc_participant_messages_total',
    help: '3PC participant messages handled, by phase and result',
    labelNames: ['phase', 'result'],
    registers: [app.metrics],
  });

  app.get('/events/:eventId', async (request, reply) => {
    const event = await repo.getEvent(request.params.eventId);
    if (!event) return reply.code(404).send({ error: 'not_found' });
    return event;
  });

  app.get('/events', async (request, reply) => {
    const { query, error } = parseEventsQuery(request.query ?? {}, now);
    if (error) return badRequest(reply, error);
    try {
      const { items, nextCursor } = await repo.queryEvents(query);
      return { items, nextCursor };
    } catch (err) {
      if (err instanceof InvalidCursorError) return badRequest(reply, 'cursor is invalid');
      throw err;
    }
  });

  app.get('/correlations/:correlationId', async (request, reply) => {
    const { correlationId } = request.params;
    const versions = await repo.getCorrelation(correlationId);
    if (versions.length === 0) return reply.code(404).send({ error: 'not_found' });
    return { correlationId, versions };
  });

  // 3PC participant routes. TpcStateError -> 409 {error:"bad_state", state}, where
  // state is this participant's current state for the txId (null when unknown).
  function tpcRoute(path, phase, handler) {
    app.post(`/tpc/:txId/${path}`, async (request, reply) => {
      const { txId } = request.params;
      if (!txId || txId.length > MAX_ID_LENGTH) return badRequest(reply, 'invalid txId');
      try {
        const { body, result } = await handler(txId, request);
        tpcMessages.inc({ phase, result });
        return body;
      } catch (err) {
        if (err instanceof TpcStateError) {
          tpcMessages.inc({ phase, result: 'bad_state' });
          request.log.warn({ txId, phase, state: err.state }, err.message);
          return reply.code(409).send({ error: 'bad_state', state: err.state });
        }
        tpcMessages.inc({ phase, result: 'error' });
        request.log.error({ err, txId, phase }, '3pc message failed');
        return reply.code(500).send({ error: 'internal', message: err.message });
      }
    });
  }

  const ack = { body: { ack: true }, result: 'ack' };

  tpcRoute('can-commit', 'can_commit', async (txId, request) => {
    const vote = await participant.canCommit(txId, request.body?.payload);
    request.log.info({ txId, vote }, '3pc canCommit');
    return { body: { vote }, result: vote };
  });
  tpcRoute('pre-commit', 'pre_commit', async (txId) => {
    await participant.preCommit(txId);
    return ack;
  });
  tpcRoute('do-commit', 'do_commit', async (txId, request) => {
    await participant.doCommit(txId);
    request.log.info({ txId }, '3pc doCommit');
    return ack;
  });
  tpcRoute('abort', 'abort', async (txId, request) => {
    await participant.abort(txId);
    request.log.info({ txId }, '3pc abort');
    return ack;
  });

  return app;
}

// Fastify app with /health, /ready, /metrics and the event-store routes.
export function buildServer({ repo, participant, logger, ready, now }) {
  const app = buildApp({
    service: SERVICE_NAME,
    ready,
    logger,
    // IDs (alert-derived correlation IDs, IPv6 entity keys) can exceed the default 100.
    fastifyOptions: { routerOptions: { maxParamLength: MAX_ID_LENGTH } },
  });
  return registerRoutes(app, { repo, participant, now });
}
