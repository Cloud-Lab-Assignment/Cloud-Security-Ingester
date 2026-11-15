import { randomUUID } from 'node:crypto';
import { env } from '../config/env.js';
import { eventSchema } from '../schemas/event.js';

export function buildEvent(request) {
  const event = {
    schemaVersion: '1.0',
    eventId: randomUUID(),
    receivedAt: new Date().toISOString(),
    eventTime: request.eventTime ?? null,
    source: {
      type: request.source.type,
      name: request.source.name,
      host: request.source.host ?? null,
      region: request.source.region ?? null,
      accountId: request.source.accountId ?? null
    },
    format: request.format,
    payload: request.payload,
    ingestedBy: {
      name: env.KAFKA_CLIENT_ID,
      version: env.SERVICE_VERSION
    }
  };

  const result = eventSchema.safeParse(event);

  if (!result.success) {
    throw new Error(`Generated event is invalid: ${result.error.message}`);
  }

  const message = JSON.stringify(result.data);
  const size = Buffer.byteLength(message, 'utf8');

  if (size > env.INGEST_MAX_PAYLOAD_BYTES) {
    const error = new Error('Event exceeds the maximum message size');
    error.statusCode = 413;
    throw error;
  }

  return result.data;
}
