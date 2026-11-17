import client from 'prom-client';
import { isIsoTimestamp } from '@core/common';
import { rawKey } from './s3-archive.js';

const nonEmpty = (v) => typeof v === 'string' && v.length > 0;
const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

// What the store needs from a normalized event to key it.
export function isStorableEvent(e) {
  return isObject(e) && nonEmpty(e.eventId) && nonEmpty(e.entityKey) && isIsoTimestamp(e.eventTime);
}

export function isArchivableRaw(e) {
  if (!isObject(e)) return false;
  try {
    rawKey(e);
    return true;
  } catch {
    return false;
  }
}

function parse(message) {
  if (!message.value) return undefined;
  try {
    return JSON.parse(message.value.toString('utf8'));
  } catch {
    return undefined;
  }
}

// One consumer group on both topics. raw-events -> archiveRaw (S3),
// normalized-events -> putEvent (DynamoDB). Invalid messages are logged and
// skipped; storage errors are thrown so kafkajs retries the message.
export async function startConsumers({
  kafka,
  logger,
  groupId,
  rawTopic,
  normalizedTopic,
  archiveRaw,
  putEvent,
  registry = new client.Registry(),
}) {
  const consumed = new client.Counter({
    name: 'events_consumed_total',
    help: 'Kafka messages consumed by the event store, by topic and result',
    labelNames: ['topic', 'result'],
    registers: [registry],
  });

  // Heartbeat after each storage call so a slow (but deadline-bounded) call
  // does not eat into the next message's session budget. A failed heartbeat
  // (e.g. a rebalance) must not fail a message that is already stored;
  // kafkajs notices the rebalance on its own.
  async function beat(heartbeat) {
    if (!heartbeat) return;
    try {
      await heartbeat();
    } catch (err) {
      logger.debug({ err: err?.message }, 'kafka heartbeat failed');
    }
  }

  async function handle({ topic, partition, message, heartbeat }) {
    const where = { topic, partition, offset: message.offset };
    const event = parse(message);
    const isRaw = topic === rawTopic;
    const valid = isRaw ? isArchivableRaw(event) : isStorableEvent(event);
    if (!valid) {
      logger.warn(where, event === undefined ? 'skipping message that is not JSON' : 'skipping invalid event');
      consumed.inc({ topic, result: 'invalid' });
      return;
    }
    try {
      if (isRaw) {
        const key = await archiveRaw(event);
        await beat(heartbeat);
        consumed.inc({ topic, result: 'archived' });
        logger.debug({ ...where, eventId: event.eventId, key }, 'archived raw event');
      } else {
        const result = await putEvent(event);
        await beat(heartbeat);
        consumed.inc({ topic, result });
        logger.debug({ ...where, eventId: event.eventId, result }, 'stored normalized event');
      }
    } catch (err) {
      // DynamoDB rejects the item itself (e.g. over 400 KB): retrying can never succeed.
      if (err?.name === 'ValidationException') {
        logger.error({ ...where, err, eventId: event.eventId }, 'event rejected by storage; skipping');
        consumed.inc({ topic, result: 'invalid' });
        return;
      }
      consumed.inc({ topic, result: 'error' });
      logger.error({ ...where, err, eventId: event.eventId }, 'storage failed; message will be retried');
      throw err;
    }
  }

  const consumer = kafka.consumer({ groupId });
  let ready = false;
  const { GROUP_JOIN, CRASH, DISCONNECT, STOP } = consumer.events;
  consumer.on(GROUP_JOIN, () => {
    ready = true;
  });
  consumer.on(CRASH, (e) => {
    ready = false;
    logger.error({ err: e?.payload?.error, restart: e?.payload?.restart }, 'kafka consumer crashed');
  });
  consumer.on(DISCONNECT, () => {
    ready = false;
  });
  consumer.on(STOP, () => {
    ready = false;
  });

  await consumer.connect();
  await consumer.subscribe({ topics: [rawTopic, normalizedTopic], fromBeginning: true });
  await consumer.run({ eachMessage: handle });

  let stopped = false;
  return {
    isReady: () => ready && !stopped,
    async stop() {
      stopped = true;
      ready = false;
      await consumer.disconnect();
    },
  };
}
