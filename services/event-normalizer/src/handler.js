import { normalizeEvent } from './services/normalizer.js';
import {
  producer,
  connectProducer,
  disconnectProducer
} from './services/kafka.js';

const NORMALIZED_TOPIC = 'normalized-events';
const DLQ_TOPIC = 'raw-events-dlq';
const MAX_EVENT_SIZE = 262144;
const SUPPORTED_SCHEMA_MAJOR = '1';

export function createHandler({
  normalize = normalizeEvent,
  kafkaProducer = producer,
  connect = connectProducer,
  disconnect = disconnectProducer
} = {}) {
  return async function handler(event) {
    await connect();

    try {
      const records = event.records ?? {};
      let processed = 0;
      let failed = 0;

      for (const [topicPartition, messages] of Object.entries(records)) {
        const separator = topicPartition.lastIndexOf('-');
        const topic = topicPartition.slice(0, separator);
        const partition = Number(topicPartition.slice(separator + 1));

        for (const record of messages) {
          const origin = {
            topic,
            partition,
            offset: Number(record.offset)
          };

          let rawText;
          let normalizedEvent;
          let validationError = null;

          try {
            rawText = Buffer.from(record.value, 'base64').toString('utf8');

            if (Buffer.byteLength(rawText, 'utf8') > MAX_EVENT_SIZE) {
              throw new Error('Event exceeds 256 KiB');
            }

            const rawEvent = JSON.parse(rawText);
            const schemaMajor = rawEvent.schemaVersion?.split('.')[0];

            if (schemaMajor !== SUPPORTED_SCHEMA_MAJOR) {
              const error = new Error('Unsupported schema major version');
              error.code = 'UNSUPPORTED_SCHEMA_VERSION';
              throw error;
            }

            normalizedEvent = normalize(rawEvent, origin);
          } catch (error) {
            validationError = error;
          }

          if (validationError) {
            await kafkaProducer.send({
              topic: DLQ_TOPIC,
              acks: -1,
              messages: [{
                key: `${topic}-${partition}-${record.offset}`,
                value: JSON.stringify({
                  errorCode: validationError.code ?? 'SCHEMA_VALIDATION_FAILED',
                  errorMessage: validationError.message,
                  originalMessage: rawText ?? record.value,
                  origin
                })
              }]
            });

            failed++;
            continue;
          }

          await kafkaProducer.send({
            topic: NORMALIZED_TOPIC,
            acks: -1,
            messages: [{
              key: normalizedEvent.entityKey,
              value: JSON.stringify(normalizedEvent)
            }]
          });

          processed++;
        }
      }

      return { processed, failed };
    } finally {
      await disconnect();
    }
  };
}

export const handler = createHandler();
