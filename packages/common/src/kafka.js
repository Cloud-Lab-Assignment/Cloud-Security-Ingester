import { Kafka, logLevel } from 'kafkajs';

// Connection settings match the group repo exactly:
// KAFKA_BROKERS (comma-separated), KAFKA_SSL ("true" enables TLS),
// KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_SASL_MECHANISM (default "plain").
export function kafkaConfigFromEnv(env = process.env) {
  const brokers = (env.KAFKA_BROKERS ?? 'localhost:29092')
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean);
  const config = { brokers, ssl: env.KAFKA_SSL === 'true' };
  if (env.KAFKA_USERNAME) {
    config.sasl = {
      mechanism: (env.KAFKA_SASL_MECHANISM || 'plain').toLowerCase(),
      username: env.KAFKA_USERNAME,
      password: env.KAFKA_PASSWORD ?? '',
    };
  }
  return config;
}

const PINO_LEVEL = {
  [logLevel.ERROR]: 'error',
  [logLevel.NOTHING]: 'error',
  [logLevel.WARN]: 'warn',
  [logLevel.INFO]: 'info',
  [logLevel.DEBUG]: 'debug',
};

// Route kafkajs logs through pino so every line is JSON on stdout.
function pinoLogCreator(logger) {
  return () => ({ level, log }) => {
    const { message, ...extra } = log;
    logger[PINO_LEVEL[level] ?? 'info']({ kafka: extra }, message);
  };
}

export function createKafka({ clientId, logger, env = process.env }) {
  return new Kafka({
    clientId,
    ...kafkaConfigFromEnv(env),
    logLevel: logLevel.WARN,
    ...(logger ? { logCreator: pinoLogCreator(logger) } : {}),
  });
}

// Producers follow the group convention: idempotent, acks=-1, no auto-created topics.
export function createProducer(kafka) {
  return kafka.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false });
}

export async function sendJson(producer, topic, key, value) {
  return producer.send({
    topic,
    acks: -1,
    messages: [{ key, value: JSON.stringify(value) }],
  });
}
