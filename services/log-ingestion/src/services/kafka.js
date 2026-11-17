import { Kafka, logLevel } from 'kafkajs';
import { env } from '../config/env.js';

// Same connection variables as the other services: KAFKA_SSL ("true" enables TLS),
// KAFKA_USERNAME / KAFKA_PASSWORD (SASL, e.g. a Confluent Cloud API key),
// KAFKA_SASL_MECHANISM (default "plain").
const sasl = process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD
  ? {
      mechanism: (process.env.KAFKA_SASL_MECHANISM ?? 'plain').toLowerCase(),
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD
    }
  : undefined;

const kafka = new Kafka({
  clientId: env.KAFKA_CLIENT_ID,
  brokers: env.KAFKA_BROKERS,
  ssl: process.env.KAFKA_SSL === 'true',
  ...(sasl ? { sasl } : {}),
  logLevel: logLevel.ERROR,
  retry: {
    retries: env.KAFKA_PRODUCER_RETRIES,
    initialRetryTime: 300,
    multiplier: 2,
    maxRetryTime: 30000
  }
});

const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
  allowAutoTopicCreation: false
});

const admin = kafka.admin();

let connected = false;

export async function connectKafka() {
  if (connected) return;

  await producer.connect();
  connected = true;

  console.log('Kafka producer connected');
}

export async function checkKafka() {
  await admin.connect();

  try {
    await admin.describeCluster();
    return true;
  } finally {
    await admin.disconnect();
  }
}

export async function publishEvent(event) {
  if (!connected) {
    throw new Error('Kafka producer is not connected');
  }

  await producer.send({
    topic: env.TOPIC_RAW_EVENTS,
    acks: -1,
    messages: [
      {
        key: event.source.name,
        value: JSON.stringify(event)
      }
    ]
  });
}

export async function disconnectKafka() {
  if (connected) {
    await producer.disconnect();
    connected = false;
    console.log('Kafka producer disconnected');
  }
}
