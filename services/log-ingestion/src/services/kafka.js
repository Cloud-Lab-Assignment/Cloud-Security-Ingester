import { Kafka, logLevel } from 'kafkajs';
import { env } from '../config/env.js';

const kafka = new Kafka({
  clientId: env.KAFKA_CLIENT_ID,
  brokers: env.KAFKA_BROKERS,
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
