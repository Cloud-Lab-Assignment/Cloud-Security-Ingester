import { Kafka } from 'kafkajs';

const brokers = (process.env.KAFKA_BROKERS ?? '')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);

if (brokers.length === 0) {
  throw new Error('KAFKA_BROKERS environment variable is required');
}

const kafkaConfig = {
  clientId: 'event-normalizer',
  brokers,
  ssl: process.env.KAFKA_SSL === 'true',
  retry: {
    retries: 5,
    initialRetryTime: 300,
    multiplier: 2,
    factor: 0,
    maxRetryTime: 30000
  }
};

if (process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD) {
  kafkaConfig.sasl = {
    mechanism: process.env.KAFKA_SASL_MECHANISM ?? 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD
  };
}

const kafka = new Kafka(kafkaConfig);

export const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5
});

let connected = false;

export async function connectProducer() {
  if (!connected) {
    await producer.connect();
    connected = true;
  }
}

export async function disconnectProducer() {
  if (connected) {
    await producer.disconnect();
    connected = false;
  }
}
