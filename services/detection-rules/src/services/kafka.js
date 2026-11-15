import { Kafka } from 'kafkajs';
import { config } from '../config/env.js';

const kafkaConfig = {
  clientId: config.serviceName,
  brokers: config.brokers,
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

const producerKafka = new Kafka({
  ...kafkaConfig,
  clientId: `${config.serviceName}-producer`,
  retry: {
    ...kafkaConfig.retry,
    retries: Number.MAX_SAFE_INTEGER
  }
});

export const consumer = kafka.consumer({
  groupId: config.groupId
});

export const producer = producerKafka.producer({
  idempotent: true,
  maxInFlightRequests: 5
});

export const admin = kafka.admin();
