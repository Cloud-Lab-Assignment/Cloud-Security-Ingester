export { buildApp } from './app.js';
export { createKafka, createProducer, sendJson, kafkaConfigFromEnv } from './kafka.js';
export { createLogger } from './logger.js';
export { isoTimestamp, isIsoTimestamp } from './time.js';
export { envStr, envInt, envBool } from './env.js';

export const SERVICE_VERSION = '1.0.0';
