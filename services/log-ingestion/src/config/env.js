import 'dotenv/config';
import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test'])
    .default('development'),

  PORT: z.coerce.number().int().min(1).max(65535).default(3000),

  HOST: z.string().default('0.0.0.0'),

  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),

  KAFKA_CLIENT_ID: z.string()
    .default('log-ingestion-service'),

  TOPIC_RAW_EVENTS: z.string()
    .default('raw-events'),

  INGEST_MAX_PAYLOAD_BYTES: z.coerce.number()
    .int()
    .positive()
    .default(262144),

  KAFKA_PRODUCER_RETRIES: z.coerce.number()
    .int()
    .nonnegative()
    .default(5),

  SERVICE_VERSION: z.string()
    .default('1.0.0')
});

const result = envSchema.safeParse(process.env);

if (!result.success) {
  console.error('Invalid environment configuration:', result.error.format());
  process.exit(1);
}

export const env = {
  ...result.data,
  KAFKA_BROKERS: result.data.KAFKA_BROKERS
    .split(',')
    .map(broker => broker.trim())
};
