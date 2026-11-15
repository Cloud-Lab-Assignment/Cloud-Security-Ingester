import 'dotenv/config';

export const config = {
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean),
  groupId: process.env.KAFKA_GROUP_ID ?? 'detection-rules-cg',
  inputTopic: process.env.INPUT_TOPIC ?? 'normalized-events',
  alertTopic: process.env.ALERT_TOPIC ?? 'security-alerts',
  dlqTopic: process.env.DLQ_TOPIC ?? 'normalized-events-dlq',
  serviceName: 'detection-rules-service',
  serviceVersion: '1.0.0'
};
