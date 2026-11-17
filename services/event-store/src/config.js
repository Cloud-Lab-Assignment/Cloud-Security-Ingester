import { envStr, envInt } from '@core/common';

export const SERVICE_NAME = 'event-store';

// Environment with defaults. DYNAMODB_ENDPOINT / S3_ENDPOINT are unset in AWS
// and point at local emulators in development.
export function loadConfig() {
  const forceVote = envStr('TPC_FORCE_VOTE', undefined);
  if (forceVote !== undefined && forceVote !== 'yes' && forceVote !== 'no') {
    throw new Error(`env TPC_FORCE_VOTE must be "yes" or "no", got "${forceVote}"`);
  }
  return {
    port: envInt('PORT', 8081),
    kafkaGroupId: envStr('KAFKA_GROUP_ID', 'event-store-cg'),
    rawTopic: envStr('RAW_TOPIC', 'raw-events'),
    normalizedTopic: envStr('NORMALIZED_TOPIC', 'normalized-events'),
    awsRegion: envStr('AWS_REGION', 'ap-south-1'),
    dynamodbEndpoint: envStr('DYNAMODB_ENDPOINT', undefined),
    s3Endpoint: envStr('S3_ENDPOINT', undefined),
    rawBucket: envStr('RAW_BUCKET', 'cs-raw-events'),
    tablePrefix: envStr('TABLE_PREFIX', 'cs-'),
    // Per-attempt deadline and attempts for every DynamoDB/S3 call; keep
    // attempts x (timeout + 1 s connect) well below the 30 s Kafka session timeout.
    awsRequestTimeoutMs: envInt('AWS_REQUEST_TIMEOUT_MS', 5000),
    awsMaxAttempts: envInt('AWS_MAX_ATTEMPTS', 3),
    tpcTimeoutMs: envInt('TPC_TIMEOUT_MS', 10_000),
    tpcForceVote: forceVote,
    // 3PC coordinator (incident-management) asked for its decision before a
    // timed-out transaction is resolved locally. Empty string disables it.
    coordinatorUrl: process.env.COORDINATOR_URL ?? 'http://localhost:8082',
    coordinatorTimeoutMs: envInt('COORDINATOR_TIMEOUT_MS', 2000),
  };
}
