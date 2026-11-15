import { envStr, envInt } from '@core/common';

// Configuration from the environment, with local defaults.
// The KAFKA_* connection variables are read by createKafka (@core/common).
export function loadConfig() {
  return {
    port: envInt('PORT', 8083),
    kafkaGroupId: envStr('KAFKA_GROUP_ID', 'correlation-engine-cg'),
    alertTopic: envStr('ALERT_TOPIC', 'security-alerts'),
    incidentGrpcAddr: envStr('INCIDENT_GRPC_ADDR', 'localhost:50051'),
    eventStoreUrl: envStr('EVENT_STORE_URL', 'http://localhost:8081'),
    correlationWindowMs: envInt('CORRELATION_WINDOW_MS', 900000),
    escalateAlertCount: envInt('ESCALATE_ALERT_COUNT', 3),
    enrichTimeoutMs: envInt('ENRICH_TIMEOUT_MS', 2000),
    grpcDeadlineMs: envInt('GRPC_DEADLINE_MS', 15000),
    protoPath: envStr('PROTO_PATH', undefined),
  };
}
