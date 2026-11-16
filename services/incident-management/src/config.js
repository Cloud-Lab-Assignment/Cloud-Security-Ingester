const LOCAL_DATABASE_URL = 'postgres://incidents:incidents@localhost:5432/incidents';

export function loadConfig(env = process.env) {
  const str = (name, fallback) => (env[name] === undefined || env[name] === '' ? fallback : env[name]);
  const int = (name, fallback) => {
    const v = str(name, undefined);
    if (v === undefined) return fallback;
    const n = Number.parseInt(v, 10);
    if (!Number.isFinite(n)) throw new Error(`env ${name} must be an integer, got "${v}"`);
    return n;
  };
  return {
    // In AWS, DATABASE_URL stays unset and pg reads PGHOST/PGPORT/PGDATABASE/
    // PGUSER/PGPASSWORD (from the RDS master secret) natively. The local
    // default applies only when neither is provided.
    databaseUrl: str('DATABASE_URL', env.PGHOST ? undefined : LOCAL_DATABASE_URL),
    databaseSsl: str('DATABASE_SSL', 'false') === 'true',
    databaseSslCa: str('DATABASE_SSL_CA', '/etc/ssl/rds/global-bundle.pem'),
    port: int('PORT', 8082),
    grpcPort: int('GRPC_PORT', 50051),
    eventStoreUrl: str('EVENT_STORE_URL', 'http://localhost:8081'),
    incidentTopic: str('INCIDENT_TOPIC', 'incident-updates'),
    tpcTimeoutMs: int('TPC_TIMEOUT_MS', 3000),
    tpcParticipantTimeoutMs: int('TPC_PARTICIPANT_TIMEOUT_MS', 10000),
    tpcRecoveryIntervalMs: int('TPC_RECOVERY_INTERVAL_MS', 15000),
    correlationWindowMs: int('CORRELATION_WINDOW_MS', 900000),
    tpcFault: str('TPC_FAULT', undefined),
  };
}
