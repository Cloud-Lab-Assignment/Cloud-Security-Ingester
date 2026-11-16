// Deadlines for every AWS SDK call. kafkajs drops a consumer from its group
// when it misses heartbeats for sessionTimeout (30 s by default), so one
// storage call, including the SDK's own retries, must finish well before
// that: worst case maxAttempts x (connect + request timeout) = 3 x 6 s = 18 s
// with the defaults (plus a short retry backoff).
const CONNECTION_TIMEOUT_MS = 1000;

export function awsClientOptions({ requestTimeoutMs = 5000, maxAttempts = 3 } = {}) {
  return {
    maxAttempts,
    requestHandler: {
      connectionTimeout: Math.min(CONNECTION_TIMEOUT_MS, requestTimeoutMs),
      requestTimeout: requestTimeoutMs,
      // Without this the handler only logs a warning when requestTimeout passes.
      throwOnRequestTimeout: true,
    },
  };
}
