import pino from 'pino';

// pino JSON logs on stdout, one base field identifying the service.
export function createLogger(service, opts = {}) {
  return pino({
    level: process.env.LOG_LEVEL ?? 'info',
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: { level: (label) => ({ level: label }) },
    ...opts,
  });
}
