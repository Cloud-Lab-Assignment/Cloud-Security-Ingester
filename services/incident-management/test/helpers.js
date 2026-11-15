import pg from 'pg';
import { createParticipant } from '@core/tpc';
import { createPool } from '../src/db.js';

const ADMIN_URL = process.env.TEST_DATABASE_ADMIN_URL ?? 'postgres://incidents:incidents@localhost:5432/incidents';

// A throwaway database per test file, dropped afterwards.
export async function createTestDatabase() {
  const name = `im_test_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
  const admin = new pg.Client({ connectionString: ADMIN_URL });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${name}`);
  await admin.end();
  const url = new URL(ADMIN_URL);
  url.pathname = `/${name}`;
  const pool = createPool({ databaseUrl: url.toString(), databaseSsl: false });
  return {
    pool,
    async drop() {
      await pool.end();
      const a = new pg.Client({ connectionString: ADMIN_URL });
      await a.connect();
      await a.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      await a.end();
    },
  };
}

function memoryStore() {
  const map = new Map();
  return {
    map,
    async get(txId) {
      return map.has(txId) ? structuredClone(map.get(txId)) : null;
    },
    async put(r) {
      map.set(r.txId, structuredClone(r));
    },
    async listPending() {
      return [...map.values()].filter((r) => r.state === 'UNCERTAIN' || r.state === 'PRECOMMITTED');
    },
  };
}

// In-memory stand-in for the Event Store 3PC participant.
export function fakeEventStore({ vote = 'yes' } = {}) {
  const store = memoryStore();
  const applied = [];
  const sm = createParticipant({
    store,
    timeoutMs: 60_000,
    validate: async () => fake.vote === 'yes',
    apply: async (payload) => {
      applied.push(payload);
    },
  });
  const fake = { name: 'event-store', vote, store, applied, ...sm };
  return fake;
}

export function fakePublisher() {
  const published = [];
  return {
    published,
    async publishIncidentUpdate(type, incident) {
      published.push({ type, incident });
    },
  };
}

export const silentLogger = {
  info() {}, warn() {}, error() {}, debug() {}, fatal() {}, trace() {},
  child() { return silentLogger; },
};

let seq = 0;
export function candidate(over = {}) {
  seq += 1;
  const entityKey = over.entityKey ?? `198.51.100.${Math.floor(Math.random() * 250) + 1}-${Date.now()}-${seq}`;
  return {
    correlationId: `cor-${Date.now().toString(16)}${seq}`,
    entityKey,
    title: `1 correlated alert(s) for ${entityKey}`,
    severity: 'high',
    alertIds: [`RULE-AUTH-001|${entityKey}|ev-${seq}`],
    ruleIds: ['RULE-AUTH-001'],
    firstSeen: '2026-11-16T09:00:00.000Z',
    lastSeen: '2026-11-16T09:01:00.000Z',
    eventCount: 5,
    relatedEventIds: [`ev-${seq}`],
    entity: { srcIp: entityKey, userId: null, host: null },
    enrichment: { storedEventCount: 5, distinctUsers: 1, failures: 5 },
    ...over,
  };
}
