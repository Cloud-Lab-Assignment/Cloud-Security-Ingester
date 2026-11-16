import { incidentUpdateMessage } from './publisher.js';

// Queue an incident-updates message inside the caller's transaction (`db` is a
// client with an open transaction). The payload is the full message, built
// from the incident snapshot at that moment. `txId` marks a 3PC row, which is
// held back until the coordinator logs that transaction COMMITTED (R2-5b).
export async function enqueueIncidentUpdate(db, type, incident, now = Date.now, txId = null) {
  const message = incidentUpdateMessage(type, incident, now);
  await db.query('INSERT INTO outbox (type, incident_id, payload, tx_id) VALUES ($1, $2, $3, $4)', [
    type,
    incident.incidentId,
    JSON.stringify(message),
    txId,
  ]);
  return message;
}

// Arbitrary constant for the relay's advisory lock: one relay batch at a time
// across processes, so overlapping relays (e.g. a rollout) keep the order (R2-5a).
const RELAY_LOCK_KEY = 527_25_5;

// Publishes unpublished outbox rows in id order and marks them published.
// Delivery is at-least-once: a crash between the Kafka ack and the UPDATE
// re-sends those rows, so consumers dedupe on (incidentId, version, type).
export function createOutboxRelay({ pool, producer, topic, logger, intervalMs = 1000, batchSize = 100 }) {
  async function publishPending() {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // Serialise batches across relays: a second relay waits here until the
      // first one's batch is published and marked, then continues after it.
      await client.query('SELECT pg_advisory_xact_lock($1)', [RELAY_LOCK_KEY]);
      // A 3PC row is held back until its transaction is COMMITTED in the
      // coordinator log, and so is every later row of the same incident, so
      // per-incident order is kept (a PATCH can follow a commit whose
      // event-store doCommit is still unacknowledged).
      const { rows } = await client.query(
        `SELECT o.id, o.incident_id, o.payload FROM outbox o
          WHERE o.published_at IS NULL
            AND NOT EXISTS (
              SELECT 1 FROM outbox g
               WHERE g.published_at IS NULL AND g.incident_id = o.incident_id AND g.id <= o.id
                 AND g.tx_id IS NOT NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM tpc_coordinator c WHERE c.tx_id = g.tx_id AND c.state = 'COMMITTED'))
          ORDER BY o.id LIMIT $1`,
        [batchSize],
      );
      if (rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }
      // One send keeps the batch in order (per incidentId partition).
      await producer.send({
        topic,
        acks: -1,
        messages: rows.map((r) => ({ key: r.incident_id, value: JSON.stringify(r.payload) })),
      });
      await client.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])', [rows.map((r) => r.id)]);
      await client.query('COMMIT');
      return rows.length;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  let timer = null;
  let running = null;
  async function tick() {
    try {
      // Drain everything queued, batch by batch.
      while ((await publishPending()) === batchSize);
    } catch (err) {
      logger?.warn({ err }, 'outbox publish failed, will retry');
    }
  }
  return {
    publishPending,
    start() {
      if (timer) return;
      timer = setInterval(() => {
        if (running) return;
        running = tick().finally(() => {
          running = null;
        });
      }, intervalMs);
    },
    async stop() {
      clearInterval(timer);
      timer = null;
      if (running) await running;
    },
  };
}
