import { incidentUpdateMessage } from './publisher.js';

// Queue an incident-updates message inside the caller's transaction (`db` is a
// client with an open transaction). The payload is the full message, built
// from the incident snapshot at that moment.
export async function enqueueIncidentUpdate(db, type, incident, now = Date.now) {
  const message = incidentUpdateMessage(type, incident, now);
  await db.query('INSERT INTO outbox (type, incident_id, payload) VALUES ($1, $2, $3)', [
    type,
    incident.incidentId,
    JSON.stringify(message),
  ]);
  return message;
}

// Publishes unpublished outbox rows in id order and marks them published.
// Delivery is at-least-once: a crash between the Kafka ack and the UPDATE
// re-sends those rows, so consumers dedupe on (incidentId, version, type).
export function createOutboxRelay({ pool, producer, topic, logger, intervalMs = 1000, batchSize = 100 }) {
  async function publishPending() {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // SKIP LOCKED: a second relay (e.g. during a rollout overlap) never sends the same rows concurrently.
      const { rows } = await client.query(
        `SELECT id, incident_id, payload FROM outbox WHERE published_at IS NULL
          ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`,
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
