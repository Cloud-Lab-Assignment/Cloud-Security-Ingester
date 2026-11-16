import promClient from 'prom-client';
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
// A 3PC row held back (its transaction not COMMITTED) for longer than this is
// counted in outbox_held_rows, so a stuck incident is visible (R3-4).
const HELD_ALERT_MS = 5 * 60 * 1000;

export function createOutboxRelay({ pool, producer, topic, logger, intervalMs = 1000, batchSize = 100, registry }) {
  // Rows held back by the 3PC gate (not discarded, transaction not COMMITTED)
  // and older than HELD_ALERT_MS.
  async function heldRows() {
    const { rows } = await pool.query(
      `SELECT count(*)::int AS n FROM outbox o
        WHERE o.published_at IS NULL AND o.disposition IS NULL AND o.tx_id IS NOT NULL
          AND o.created_at < now() - ($1::int * interval '1 millisecond')
          AND NOT EXISTS (SELECT 1 FROM tpc_coordinator c WHERE c.tx_id = o.tx_id AND c.state = 'COMMITTED')`,
      [HELD_ALERT_MS],
    );
    return rows[0].n;
  }

  if (registry) {
    // eslint-disable-next-line no-new
    new promClient.Gauge({
      name: 'outbox_held_rows',
      help: 'incident-updates outbox rows held back for over 5 minutes because their 3PC transaction is not COMMITTED',
      registers: [registry],
      async collect() {
        try {
          this.set(await heldRows());
        } catch (err) {
          logger?.warn({ err }, 'outbox_held_rows query failed');
        }
      },
    });
  }

  async function publishPending() {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // Serialise batches across relays: a second relay waits here until the
      // first one's batch is published and marked, then continues after it.
      await client.query('SELECT pg_advisory_xact_lock($1)', [RELAY_LOCK_KEY]);
      // The coordinator's decision wins: a 3PC row whose transaction ended
      // ABORTED (only possible after a timeout split) is never published and
      // must not block the incident's later rows (R3-4). PRECOMMITTED rows are
      // never expired: recovery will still commit them.
      const discarded = await client.query(
        `UPDATE outbox o SET disposition = 'discarded'
           FROM tpc_coordinator c
          WHERE o.published_at IS NULL AND o.disposition IS NULL
            AND o.tx_id = c.tx_id AND c.state = 'ABORTED'
          RETURNING o.id, o.incident_id, o.tx_id, o.type`,
      );
      for (const r of discarded.rows) {
        logger?.error(
          { outboxId: String(r.id), incidentId: r.incident_id, txId: r.tx_id, type: r.type },
          'discarding outbox row: its 3PC transaction is ABORTED although Postgres applied it (atomicity split)',
        );
      }
      // A 3PC row is held back until its transaction is COMMITTED in the
      // coordinator log, and so is every later row of the same incident, so
      // per-incident order is kept (a PATCH can follow a commit whose
      // event-store doCommit is still unacknowledged).
      const { rows } = await client.query(
        `SELECT o.id, o.incident_id, o.payload FROM outbox o
          WHERE o.published_at IS NULL AND o.disposition IS NULL
            AND NOT EXISTS (
              SELECT 1 FROM outbox g
               WHERE g.published_at IS NULL AND g.disposition IS NULL
                 AND g.incident_id = o.incident_id AND g.id <= o.id
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
    heldRows,
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
