-- 3PC outbox rows carry their transaction id (R2-5b). The relay publishes such
-- a row only once the coordinator has logged the transaction COMMITTED.
-- Rows without a tx_id (status changes, rows from before this migration)
-- publish immediately.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS tx_id text NULL;
