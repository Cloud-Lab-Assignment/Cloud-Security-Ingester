-- Transactional outbox for incident-updates (P1-3). Rows are written in the
-- same transaction as the change they describe and published by a relay.
CREATE TABLE IF NOT EXISTS outbox (
  id           bigserial PRIMARY KEY,
  type         text NOT NULL,
  incident_id  uuid NOT NULL,
  payload      jsonb NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz NULL
);

CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
