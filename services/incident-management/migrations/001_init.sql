-- Incident Management schema: incidents, their alerts and history, and the
-- durable 3PC logs (participant records and coordinator log).

CREATE TABLE IF NOT EXISTS incidents (
  incident_id       uuid PRIMARY KEY,
  correlation_id    text UNIQUE NOT NULL,
  entity_key        text,
  title             text,
  severity          text CHECK (severity IN ('low', 'medium', 'high', 'critical')),
  status            text NOT NULL DEFAULT 'open'
                      CHECK (status IN ('open', 'investigating', 'resolved', 'false_positive')),
  alert_count       int,
  event_count       int,
  rule_ids          text[],
  related_event_ids text[],
  entity            jsonb,
  enrichment        jsonb,
  first_seen        timestamptz,
  last_seen         timestamptz,
  version           int NOT NULL,
  created_at        timestamptz,
  updated_at        timestamptz
);

CREATE INDEX IF NOT EXISTS incidents_entity_status_last_seen
  ON incidents (entity_key, status, last_seen);

CREATE TABLE IF NOT EXISTS incident_alerts (
  incident_id uuid NOT NULL REFERENCES incidents (incident_id),
  alert_id    text NOT NULL,
  PRIMARY KEY (incident_id, alert_id)
);

CREATE TABLE IF NOT EXISTS incident_history (
  id          bigserial PRIMARY KEY,
  incident_id uuid NOT NULL REFERENCES incidents (incident_id),
  tx_id       text UNIQUE NULL,
  change      jsonb,
  actor       text,
  at          timestamptz
);

-- 3PC participant records for the local incident-db participant.
CREATE TABLE IF NOT EXISTS tpc_participant (
  tx_id      text PRIMARY KEY,
  state      text NOT NULL,
  payload    jsonb,
  updated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS tpc_participant_pending
  ON tpc_participant (state) WHERE state IN ('UNCERTAIN', 'PRECOMMITTED');

-- 3PC coordinator log. reason and created_at are additions for the demo API.
CREATE TABLE IF NOT EXISTS tpc_coordinator (
  tx_id      text PRIMARY KEY,
  state      text NOT NULL,
  payloads   jsonb,
  reason     text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS tpc_coordinator_unfinished
  ON tpc_coordinator (state) WHERE state IN ('WAITING', 'PRECOMMITTED');
CREATE INDEX IF NOT EXISTS tpc_coordinator_updated ON tpc_coordinator (updated_at DESC);
