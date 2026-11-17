-- Unpublished correlation rows written before 004 have no tx_id, so they would
-- skip the 3PC gate and publish while their transaction is still PRECOMMITTED
-- (R3-3). Backfill tx_id from the incident history row of the same incident and
-- version (applyDecision writes both in one transaction). Status-change rows
-- (incident.status_changed) are not 3PC and keep tx_id NULL.
UPDATE outbox o
   SET tx_id = h.tx_id
  FROM incident_history h
 WHERE o.tx_id IS NULL
   AND o.published_at IS NULL
   AND o.type IN ('incident.created', 'incident.updated')
   AND h.incident_id = o.incident_id
   AND h.tx_id IS NOT NULL
   AND (h.change->>'version') = (o.payload->'incident'->>'version');
