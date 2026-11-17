-- An alert belongs to at most one incident (P2-1). Together with findTarget's
-- owner lookup this stops a redelivered alert from opening a duplicate incident.
--
-- Databases from before this migration may already link one alert to several
-- incidents (R2-4). Keep the earliest association per alert_id (by the
-- incident's created_at, then incident_id) and delete the rest, so the unique
-- index can be created instead of crash-looping the service on startup.
-- The affected incidents' alert_count and title are recomputed by 007 (R3-5).
DO $$
DECLARE
  removed integer;
BEGIN
  WITH ranked AS (
    SELECT ia.incident_id, ia.alert_id,
           row_number() OVER (
             PARTITION BY ia.alert_id
             ORDER BY i.created_at NULLS LAST, ia.incident_id
           ) AS rn
      FROM incident_alerts ia
      JOIN incidents i ON i.incident_id = ia.incident_id
  )
  DELETE FROM incident_alerts d
   USING ranked r
   WHERE d.incident_id = r.incident_id AND d.alert_id = r.alert_id AND r.rn > 1;
  GET DIAGNOSTICS removed = ROW_COUNT;
  RAISE NOTICE 'incident_alerts: removed % duplicate alert association(s)', removed;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS incident_alerts_alert_id_unique ON incident_alerts (alert_id);
