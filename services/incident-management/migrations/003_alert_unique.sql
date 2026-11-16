-- An alert belongs to at most one incident (P2-1). Together with findTarget's
-- owner lookup this stops a redelivered alert from opening a duplicate incident.
CREATE UNIQUE INDEX IF NOT EXISTS incident_alerts_alert_id_unique ON incident_alerts (alert_id);
