-- 003 removed duplicate alert associations but left alert_count (and the title
-- generated from it) as they were (R3-5). Recompute both from incident_alerts.
-- Only titles in the generated form "<n> correlated alert(s) for <entityKey>"
-- are rewritten; any other title is left alone.
UPDATE incidents i
   SET alert_count = a.n,
       title = CASE
                 WHEN i.title ~ '^[0-9]+ correlated alert\(s\) for '
                 THEN a.n || ' correlated alert(s) for ' || i.entity_key
                 ELSE i.title
               END
  FROM (SELECT incident_id, count(*)::int AS n FROM incident_alerts GROUP BY incident_id) a
 WHERE a.incident_id = i.incident_id
   AND i.alert_count IS DISTINCT FROM a.n;
