-- A held 3PC row whose transaction the coordinator logged ABORTED is marked
-- 'discarded' by the relay (R3-4), so it no longer blocks later rows of the
-- same incident. NULL = normal row.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS disposition text NULL
  CHECK (disposition IS NULL OR disposition IN ('discarded'));
