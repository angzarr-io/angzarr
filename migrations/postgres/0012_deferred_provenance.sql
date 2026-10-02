-- Widen the deferred-idempotency key (O1): (source_edition, source_domain,
-- source_root, source_seq) identifies only the triggering EVENT, not the
-- emitted command. Every command of one saga/PM invocation shared the key,
-- so `find_by_source` matched the first command's persisted events and the
-- second command was silently swallowed as a duplicate. Two distinct
-- components reacting to the same source event collided the same way.
--
-- The key now also carries the producing component's registered name and the
-- command's position within the invocation's emitted list (mirrors the new
-- AngzarrDeferredSequence.source_component / .command_index proto fields).
--
-- NOT NULL DEFAULT: no backfill. Old rows keep ''/0, which is exactly what a
-- pre-upgrade in-flight message decodes to, so pre-upgrade redeliveries still
-- match their claim. The one-window exception: a pre-upgrade in-flight
-- command redelivered AFTER its producer upgraded carries component+index,
-- misses the old ''/0 row, and re-executes once — caught by the sequence
-- fence as a NoOp.
ALTER TABLE events ADD COLUMN IF NOT EXISTS source_component TEXT NOT NULL DEFAULT '';
ALTER TABLE events ADD COLUMN IF NOT EXISTS source_command_index INTEGER NOT NULL DEFAULT 0;

-- Rebuild the idempotency-lookup index with the widened key.
DROP INDEX IF EXISTS idx_events_source;
CREATE INDEX IF NOT EXISTS idx_events_source
    ON events (domain, edition, root, source_edition, source_domain, source_root, source_seq, source_component, source_command_index)
    WHERE source_edition IS NOT NULL;
