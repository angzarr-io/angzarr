-- The idempotency-lookup index `idx_events_source` was partial on
-- `source_edition IS NOT NULL`, but a source on the main timeline stores
-- `source_edition` as NULL, so every main-timeline saga/PM claim — the common
-- case — fell outside the index and `find_by_source` scanned the aggregate.
-- Every row written with source info carries a non-NULL `source_domain`, so
-- that is the predicate that covers exactly the claim rows. A lookup's
-- `source_domain = ?` implies it, so the planner can use the index.
DROP INDEX IF EXISTS idx_events_source;
CREATE INDEX IF NOT EXISTS idx_events_source
    ON events (domain, edition, root, source_edition, source_domain, source_root, source_seq, source_component, source_command_index)
    WHERE source_domain IS NOT NULL;
