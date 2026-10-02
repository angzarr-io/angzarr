-- The deferred-idempotency key carries what the provenance tuple is attached
-- to: a deferred command, or a Notification delivery envelope carrying a
-- RejectionNotification or a Compensate. A Compensate envelope carries the
-- provenance tuple of the command it undoes, so without the kind it would be
-- deduplicated against that command and never applied.
ALTER TABLE events ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'command';

DROP INDEX IF EXISTS idx_events_source;
CREATE INDEX IF NOT EXISTS idx_events_source
    ON events (domain, edition, root, source_edition, source_domain, source_root, source_seq, source_component, source_command_index, source_kind)
    WHERE source_domain IS NOT NULL;
