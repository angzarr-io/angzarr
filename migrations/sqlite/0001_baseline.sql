-- Baseline schema for the latest angzarr proto (io.angzarr.v1, spec
-- 20261019). Replaces every earlier migration: databases from the old
-- schema are refused at startup and must be deleted (pre-production; their
-- events cannot be decoded by this build). See
-- plans/2026-10-20-sqlite-baseline-schema.md.

-- One row per event. The page (payload, sequence, EventMeta) is stored
-- whole in event_data; the other columns are the keys reads select by.
CREATE TABLE events (
    domain         TEXT    NOT NULL,
    edition        TEXT,              -- NULL: the main timeline
    root           BLOB    NOT NULL,  -- 16-byte UUID
    sequence       INTEGER NOT NULL,
    recorded_at    TEXT    NOT NULL,  -- EventMeta.recorded_at, fixed-width RFC 3339
    command_id     BLOB,              -- EventMeta.causation.command_id; NULL for a PM process event
    correlation_id BLOB    NOT NULL,  -- EventMeta.correlation_id, 16 bytes
    event_data     BLOB    NOT NULL   -- the EventPage, protobuf-encoded
);

-- The stream key, unique per sequence (the optimistic append relies on it).
CREATE UNIQUE INDEX uq_events_stream_sequence
    ON events (domain, COALESCE(edition, ''), root, sequence);

-- Deduplication: (stream, command_id), not unique (one command may write
-- several events). Backlog CI-1.
CREATE INDEX idx_events_command
    ON events (domain, edition, root, command_id)
    WHERE command_id IS NOT NULL;

-- Queries by correlation, within a domain or across all.
CREATE INDEX idx_events_correlation
    ON events (correlation_id, domain);

-- The newest snapshot per stream (the default policy keeps only that one).
CREATE TABLE snapshots (
    domain         TEXT    NOT NULL,
    edition        TEXT,
    root           BLOB    NOT NULL,
    sequence       INTEGER NOT NULL,  -- the last event folded in
    state_type_url TEXT    NOT NULL,  -- checked against the registered state type
    state_value    BLOB    NOT NULL,  -- the state's bytes, offered unchanged
    rules_version  TEXT    NOT NULL   -- the full version of the response that returned it
);

CREATE UNIQUE INDEX uq_snapshots_stream
    ON snapshots (domain, COALESCE(edition, ''), root);
