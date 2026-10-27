-- Baseline Postgres schema for the latest angzarr proto (io.angzarr.v1,
-- spec 20261019): the same tables, keys and indexes as
-- migrations/sqlite/0001_baseline.sql, in Postgres types. It replaces every
-- earlier Postgres migration (0001..0012 at 16db7169): a database from the
-- old schema is refused at startup and must be recreated (its events cannot
-- be decoded by this build).

-- One row per event. The page (payload, sequence, EventMeta) is stored
-- whole in event_data; the other columns are the keys reads select by.
CREATE TABLE events (
    domain         TEXT   NOT NULL,
    edition        TEXT,              -- NULL: the main timeline
    root           BYTEA  NOT NULL,   -- 16-byte UUID
    sequence       BIGINT NOT NULL,
    recorded_at    TEXT   NOT NULL,   -- EventMeta.recorded_at, fixed-width RFC 3339 (nanoseconds)
    command_id     BYTEA,             -- EventMeta.causation.command_id; NULL for a PM process event
    correlation_id BYTEA  NOT NULL,   -- EventMeta.correlation_id, 16 bytes
    event_data     BYTEA  NOT NULL    -- the EventPage, protobuf-encoded
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
    domain         TEXT   NOT NULL,
    edition        TEXT,
    root           BYTEA  NOT NULL,
    sequence       BIGINT NOT NULL,   -- the last event folded in
    state_type_url TEXT   NOT NULL,   -- checked against the registered state type
    state_value    BYTEA  NOT NULL,   -- the state's bytes, offered unchanged
    rules_version  TEXT   NOT NULL    -- the full version of the response that returned it
);

CREATE UNIQUE INDEX uq_snapshots_stream
    ON snapshots (domain, COALESCE(edition, ''), root);
