-- What angzarr-ports needs beyond the released schema.
--
-- Each handler's position on each stream it consumes: the next sequence it
-- has yet to handle (a missing row is 0).
CREATE TABLE positions (
    handler TEXT NOT NULL,
    domain TEXT NOT NULL,
    edition TEXT NOT NULL,
    root BLOB NOT NULL,
    next INTEGER NOT NULL,
    PRIMARY KEY (domain, edition, root, handler)
) WITHOUT ROWID;

-- The transactional outbox: entries pending delivery, oldest write first,
-- then in the order put (`seq`). An id is pending at most once; cleared, it
-- may be put again.
CREATE TABLE outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id BLOB NOT NULL UNIQUE,
    origin TEXT NOT NULL,
    -- The message, a google.protobuf.Any as written.
    message BLOB NOT NULL,
    -- The edition whose processing wrote it; NULL for main's.
    edition TEXT
);

-- EventMeta's correlation id and recorded instant (nanoseconds since the
-- epoch), copied out of each page for the queries by correlation and by
-- time. NULL until filled: a page written before this migration is filled
-- once, by the store's backfill, which SQL cannot do (it decodes the page).
ALTER TABLE events ADD COLUMN correlation_id BLOB;
ALTER TABLE events ADD COLUMN recorded_at INTEGER;
CREATE INDEX events_by_correlation ON events (correlation_id, domain, root);
CREATE INDEX events_by_time ON events (domain, edition, root, recorded_at);

-- Backfills the store runs once, under the lock of the migration that
-- needs them: `done` is 0 until it has run.
CREATE TABLE backfills (
    name TEXT PRIMARY KEY NOT NULL,
    done INTEGER NOT NULL
) WITHOUT ROWID;

INSERT INTO backfills (name, done) VALUES ('events-meta-columns', 0);
