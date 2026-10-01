-- Make `edition` genuinely nullable (SQLite variant).
--
-- SQLite can't ALTER a column's NOT NULL status in-place, so we rebuild
-- each affected table. SQLite's UNIQUE constraint natively treats NULLs
-- as distinct, which for our composite PK `(domain, edition, root,
-- sequence)` is exactly the semantic we want: two rows with NULL
-- edition + same (domain, root, seq) differ only via the composite
-- columns, which is the real uniqueness boundary anyway.

-- Events table rebuild (preserves all columns added by 0002-0003).
CREATE TABLE events_new (
    domain TEXT NOT NULL,
    edition TEXT,                                    -- was NOT NULL
    root TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    event_data BLOB NOT NULL,
    correlation_id TEXT NOT NULL DEFAULT '',
    external_id TEXT NOT NULL DEFAULT '',
    source_edition TEXT,
    source_domain TEXT,
    source_root TEXT,
    source_seq INTEGER,
    PRIMARY KEY (domain, edition, root, sequence)
);

INSERT INTO events_new (
    domain, edition, root, sequence, created_at, event_data, correlation_id,
    external_id, source_edition, source_domain, source_root, source_seq
)
SELECT
    domain,
    CASE WHEN edition IN ('angzarr', '') THEN NULL ELSE edition END,
    root, sequence, created_at, event_data, correlation_id,
    external_id,
    CASE WHEN source_edition IN ('angzarr', '') THEN NULL ELSE source_edition END,
    source_domain, source_root, source_seq
FROM events;

DROP TABLE events;
ALTER TABLE events_new RENAME TO events;

CREATE INDEX IF NOT EXISTS idx_events_domain_edition_root
    ON events (domain, edition, root);
CREATE INDEX IF NOT EXISTS idx_events_correlation_id
    ON events (correlation_id);
CREATE INDEX IF NOT EXISTS idx_events_domain_edition_root_created_at
    ON events (domain, edition, root, created_at);
CREATE INDEX IF NOT EXISTS idx_events_external_id
    ON events (domain, edition, root, external_id) WHERE external_id != '';
CREATE INDEX IF NOT EXISTS idx_events_source
    ON events (domain, edition, root, source_edition, source_domain, source_root, source_seq)
    WHERE source_edition IS NOT NULL;

-- Snapshots table rebuild (preserves retention from 0005).
CREATE TABLE snapshots_new (
    domain TEXT NOT NULL,
    edition TEXT,                                    -- was NOT NULL
    root TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    state_data BLOB NOT NULL,
    retention INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    PRIMARY KEY (domain, edition, root, sequence)
);

INSERT INTO snapshots_new
SELECT
    domain,
    CASE WHEN edition IN ('angzarr', '') THEN NULL ELSE edition END,
    root, sequence, state_data, retention, created_at
FROM snapshots;

DROP TABLE snapshots;
ALTER TABLE snapshots_new RENAME TO snapshots;

CREATE INDEX IF NOT EXISTS idx_snapshots_latest
    ON snapshots (domain, edition, root, sequence DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_retention
    ON snapshots (domain, edition, root, retention, sequence);

-- Positions table rebuild.
CREATE TABLE positions_new (
    handler TEXT NOT NULL,
    domain TEXT NOT NULL,
    edition TEXT,                                    -- was NOT NULL
    root BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (handler, domain, edition, root)
);

INSERT INTO positions_new
SELECT
    handler, domain,
    CASE WHEN edition IN ('angzarr', '') THEN NULL ELSE edition END,
    root, sequence, updated_at
FROM positions;

DROP TABLE positions;
ALTER TABLE positions_new RENAME TO positions;
