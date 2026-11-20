-- A project's events and snapshots. A stream's main timeline is the edition
-- '': SQLite keeps NULLs distinct in a key, and annals refuses an empty
-- edition, so '' names no real one (and sorts first, as covers lists the main
-- timeline first).

CREATE TABLE events (
    domain TEXT NOT NULL,
    edition TEXT NOT NULL,
    root BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    -- EventMeta.causation.command_id, copied out for the deduplication lookup.
    command_id BLOB,
    -- The EventPage as written; never decoded except to read it back.
    page BLOB NOT NULL,
    PRIMARY KEY (domain, edition, root, sequence)
) WITHOUT ROWID;

CREATE INDEX events_by_command ON events (domain, edition, root, command_id);

CREATE TABLE snapshots (
    domain TEXT NOT NULL,
    edition TEXT NOT NULL,
    root BLOB NOT NULL,
    snapshot BLOB NOT NULL,
    PRIMARY KEY (domain, edition, root)
) WITHOUT ROWID;
