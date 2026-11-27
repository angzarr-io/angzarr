-- The wire package that wrote each stored page and snapshot (decision
-- 14.113). Every row says its version, so a store written by two versions
-- decodes each row as its own; the stamp says the oldest a row may hold and
-- the newest ever written, so a build refuses a store of a version it does
-- not know before it reads a row.
--
-- SQL cannot tell a row's version (it is in the bytes), so the rows
-- migrated here are marked 255, unstamped, and the store's one-time
-- backfill ('wire-stamp', run under this migration's lock) classifies each
-- by shape, marks it, and inserts the stamp; a store with no row is stamped
-- with the build's version. An old build cannot open the store past this
-- migration (its ledger is newer than that build).

CREATE TABLE wire_stamp (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    oldest INTEGER NOT NULL,
    newest INTEGER NOT NULL,
    CHECK (oldest <= newest)
);

ALTER TABLE events ADD COLUMN wire INTEGER NOT NULL DEFAULT 255;
ALTER TABLE snapshots ADD COLUMN wire INTEGER NOT NULL DEFAULT 255;

-- A write raises the newest version in the write's own transaction: the
-- rows a build writes carry its version, and nothing else writes the stamp.
CREATE TRIGGER events_wire_newest AFTER INSERT ON events
WHEN NEW.wire <> 255
BEGIN
    UPDATE wire_stamp SET newest = max(newest, NEW.wire);
END;

CREATE TRIGGER snapshots_wire_newest_insert AFTER INSERT ON snapshots
WHEN NEW.wire <> 255
BEGIN
    UPDATE wire_stamp SET newest = max(newest, NEW.wire);
END;

CREATE TRIGGER snapshots_wire_newest_update AFTER UPDATE OF wire ON snapshots
WHEN NEW.wire <> 255
BEGIN
    UPDATE wire_stamp SET newest = max(newest, NEW.wire);
END;

INSERT INTO backfills (name, done) VALUES ('wire-stamp', 0);
