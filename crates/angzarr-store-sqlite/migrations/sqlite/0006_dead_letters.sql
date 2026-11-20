-- Dead letters, durable (owner ruling 2026-10-31, Q5): what a step gave up
-- on, written in the same transaction as the position it moved past.
-- Keyed by the stream a letter is about (`domain`, `edition`, `root`), its
-- place there and the component that could not take it, so a letter
-- written again under its key replaces the one kept. Ordered per stream
-- only: by place, then component; nothing orders one stream's letters
-- against another's.
--
-- `at_kind` 0 is an event's sequence, `at` its eight bytes big-endian
-- (bytewise order is the sequences' order); `at_kind` 1 is a command id,
-- `at` its sixteen bytes.
CREATE TABLE dead_letters (
    domain TEXT NOT NULL,
    edition TEXT NOT NULL,
    root BLOB NOT NULL,
    at_kind INTEGER NOT NULL,
    at BLOB NOT NULL,
    component TEXT NOT NULL,
    -- The letter, an io.angzarr.v1.AngzarrDeadLetter as written.
    letter BLOB NOT NULL,
    PRIMARY KEY (domain, edition, root, at_kind, at, component)
) WITHOUT ROWID;
