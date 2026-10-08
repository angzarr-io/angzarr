-- S1: restore uniqueness for main-timeline rows (SQLite variant).
--
-- Migration 0006 made `edition` nullable and asserted that SQLite's
-- NULL-distinct UNIQUE semantics were "exactly what we want". That was
-- backwards: with C-15 storing main-timeline editions as SQL NULL, the
-- composite PRIMARY KEYs stopped enforcing ANY uniqueness for main-timeline
-- rows. Consequences:
--   * positions: the upsert's ON CONFLICT (handler, edition, domain, root)
--     could never fire — every checkpoint put INSERTED a new row, `get`
--     returned an arbitrary one, and projector checkpoints froze
--     (proven by test_put_update_main_timeline).
--   * snapshots: a re-put at the same sequence duplicated the row; the
--     `ORDER BY sequence DESC LIMIT 1` read picks arbitrarily among
--     same-sequence duplicates.
--   * events: the PK was the last-resort duplicate-sequence guard behind
--     the in-transaction fence; for main-timeline rows it guarded nothing.
--
-- Postgres got this right with UNIQUE NULLS NOT DISTINCT (its migrations
-- 0007/0009). SQLite has no such clause; instead we add UNIQUE indexes on
-- COALESCE(edition, '') so NULL collapses to a comparable value. The
-- upsert conflict targets are switched to matching COALESCE expressions on
-- the SQLite side (see SqlDatabase::positions_conflict_target).
--
-- Dedupe first: a database that lived through the bug may already carry
-- duplicate main-timeline rows, which would make CREATE UNIQUE INDEX fail.

-- positions: keep the highest sequence per logical key (the checkpoint
-- semantics C-17 wants — never regress).
DELETE FROM positions WHERE rowid NOT IN (
    SELECT keep FROM (
        SELECT rowid AS keep,
               ROW_NUMBER() OVER (
                   PARTITION BY handler, domain, COALESCE(edition, ''), root
                   ORDER BY sequence DESC, rowid DESC
               ) AS rn
        FROM positions
    ) WHERE rn = 1
);

-- snapshots: keep the most recently written row per (key, sequence) —
-- later writes reflect later state for the same sequence.
DELETE FROM snapshots WHERE rowid NOT IN (
    SELECT keep FROM (
        SELECT rowid AS keep,
               ROW_NUMBER() OVER (
                   PARTITION BY domain, COALESCE(edition, ''), root, sequence
                   ORDER BY rowid DESC
               ) AS rn
        FROM snapshots
    ) WHERE rn = 1
);

-- events: keep the FIRST-written row per (key, sequence) — events are
-- immutable facts; the first write is the one consumers may have seen.
DELETE FROM events WHERE rowid NOT IN (
    SELECT keep FROM (
        SELECT rowid AS keep,
               ROW_NUMBER() OVER (
                   PARTITION BY domain, COALESCE(edition, ''), root, sequence
                   ORDER BY rowid ASC
               ) AS rn
        FROM events
    ) WHERE rn = 1
);

-- Uniqueness that treats both main-timeline representations (NULL via
-- C-15) and named editions uniformly. Named editions were already unique
-- via the PKs; these indexes additionally close the NULL hole.
CREATE UNIQUE INDEX IF NOT EXISTS uq_positions_main
    ON positions (handler, domain, COALESCE(edition, ''), root);

CREATE UNIQUE INDEX IF NOT EXISTS uq_snapshots_main
    ON snapshots (domain, COALESCE(edition, ''), root, sequence);

CREATE UNIQUE INDEX IF NOT EXISTS uq_events_main
    ON events (domain, COALESCE(edition, ''), root, sequence);
