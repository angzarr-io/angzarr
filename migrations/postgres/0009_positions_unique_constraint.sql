-- Restore the positions unique constraint that migration 0007 dropped but
-- never re-added.
--
-- Migration 0007 dropped `positions_pkey` (step 2) and added the new
-- `UNIQUE NULLS NOT DISTINCT` constraints back onto `events` and `snapshots`
-- (step 4), but forgot to re-add the equivalent on `positions`. The result:
-- every `ON CONFLICT (handler, edition, domain, root) DO UPDATE ...` against
-- the positions table fails with
--   42P10: there is no unique or exclusion constraint matching the
--          ON CONFLICT specification
-- on Postgres 15+. SQLite's migration 0006 retained the PRIMARY KEY via
-- table-rebuild, so SQLite-backed projectors continue to work; only the
-- Postgres backend is affected.
--
-- This came to light while landing C-17 (SQL PositionStore monotonicity
-- fix) — the new `WHERE positions.sequence < excluded.sequence` clause
-- exposed the missing arbiter constraint, but the underlying upsert was
-- broken on Postgres even before C-17.
--
-- `NULLS NOT DISTINCT` matches the events/snapshots semantic from migration
-- 0007: two rows with NULL edition and identical (handler, domain, root)
-- are treated as a single position.

ALTER TABLE positions
    ADD CONSTRAINT positions_pkey
    UNIQUE NULLS NOT DISTINCT (handler, edition, domain, root);
