-- The store's commit position: one row, the position the next append of
-- events lands at. It rises with each such append, in commit order (SQLite
-- runs one writer at a time), and nothing lowers it, a purge included.
-- Every event row is stamped with the position it landed at. Events stored
-- before it existed are at 0, so it starts at 1.

CREATE TABLE commit_position (
    next INTEGER NOT NULL
);

INSERT INTO commit_position (next) VALUES (1);

ALTER TABLE events ADD COLUMN position INTEGER NOT NULL DEFAULT 0;
