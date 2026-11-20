-- No store-wide position (decision 14.35): ordering is per stream, by
-- sequence, and nothing reads a global commit position. 0002's table and
-- its stamp on every event row go; 0002 itself stays as released.
-- (DROP COLUMN needs SQLite 3.35 or later.)

DROP TABLE commit_position;

ALTER TABLE events DROP COLUMN position;
