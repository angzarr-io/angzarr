-- The `editions` table (0001) was never read or written: an edition's
-- divergence point travels with each request (`Cover.edition.divergences`)
-- or is derived from the edition's first event. Drop the unused table.
DROP TABLE IF EXISTS editions;
