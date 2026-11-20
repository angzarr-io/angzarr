-- The outbox orders per stream, by the stream's own event sequence (owner
-- ruling 2026-10-31, Q7): no outbox sequence and no order across streams.
-- 0003's table, keyed by its store-wide `seq INTEGER PRIMARY KEY
-- AUTOINCREMENT`, is rebuilt keyed by its id and ordered by the stream its
-- write appended to (`domain`, `stream_edition`, `root`), that write's
-- first sequence and the entry's place in the write. A write that appends
-- to no stream keeps its entries under no stream (an empty domain), at
-- sequence 0. 0003 itself stays as released.
--
-- An entry still pending from before this has no stream on record: it is
-- kept under no stream, its old `seq` carried once as its sequence, so the
-- entries already pending keep their order.
CREATE TABLE outbox_per_stream (
    id BLOB PRIMARY KEY NOT NULL,
    domain TEXT NOT NULL,
    stream_edition TEXT NOT NULL,
    root BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    place INTEGER NOT NULL,
    origin TEXT NOT NULL,
    -- The message, a google.protobuf.Any as written.
    message BLOB NOT NULL,
    -- The edition whose processing wrote it; NULL for main's.
    edition TEXT
) WITHOUT ROWID;

INSERT INTO outbox_per_stream
    (id, domain, stream_edition, root, sequence, place, origin, message, edition)
SELECT id, '', '', x'', seq, 0, origin, message, edition FROM outbox;

DROP TABLE outbox;

ALTER TABLE outbox_per_stream RENAME TO outbox;

CREATE INDEX outbox_by_stream ON outbox (domain, stream_edition, root, sequence, place);
