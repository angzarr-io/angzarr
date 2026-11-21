-- The edition administration (angzarr-ports' `Editions`): an edition exists
-- while a fork of it remains, so no table names one; two lookups need an
-- index and one fact needs a table.
--
-- Listing the editions, and asking whether one exists (a read unit refuses a
-- stream of an edition that does not, a step's position on a removed stream
-- of one is refused), look across every domain for the rows of an edition
-- name, which the key (domain, edition, root, sequence) cannot find. The index
-- holds the edition streams' rows alone (main's are the table's far greater
-- part), by edition, then stream, then sequence, so the first row of a
-- stream in an edition, its fork, is the least of its group.
CREATE INDEX events_of_editions ON events (edition, domain, root, sequence)
    WHERE edition <> '';

-- The edition streams a write that put an outbox entry of an edition
-- touched (the stream it appended to, and those its positions are on), so
-- removing an edition stream removes the entries written with it, undelivered
-- (decision 14.107). An entry carries one stream, the one it is ordered on;
-- a write may touch more. A row goes with its entry (the trigger).
CREATE TABLE outbox_edition_streams (
    id BLOB NOT NULL,
    domain TEXT NOT NULL,
    edition TEXT NOT NULL,
    root BLOB NOT NULL,
    PRIMARY KEY (id, domain, edition, root)
) WITHOUT ROWID;

CREATE INDEX outbox_edition_streams_by_stream
    ON outbox_edition_streams (domain, edition, root);

CREATE TRIGGER outbox_edition_streams_go_with_their_entry AFTER DELETE ON outbox
BEGIN
    DELETE FROM outbox_edition_streams WHERE id = old.id;
END;

-- An entry pending from before this is known by the stream it is ordered
-- on alone, when that is a stream of its own edition.
INSERT INTO outbox_edition_streams (id, domain, edition, root)
SELECT id, domain, stream_edition, root FROM outbox
WHERE edition IS NOT NULL AND stream_edition = edition;
