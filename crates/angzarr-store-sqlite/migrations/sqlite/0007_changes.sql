-- The change feed (angzarr-ports' `Changes`): which streams a commit
-- touched, so a follower refreshes those and not every stream. One row per
-- commit that landed events on a stream (`purged` 0) or purged it (1),
-- written in the commit's own transaction, in the order the commits landed
-- (`seq`, never reused). The last 65,536 rows are kept; a cursor from
-- before them is an overflow, and the follower reads everything.
--
-- `feed.identity` names this database's feed: a cursor made of another's is
-- an overflow too. It is made here, at random, once.
CREATE TABLE changes (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    edition TEXT NOT NULL,
    root BLOB NOT NULL,
    purged INTEGER NOT NULL
);

CREATE TABLE feed (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    identity INTEGER NOT NULL
);

INSERT INTO feed (id, identity) VALUES (1, abs(random()));
