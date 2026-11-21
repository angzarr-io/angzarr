//! The file's tables, created once in one transaction when a new file is
//! opened. The format number is the file's, in `meta`: a build reads files
//! up to its [`FORMAT`] and leaves a newer one untouched.
//!
//! **Why these tables.** DuckDB is columnar, optimistic and checkpointed:
//! a statement costs about a quarter of a millisecond before it does any
//! work, a scan is cheap, and a commit is a WAL append plus an `fsync`.
//! The schema is shaped by that.
//!
//! - `events` keeps every page as written (`page`, never decoded except to
//!   read it back) with the three things the store queries by copied out
//!   beside it (`command_id`, `correlation_id`, `recorded_at`), as the SQLite
//!   store does. It has a non-unique index on the stream (`domain`,
//!   `edition`, `root`), which is what makes a stream's read a lookup and not
//!   a scan: a root is a random UUID, so the columns' min/max statistics
//!   prune nothing. It has no primary key on the sequence: the writer is
//!   one connection, which checks the head under its lock, and a unique
//!   index over every page would be paid by every append for a property the
//!   write path already holds.
//! - `streams` has one row per stream that has pages of its own: its first
//!   own sequence (an edition stream's fork), its next sequence and the
//!   instant of its last page. A head, `covers`, `list_streams` and the
//!   fork's place are each one row read, not an aggregate over `events`:
//!   every append asks for the head, and `max(sequence)` over millions of
//!   pages is the one query a columnar engine does worse than a B-tree.
//! - `snapshots`, `positions`, `outbox` and `dead_letters` are keyed
//!   latest-wins tables: a primary key gives them their upsert
//!   (`ON CONFLICT`), and they are small beside `events`.
//! - `changes` is the feed: one row per commit that landed events on a
//!   stream or purged it, written in the commit's own transaction; the last
//!   [`CHANGES_KEPT`](crate::statements::CHANGES_KEPT) are kept.
//! - `meta` holds the format number and the feed's identity, made once, at
//!   random.

use angzarr_ports::StoreError;
use duckdb::Connection;

use crate::error::{unavailable, DuckError};
use crate::queries::Queries;
use crate::store::{meta_value, random_identity};
use crate::{statements, writes};

/// The format this build reads and writes.
pub(crate) const FORMAT: i64 = 1;

/// The key of the format number in `meta`.
pub(crate) const FORMAT_KEY: &str = "format";

/// The key of the feed's identity in `meta`.
pub(crate) const IDENTITY_KEY: &str = "feed_identity";

/// The tables, in dependency order. A stream's main timeline is the
/// edition `''`: it names no real edition (the ports refuse an empty one)
/// and sorts first, as `covers` lists the main timeline first.
pub(crate) const TABLES: &str = "
CREATE TABLE meta (
    key VARCHAR PRIMARY KEY,
    value BIGINT NOT NULL
);
CREATE TABLE streams (
    domain VARCHAR NOT NULL,
    edition VARCHAR NOT NULL,
    root BLOB NOT NULL,
    first_sequence BIGINT NOT NULL,
    next_sequence BIGINT NOT NULL,
    last_seconds BIGINT,
    last_nanos INTEGER,
    PRIMARY KEY (domain, edition, root)
);
CREATE TABLE events (
    domain VARCHAR NOT NULL,
    edition VARCHAR NOT NULL,
    root BLOB NOT NULL,
    sequence BIGINT NOT NULL,
    command_id BLOB,
    correlation_id BLOB,
    recorded_at BIGINT,
    page BLOB NOT NULL
);
CREATE INDEX events_by_stream ON events (domain, edition, root);
CREATE TABLE snapshots (
    domain VARCHAR NOT NULL,
    edition VARCHAR NOT NULL,
    root BLOB NOT NULL,
    snapshot BLOB NOT NULL,
    PRIMARY KEY (domain, edition, root)
);
CREATE TABLE positions (
    domain VARCHAR NOT NULL,
    edition VARCHAR NOT NULL,
    root BLOB NOT NULL,
    handler VARCHAR NOT NULL,
    next BIGINT NOT NULL,
    PRIMARY KEY (domain, edition, root, handler)
);
CREATE TABLE outbox (
    id BLOB PRIMARY KEY,
    domain VARCHAR NOT NULL,
    stream_edition VARCHAR NOT NULL,
    root BLOB NOT NULL,
    sequence BIGINT NOT NULL,
    place BIGINT NOT NULL,
    origin VARCHAR NOT NULL,
    message BLOB NOT NULL,
    edition VARCHAR
);
CREATE TABLE dead_letters (
    domain VARCHAR NOT NULL,
    edition VARCHAR NOT NULL,
    root BLOB NOT NULL,
    at_kind BIGINT NOT NULL,
    at_bytes BLOB NOT NULL,
    component VARCHAR NOT NULL,
    letter BLOB NOT NULL,
    PRIMARY KEY (domain, edition, root, at_kind, at_bytes, component)
);
CREATE TABLE changes (
    seq BIGINT NOT NULL,
    domain VARCHAR NOT NULL,
    edition VARCHAR NOT NULL,
    root BLOB NOT NULL,
    purged BOOLEAN NOT NULL
);
";

/// Make this store's tables in a file that has none, or check a file that
/// has: the feed's identity. A file of a newer format, or a database with
/// tables but no `meta` of this store's, is refused untouched.
pub(crate) fn ensure(conn: &Connection) -> Result<u64, StoreError> {
    let tables = conn
        .first(statements::TABLE_COUNT, &[], |row| row.get::<_, i64>(0))?
        .unwrap_or_default();
    if tables == 0 {
        return create(conn);
    }
    check(conn)
}

/// The tables, the format and a new feed identity, in one transaction.
fn create(conn: &Connection) -> Result<u64, StoreError> {
    let identity = random_identity();
    writes::in_transaction(conn, |conn| {
        conn.execute_batch(TABLES).map_err(unavailable)?;
        let rows: [(&str, i64); 2] = [(FORMAT_KEY, FORMAT), (IDENTITY_KEY, identity)];
        for (key, value) in rows {
            conn.run(statements::PUT_META, &[&key, &value])?;
        }
        Ok(())
    })?;
    Ok(identity.unsigned_abs())
}

/// An existing file's format and identity.
fn check(conn: &Connection) -> Result<u64, StoreError> {
    let not_ours = |detail: &str| {
        StoreError::Unavailable(
            DuckError::NotAStore {
                detail: detail.to_string(),
            }
            .cause(),
        )
    };
    let format = meta_value(conn, FORMAT_KEY)
        .map_err(|_| not_ours("no meta table"))?
        .ok_or_else(|| not_ours("no format number"))?;
    if format > FORMAT {
        return Err(StoreError::Unavailable(
            DuckError::NewerFormat {
                found: format.unsigned_abs(),
                supported: FORMAT.unsigned_abs(),
            }
            .cause(),
        ));
    }
    let identity = meta_value(conn, IDENTITY_KEY)?.ok_or_else(|| not_ours("no feed identity"))?;
    Ok(identity.unsigned_abs())
}
