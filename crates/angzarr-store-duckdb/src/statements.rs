//! Every statement the store runs, with numbered parameters (`$1`...).
//! A stream is always the three parameters (`domain`, `edition` column,
//! `root`), and every statement on `events` matches all three by equality,
//! which is what the stream index serves.

use angzarr_ports::angzarr_proto::v1::event_meta::Causation;
use angzarr_ports::angzarr_proto::v1::EventPage;
use prost_types::Timestamp;

/// A sequence as SQL's 64-bit signed integer. Sequences count events in one
/// stream, so they never reach the sign bit.
pub(crate) fn sequence_column(sequence: u64) -> i64 {
    i64::try_from(sequence).unwrap_or(i64::MAX)
}

/// An instant as the `recorded_at` column keeps it: nanoseconds since the
/// epoch, saturating at the column's range.
pub(crate) fn instant_column(at: &Timestamp) -> i64 {
    let nanos = i128::from(at.seconds) * 1_000_000_000 + i128::from(at.nanos);
    i64::try_from(nanos).unwrap_or(if nanos < 0 { i64::MIN } else { i64::MAX })
}

/// The command id a page records, copied out for the dedup lookup.
pub(crate) fn command_column(page: &EventPage) -> Option<Vec<u8>> {
    page.meta
        .as_ref()
        .and_then(|m| m.causation.as_ref())
        .and_then(|causation| match causation {
            Causation::Command(cause) => cause.command_id.as_ref(),
            Causation::Fact(cause) => cause.command_id.as_ref(),
        })
        .map(|id| id.value.to_vec())
}

/// The columns a page's metadata fills: its correlation id and instant.
pub(crate) fn meta_columns(page: &EventPage) -> (Option<Vec<u8>>, Option<i64>) {
    let meta = page.meta.as_ref();
    (
        meta.and_then(|m| m.correlation_id.as_ref())
            .map(|id| id.value.to_vec()),
        meta.and_then(|m| m.recorded_at.as_ref())
            .map(instant_column),
    )
}

/// Rows `(sequence, page)` of one stream's own rows (`$1` domain, `$2`
/// edition, `$3` root), `$4 <= sequence < $5`, in order, and `$cond`.
macro_rules! own_rows {
    ($cond:literal) => {
        concat!(
            "SELECT sequence, page FROM events WHERE domain = $1 AND edition = $2 AND root = $3 \
             AND sequence >= $4 AND sequence < $5",
            $cond,
            " ORDER BY sequence"
        )
    };
}

/// A stream's own pages in a range.
pub(crate) const PAGES: &str = own_rows!("");

/// The pages the command `$6` caused: the dedup lookup.
pub(crate) const OF_COMMAND: &str = own_rows!(" AND command_id = $6");

/// The pages of the correlation `$6`.
pub(crate) const OF_CORRELATION: &str = own_rows!(" AND correlation_id = $6");

/// The pages recorded at or before `$6` (nanoseconds).
pub(crate) const UNTIL: &str = own_rows!(" AND recorded_at <= $6");

/// Row `(first_sequence, next_sequence, last_seconds, last_nanos)`: a
/// stream's own extent, if it has pages of its own.
pub(crate) const EXTENT: &str = "SELECT first_sequence, next_sequence, last_seconds, last_nanos \
     FROM streams WHERE domain = $1 AND edition = $2 AND root = $3";

/// Rows `(edition, root)` of `$1`'s streams holding pages of their own,
/// main first (`''` sorts first), then by edition, each by root.
pub(crate) const COVERS: &str =
    "SELECT edition, root FROM streams WHERE domain = $1 ORDER BY edition, root";

/// Rows `(domain, edition, root)` of every stream holding pages, in order.
pub(crate) const ALL_STREAMS: &str =
    "SELECT domain, edition, root FROM streams ORDER BY domain, edition, root";

/// Whether any stream is in edition `$1`: an edition exists while a fork
/// of it does.
pub(crate) const EDITION_KNOWN: &str = "SELECT EXISTS (SELECT 1 FROM streams WHERE edition = $1)";

/// Rows `(domain, root)` with a page of correlation `$1` in main or in
/// edition `$2`, of domain `$3` when it is not NULL.
pub(crate) const CORRELATED_STREAMS: &str =
    "SELECT DISTINCT domain, root FROM events WHERE correlation_id = $1 \
     AND (edition = '' OR edition = $2) AND ($3 IS NULL OR domain = $3) \
     ORDER BY domain, root";

/// Insert one page: stream (`$1` to `$3`), sequence, command id,
/// correlation id, instant, bytes.
pub(crate) const INSERT_PAGE: &str =
    "INSERT INTO events (domain, edition, root, sequence, command_id, correlation_id, \
     recorded_at, page) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";

/// Set a stream's extent after an append: stream (`$1` to `$3`), the first
/// own sequence (kept if the stream has one), the next, the last instant.
pub(crate) const UPSERT_EXTENT: &str =
    "INSERT INTO streams (domain, edition, root, first_sequence, next_sequence, last_seconds, \
     last_nanos) VALUES ($1, $2, $3, $4, $5, $6, $7) \
     ON CONFLICT (domain, edition, root) DO UPDATE SET next_sequence = excluded.next_sequence, \
     last_seconds = excluded.last_seconds, last_nanos = excluded.last_nanos";

/// Delete a stream's own pages, extent, snapshot.
pub(crate) const DELETE_EVENTS: &str =
    "DELETE FROM events WHERE domain = $1 AND edition = $2 AND root = $3";
pub(crate) const DELETE_EXTENT: &str =
    "DELETE FROM streams WHERE domain = $1 AND edition = $2 AND root = $3";
pub(crate) const DELETE_SNAPSHOT: &str =
    "DELETE FROM snapshots WHERE domain = $1 AND edition = $2 AND root = $3";

/// Row `(snapshot)`: the stream's newest snapshot.
pub(crate) const SNAPSHOT: &str =
    "SELECT snapshot FROM snapshots WHERE domain = $1 AND edition = $2 AND root = $3";

/// Keep a snapshot as the stream's newest, replacing the one before.
pub(crate) const UPSERT_SNAPSHOT: &str =
    "INSERT INTO snapshots (domain, edition, root, snapshot) VALUES ($1, $2, $3, $4) \
     ON CONFLICT (domain, edition, root) DO UPDATE SET snapshot = excluded.snapshot";

/// Row `(next)`: a handler's position (`$4`) on a stream.
pub(crate) const POSITION: &str = "SELECT next FROM positions \
     WHERE domain = $1 AND edition = $2 AND root = $3 AND handler = $4";

/// Rows `(domain, edition, root, next)` of handler `$1`'s positions above
/// 0, in stream order.
pub(crate) const HANDLER_POSITIONS: &str = "SELECT domain, edition, root, next FROM positions \
     WHERE handler = $1 AND next > 0 ORDER BY domain, edition, root";

/// Put a handler's position on a stream: a plain upsert.
pub(crate) const PUT_POSITION: &str =
    "INSERT INTO positions (domain, edition, root, handler, next) VALUES ($1, $2, $3, $4, $5) \
     ON CONFLICT (domain, edition, root, handler) DO UPDATE SET next = excluded.next";

/// Keep an outbox entry, unless its id is pending already: id, the stream
/// its write appended to (domain, edition, root; an empty domain for
/// none), that write's first sequence, the entry's place in the write, its
/// origin, message and edition.
pub(crate) const PUT_OUTBOX: &str =
    "INSERT INTO outbox (id, domain, stream_edition, root, sequence, place, origin, message, \
     edition) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO NOTHING";

/// Rows `(id, origin, message, edition)`: up to `$1` pending entries, each
/// stream's in its order, so a limit takes each stream's oldest first.
/// Entries of no stream, or two of one write's place, are ordered by id, so
/// the answer is the same each time.
pub(crate) const PENDING: &str = "SELECT id, origin, message, edition FROM outbox \
     ORDER BY domain, stream_edition, root, sequence, place, id LIMIT $1";

/// Clear one pending entry by its id.
pub(crate) const CLEAR: &str = "DELETE FROM outbox WHERE id = $1";

/// Keep a dead letter (`$1` to `$3`: stream, `$4` its place's kind, `$5` its
/// place, `$6` its component, `$7` the letter), replacing the one kept
/// under its key.
pub(crate) const PUT_DEAD_LETTER: &str =
    "INSERT INTO dead_letters (domain, edition, root, at_kind, at_bytes, component, letter) \
     VALUES ($1, $2, $3, $4, $5, $6, $7) \
     ON CONFLICT (domain, edition, root, at_kind, at_bytes, component) DO UPDATE SET letter = \
     excluded.letter";

/// Rows `(at_kind, at, letter)`: a stream's dead letters, in key order.
pub(crate) const DEAD_LETTERS: &str = "SELECT at_kind, at_bytes, letter FROM dead_letters \
     WHERE domain = $1 AND edition = $2 AND root = $3 ORDER BY at_kind, at_bytes, component";

/// Rows `(at_kind, at, letter)`: the dead letter of one key.
pub(crate) const DEAD_LETTER: &str = "SELECT at_kind, at_bytes, letter FROM dead_letters \
     WHERE domain = $1 AND edition = $2 AND root = $3 AND at_kind = $4 AND at_bytes = $5 \
     AND component = $6";

/// Rows `(edition, root)` of `$1`'s streams a dead letter is about.
pub(crate) const DEAD_LETTERED: &str =
    "SELECT DISTINCT edition, root FROM dead_letters WHERE domain = $1 ORDER BY edition, root";

/// Delete the dead letter of one key.
pub(crate) const REMOVE_DEAD_LETTER: &str = "DELETE FROM dead_letters \
     WHERE domain = $1 AND edition = $2 AND root = $3 AND at_kind = $4 AND at_bytes = $5 \
     AND component = $6";

/// Delete every dead letter about a stream.
pub(crate) const DELETE_DEAD_LETTERS: &str =
    "DELETE FROM dead_letters WHERE domain = $1 AND edition = $2 AND root = $3";

/// Rows `(domain, edition, root, sequence, page)`: every stored page.
pub(crate) const EXPORT_PAGES: &str = "SELECT domain, edition, root, sequence, page FROM events \
     ORDER BY domain, edition, root, sequence";

/// Rows `(domain, edition, root, snapshot)`: every stored snapshot.
pub(crate) const EXPORT_SNAPSHOTS: &str =
    "SELECT domain, edition, root, snapshot FROM snapshots ORDER BY domain, edition, root";

/// Rows `(domain, edition, root, handler, next)`: every position above 0.
pub(crate) const EXPORT_POSITIONS: &str =
    "SELECT domain, edition, root, handler, next FROM positions \
     WHERE next > 0 ORDER BY domain, edition, root, handler";

/// Rows `(domain, edition, root, at_kind, at_bytes, component, letter)`.
pub(crate) const EXPORT_DEAD_LETTERS: &str =
    "SELECT domain, edition, root, at_kind, at_bytes, component, letter FROM dead_letters \
     ORDER BY domain, edition, root, at_kind, at_bytes, component";

/// Row `(head)`: the sequence of the feed's last change (0 if none).
pub(crate) const FEED_HEAD: &str = "SELECT COALESCE(MAX(seq), 0) FROM changes";

/// Row `(value)`: a `meta` entry.
pub(crate) const META: &str = "SELECT value FROM meta WHERE key = $1";

/// Set a `meta` entry.
pub(crate) const PUT_META: &str = "INSERT INTO meta (key, value) VALUES ($1, $2)";

/// Rows `(seq, domain, edition, root, purged)` of the changes after `$1`,
/// at most `$2`, in the order they landed.
pub(crate) const CHANGES_AFTER: &str = "SELECT seq, domain, edition, root, purged FROM changes \
     WHERE seq > $1 ORDER BY seq LIMIT $2";

/// A change: stream (`$1` to `$3`) was written to (`$4` false) or purged.
/// Its place in the feed is the last change's plus one.
pub(crate) const TOUCH: &str =
    "INSERT INTO changes SELECT COALESCE(MAX(seq), 0) + 1, $1, $2, $3, $4 FROM changes";

/// How many changes the feed keeps.
pub(crate) const CHANGES_KEPT: i64 = 65_536;

/// The feed is trimmed on the commits whose place is a multiple of this:
/// what is kept is [`CHANGES_KEPT`] at least, and a read judges a cursor
/// against the exact bound, not the trimmed table.
pub(crate) const TRIM_EVERY: i64 = 1_024;

/// Forget the changes before the last [`CHANGES_KEPT`].
pub(crate) const TRIM: &str =
    "DELETE FROM changes WHERE seq <= (SELECT MAX(seq) FROM changes) - 65536";

/// Whether the file holds any table at all.
pub(crate) const TABLE_COUNT: &str =
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'";

#[cfg(test)]
#[path = "statements.test.rs"]
mod tests;
