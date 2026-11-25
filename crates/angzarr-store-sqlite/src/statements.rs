//! Every statement the store runs. The writes of rows are built with
//! sea-query from the schema's identifiers (`schema`); the reads are SQL
//! over one shared condition, [`view!`], with numbered parameters.

use angzarr_ports::angzarr_proto::v1::event_meta::Causation;
use angzarr_ports::angzarr_proto::v1::{Cover, EventPage, Snapshot};
use angzarr_ports::StoreError;
use prost::Message;
use prost_types::Timestamp;
use sea_query::{Cond, Condition, DeleteStatement, Expr, InsertStatement, OnConflict, Query};

use crate::error::unavailable;
use crate::schema::{Events, Snapshots};
use crate::Dialect;

/// The root's bytes: the key column every statement matches on.
pub(crate) fn root(cover: &Cover) -> Vec<u8> {
    cover
        .root
        .as_ref()
        .map(|r| r.value.to_vec())
        .unwrap_or_default()
}

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

/// The rows of one stream as read in its edition (parameters `?1` domain,
/// `?2` root, `?3` the edition column's value, `''` for main): its own rows,
/// and main's below its fork, the sequence of its first own row (its
/// fork). A main stream, and an edition stream with no row of its own,
/// is main's rows alone. `edition IN ('', ?3)` says nothing the rest does
/// not; it is there so SQLite searches the key (domain, edition, root) for
/// the stream's rows, not the domain's every row: without it, a read of
/// one stream costs as much as the whole domain.
macro_rules! view {
    () => {
        "domain = ?1 AND root = ?2 AND edition IN ('', ?3) AND ((edition = '' AND sequence < \
         COALESCE((SELECT MIN(f.sequence) FROM events AS f \
         WHERE f.domain = ?1 AND f.edition = ?3 AND f.root = ?2 AND ?3 <> ''), \
         9223372036854775807)) OR (?3 <> '' AND edition = ?3))"
    };
}

/// Rows `(sequence, page)`: the stream's last page, as read in its edition.
pub(crate) const LAST: &str = concat!(
    "SELECT sequence, page FROM events WHERE ",
    view!(),
    " ORDER BY sequence DESC LIMIT 1"
);

/// Rows `(sequence, page)` with `?4 <= sequence < ?5`, in order.
pub(crate) const PAGES: &str = concat!(
    "SELECT sequence, page FROM events WHERE ",
    view!(),
    " AND sequence >= ?4 AND sequence < ?5 ORDER BY sequence"
);

/// Rows `(sequence, page)` the command `?4` caused: the dedup lookup.
pub(crate) const OF_COMMAND: &str = concat!(
    "SELECT sequence, page FROM events WHERE ",
    view!(),
    " AND command_id = ?4 ORDER BY sequence"
);

/// Rows `(sequence, page)` of the correlation `?4`.
pub(crate) const OF_CORRELATION: &str = concat!(
    "SELECT sequence, page FROM events WHERE ",
    view!(),
    " AND correlation_id = ?4 ORDER BY sequence"
);

/// Rows `(sequence, page)` recorded at or before `?4` (nanoseconds).
pub(crate) const UNTIL: &str = concat!(
    "SELECT sequence, page FROM events WHERE ",
    view!(),
    " AND recorded_at <= ?4 ORDER BY sequence"
);

/// Row `(MIN(sequence))`: the stream's own first sequence in its edition
/// (`?2`), NULL while it has no row of its own there.
pub(crate) const OWN_FIRST: &str =
    "SELECT MIN(sequence) FROM events WHERE domain = ?1 AND edition = ?2 AND root = ?3";

/// Rows `(edition, domain, root, fork)` of every edition stream holding a
/// row of its own, `fork` its first, the sequence of its fork: by edition
/// name, then stream, as `ListEditions` gives them. The partial index
/// `events_of_editions` holds exactly these rows, so `edition <> ''` stays
/// in the condition for the planner to use it.
pub(crate) const LIST_EDITIONS: &str = "SELECT edition, domain, root, MIN(sequence) FROM events \
     WHERE edition <> '' GROUP BY edition, domain, root ORDER BY edition, domain, root";

/// Row `(1)` while some stream holds a row of edition `?1`, a fork of it
/// remaining: the edition exists.
pub(crate) const EDITION_EXISTS: &str =
    "SELECT 1 FROM events WHERE edition = ?1 AND edition <> '' LIMIT 1";

/// Delete every position on a stream (`?1` domain, `?2` edition column,
/// `?3` root).
pub(crate) const DELETE_POSITIONS: &str =
    "DELETE FROM positions WHERE domain = ?1 AND edition = ?2 AND root = ?3";

/// Keep that the write of an outbox entry (`?1`) touched edition stream
/// `?2` (domain), `?3` (edition), `?4` (root).
pub(crate) const PUT_OUTBOX_EDITION_STREAM: &str =
    "INSERT INTO outbox_edition_streams (id, domain, edition, root) VALUES (?1, ?2, ?3, ?4) \
     ON CONFLICT DO NOTHING";

/// Delete the pending entries of edition `?3` written with its stream
/// `?1` (domain), `?2` (root): the stream's removal takes them,
/// undelivered. Their stream rows go with them (the migration's trigger).
pub(crate) const DELETE_EDITION_OUTBOX: &str = "DELETE FROM outbox WHERE edition = ?3 AND id IN \
     (SELECT id FROM outbox_edition_streams WHERE domain = ?1 AND edition = ?3 AND root = ?2)";

/// Rows `(edition, root)` of `?1`'s streams holding a row of their own,
/// main first (`''` sorts first), then by edition, each by root.
pub(crate) const COVERS: &str =
    "SELECT DISTINCT edition, root FROM events WHERE domain = ?1 ORDER BY edition, root";

/// Rows `(domain, edition, root)` of every stream holding a row, in order.
pub(crate) const ALL_STREAMS: &str =
    "SELECT DISTINCT domain, edition, root FROM events ORDER BY domain, edition, root";

/// Rows `(domain, root)` with a row of correlation `?1` in main or in
/// edition `?2`, of domain `?3` when it is not NULL.
pub(crate) const CORRELATED_STREAMS: &str =
    "SELECT DISTINCT domain, root FROM events WHERE correlation_id = ?1 \
     AND (edition = '' OR edition = ?2) AND (?3 IS NULL OR domain = ?3) \
     ORDER BY domain, root";

/// Row `(next)`: a handler's position (`?4`) on a stream.
pub(crate) const POSITION: &str = "SELECT next FROM positions \
     WHERE domain = ?1 AND edition = ?2 AND root = ?3 AND handler = ?4";

/// Rows `(domain, edition, root, next)` of handler `?1`'s positions above
/// 0, in stream order.
pub(crate) const HANDLER_POSITIONS: &str = "SELECT domain, edition, root, next FROM positions \
     WHERE handler = ?1 AND next > 0 ORDER BY domain, edition, root";

/// Put a handler's position on a stream: a plain upsert.
pub(crate) const PUT_POSITION: &str =
    "INSERT INTO positions (domain, edition, root, handler, next) VALUES (?1, ?2, ?3, ?4, ?5) \
     ON CONFLICT (domain, edition, root, handler) DO UPDATE SET next = excluded.next";

/// Keep an outbox entry, unless its id is pending already, under the
/// stream its write appended to (`?5` to `?7`: domain, edition, root; an
/// empty domain for none), that write's first sequence (`?8`) and its
/// place in the write (`?9`).
pub(crate) const PUT_OUTBOX: &str =
    "INSERT INTO outbox (id, origin, message, edition, domain, stream_edition, root, sequence, place) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) ON CONFLICT (id) DO NOTHING";

/// Rows `(id, origin, message, edition)`: up to `?1` pending entries, each
/// stream's in its order (its sequence, then the place in the write), so a
/// limit takes each stream's oldest first. The order across streams is
/// the index's, which promises nothing.
pub(crate) const PENDING: &str = "SELECT id, origin, message, edition FROM outbox \
     ORDER BY domain, stream_edition, root, sequence, place LIMIT ?1";

/// Rows `(domain, edition, root, sequence, page)`: every stored page, by
/// stream (the key's order: main's edition `''` first) and sequence. The
/// export's.
pub(crate) const EXPORT_PAGES: &str = "SELECT domain, edition, root, sequence, page FROM events \
     ORDER BY domain, edition, root, sequence";

/// Rows `(domain, edition, root, snapshot)`: every stored snapshot.
pub(crate) const EXPORT_SNAPSHOTS: &str =
    "SELECT domain, edition, root, snapshot FROM snapshots ORDER BY domain, edition, root";

/// Rows `(domain, edition, root, handler, next)`: every position above 0,
/// by stream and handler.
pub(crate) const EXPORT_POSITIONS: &str =
    "SELECT domain, edition, root, handler, next FROM positions \
     WHERE next > 0 ORDER BY domain, edition, root, handler";

/// Rows `(domain, edition, root, at_kind, at, component, letter)`: every
/// dead letter, by stream and then key (place, component).
pub(crate) const EXPORT_DEAD_LETTERS: &str =
    "SELECT domain, edition, root, at_kind, at, component, letter FROM dead_letters \
     ORDER BY domain, edition, root, at_kind, at, component";

/// Clear one pending entry by its id.
pub(crate) const CLEAR: &str = "DELETE FROM outbox WHERE id = ?1";

/// Keep a dead letter (`?1` to `?3`: domain, edition, root; `?4` its
/// place's kind, `?5` its place, `?6` its component, `?7` the letter),
/// replacing the one kept under its key.
pub(crate) const PUT_DEAD_LETTER: &str =
    "INSERT INTO dead_letters (domain, edition, root, at_kind, at, component, letter) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
     ON CONFLICT (domain, edition, root, at_kind, at, component) DO UPDATE SET letter = excluded.letter";

/// Rows `(at_kind, at, letter)`: a stream's dead letters, in key order
/// (place, then component).
pub(crate) const DEAD_LETTERS: &str = "SELECT at_kind, at, letter FROM dead_letters \
     WHERE domain = ?1 AND edition = ?2 AND root = ?3 ORDER BY at_kind, at, component";

/// Rows `(at_kind, at, letter)`: the dead letter of one key.
pub(crate) const DEAD_LETTER: &str = "SELECT at_kind, at, letter FROM dead_letters \
     WHERE domain = ?1 AND edition = ?2 AND root = ?3 AND at_kind = ?4 AND at = ?5 \
     AND component = ?6";

/// Rows `(edition, root)` of `?1`'s streams a dead letter is about, main
/// first, then by edition, each by root.
pub(crate) const DEAD_LETTERED: &str =
    "SELECT DISTINCT edition, root FROM dead_letters WHERE domain = ?1 ORDER BY edition, root";

/// Delete the dead letter of one key.
pub(crate) const REMOVE_DEAD_LETTER: &str = "DELETE FROM dead_letters \
     WHERE domain = ?1 AND edition = ?2 AND root = ?3 AND at_kind = ?4 AND at = ?5 \
     AND component = ?6";

/// Delete every dead letter about a stream.
pub(crate) const DELETE_DEAD_LETTERS: &str =
    "DELETE FROM dead_letters WHERE domain = ?1 AND edition = ?2 AND root = ?3";

/// Row `(data_version, total_changes())`: what a change token is made of.
/// Row `(identity, head)`: the feed's identity and the sequence of its last
/// change (0 if none).
pub(crate) const FEED: &str =
    "SELECT identity, COALESCE((SELECT MAX(seq) FROM changes), 0) FROM feed";

/// Rows `(seq, domain, edition, root, purged)` of the changes after `?1`, at
/// most `?2`, in the order they landed.
pub(crate) const CHANGES_AFTER: &str = "SELECT seq, domain, edition, root, purged FROM changes \
     WHERE seq > ?1 ORDER BY seq LIMIT ?2";

/// A change: stream `?1` (domain), `?2` (edition column), `?3` (root) was
/// written to (`?4` 0) or purged (1).
pub(crate) const TOUCH: &str =
    "INSERT INTO changes (domain, edition, root, purged) VALUES (?1, ?2, ?3, ?4)";

/// How many changes the feed keeps.
pub(crate) const CHANGES_KEPT: i64 = 65_536;

/// Forget the changes before the last [`CHANGES_KEPT`].
pub(crate) const TRIM: &str =
    "DELETE FROM changes WHERE seq <= (SELECT MAX(seq) FROM changes) - 65536";

pub(crate) const CHANGE_TOKEN: &str =
    "SELECT data_version, total_changes() FROM pragma_data_version";

/// Row `(done)`: whether the events' meta columns are filled.
pub(crate) const BACKFILL_DONE: &str = "SELECT done FROM backfills WHERE name = ?1";

/// Rows `(domain, edition, root, sequence, page)` with no meta columns.
pub(crate) const UNFILLED: &str =
    "SELECT domain, edition, root, sequence, page FROM events WHERE recorded_at IS NULL";

/// Fill one event's meta columns.
pub(crate) const FILL: &str = "UPDATE events SET correlation_id = ?5, recorded_at = ?6 \
     WHERE domain = ?1 AND edition = ?2 AND root = ?3 AND sequence = ?4";

/// Mark a backfill done.
pub(crate) const BACKFILL_RAN: &str = "UPDATE backfills SET done = 1 WHERE name = ?1";

/// The backfill of the events' correlation and time columns.
pub(crate) const EVENTS_META_BACKFILL: &str = "events-meta-columns";

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

/// Whether a row is `cover`'s, by its table's key columns.
fn of_stream<D: Dialect>(
    cover: &Cover,
    domain: impl sea_query::IntoColumnRef,
    edition: impl sea_query::IntoColumnRef,
    root_column: impl sea_query::IntoColumnRef,
) -> Condition {
    Cond::all()
        .add(Expr::col(domain).eq(cover.domain.clone()))
        .add(Expr::col(edition).eq(D::edition(cover)))
        .add(Expr::col(root_column).eq(root(cover)))
}

fn event_of<D: Dialect>(cover: &Cover) -> Condition {
    of_stream::<D>(cover, Events::Domain, Events::Edition, Events::Root)
}

fn snapshot_of_stream<D: Dialect>(cover: &Cover) -> Condition {
    of_stream::<D>(
        cover,
        Snapshots::Domain,
        Snapshots::Edition,
        Snapshots::Root,
    )
}

/// `pages`, as the rows they are stored as: one statement.
pub(crate) fn insert_pages<D: Dialect>(
    cover: &Cover,
    pages: &[EventPage],
) -> Result<InsertStatement, StoreError> {
    let mut insert = Query::insert()
        .into_table(Events::Table)
        .columns([
            Events::Domain,
            Events::Edition,
            Events::Root,
            Events::Sequence,
            Events::CommandId,
            Events::Page,
            Events::CorrelationId,
            Events::RecordedAt,
        ])
        .to_owned();
    for page in pages {
        let command_id = page
            .meta
            .as_ref()
            .and_then(|m| m.causation.as_ref())
            .and_then(|causation| match causation {
                Causation::Command(cause) => cause.command_id.as_ref(),
                Causation::Fact(cause) => cause.command_id.as_ref(),
            })
            .map(|id| id.value.to_vec());
        let (correlation_id, recorded_at) = meta_columns(page);
        insert
            .values([
                cover.domain.clone().into(),
                D::edition(cover).into(),
                root(cover).into(),
                sequence_column(page.sequence).into(),
                command_id.into(),
                page.encode_to_vec().into(),
                correlation_id.into(),
                recorded_at.into(),
            ])
            .map_err(unavailable)?;
    }
    Ok(insert)
}

/// Row `(snapshot)`: the stream's newest snapshot.
pub(crate) fn snapshot_of<D: Dialect>(cover: &Cover) -> sea_query::SelectStatement {
    Query::select()
        .column(Snapshots::Snapshot)
        .from(Snapshots::Table)
        .cond_where(snapshot_of_stream::<D>(cover))
        .to_owned()
}

/// Delete the stream's own events, and with them the command ids recorded
/// in their rows.
pub(crate) fn delete_events<D: Dialect>(cover: &Cover) -> DeleteStatement {
    Query::delete()
        .from_table(Events::Table)
        .cond_where(event_of::<D>(cover))
        .to_owned()
}

/// Delete the main stream's events from `sequence` on, and with them the
/// command ids recorded in their rows.
pub(crate) fn delete_events_from<D: Dialect>(cover: &Cover, sequence: u64) -> DeleteStatement {
    Query::delete()
        .from_table(Events::Table)
        .cond_where(event_of::<D>(cover))
        .and_where(Expr::col(Events::Sequence).gte(sequence_column(sequence)))
        .to_owned()
}

/// Rows `(sequence, page)` of a main stream whose page holds the bytes `?4`
/// (the tombstone's type URL), the latest first: candidates for its
/// tombstones, which the caller decodes to be sure.
pub(crate) const TOMBSTONE_CANDIDATES: &str = "SELECT sequence, page FROM events WHERE \
     domain = ?1 AND root = ?2 AND edition = ?3 AND instr(page, ?4) > 0 ORDER BY sequence DESC";

/// Delete the stream's snapshot.
pub(crate) fn delete_snapshot<D: Dialect>(cover: &Cover) -> DeleteStatement {
    Query::delete()
        .from_table(Snapshots::Table)
        .cond_where(snapshot_of_stream::<D>(cover))
        .to_owned()
}

/// Keep `snapshot` as the stream's newest, replacing the one before.
pub(crate) fn upsert_snapshot<D: Dialect>(
    cover: &Cover,
    snapshot: &Snapshot,
) -> Result<InsertStatement, StoreError> {
    Ok(Query::insert()
        .into_table(Snapshots::Table)
        .columns([
            Snapshots::Domain,
            Snapshots::Edition,
            Snapshots::Root,
            Snapshots::Snapshot,
        ])
        .values([
            cover.domain.clone().into(),
            D::edition(cover).into(),
            root(cover).into(),
            snapshot.encode_to_vec().into(),
        ])
        .map_err(unavailable)?
        .on_conflict(
            OnConflict::columns([Snapshots::Domain, Snapshots::Edition, Snapshots::Root])
                .update_column(Snapshots::Snapshot)
                .to_owned(),
        )
        .to_owned())
}

#[cfg(test)]
#[path = "statements.test.rs"]
mod tests;
