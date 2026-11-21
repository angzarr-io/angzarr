//! The change feed (angzarr-ports' `Changes`), kept in the database in the
//! transactions of the commits it tells of: it survives a reopen, and an
//! unreadable cursor is an overflow.

use angzarr_ports::{ChangeCursor, Changed, Changes, Head, StoreError, StreamChange, StreamRef};
use duckdb::Connection;

use crate::queries::Queries;
use crate::{rows, statements, DuckStore};

/// One row of the feed: `(seq, domain, edition, root, purged)`.
type Entry = (i64, String, String, Vec<u8>, bool);

/// The feed's last change's place.
fn feed_head(conn: &Connection) -> Result<u64, StoreError> {
    let head = conn.first(statements::FEED_HEAD, &[], |row| row.get::<_, i64>(0))?;
    Ok(head.unwrap_or_default().unsigned_abs())
}

/// Whether `position` of feed `identity` is one the feed can continue
/// from: this feed's, within what it keeps, up to its head.
fn understood(since: &ChangeCursor, identity: u64, head: u64) -> Option<u64> {
    let floor = head.saturating_sub(statements::CHANGES_KEPT.unsigned_abs());
    match since.parts() {
        Some((same, position)) if same == identity && (floor..=head).contains(&position) => {
            Some(position)
        }
        _ => None,
    }
}

/// The entries folded into the streams they touched, each once, in the
/// order first touched, and the place of the last.
fn folded(entries: Vec<Entry>, from: u64) -> Result<(Vec<StreamChange>, u64), StoreError> {
    let mut next = from;
    let mut changes: Vec<StreamChange> = Vec::new();
    for (seq, domain, edition, root, purged) in entries {
        next = seq.unsigned_abs();
        let stream = rows::cover(&domain, edition, &root)?;
        match changes.iter_mut().find(|change| change.stream == stream) {
            Some(seen) => seen.purged |= purged,
            None => changes.push(StreamChange {
                stream,
                head: 0,
                purged,
            }),
        }
    }
    Ok((changes, next))
}

impl DuckStore {
    /// The streams touched after `at`, at most `limit` entries' worth,
    /// each with its head now.
    fn touched_after(
        conn: &Connection,
        at: u64,
        limit: usize,
    ) -> Result<(Vec<StreamChange>, u64), StoreError> {
        let after = i64::try_from(at).unwrap_or(i64::MAX);
        let limit = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        let entries = conn.rows(statements::CHANGES_AFTER, &[&after, &limit], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;
        let (mut changes, next) = folded(entries, at)?;
        for change in &mut changes {
            let head: Head = Queries::head(conn, StreamRef::try_from(&change.stream)?)?;
            change.head = head.next_sequence;
        }
        Ok((changes, next))
    }
}

impl Changes for DuckStore {
    async fn changes_cursor(&self) -> Result<ChangeCursor, StoreError> {
        let head = self.with(feed_head)?;
        Ok(ChangeCursor::at(self.shared.identity, head))
    }

    async fn changed_since(
        &self,
        since: &ChangeCursor,
        limit: usize,
    ) -> Result<Changed, StoreError> {
        let identity = self.shared.identity;
        self.with(|conn| {
            let head = feed_head(conn)?;
            let Some(at) = understood(since, identity, head) else {
                return Ok(Changed::Overflow {
                    next: ChangeCursor::at(identity, head),
                });
            };
            let (changes, next) = DuckStore::touched_after(conn, at, limit)?;
            Ok(Changed::Streams {
                changes,
                next: ChangeCursor::at(identity, next),
            })
        })
    }
}

#[cfg(test)]
#[path = "feed.test.rs"]
mod tests;
