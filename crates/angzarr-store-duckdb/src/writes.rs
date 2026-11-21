//! The writes: one handling step's transaction, a purge, a drain. Each is a
//! DuckDB transaction begun and committed inside the call, so nothing is
//! held open between calls and a failure lands nothing (an uncommitted
//! transaction rolls back when dropped).

use angzarr_ports::angzarr_proto::v1::EventPage;
use angzarr_ports::{
    is_fork, Append, DeadLetter, DeadLetterKey, OutboxEntry, PositionKey, StoreError, StreamRef,
    Uuid, Write, Written,
};
use duckdb::types::ToSql;
use duckdb::Connection;
use prost::Message;

use crate::error::unavailable;
use crate::queries::{Key, Queries};
use crate::store::Feed;
use crate::{rows, statements};

/// The stream a write's outbox entries order on, and the sequence there.
type OrderedOn<'a> = Option<(StreamRef<'a>, u64)>;

/// The stream the entries order on: the one the write appends to, at its
/// first sequence; for a write appending none (a policy's step), its
/// trigger's, at the triggering event's sequence, the position it moves to
/// less one (angzarr-ports' O7).
fn ordered_on<'a>(write: &Write<'a>) -> OrderedOn<'a> {
    match write.events {
        Some(append) => Some((append.stream, append.expected)),
        None => write
            .positions
            .first()
            .map(|(key, next)| (key.stream, next.saturating_sub(1))),
    }
}

/// The writes, as an extension of DuckDB's connection. They run inside a
/// transaction the caller began: a failure returns, and dropping the
/// transaction rolls it back.
pub(crate) trait Writes: Queries {
    /// A whole write: events, positions, cleared and new outbox entries,
    /// dead letters.
    fn apply(&self, feed: &Feed, write: &Write<'_>) -> Result<Written, StoreError> {
        let next_sequence = match write.events {
            Some(append) => Some(self.append(append)?),
            None => None,
        };
        if let Some(append) = write.events.filter(|append| !append.pages.is_empty()) {
            self.touch(feed, append.stream, false)?;
        }
        for (key, next) in write.positions {
            self.put_position(*key, *next)?;
        }
        let at = ordered_on(write);
        for id in write.cleared {
            self.run(statements::CLEAR, &[&id.as_bytes().as_slice()])?;
        }
        for (place, entry) in write.outbox.iter().enumerate() {
            self.put_outbox(entry, at, place)?;
        }
        for letter in write.dead_letters {
            self.put_dead_letter(letter)?;
        }
        Ok(Written { next_sequence })
    }

    /// The events of a write: checked against the stream as read in its
    /// edition, then inserted. The stream's new length.
    fn append(&self, append: Append<'_>) -> Result<u64, StoreError> {
        let Append {
            stream,
            expected,
            pages,
        } = append;
        let actual = self.head(stream)?.next_sequence;
        let creates = pages.first().is_some_and(is_fork);
        match stream.edition {
            None if creates => return Err(StoreError::EditionNameRequired),
            None => {}
            Some(_) => {
                let own = self.extent(&Key::from(stream))?;
                match (own.is_some(), creates) {
                    (true, true) => return Err(StoreError::edition_already_forked(stream)),
                    (false, false) if !pages.is_empty() => {
                        return Err(StoreError::edition_not_forked(stream))
                    }
                    // A fork lands at its fork sequence, its own page's
                    // sequence: main's head or any sequence below it.
                    (false, true) if expected <= actual => {
                        return self.insert(stream, expected, pages);
                    }
                    _ => {}
                }
            }
        }
        if expected != actual {
            return Err(StoreError::conflict(stream, expected, actual));
        }
        self.insert(stream, expected, pages)
    }

    /// `pages`, continuing the stream from `expected`, inserted: the
    /// stream's new length.
    fn insert(
        &self,
        stream: StreamRef<'_>,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        for (due, page) in (expected..).zip(pages) {
            if page.sequence != due {
                return Err(StoreError::discontinuous(stream, due, page.sequence));
            }
        }
        let Some(last) = pages.last() else {
            return Ok(expected);
        };
        let key = Key::from(stream);
        for page in pages {
            self.insert_page(&key, page)?;
        }
        let next = expected + pages.len() as u64;
        self.set_extent(&key, (expected, next), last)?;
        Ok(next)
    }

    /// One page as a row.
    fn insert_page(&self, key: &Key, page: &EventPage) -> Result<(), StoreError> {
        let sequence = statements::sequence_column(page.sequence);
        let command = statements::command_column(page);
        let (correlation, recorded_at) = statements::meta_columns(page);
        let bytes = page.encode_to_vec();
        let params = key.with(&[&sequence, &command, &correlation, &recorded_at, &bytes]);
        self.run(statements::INSERT_PAGE, &params).map(drop)
    }

    /// The stream's extent after an append: its first sequence stays, if it
    /// had one.
    fn set_extent(
        &self,
        key: &Key,
        (first, next): (u64, u64),
        last: &EventPage,
    ) -> Result<(), StoreError> {
        let at = last
            .meta
            .as_ref()
            .and_then(|meta| meta.recorded_at.as_ref());
        let first = statements::sequence_column(first);
        let next = statements::sequence_column(next);
        let seconds = at.map(|at| at.seconds);
        let nanos = at.map(|at| at.nanos);
        let moved = key.with(&[&next, &seconds, &nanos]);
        if self.run(statements::UPDATE_EXTENT, &moved)? == 0 {
            let made = key.with(&[&first, &next, &seconds, &nanos]);
            self.run(statements::INSERT_EXTENT, &made)?;
        }
        Ok(())
    }

    /// The change feed learns `stream` was written to, or purged, in this
    /// transaction; it keeps the last [`statements::CHANGES_KEPT`].
    fn touch(&self, feed: &Feed, stream: StreamRef<'_>, purged: bool) -> Result<(), StoreError> {
        let key = Key::from(stream);
        let seq = feed.next();
        let params: [&dyn ToSql; 5] = [&seq, &key.domain, &key.edition, &key.root, &purged];
        self.run(statements::TOUCH, &params)?;
        if seq % statements::TRIM_EVERY == 0 {
            self.run(statements::TRIM, &[])?;
        }
        Ok(())
    }

    fn put_position(&self, key: PositionKey<'_>, next: u64) -> Result<(), StoreError> {
        let stream = Key::from(key.stream);
        let next = statements::sequence_column(next);
        let moved = stream.with(&[&key.handler, &next]);
        if self.run(statements::UPDATE_POSITION, &moved)? == 0 {
            self.run(statements::INSERT_POSITION, &moved)?;
        }
        Ok(())
    }

    /// Keep `entry`, the `place`th of its write, under `at`: the stream it
    /// orders on and the sequence there, or no stream at 0.
    fn put_outbox(
        &self,
        entry: &OutboxEntry,
        at: OrderedOn<'_>,
        place: usize,
    ) -> Result<(), StoreError> {
        let (stream, sequence) = match at {
            Some((stream, sequence)) => (Key::from(stream), sequence),
            None => (
                Key {
                    domain: String::new(),
                    edition: String::new(),
                    root: Vec::new(),
                },
                0,
            ),
        };
        let id = entry.id.as_bytes().to_vec();
        let message = entry.message.encode_to_vec();
        let sequence = statements::sequence_column(sequence);
        let place = i64::try_from(place).unwrap_or(i64::MAX);
        let params: [&dyn ToSql; 9] = [
            &id,
            &stream.domain,
            &stream.edition,
            &stream.root,
            &sequence,
            &place,
            &entry.origin,
            &message,
            &entry.edition,
        ];
        self.run(statements::PUT_OUTBOX, &params).map(drop)
    }

    /// Keep `letter`, replacing the one kept under its key.
    fn put_dead_letter(&self, letter: &DeadLetter) -> Result<(), StoreError> {
        let key = letter.key()?;
        let stream = Key::from(key.stream);
        let (kind, at) = rows::place(key.at);
        let bytes = letter.letter.encode_to_vec();
        let params = stream.with(&[&kind, &at, &key.component, &bytes]);
        self.run(statements::PUT_DEAD_LETTER, &params).map(drop)
    }

    /// Delete every letter about `stream`: how many.
    fn delete_letters(&self, stream: StreamRef<'_>) -> Result<usize, StoreError> {
        self.run(statements::DELETE_DEAD_LETTERS, &Key::from(stream).params())
    }

    /// Delete the letter of one key: how many.
    fn delete_letter(&self, key: DeadLetterKey<'_>) -> Result<usize, StoreError> {
        let stream = Key::from(key.stream);
        let (kind, at) = rows::place(key.at);
        let params = stream.with(&[&kind, &at, &key.component]);
        self.run(statements::REMOVE_DEAD_LETTER, &params)
    }

    /// A purge, its head checked against `expected`: the stream's own pages,
    /// extent, snapshot and letters deleted, positions kept. How many pages.
    fn purge_stream(
        &self,
        feed: &Feed,
        stream: StreamRef<'_>,
        expected: u64,
    ) -> Result<u64, StoreError> {
        let actual = self.head(stream)?.next_sequence;
        if actual != expected {
            return Err(StoreError::conflict(stream, expected, actual));
        }
        let key = Key::from(stream);
        let deleted = self.run(statements::DELETE_EVENTS, &key.params())?;
        self.run(statements::DELETE_EXTENT, &key.params())?;
        self.run(statements::DELETE_SNAPSHOT, &key.params())?;
        self.delete_letters(stream)?;
        self.touch(feed, stream, true)?;
        Ok(deleted as u64)
    }

    /// Clear delivered outbox entries by id: how many.
    fn clear_outbox(&self, ids: &[Uuid]) -> Result<u64, StoreError> {
        let mut cleared = 0;
        for id in ids {
            cleared += self.run(statements::CLEAR, &[&id.as_bytes().as_slice()])?;
        }
        Ok(cleared as u64)
    }
}

impl Writes for Connection {}

/// Run `work` in a transaction on `conn`: committed if it succeeds, rolled
/// back by the drop if it fails.
pub(crate) fn in_transaction<T>(
    conn: &Connection,
    work: impl FnOnce(&Connection) -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    let tx = conn.unchecked_transaction().map_err(unavailable)?;
    let done = work(&tx)?;
    tx.commit().map_err(unavailable)?;
    Ok(done)
}

#[cfg(test)]
#[path = "writes.test.rs"]
mod tests;
