//! The base ports and the opt-in ones that are one statement or one
//! transaction each: events, snapshots, positions, purge, the outbox, dead
//! letters, the commit signal and the change token.

use angzarr_ports::angzarr_proto::v1::{Cover, EventPage, Snapshot};
use angzarr_ports::{
    ChangeToken, CommitNotify, CommitWait, DeadLetter, DeadLetterKey, DeadLetterStore, EventStore,
    Head, Outbox, OutboxEntry, PositionKey, PositionStore, Purge, Slice, SnapshotStore, StoreError,
    StreamRef, Uuid, Watch, Write, Written,
};
use prost::Message;

use crate::error::undecodable;
use crate::queries::{Key, Queries};
use crate::writes::{in_transaction, Writes};
use crate::{rows, statements, DuckStore};

impl EventStore for DuckStore {
    /// One transaction, begun and committed inside the call: the stream's
    /// length checked against `expected`, its pages inserted, the positions
    /// upserted, the outbox entries it clears cleared and then its own and
    /// its dead letters kept. A failure lands nothing.
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        let done = self.with(|conn| in_transaction(conn, |tx| tx.apply(&self.shared.feed, &write)));
        self.shared.pushed(done)
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.with(|conn| conn.read(stream, from, to))
    }

    async fn head(&self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        self.with(|conn| Queries::head(conn, stream))
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.with(|conn| conn.find_command(stream, command_id))
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.with(|conn| Queries::covers(conn, domain))
    }
}

impl SnapshotStore for DuckStore {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        self.with(|conn| conn.snapshot(stream))
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        let key = Key::from(stream);
        let bytes = snapshot.encode_to_vec();
        let written = self.with(|conn| {
            conn.run(statements::UPSERT_SNAPSHOT, &key.with(&[&bytes]))
                .map(drop)
        });
        self.shared.pushed(written)
    }
}

impl PositionStore for DuckStore {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        self.with(|conn| Queries::position(conn, key))
    }
}

impl Purge for DuckStore {
    /// One transaction, as a write's: a purge lands wholly before or wholly
    /// after any write, or not at all. The stream's head is checked against
    /// `expected` inside it; a refusal rolls back, deleting nothing.
    async fn purge(&self, stream: StreamRef<'_>, expected: u64) -> Result<u64, StoreError> {
        let purged = self.with(|conn| {
            in_transaction(conn, |tx| {
                tx.purge_stream(&self.shared.feed, stream, expected)
            })
        });
        self.shared.pushed(purged)
    }
}

impl DeadLetterStore for DuckStore {
    async fn dead_letters(&self, stream: StreamRef<'_>) -> Result<Vec<DeadLetter>, StoreError> {
        self.with(|conn| conn.letters_of(stream))
    }

    async fn dead_letter(&self, key: DeadLetterKey<'_>) -> Result<Option<DeadLetter>, StoreError> {
        self.with(|conn| conn.letter(key))
    }

    async fn dead_lettered(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        let stored = self.with(|conn| {
            conn.rows(statements::DEAD_LETTERED, &[&domain], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
        })?;
        stored
            .into_iter()
            .map(|(edition, root)| rows::cover(domain, edition, &root))
            .collect()
    }

    /// One transaction, as a write's, so what it answers is what it removed.
    async fn drain_dead_letters(
        &self,
        stream: StreamRef<'_>,
    ) -> Result<Vec<DeadLetter>, StoreError> {
        let drained = self.with(|conn| {
            in_transaction(conn, |tx| {
                let letters = tx.letters_of(stream)?;
                tx.delete_letters(stream)?;
                Ok(letters)
            })
        });
        self.shared.pushed(drained)
    }

    async fn remove_dead_letters(&self, keys: &[DeadLetterKey<'_>]) -> Result<u64, StoreError> {
        let removed = self.with(|conn| {
            in_transaction(conn, |tx| {
                let mut removed = 0;
                for key in keys {
                    removed += tx.delete_letter(*key)?;
                }
                Ok(removed as u64)
            })
        });
        self.shared.pushed(removed)
    }
}

/// An outbox row as stored: `(id, origin, message, edition)`.
type StoredEntry = (Vec<u8>, String, Vec<u8>, Option<String>);

/// Stored rows as outbox entries.
pub(crate) fn outbox_entries(stored: Vec<StoredEntry>) -> Result<Vec<OutboxEntry>, StoreError> {
    stored
        .into_iter()
        .map(|(id, origin, message, edition)| {
            Ok(OutboxEntry {
                id: rows::stream_root(&id)?,
                origin,
                message: prost_types::Any::decode(message.as_slice())
                    .map_err(|e| StoreError::Unavailable(undecodable(e)))?,
                edition,
            })
        })
        .collect()
}

/// The pending outbox, up to `limit`.
pub(crate) fn pending(
    conn: &duckdb::Connection,
    limit: i64,
) -> Result<Vec<OutboxEntry>, StoreError> {
    let stored = conn.rows(statements::PENDING, &[&limit], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    })?;
    outbox_entries(stored)
}

impl Outbox for DuckStore {
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.with(|conn| pending(conn, limit))
    }

    async fn clear(&self, ids: &[Uuid]) -> Result<u64, StoreError> {
        let cleared = self.with(|conn| in_transaction(conn, |tx| tx.clear_outbox(ids)));
        self.shared.pushed(cleared)
    }
}

impl CommitNotify for DuckStore {
    /// Every commit through any handle of this store: a write, a snapshot,
    /// a purge, a dead letter drained or removed, an outbox entry cleared.
    fn next_commit(&self) -> CommitWait {
        self.shared.signal.wait()
    }
}

impl Watch for DuckStore {
    /// How many commits the store has made: it moves with every commit
    /// through any handle. (Another process cannot commit: the file has
    /// one owner.)
    async fn change_token(&self) -> Result<ChangeToken, StoreError> {
        Ok(ChangeToken::new([
            self.shared.commits(),
            self.shared.identity,
        ]))
    }
}

#[cfg(test)]
#[path = "ports.test.rs"]
mod tests;
