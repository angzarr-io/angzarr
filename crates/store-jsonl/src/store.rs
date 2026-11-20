//! The store: the in-memory store, and the journal that makes it durable.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use angzarr_ports::angzarr_proto::v1::{Cover, EditionSummary, EventBook, EventPage, Snapshot};
use angzarr_ports::memory::{MemoryRead, MemoryStore};
use angzarr_ports::{
    ChangeCursor, ChangeToken, Changed, Changes, CommitNotify, CommitSignal, CommitWait,
    DeadLetter, DeadLetterKey, DeadLetterStore, Editions, EventStore, Head, ListStreams, Outbox,
    OutboxEntry, PositionKey, PositionStore, Processed, Purge, ReadByCorrelation, ReadStore,
    ReadUntilTime, Slice, SnapshotStore, StoreError, StreamRef, Uuid, Watch, Write, Written,
};
use file_system::Fs;
use prost_types::Timestamp;

use crate::entry::{self, Plan};
use crate::error::{Failure, OpenError, TornTail};
use crate::journal;
pub use crate::journal::Durability;
pub(crate) use crate::journal::Sink;
use crate::ready::ready;
use crate::schema::Schema;
use file_lock::Locked;

/// How a store is opened.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Options {
    pub durability: Durability,
}

/// An opened store and what opening had to repair.
#[derive(Debug)]
pub struct Opened {
    pub store: JsonlStore,
    /// The torn last line opening removed, if there was one.
    pub recovered: Option<TornTail>,
}

/// The store. Clones are handles on one store and share its lock; the
/// journal is released when the last one is dropped.
#[derive(Clone, Debug)]
pub struct JsonlStore {
    shared: Arc<Shared>,
}

/// What every handle shares.
struct Shared {
    path: PathBuf,
    inner: MemoryStore,
    schema: Schema,
    /// Held by every operation, reads included, for as long as it runs:
    /// the in-memory commit and the append are one step, so the journal's
    /// order is the commit order and no reader sees a state the journal
    /// does not hold.
    gate: Mutex<Gate>,
    /// Notified once an operation's line is in the journal, never before
    /// it lands and never for one refused or whose line did not land: the
    /// in-memory store's own signal fires before the append, so it is not
    /// handed out.
    commits: CommitSignal,
    /// The filesystem the journal is on, kept for as long as the store is.
    _fs: Arc<dyn Fs>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonlStore")
            .field("journal", &self.path)
            .finish_non_exhaustive()
    }
}

struct Gate {
    sink: Box<dyn Sink>,
    /// An append failed: the in-memory store may be ahead of the journal.
    poisoned: bool,
    /// Released after the sink, when the last handle goes.
    _lock: Locked,
}

impl JsonlStore {
    /// Open the journal at `journal`, an absolute path, with the default
    /// [`Options`]. See [`JsonlStore::open_with`].
    pub fn open(fs: Arc<dyn Fs>, journal: &Path) -> Result<Opened, OpenError> {
        JsonlStore::open_with(fs, journal, Options::default())
    }

    /// Open the journal at `journal`, an absolute path, making it (and its
    /// directory) if it is not there: take its exclusive lock (refused with
    /// [`OpenError::InUse`] while another store holds it), replay it into
    /// memory, and repair a torn last line, reported in
    /// [`Opened::recovered`].
    pub fn open_with(
        fs: Arc<dyn Fs>,
        journal: &Path,
        options: Options,
    ) -> Result<Opened, OpenError> {
        JsonlStore::open_wrapping(fs, journal, options, |sink| sink)
    }

    /// Open as [`JsonlStore::open_with`], with the journal's sink passed
    /// through `wrap`: how the tests fail or tear an append.
    pub(crate) fn open_wrapping(
        fs: Arc<dyn Fs>,
        journal: &Path,
        options: Options,
        wrap: impl FnOnce(Box<dyn Sink>) -> Box<dyn Sink>,
    ) -> Result<Opened, OpenError> {
        let schema = Schema::new();
        let inner = MemoryStore::new();
        let opened = journal::open(&fs, journal, options.durability, &schema, &inner)?;
        let gate = Gate {
            sink: wrap(opened.sink),
            poisoned: false,
            _lock: opened.lock,
        };
        let store = JsonlStore {
            shared: Arc::new(Shared {
                path: journal.to_path_buf(),
                inner,
                schema,
                gate: Mutex::new(gate),
                commits: CommitSignal::new(),
                _fs: fs,
            }),
        };
        Ok(Opened {
            store,
            recovered: opened.recovered,
        })
    }

    /// The journal's path.
    pub fn journal(&self) -> &Path {
        &self.shared.path
    }

    fn gate(&self) -> MutexGuard<'_, Gate> {
        // A panic under the lock leaves the journal whole (a line is
        // appended or not) and the in-memory store checked-then-changed in
        // one step, so a poisoned mutex is still sound.
        self.shared
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Run a read against the in-memory store, under the lock the writes
    /// take, so it sees only what the journal holds.
    fn observe<T>(
        &self,
        read: impl AsyncFnOnce(&MemoryStore) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let gate = self.gate();
        if gate.poisoned {
            return Err(Failure::Poisoned.into_store_error());
        }
        ready(read(&self.shared.inner))
            .unwrap_or_else(|| Err(Failure::Suspended.into_store_error()))
    }

    /// Run an operation against the in-memory store and, if it accepted it,
    /// append `plan`'s line before returning. An operation the store refuses
    /// appends nothing; one it accepted whose line could not be appended
    /// leaves the store poisoned.
    fn commit<T>(
        &self,
        plan: Plan,
        op: impl AsyncFnOnce(&MemoryStore) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut gate = self.gate();
        if gate.poisoned {
            return Err(Failure::Poisoned.into_store_error());
        }
        // The line is made before the operation runs, so that a line that
        // cannot be made refuses the operation instead of following it.
        let line =
            match &plan {
                Plan::Line(entry) => Some(journal::encode(&self.shared.schema, entry).map_err(
                    |ext_err_txt| Failure::EncodeFailed { ext_err_txt }.into_store_error(),
                )?),
                Plan::Skip | Plan::Unencodable => None,
            };
        let Some(result) = ready(op(&self.shared.inner)) else {
            gate.poisoned = true;
            return Err(Failure::Suspended.into_store_error());
        };
        let value = result?;
        let appended = match (line, plan) {
            (Some(line), _) => gate
                .sink
                .append(line.as_bytes())
                .map(|()| true)
                .map_err(|e| Failure::AppendFailed {
                    ext_err_txt: e.to_string(),
                }),
            // The store accepted what no entry can record.
            (None, Plan::Unencodable) => Err(Failure::EncodeFailed {
                ext_err_txt: "an accepted operation has no entry".to_string(),
            }),
            // Nothing changed: no line, and no commit to wake anyone for.
            (None, _) => Ok(false),
        };
        match appended {
            Err(failure) => {
                gate.poisoned = true;
                Err(failure.into_store_error())
            }
            Ok(true) => {
                drop(gate);
                self.shared.commits.notify();
                Ok(value)
            }
            Ok(false) => Ok(value),
        }
    }
}

impl EventStore for JsonlStore {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        self.commit(entry::write(&write), async |inner| inner.write(write).await)
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.observe(async |inner| inner.read(stream, from, to).await)
    }

    async fn head(&self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        self.observe(async |inner| inner.head(stream).await)
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.observe(async |inner| inner.find_command(stream, command_id).await)
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.observe(async |inner| inner.covers(domain).await)
    }
}

impl SnapshotStore for JsonlStore {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        self.observe(async |inner| inner.read_snapshot(stream).await)
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        self.commit(entry::snapshot(stream, snapshot), async |inner| {
            inner.write_snapshot(stream, snapshot).await
        })
    }
}

impl PositionStore for JsonlStore {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        self.observe(async |inner| inner.position(key).await)
    }
}

impl Purge for JsonlStore {
    async fn purge(&self, stream: StreamRef<'_>, expected: u64) -> Result<u64, StoreError> {
        self.commit(entry::purge(stream, expected), async |inner| {
            inner.purge(stream, expected).await
        })
    }
}

impl Editions for JsonlStore {
    async fn list_editions(&self) -> Result<Vec<EditionSummary>, StoreError> {
        self.observe(async |inner| inner.list_editions().await)
    }

    async fn remove_edition_stream(&self, stream: StreamRef<'_>) -> Result<u64, StoreError> {
        self.commit(entry::remove_edition_stream(stream), async |inner| {
            inner.remove_edition_stream(stream).await
        })
    }
}

impl ReadStore for JsonlStore {
    type Reads<'s> = MemoryRead;

    async fn begin_read(&self) -> Result<MemoryRead, StoreError> {
        self.observe(async |inner| inner.begin_read().await)
    }
}

impl Changes for JsonlStore {
    /// The feed of the in-memory store the journal is replayed into, read
    /// under the gate like every read, so it tells only what is in the
    /// journal. It is a process's: a cursor from before a reopen is another
    /// feed's, and an overflow, so a follower reads everything once.
    async fn changes_cursor(&self) -> Result<ChangeCursor, StoreError> {
        self.observe(async |inner| inner.changes_cursor().await)
    }

    async fn changed_since(
        &self,
        since: &ChangeCursor,
        limit: usize,
    ) -> Result<Changed, StoreError> {
        self.observe(async |inner| inner.changed_since(since, limit).await)
    }
}

impl Watch for JsonlStore {
    async fn change_token(&self) -> Result<ChangeToken, StoreError> {
        self.observe(async |inner| inner.change_token().await)
    }
}

impl CommitNotify for JsonlStore {
    fn next_commit(&self) -> CommitWait {
        self.shared.commits.wait()
    }
}

impl ReadUntilTime for JsonlStore {
    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Slice, StoreError> {
        self.observe(async |inner| inner.read_until_time(stream, until).await)
    }
}

impl ReadByCorrelation for JsonlStore {
    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: Uuid,
    ) -> Result<Vec<EventBook>, StoreError> {
        self.observe(async |inner| {
            inner
                .read_by_correlation(domain, edition, correlation_id)
                .await
        })
    }
}

impl ListStreams for JsonlStore {
    async fn list_streams(&self) -> Result<Vec<Cover>, StoreError> {
        self.observe(async |inner| inner.list_streams().await)
    }
}

impl Processed for JsonlStore {
    async fn processed(&self, handler: &str) -> Result<Vec<(Cover, u64)>, StoreError> {
        self.observe(async |inner| inner.processed(handler).await)
    }
}

impl Outbox for JsonlStore {
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, StoreError> {
        self.observe(async |inner| inner.pending(limit).await)
    }

    async fn clear(&self, ids: &[Uuid]) -> Result<u64, StoreError> {
        self.commit(entry::clear(ids), async |inner| inner.clear(ids).await)
    }
}

impl DeadLetterStore for JsonlStore {
    async fn dead_letters(&self, stream: StreamRef<'_>) -> Result<Vec<DeadLetter>, StoreError> {
        self.observe(async |inner| inner.dead_letters(stream).await)
    }

    async fn dead_letter(&self, key: DeadLetterKey<'_>) -> Result<Option<DeadLetter>, StoreError> {
        self.observe(async |inner| inner.dead_letter(key).await)
    }

    async fn dead_lettered(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.observe(async |inner| inner.dead_lettered(domain).await)
    }

    async fn drain_dead_letters(
        &self,
        stream: StreamRef<'_>,
    ) -> Result<Vec<DeadLetter>, StoreError> {
        self.commit(entry::drain(stream), async |inner| {
            inner.drain_dead_letters(stream).await
        })
    }

    async fn remove_dead_letters(&self, keys: &[DeadLetterKey<'_>]) -> Result<u64, StoreError> {
        self.commit(entry::remove_dead_letters(keys), async |inner| {
            inner.remove_dead_letters(keys).await
        })
    }
}

#[cfg(test)]
#[path = "store.contract.test.rs"]
mod contract_tests;

#[cfg(test)]
#[path = "store.test.rs"]
mod tests;
