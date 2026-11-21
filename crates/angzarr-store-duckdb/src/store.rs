//! The store: one DuckDB database, one connection behind a lock, and the
//! handles that share it.

use std::collections::hash_map::RandomState;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use angzarr_ports::{CommitSignal, StoreError};
use duckdb::{Config, Connection};

use crate::error::{opening, unavailable};
use crate::queries::Queries;
use crate::{schema, statements};

/// How many prepared statements a connection keeps: the store runs a few
/// dozen distinct statements, so each is prepared once.
const STATEMENTS_KEPT: usize = 64;

/// A store in one DuckDB database. Its handles are clones of it and share
/// everything: the connection (one call at a time runs on it), the commit
/// signal and the change token.
///
/// DuckDB lets one process own a database file for writing, and this store
/// is that owner: a second open of the file, from this process or another,
/// is refused ([`crate::DuckError::HeldElsewhere`]). That is the store's one-writer
/// rule, and it is why the handles are clones and not connections.
///
/// A port call takes the connection's lock, runs its statements and
/// returns: it never awaits with the lock held, and it blocks its thread
/// for the length of one transaction (a commit is a WAL append and an
/// `fsync`).
#[derive(Clone)]
pub struct DuckStore {
    pub(crate) shared: Arc<Shared>,
}

/// The stores this process has open on files, by canonical path, held
/// weakly: a file no handle holds any more is forgotten.
static OPEN: Mutex<BTreeMap<PathBuf, Weak<Shared>>> = Mutex::new(BTreeMap::new());

/// What the handles of one store share.
pub(crate) struct Shared {
    conn: Mutex<Connection>,
    path: Option<PathBuf>,
    /// Notified after each commit through any handle.
    pub(crate) signal: CommitSignal,
    /// How many commits this process made: the change token.
    commits: AtomicU64,
    /// The feed's identity: a cursor of another's is an overflow.
    pub(crate) identity: u64,
    /// The place of the feed's last change: the next is one more. The store
    /// is the file's one writer, so it numbers the changes itself and a
    /// commit does not scan the feed for its place.
    pub(crate) feed: Feed,
}

/// The place of the feed's last change (see [`Shared::feed`]).
pub(crate) struct Feed(AtomicI64);

impl Feed {
    /// The place of the next change.
    pub(crate) fn next(&self) -> i64 {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Take the feed's last place from the file.
    pub(crate) fn align(&self, conn: &Connection) -> Result<(), StoreError> {
        let head = conn.first(statements::FEED_HEAD, &[], |row| row.get::<_, i64>(0))?;
        self.0.store(head.unwrap_or_default(), Ordering::SeqCst);
        Ok(())
    }
}

impl Shared {
    /// The connection, once no other call holds it.
    pub(crate) fn conn(&self) -> MutexGuard<'_, Connection> {
        // Nothing panics under the lock with a statement half-run.
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `done`, and a commit announced if it succeeded: the signal wakes
    /// followers and the change token moves.
    pub(crate) fn pushed<T>(&self, done: Result<T, StoreError>) -> Result<T, StoreError> {
        if done.is_ok() {
            self.commits.fetch_add(1, Ordering::SeqCst);
            self.signal.notify();
        }
        done
    }

    /// How many commits this store has made since it was opened.
    pub(crate) fn commits(&self) -> u64 {
        self.commits.load(Ordering::SeqCst)
    }
}

impl DuckStore {
    /// The database at `path`, created if absent, and checked: a new file
    /// gets this store's tables; a file of this store is opened as it is
    /// (what was committed before a crash is replayed from the write-ahead
    /// log); a file of a newer format, or a database that is not this
    /// store's, is refused untouched.
    ///
    /// A file this process already has open is not opened twice: DuckDB
    /// gives the second open the first's database, so two stores would
    /// each check a stream's head and each write to it. The open returns
    /// another handle on the store that has it.
    pub fn open(path: &Path) -> Result<DuckStore, StoreError> {
        let conn = Connection::open_with_flags(path, Config::default()).map_err(opening)?;
        let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let mut open = OPEN.lock().unwrap_or_else(PoisonError::into_inner);
        open.retain(|_, store| store.strong_count() > 0);
        if let Some(shared) = open.get(&key).and_then(Weak::upgrade) {
            return Ok(DuckStore { shared });
        }
        let store = DuckStore::on(conn, Some(key.clone()))?;
        open.insert(key, Arc::downgrade(&store.shared));
        Ok(store)
    }

    /// A store in memory: gone with its last handle.
    pub fn open_in_memory() -> Result<DuckStore, StoreError> {
        let conn = Connection::open_in_memory().map_err(opening)?;
        DuckStore::on(conn, None)
    }

    /// The store on `conn`, its tables made or checked.
    fn on(conn: Connection, path: Option<PathBuf>) -> Result<DuckStore, StoreError> {
        conn.set_prepared_statement_cache_capacity(STATEMENTS_KEPT);
        let identity = schema::ensure(&conn)?;
        let feed = Feed(AtomicI64::new(0));
        feed.align(&conn)?;
        Ok(DuckStore {
            shared: Arc::new(Shared {
                conn: Mutex::new(conn),
                path,
                signal: CommitSignal::new(),
                commits: AtomicU64::new(0),
                identity,
                feed,
            }),
        })
    }

    /// The file the store is on, none for one in memory.
    pub fn path(&self) -> Option<&Path> {
        self.shared.path.as_deref()
    }

    /// Close this handle; the last one closes the database, which
    /// checkpoints its log into the file, so the file is whole on its own
    /// once this returns.
    pub fn close(self) -> Result<(), StoreError> {
        match Arc::try_unwrap(self.shared) {
            Ok(shared) => {
                let conn = shared
                    .conn
                    .into_inner()
                    .unwrap_or_else(PoisonError::into_inner);
                conn.close().map_err(|(_, error)| unavailable(error))
            }
            Err(_) => Ok(()),
        }
    }

    /// Run `work` on the store's connection.
    pub(crate) fn with<T>(
        &self,
        work: impl FnOnce(&Connection) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        work(&self.shared.conn())
    }
}

/// A random 63-bit number, for a feed's identity: std's hasher is seeded
/// from the operating system's randomness on each construction.
pub(crate) fn random_identity() -> i64 {
    let bits = RandomState::new().build_hasher().finish();
    i64::try_from(bits >> 1).unwrap_or_default()
}

/// The statement text of a `meta` read, for the open's checks.
pub(crate) fn meta_value(conn: &Connection, key: &str) -> Result<Option<i64>, StoreError> {
    conn.first(statements::META, &[&key], |row| row.get::<_, i64>(0))
}

#[cfg(test)]
#[path = "store.contract.test.rs"]
mod contract;

#[cfg(test)]
#[path = "store.test.rs"]
mod tests;
