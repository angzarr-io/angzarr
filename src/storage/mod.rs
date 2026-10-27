//! Event and snapshot storage.
//!
//! - [`EventStore`]: one append-only log per stream (domain, edition, root),
//!   pages stored whole with their `EventMeta`.
//! - [`SnapshotStore`]: the newest snapshot per stream.
//! - Backends: SQLite (always compiled), Postgres (feature `postgres`) and
//!   an in-memory store for tests and in-process use. The other backends are
//!   not yet ported to the latest proto (see `factory`).

pub mod config;
pub mod error;
pub mod factory;
pub mod memory;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod schema;
pub mod sqlite;

mod event_store;
mod snapshot_store;

pub use config::{BackendConfig, SnapshotsEnableConfig, SqliteConfig, StorageRegistryConfig};
pub use error::{errmsg, Result, StorageError};
pub use event_store::{EventStore, StreamHead, StreamRef};
pub use factory::{init_event_store, init_snapshot_store};
pub use memory::{MemoryEventStore, MemorySnapshotStore};
#[cfg(feature = "postgres")]
pub use postgres::{postgres_pool, PostgresEventStore, PostgresSnapshotStore};
pub use snapshot_store::SnapshotStore;
pub use sqlite::{SqliteEventStore, SqliteSnapshotStore};

use prost_types::Timestamp;

/// `recorded_at` as fixed-width RFC 3339 text (nanosecond precision, `Z`),
/// whose text order is time order: the form the SQL `created_at` column
/// holds and time-bounded reads compare.
pub fn recorded_at_text(ts: &Timestamp) -> Result<String> {
    let dt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos.max(0) as u32).ok_or(
        StorageError::InvalidTimestamp {
            seconds: ts.seconds,
            nanos: ts.nanos,
        },
    )?;
    Ok(dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;

#[cfg(test)]
pub(crate) mod contract;
