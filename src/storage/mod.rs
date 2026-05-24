//! Event and snapshot storage.
//!
//! This module contains:
//! - `EventStore` trait: Event persistence
//! - `SnapshotStore` trait: Snapshot optimization
//! - `PositionStore` trait: Handler checkpoint tracking
//! - `DomainStorage`: Per-domain storage wrapper
//! - Storage configuration types
//! - Implementations: PostgreSQL, SQLite, Redis, Bigtable, DynamoDB, NATS, ImmuDB

use std::sync::Arc;

use crate::repository::EventBookRepository;

// Submodules
pub mod config;
pub mod error;
pub mod factory;

// Trait modules
mod event_store;
mod position_store;
mod snapshot_store;

pub use event_store::{AddOutcome, CascadeParticipant, EventStore, SourceInfo};
pub use position_store::PositionStore;
pub use snapshot_store::SnapshotStore;

// Re-export from submodules
pub use config::{PostgresConfig, RedisConfig, SnapshotsEnableConfig, SqliteConfig, StorageConfig};
pub use error::{errmsg, Result, StorageError};
pub use factory::{init_position_store, init_storage, PositionBackend, StoresBackend};

// Implementation modules
#[cfg(feature = "bigtable")]
pub mod bigtable;
#[cfg(feature = "dynamo")]
pub mod dynamo;
pub mod helpers;
#[cfg(feature = "immudb")]
pub mod immudb;
pub mod mock;
#[cfg(feature = "nats")]
pub mod nats;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;
// Schema is always compiled (sqlite is always on, postgres/immudb add more)
pub mod schema;
// SQLite is always compiled for local orchestration
pub mod sqlite;
// Unified SQL implementations (shared by postgres and sqlite)
pub mod sql;

// Backend re-exports
#[cfg(feature = "bigtable")]
pub use bigtable::{
    BigtableConfig, BigtableEventStore, BigtablePositionStore, BigtableSnapshotStore,
};
#[cfg(feature = "dynamo")]
pub use dynamo::{DynamoConfig, DynamoEventStore, DynamoPositionStore, DynamoSnapshotStore};
#[cfg(feature = "immudb")]
pub use immudb::ImmudbEventStore;
pub use mock::{MockEventStore, MockPositionStore, MockSnapshotStore};
#[cfg(feature = "nats")]
pub use nats::{NatsEventStore, NatsPositionStore, NatsSnapshotStore};
#[cfg(feature = "postgres")]
pub use postgres::{PostgresEventStore, PostgresPositionStore, PostgresSnapshotStore};
#[cfg(feature = "redis")]
pub use redis::RedisSnapshotStore;
// SQLite is always compiled
pub use sqlite::{SqliteEventStore, SqlitePositionStore, SqliteSnapshotStore};

// ============================================================================
// Domain Storage
// ============================================================================

/// Per-domain event and snapshot storage.
///
/// Bundles the event store and snapshot store for a single domain.
/// Used by in-process mode to route storage operations by domain.
#[derive(Clone)]
pub struct DomainStorage {
    /// Event store for this domain.
    pub event_store: Arc<dyn EventStore>,
    /// Snapshot store for this domain.
    pub snapshot_store: Arc<dyn SnapshotStore>,
}

impl DomainStorage {
    /// Create a new domain storage wrapper.
    pub fn new(event_store: Arc<dyn EventStore>, snapshot_store: Arc<dyn SnapshotStore>) -> Self {
        Self {
            event_store,
            snapshot_store,
        }
    }

    /// Create an EventBookRepository for this domain's stores.
    ///
    /// Wraps the snapshot store in a default-policy
    /// `SnapshotRepository` (both reads and writes enabled). Callers
    /// needing explicit policy should build the `SnapshotRepository`
    /// themselves and construct `EventBookRepository::new` directly.
    pub fn event_book_repo(&self) -> EventBookRepository {
        use crate::repository::SnapshotRepository;
        let snapshot_repo = Arc::new(SnapshotRepository::new(self.snapshot_store.clone()));
        EventBookRepository::new(self.event_store.clone(), snapshot_repo)
    }
}

#[cfg(test)]
#[path = "event_store.test.rs"]
mod event_store_tests;
