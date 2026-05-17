//! ImmuDB storage backend for event sourcing.
//!
//! Uses immudb's SQL interface via PostgreSQL wire protocol for familiar
//! SQL semantics while leveraging immudb's immutability guarantees.
//!
//! # Why immudb for Event Sourcing
//!
//! immudb is a natural fit for event sourcing:
//! - **Immutability**: Events are append-only, never modified or deleted
//! - **Cryptographic verification**: Merkle tree proofs ensure tamper evidence
//! - **Time-travel queries**: `SINCE TX` / `BEFORE TX` for temporal queries
//! - **Audit trail**: Built-in history tracking via `HISTORY OF` queries
//!
//! # Snapshots Are NOT Stored in immudb
//!
//! This module provides `EventStore` only—**not** `SnapshotStore`.
//!
//! Snapshots are mutable by design: they get overwritten with newer state
//! as aggregates evolve. This fundamentally conflicts with immudb's
//! immutability guarantees. Storing snapshots in immudb would mean:
//! - Every snapshot update creates a new row (history accumulates forever)
//! - No way to reclaim space from obsolete snapshots
//! - Queries must filter to "latest" snapshot, negating immudb's strengths
//!
//! **Recommended architecture:**
//! ```text
//! ┌─────────────────┐     ┌─────────────────┐
//! │     immudb      │     │   PostgreSQL    │
//! │  (EventStore)   │     │ (SnapshotStore) │
//! │                 │     │                 │
//! │  Immutable      │     │  Mutable        │
//! │  Append-only    │     │  Overwrite OK   │
//! │  Tamper-evident │     │  Space-efficient│
//! └─────────────────┘     └─────────────────┘
//! ```
//!
//! Use `PostgresSnapshotStore`, `SqliteSnapshotStore`, or `RedisSnapshotStore`
//! alongside `ImmudbEventStore`. The event store is the source of truth;
//! snapshots are a disposable optimization.
//!
//! # Connection
//!
//! immudb exposes a PostgreSQL wire protocol, allowing standard Postgres
//! drivers to connect. Configure via:
//!
//! ```text
//! IMMUDB_PGSQL_SERVER=true
//! IMMUDB_PGSQL_SERVER_PORT=5432
//! ```
//!
//! Connect using standard Postgres connection string:
//! ```text
//! postgresql://immudb:immudb@localhost:5432/defaultdb?sslmode=disable
//! ```
//!
//! # Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS events (
//!     domain      VARCHAR[128] NOT NULL,
//!     edition     VARCHAR[64] NOT NULL,
//!     root        BLOB[16] NOT NULL,          -- UUID as 16 bytes
//!     sequence    INTEGER NOT NULL,
//!     created_at  TIMESTAMP NOT NULL,
//!     event_data  BLOB NOT NULL,              -- Protobuf-encoded EventPage
//!     correlation_id VARCHAR[256],
//!     PRIMARY KEY (domain, edition, root, sequence)
//! );
//!
//! -- Secondary index for correlation queries
//! CREATE INDEX ON events(correlation_id);
//!
//! -- Index for domain+root lookups (without edition)
//! CREATE INDEX ON events(domain, root, sequence);
//! ```
//!
//! # Limitations
//!
//! - **Simple query mode only**: immudb's pgsql server only supports simple queries
//! - **No schema introspection**: Some pg_catalog queries may fail
//! - **Index creation**: Indexes must be created on empty tables
//!
//! # Feature Flag
//!
//! Enable with `--features immudb` in Cargo.toml.

mod event_store;

pub use event_store::ImmudbEventStore;

/// SQL statements for immudb schema initialization.
pub mod schema {
    /// Create the events table with indexes.
    ///
    /// Note: In immudb, indexes must be created at table creation time
    /// (tables must be empty when indexes are added).
    ///
    /// Root is stored as VARCHAR (UUID string) rather than BLOB because
    /// the implementation uses `root.to_string()` for storage.
    // immudb has a 256 byte limit for indexed columns
    // Keep VARCHAR sizes conservative to stay within limits
    pub const CREATE_EVENTS_TABLE: &str = r#"
        CREATE TABLE IF NOT EXISTS events (
            domain         VARCHAR(64) NOT NULL,
            edition        VARCHAR(32) NOT NULL,
            root           VARCHAR(36) NOT NULL,
            sequence       INTEGER NOT NULL,
            created_at     TIMESTAMP NOT NULL,
            event_data     BLOB NOT NULL,
            correlation_id VARCHAR(128),
            external_id    VARCHAR(128),
            source_edition VARCHAR(32),
            source_domain  VARCHAR(64),
            source_root    VARCHAR(36),
            source_seq     INTEGER,
            PRIMARY KEY (domain, edition, root, sequence)
        )
    "#;

    /// Create correlation index (must be on empty table).
    pub const CREATE_CORRELATION_INDEX: &str =
        "CREATE INDEX IF NOT EXISTS ON events(correlation_id)";

    /// Create domain+root index for cross-edition queries.
    pub const CREATE_DOMAIN_ROOT_INDEX: &str =
        "CREATE INDEX IF NOT EXISTS ON events(domain, root, sequence)";
}
