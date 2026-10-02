//! Redis storage implementations.
//!
//! Redis is used only for snapshot caching (reconstructed aggregate state).
//!
//! Redis is NOT used for:
//! - Event storage (requires strong durability guarantees)
//! - Position tracking (use database-backed stores for consistency)
//!
//! Use Postgres or SQLite for events and positions.

mod snapshot_store;

pub use snapshot_store::RedisSnapshotStore;
