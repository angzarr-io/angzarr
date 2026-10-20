//! The storage contract against SQLite (in memory, migrated).

crate::storage::contract::snapshot_store_contract!(crate::storage::SqliteSnapshotStore::new(
    crate::storage::factory::sqlite_pool("sqlite::memory:")
        .await
        .unwrap()
));
