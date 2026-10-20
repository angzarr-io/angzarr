//! The storage contract against SQLite (in memory, migrated).

crate::storage::contract::event_store_contract!(crate::storage::SqliteEventStore::new(
    crate::storage::factory::sqlite_pool("sqlite::memory:")
        .await
        .unwrap()
));
