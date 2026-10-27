//! The storage contract against Postgres (a fresh database per test).

crate::storage::contract::snapshot_store_contract!(super::PostgresSnapshotStore::new(
    crate::storage::postgres::test_db::fresh_pool().await
));
