//! The storage contract against Postgres (a fresh database per test).

crate::storage::contract::event_store_contract!(super::PostgresEventStore::new(
    crate::storage::postgres::test_db::fresh_pool().await
));
