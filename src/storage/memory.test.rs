//! The storage contract against the in-memory stores.

mod events {
    crate::storage::contract::event_store_contract!(crate::storage::MemoryEventStore::new());
}

mod snapshots {
    crate::storage::contract::snapshot_store_contract!(crate::storage::MemorySnapshotStore::new());
}
