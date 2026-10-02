//! Mock storage contract tests.
//!
//! The in-memory Mock stores stand in for the production backends in unit
//! tests, so they run the same contract suites the production backends do:
//! a test double that accepts what production rejects (or reads differently)
//! gives unit tests a false pass.

mod storage;

use angzarr::storage::{MockEventStore, MockPositionStore, MockSnapshotStore};

mod event_store_contract {
    use angzarr::storage::MockEventStore;

    async fn fixture() -> MockEventStore {
        MockEventStore::new()
    }

    crate::generate_event_store_tests!(fixture);
}

#[tokio::test]
async fn test_mock_event_store_concurrent() {
    let store = std::sync::Arc::new(MockEventStore::new());
    run_event_store_concurrent_tests!(store);
}

#[tokio::test]
async fn test_mock_snapshot_store_contract() {
    let store = MockSnapshotStore::new();
    run_snapshot_store_tests!(&store);
}

/// C-17: the mock PositionStore must honor the same checkpoint contract the
/// SQL backends do — monotonic (no regression on stale/replayed puts), main-
/// timeline sentinel normalization, and key isolation.
#[tokio::test]
async fn test_mock_position_store_contract() {
    let store = MockPositionStore::new();
    run_position_store_tests!(&store);
}
