//! Mock storage contract tests (H-24).
//!
//! Until H-24, the `MockEventStore` accepted duplicate / overlapping
//! sequences that every SQL backend rejects via PRIMARY KEY. Tests
//! that exercised "mock + some sequence assertion" therefore had a
//! credibility gap: production stores would have rejected what the
//! tests accepted.
//!
//! This file pins the sequence-rejection contract for mock and is the
//! companion to the new shared `test_add_rejects_duplicate_sequences`
//! in `tests/storage/event_store_tests.rs`. Other macro tests (edition
//! sentinel polarity, explicit-divergence new branches, cascade
//! reaper queries, etc.) are intentionally NOT exercised against mock
//! here — those are SQL-backend contracts and the mock's coverage of
//! them is tracked in `src/storage/mock/tests.rs`.

mod storage;

use angzarr::storage::MockEventStore;

#[tokio::test]
async fn test_mock_event_store_sequence_rejection() {
    use storage::event_store_tests::*;

    println!("=== Mock EventStore Sequence-Rejection Tests (H-24) ===");

    let store = MockEventStore::new();

    test_add_sequence_conflict(&store).await;
    println!("  test_add_sequence_conflict: PASSED");

    test_add_duplicate_sequence(&store).await;
    println!("  test_add_duplicate_sequence: PASSED");

    test_add_rejects_duplicate_sequences(&store).await;
    println!("  test_add_rejects_duplicate_sequences: PASSED");

    println!("=== Mock EventStore Sequence-Rejection Tests PASSED ===");
}
