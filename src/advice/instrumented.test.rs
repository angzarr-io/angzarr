//! Tests for metrics instrumentation wrapper.
//!
//! The Instrumented wrapper adds OpenTelemetry metrics to storage operations:
//! - Duration histograms for all operations
//! - Event/snapshot counters by domain and storage type
//! - Position update counters by handler
//!
//! Why this matters: Observability without polluting business logic.
//! Storage implementations stay pure; metrics are applied at composition time.
//!
//! Key behaviors verified:
//! - Wrapper delegates to inner implementation
//! - Errors propagate unchanged
//!
//! Note: Metric emission tests require integration tests with OTel collector.
//! These unit tests verify the wrapper doesn't break storage behavior.

use super::*;
use crate::storage::MockEventStore;

/// Instrumented wrapper delegates to inner storage.
///
/// All operations pass through; wrapper is transparent.
#[tokio::test]
async fn test_instrumented_delegates_to_inner() {
    let inner = MockEventStore::new();
    let instrumented = Instrumented::new(inner, "mock");

    let root = Uuid::new_v4();

    // Should delegate and succeed
    let events = instrumented.get("test", "angzarr", root).await.unwrap();
    assert!(events.is_empty());
}

/// Errors from inner storage propagate through wrapper.
///
/// Wrapper doesn't swallow or transform errors.
#[tokio::test]
async fn test_instrumented_preserves_errors() {
    let inner = MockEventStore::new();
    inner.set_fail_on_get(true).await;

    let instrumented = Instrumented::new(inner, "mock");
    let root = Uuid::new_v4();

    // Should propagate error
    let result = instrumented.get("test", "angzarr", root).await;
    assert!(result.is_err());
}

// ============================================================================
// Accessor Method Tests
// ============================================================================

/// inner() provides read access to wrapped storage.
///
/// Useful for inspecting state or calling methods not in the trait.
#[test]
fn test_instrumented_inner_access() {
    let inner = MockEventStore::new();
    let instrumented = Instrumented::new(inner, "mock");

    // Should provide reference to inner
    let _ = instrumented.inner();
}

/// into_inner() unwraps and returns the inner storage.
///
/// Allows recovering the original storage after instrumentation is no longer needed.
#[test]
fn test_instrumented_into_inner() {
    let inner = MockEventStore::new();
    let instrumented = Instrumented::new(inner, "mock");

    // Should consume wrapper and return inner
    let _recovered: MockEventStore = instrumented.into_inner();
}

/// Every production store is wrapped in `Instrumented`; an explicit-divergence
/// read (a new edition branch with no snapshot yet) must reach the inner
/// store's implementation, not the trait's NotImplemented default.
#[tokio::test]
async fn test_instrumented_forwards_get_with_divergence() {
    use crate::storage::AddMeta;
    let inner = MockEventStore::new();
    let root = Uuid::new_v4();
    let page = |seq: u32| EventPage {
        header: Some(crate::proto::PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(seq)),
        }),
        ..Default::default()
    };
    inner
        .add(
            "orders",
            "",
            root,
            vec![page(0), page(1), page(2)],
            &AddMeta::default(),
        )
        .await
        .unwrap();
    let instrumented = Instrumented::new(inner, "mock");

    let events = instrumented
        .get_with_divergence("orders", "branch", root, Some(2))
        .await
        .expect("divergence read forwarded to the inner store");
    assert_eq!(
        events.len(),
        2,
        "main-timeline events before the divergence"
    );
}
