//! PositionStore interface tests.
//!
//! These tests verify the contract of the PositionStore trait.
//! Each storage implementation should run these tests.

use angzarr::storage::PositionStore;

// =============================================================================
// PositionStore::get tests
// =============================================================================

pub async fn test_get_nonexistent<S: PositionStore>(store: &S) {
    let result = store
        .get("test_handler", "test_domain", "test", b"nonexistent")
        .await
        .expect("get should succeed");
    assert!(result.is_none(), "nonexistent position should be None");
}

// =============================================================================
// PositionStore::put tests
// =============================================================================

pub async fn test_put_and_get<S: PositionStore>(store: &S) {
    let handler = "test_pos_put_get";
    let domain = "test_domain";
    let root = b"root_001";

    store
        .put(handler, domain, "test", root, 42)
        .await
        .expect("put should succeed");

    let result = store
        .get(handler, domain, "test", root)
        .await
        .expect("get should succeed")
        .expect("position should exist");

    assert_eq!(result, 42, "should return stored sequence");
}

pub async fn test_put_update<S: PositionStore>(store: &S) {
    let handler = "test_pos_update";
    let domain = "test_domain";
    let root = b"root_002";

    store.put(handler, domain, "test", root, 10).await.unwrap();
    store.put(handler, domain, "test", root, 25).await.unwrap();

    let result = store
        .get(handler, domain, "test", root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, 25, "should return updated sequence");
}

/// C-17 regression: `put` must never let the stored sequence move backwards.
///
/// PositionStore is a checkpoint: projectors and sagas resume from the last
/// recorded position. A stale or replayed `put` with a sequence lower than
/// the current one (e.g., from an out-of-order checkpoint flush, a replayed
/// message on a redrive, or a slow-to-converge replica) must be a no-op,
/// not a regression. Letting it regress causes the projector to re-process
/// events on restart — silent duplicate side effects.
///
/// Contract: after `put(seq=10); put(seq=5)`, `get` returns `Some(10)`.
pub async fn test_put_monotonic_no_regression<S: PositionStore>(store: &S) {
    let handler = "test_pos_monotonic";
    let domain = "test_domain";
    let root = b"root_monotonic";

    store.put(handler, domain, "test", root, 10).await.unwrap();

    // Stale checkpoint — must be ignored.
    store
        .put(handler, domain, "test", root, 5)
        .await
        .expect("stale put must not error; it should silently no-op");

    let result = store
        .get(handler, domain, "test", root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, 10, "stale put(5) must not regress position from 10");

    // Equal-sequence put is also a no-op (idempotent re-checkpoint).
    store.put(handler, domain, "test", root, 10).await.unwrap();
    let result = store
        .get(handler, domain, "test", root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, 10, "equal put must leave position at 10");

    // Forward put still advances (monotonicity is one-directional, not frozen).
    store.put(handler, domain, "test", root, 15).await.unwrap();
    let result = store
        .get(handler, domain, "test", root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, 15, "forward put must advance position");
}

pub async fn test_put_zero_sequence<S: PositionStore>(store: &S) {
    let handler = "test_pos_zero";
    let domain = "test_domain";
    let root = b"root_003";

    store.put(handler, domain, "test", root, 0).await.unwrap();

    let result = store
        .get(handler, domain, "test", root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, 0, "should store sequence 0");
}

// =============================================================================
// Isolation tests
// =============================================================================

pub async fn test_handler_isolation<S: PositionStore>(store: &S) {
    let domain = "test_domain";
    let root = b"root_iso_handler";

    store
        .put("handler_a", domain, "test", root, 10)
        .await
        .unwrap();
    store
        .put("handler_b", domain, "test", root, 20)
        .await
        .unwrap();

    let a = store
        .get("handler_a", domain, "test", root)
        .await
        .unwrap()
        .unwrap();
    let b = store
        .get("handler_b", domain, "test", root)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(a, 10, "handler_a should be 10");
    assert_eq!(b, 20, "handler_b should be 20");
}

pub async fn test_domain_isolation<S: PositionStore>(store: &S) {
    let handler = "test_pos_dom_iso";
    let root = b"root_iso_domain";

    store
        .put(handler, "domain_x", "test", root, 5)
        .await
        .unwrap();
    store
        .put(handler, "domain_y", "test", root, 15)
        .await
        .unwrap();

    let x = store
        .get(handler, "domain_x", "test", root)
        .await
        .unwrap()
        .unwrap();
    let y = store
        .get(handler, "domain_y", "test", root)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(x, 5, "domain_x should be 5");
    assert_eq!(y, 15, "domain_y should be 15");
}

pub async fn test_root_isolation<S: PositionStore>(store: &S) {
    let handler = "test_pos_root_iso";
    let domain = "test_domain";

    store
        .put(handler, domain, "test", b"root_a", 100)
        .await
        .unwrap();
    store
        .put(handler, domain, "test", b"root_b", 200)
        .await
        .unwrap();

    let a = store
        .get(handler, domain, "test", b"root_a")
        .await
        .unwrap()
        .unwrap();
    let b = store
        .get(handler, domain, "test", b"root_b")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(a, 100, "root_a should be 100");
    assert_eq!(b, 200, "root_b should be 200");
}

// =============================================================================
// Main-timeline sentinel polarity tests (C-15)
// =============================================================================
//
// PositionStore must treat `""` and `"angzarr"` as interchangeable forms of
// the main-timeline sentinel. Projectors save positions via one form, but the
// caller that subsequently reads (post-restart, post-migration) may use the
// other — both must hit the same row. Otherwise the projector "loses" its
// position on restart and re-processes events.

/// C-15: position written via empty-string sentinel must be readable via
/// both main-timeline forms.
pub async fn test_main_timeline_sentinel_write_empty_read_both<S: PositionStore>(store: &S) {
    let handler = "test_pos_sentinel_empty";
    let domain = "test_domain";
    let root = b"root_sentinel_empty";

    store
        .put(handler, domain, "", root, 42)
        .await
        .expect("put with empty-string main-timeline sentinel should succeed");

    let via_empty = store
        .get(handler, domain, "", root)
        .await
        .expect("get via empty sentinel should succeed")
        .expect("position written via empty sentinel must be readable via empty sentinel");
    assert_eq!(via_empty, 42);

    let via_angzarr = store
        .get(handler, domain, "angzarr", root)
        .await
        .expect("get via 'angzarr' sentinel should succeed")
        .expect("position written via empty sentinel must be readable via 'angzarr' sentinel");
    assert_eq!(via_angzarr, 42);
}

/// C-15: position written via `"angzarr"` sentinel must be readable via
/// both main-timeline forms.
pub async fn test_main_timeline_sentinel_write_angzarr_read_both<S: PositionStore>(store: &S) {
    let handler = "test_pos_sentinel_angzarr";
    let domain = "test_domain";
    let root = b"root_sentinel_angzarr";

    store
        .put(handler, domain, "angzarr", root, 99)
        .await
        .expect("put with 'angzarr' main-timeline sentinel should succeed");

    let via_angzarr = store
        .get(handler, domain, "angzarr", root)
        .await
        .expect("get via 'angzarr' sentinel should succeed")
        .expect("position written via 'angzarr' must be readable via 'angzarr'");
    assert_eq!(via_angzarr, 99);

    let via_empty = store
        .get(handler, domain, "", root)
        .await
        .expect("get via empty sentinel should succeed")
        .expect("position written via 'angzarr' must be readable via empty sentinel");
    assert_eq!(via_empty, 99);
}

pub async fn test_multiple_handlers_same_root<S: PositionStore>(store: &S) {
    let domain = "test_domain";
    let root = b"shared_root";

    for i in 0..5u32 {
        let handler = format!("test_handler_{}", i);
        store
            .put(&handler, domain, "test", root, i * 10)
            .await
            .unwrap();
    }

    for i in 0..5u32 {
        let handler = format!("test_handler_{}", i);
        let result = store
            .get(&handler, domain, "test", root)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, i * 10, "handler_{} should be {}", i, i * 10);
    }
}

// =============================================================================
// Test runner macro
// =============================================================================

/// Run all PositionStore interface tests against a store implementation.
#[macro_export]
macro_rules! run_position_store_tests {
    ($store:expr) => {
        use $crate::storage::position_store_tests::*;

        // get tests
        test_get_nonexistent($store).await;
        println!("  test_get_nonexistent: PASSED");

        // put tests
        test_put_and_get($store).await;
        println!("  test_put_and_get: PASSED");

        test_put_update($store).await;
        println!("  test_put_update: PASSED");

        test_put_monotonic_no_regression($store).await;
        println!("  test_put_monotonic_no_regression: PASSED");

        test_put_zero_sequence($store).await;
        println!("  test_put_zero_sequence: PASSED");

        // isolation tests
        test_handler_isolation($store).await;
        println!("  test_handler_isolation: PASSED");

        test_domain_isolation($store).await;
        println!("  test_domain_isolation: PASSED");

        test_root_isolation($store).await;
        println!("  test_root_isolation: PASSED");

        test_multiple_handlers_same_root($store).await;
        println!("  test_multiple_handlers_same_root: PASSED");

        // main-timeline sentinel polarity tests (C-15)
        test_main_timeline_sentinel_write_empty_read_both($store).await;
        println!("  test_main_timeline_sentinel_write_empty_read_both: PASSED");

        test_main_timeline_sentinel_write_angzarr_read_both($store).await;
        println!("  test_main_timeline_sentinel_write_angzarr_read_both: PASSED");
    };
}
