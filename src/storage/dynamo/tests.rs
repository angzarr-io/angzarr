//! Unit tests for DynamoDB storage implementations.
//!
//! These tests focus on pure functions (partition key construction, range
//! arithmetic) that don't require a real DynamoDB instance. The full
//! contract suite lives in `tests/storage/event_store_tests.rs` and runs
//! against the shared macro when a testcontainers harness is available;
//! these unit tests close the gap until then.

use uuid::Uuid;

use super::event_store::DynamoEventStore;

// ============================================================================
// H-25: get_from_to(from, to=0) underflow
// ============================================================================

/// `to_inclusive(0)` must NOT panic. Pre-fix `(to - 1)` was a bare `u32`
/// subtraction that underflowed for `to == 0`. `saturating_sub` returns 0
/// instead — the half-open range `[from, 0)` is empty by definition and
/// the SDK call sites short-circuit before issuing a query.
#[test]
fn test_to_inclusive_zero_does_not_panic() {
    assert_eq!(DynamoEventStore::to_inclusive(0), 0);
}

#[test]
fn test_to_inclusive_one() {
    assert_eq!(DynamoEventStore::to_inclusive(1), 0);
}

#[test]
fn test_to_inclusive_typical() {
    assert_eq!(DynamoEventStore::to_inclusive(42), 41);
}

#[test]
fn test_to_inclusive_u32_max() {
    assert_eq!(DynamoEventStore::to_inclusive(u32::MAX), u32::MAX - 1);
}

// ============================================================================
// H-26: partition-key round-trip with `#` in components
// ============================================================================

/// H-26: `pk()`/`parse_pk()` must round-trip components that contain the
/// `#` separator. Pre-fix `parse_pk` used `splitn(3, '#')` and silently
/// mis-parsed (`"a#b"` in `domain` rolled into `edition`, etc.). Same
/// shape as bigtable row-key parsing.
#[test]
fn test_pk_round_trip_with_hash_in_components() {
    let root = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();

    for (domain, edition) in [
        ("orders", "main"),
        ("orders#alpha", "main"),
        ("orders", "v2#preview"),
        ("orders#alpha", "v2#preview"),
        ("a#b#c", "d#e#f"),
    ] {
        let key = DynamoEventStore::pk(domain, edition, root);
        let parsed = DynamoEventStore::parse_pk(&key).unwrap_or_else(|| {
            panic!(
                "round-trip failed for domain={:?} edition={:?}: parse_pk returned None for key {:?}",
                domain, edition, key
            )
        });
        assert_eq!(parsed.0, domain, "domain must round-trip");
        assert_eq!(parsed.1, edition, "edition must round-trip");
        assert_eq!(parsed.2, root, "root must round-trip");
    }
}
