//! Unit tests for SNS/SQS FIFO MessageGroupId / MessageDeduplicationId
//! construction (C-12 regression suite).
//!
//! These tests exercise the pure helper `build_fifo_attributes` so they do
//! not need an AWS SDK client, LocalStack, or Floci — the construction
//! logic is independent of the wire-level publish call, and the bug being
//! remediated lives entirely in that construction.
//!
//! C-12 (from `plans/deep-review-remediation.md`):
//!   1. `book.root_id_hex().unwrap_or_default()` produces an empty string
//!      when the EventBook has no root. AWS FIFO rejects empty
//!      MessageGroupId outright.
//!   2. If a non-FIFO broker (or mock) accepts the empty group_id, all
//!      root-less events collapse into one serialized ordering group —
//!      silently serialising independent aggregates.
//!   3. `ContentBasedDeduplication=false` + fixed format
//!      `{domain}-{root}-{max_seq}` means a legitimate retry of the same
//!      event lands inside AWS's 5-minute dedup window with an identical
//!      dedup_id and is silently dropped.
//!
//! Chosen contract (see C-12 in the plan):
//!   * Root-less EventBooks are rejected at the boundary with
//!     `BusError::Publish`. Falling back to a per-event UUID would silently
//!     weaken the documented "ordering by aggregate root" FIFO guarantee.
//!   * dedup_id includes a per-bus-instance monotonic publish counter so
//!     legitimate retries don't collide inside the 5-minute dedup window.

use super::build_fifo_attributes;
use crate::proto::{Cover, EventBook, Uuid as ProtoUuid};
use crate::test_utils::{make_cover_with_root, make_event_page};
use uuid::Uuid;

/// EventBook with a root and pages.
fn book_with_root(domain: &str, root: Uuid, max_seq: u32) -> EventBook {
    EventBook {
        cover: Some(make_cover_with_root(domain, root)),
        pages: (0..=max_seq).map(make_event_page).collect(),
        snapshot: None,
        ..Default::default()
    }
}

/// EventBook with a cover but no root (the bug surface). The cover has
/// `root: None`, which makes `root_id_hex()` return None.
fn book_without_root(domain: &str) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: None,
            correlation_id: String::new(),
            edition: None,
        }),
        pages: vec![make_event_page(0)],
        snapshot: None,
        ..Default::default()
    }
}

/// EventBook with a present-but-empty root (the second bug variant —
/// `root_id_hex()` returns `Some("")`, NOT `None`, when the ProtoUuid is
/// present but its `value` bytes are empty). The wire-level rejection from
/// AWS is identical to the missing-root case; the framework should treat
/// it the same way.
fn book_with_empty_root(domain: &str) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid { value: Vec::new() }),
            correlation_id: String::new(),
            edition: None,
        }),
        pages: vec![make_event_page(0)],
        snapshot: None,
        ..Default::default()
    }
}

/// C-12 bug 1: root-less EventBook must be REJECTED at the publisher
/// boundary, not silently published with an empty MessageGroupId.
///
/// Baseline behaviour: `build_fifo_attributes` returns `Ok(("", ...))` —
/// an empty group_id is rejected by AWS FIFO at the wire, producing an
/// opaque SDK error. The framework's job is to surface the misuse
/// explicitly here.
///
/// Fixed behaviour: return `BusError::Publish` with a message that
/// names the root-cause ("EventBook missing root") so operators don't
/// chase an AWS validation error back through several layers.
#[test]
fn build_fifo_attributes_rejects_rootless_event_book() {
    let book = book_without_root("orders");
    let result = build_fifo_attributes(&book, "nonce0", 0);
    assert!(
        result.is_err(),
        "root-less EventBook must be rejected (FIFO MessageGroupId cannot be empty); got Ok({:?})",
        result.ok()
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("missing root") || err_msg.contains("non-empty"),
        "error must explain the root-cause; got: {}",
        err_msg
    );
}

/// C-12 bug 1 (second variant): a Cover whose `root` ProtoUuid is present
/// but carries an empty byte vector also produces an empty hex string.
/// The contract on MessageGroupId is "non-empty", not "Some present".
#[test]
fn build_fifo_attributes_rejects_empty_root_bytes() {
    let book = book_with_empty_root("orders");
    let result = build_fifo_attributes(&book, "nonce0", 0);
    assert!(
        result.is_err(),
        "EventBook with present-but-empty root must be rejected; got Ok({:?})",
        result.ok()
    );
}

/// C-12 bug 3: a legitimate retry of the SAME logical EventBook must
/// produce a DISTINCT MessageDeduplicationId, or AWS's 5-minute FIFO
/// dedup window silently drops the retry. At-least-once republish flows
/// (operator-driven replay, persist-and-publish retry) depend on this.
///
/// Baseline behaviour: dedup_id is a pure function of
/// `{domain}-{root}-{max_seq}` — two calls produce identical IDs.
///
/// Fixed behaviour: dedup_id includes a per-publish monotonic counter
/// so the second call produces a distinct ID.
#[test]
fn build_fifo_attributes_retries_get_distinct_dedup_ids() {
    let root = Uuid::new_v4();
    let book = book_with_root("orders", root, 3);

    // Simulate two consecutive publishes of the same logical event
    // (e.g., a retry after a transient failure on the first attempt).
    let (group_a, dedup_a) =
        build_fifo_attributes(&book, "nonce0", 0).expect("first publish must succeed");
    let (group_b, dedup_b) = build_fifo_attributes(&book, "nonce0", 1).expect("retry must succeed");

    // Group_id must remain stable across retries — that's the entire
    // point of FIFO ordering by aggregate root.
    assert_eq!(
        group_a, group_b,
        "MessageGroupId must be stable across retries to preserve per-root ordering"
    );

    // Dedup_id must NOT match. If it does, AWS silently drops the retry.
    assert_ne!(
        dedup_a, dedup_b,
        "MessageDeduplicationId must differ between retries; identical IDs are silently \
         dropped by AWS's 5-minute dedup window. Got both = {:?}",
        dedup_a
    );
}

/// C-12 bug 2 (regression guard): once root-less events are rejected, two
/// EventBooks with DIFFERENT roots must still produce DIFFERENT
/// MessageGroupIds — the fix must not over-correct by collapsing all
/// events into one group.
#[test]
fn build_fifo_attributes_distinct_roots_get_distinct_group_ids() {
    let root_a = Uuid::new_v4();
    let root_b = Uuid::new_v4();
    let book_a = book_with_root("orders", root_a, 1);
    let book_b = book_with_root("orders", root_b, 1);

    let (group_a, _) = build_fifo_attributes(&book_a, "nonce0", 0).expect("publish A must succeed");
    let (group_b, _) = build_fifo_attributes(&book_b, "nonce0", 1).expect("publish B must succeed");

    assert_ne!(
        group_a, group_b,
        "distinct aggregate roots must land in distinct ordering groups"
    );
}

/// C-12 follow-up: the per-bus-instance nonce must differentiate dedup_ids
/// across a process restart. Any cross-restart republish (operator-driven
/// replay, persist-and-publish retry after a crash) has both the original
/// (pre-crash) process and the new process starting their `publish_counter`
/// at 0, so without the instance nonce the dedup_id would alias inside AWS's
/// 5-minute dedup window and the retry would be silently dropped.
#[test]
fn build_fifo_attributes_distinct_instance_nonces_produce_distinct_dedup_ids() {
    let root = Uuid::new_v4();
    let book = book_with_root("orders", root, 3);

    // Simulate "process P1 published this event with counter=0, then
    // crashed; process P2 starts up and republishes the same logical
    // event, also at counter=0". Without an instance nonce both would
    // compute the identical dedup_id; AWS would dedup-drop P2's retry.
    let (group_a, dedup_a) =
        build_fifo_attributes(&book, "instanceA", 0).expect("P1 publish must succeed");
    let (group_b, dedup_b) =
        build_fifo_attributes(&book, "instanceB", 0).expect("P2 republish must succeed");

    assert_eq!(
        group_a, group_b,
        "MessageGroupId must remain stable across processes for the same root"
    );
    assert_ne!(
        dedup_a, dedup_b,
        "dedup_id must differ across bus-instance nonces; identical IDs are silently \
         dropped by AWS within the 5-minute dedup window"
    );
}

/// Regression guard: a happy-path publish (root present, sequence
/// numbers present) must return non-empty values for both fields. The
/// fix must not break the existing successful path.
#[test]
fn build_fifo_attributes_happy_path_returns_non_empty_values() {
    let root = Uuid::new_v4();
    let book = book_with_root("orders", root, 5);
    let (group_id, dedup_id) =
        build_fifo_attributes(&book, "nonce0", 42).expect("happy-path publish must succeed");

    assert!(
        !group_id.is_empty(),
        "happy-path group_id must be non-empty"
    );
    assert!(
        !dedup_id.is_empty(),
        "happy-path dedup_id must be non-empty"
    );
    assert!(
        dedup_id.contains("orders"),
        "dedup_id should embed the domain for operator readability; got: {}",
        dedup_id
    );
    assert!(
        dedup_id.contains(&group_id),
        "dedup_id should embed the root for operator readability; got: {}",
        dedup_id
    );
}
