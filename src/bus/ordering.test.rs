//! Tests for per-root ordering after a handler failure.

use super::*;

/// After a message of group G fails, later messages of G in the same batch
/// must be skipped (left in the queue): deleting them first would apply
/// that aggregate root's later events before the failed earlier one.
#[test]
fn failed_group_blocks_its_later_messages() {
    let mut failed = FailedGroups::default();
    assert!(!failed.is_blocked(Some("root-a")));

    failed.record_failure(Some("root-a"));

    assert!(failed.is_blocked(Some("root-a")));
    assert!(
        !failed.is_blocked(Some("root-b")),
        "other roots keep flowing"
    );
}

/// A message whose group SQS did not report might belong to the failed
/// group, so it is held back too.
#[test]
fn unknown_group_is_blocked_once_any_group_failed() {
    let mut failed = FailedGroups::default();
    assert!(!failed.is_blocked(None));
    failed.record_failure(Some("root-a"));
    assert!(failed.is_blocked(None));
}

/// A failed message whose group is unknown blocks the rest of the batch.
#[test]
fn failure_without_group_blocks_whole_batch() {
    let mut failed = FailedGroups::default();
    failed.record_failure(None);
    assert!(failed.is_blocked(Some("root-b")));
    assert!(failed.is_blocked(None));
}

// ============================================================================
// require_ordering_key
// ============================================================================

fn book_with_root(root: Option<Vec<u8>>) -> EventBook {
    EventBook {
        cover: Some(crate::proto::Cover {
            domain: "order".into(),
            root: root.map(|value| crate::proto::Uuid { value }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The ordering key is the hex root, matching Kafka's partition key and
/// SNS's message group.
#[test]
fn ordering_key_is_hex_root() {
    let key = require_ordering_key(&book_with_root(Some(vec![0xab, 0x01])), "Pub/Sub")
        .expect("rooted book has a key");
    assert_eq!(key, "ab01");
}

/// A rootless book would be published unordered; it is rejected instead.
#[test]
fn ordering_key_rejects_missing_root() {
    let err = require_ordering_key(&book_with_root(None), "Pub/Sub").unwrap_err();
    assert!(
        matches!(err, BusError::Publish(ref m) if m.contains("Pub/Sub")),
        "{err}"
    );
}

/// An empty root is as unordered as a missing one.
#[test]
fn ordering_key_rejects_empty_root() {
    assert!(require_ordering_key(&book_with_root(Some(vec![])), "Pub/Sub").is_err());
}
