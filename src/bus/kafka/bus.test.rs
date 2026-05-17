//! Unit tests for the Kafka publisher's routing-key boundary (H-10
//! regression suite).
//!
//! H-10 (from `plans/deep-review-remediation.md`):
//!   The publisher previously did
//!     `if let Some(ref k) = key { record = record.key(k); }`
//!   If `book.root_id_hex()` returned `None`, no key was set and the
//!   default partitioner round-robined the message across partitions.
//!   Root-less events therefore had no ordering relationship with
//!   anything — silently bypassing the per-aggregate-root ordering
//!   guarantee the rest of the bus layer documents.
//!
//! Chosen contract (mirrors C-12 on SNS/SQS FIFO):
//!   * Root-less EventBooks are REJECTED at the publisher boundary
//!     with `BusError::Publish`. There is no legitimate use of root-
//!     less Kafka events in this framework (aggregates own roots;
//!     sagas/PMs propagate them through the cover). Falling back to
//!     round-robin would silently disable ordering for whoever
//!     accidentally produced a root-less event.
//!   * The error message explicitly names the root cause so operators
//!     don't chase a downstream consumer-ordering symptom.
//!
//! The construction helper `validate_publish_key` is a pure function so
//! these tests do not need a Kafka broker.

use super::validate_publish_key;
use crate::proto::{Cover, EventBook, Uuid as ProtoUuid};
use crate::test_utils::{make_cover_with_root, make_event_page};
use uuid::Uuid;

fn book_with_root(domain: &str, root: Uuid) -> EventBook {
    EventBook {
        cover: Some(make_cover_with_root(domain, root)),
        pages: vec![make_event_page(0)],
        snapshot: None,
        ..Default::default()
    }
}

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

/// H-10: an EventBook without a root must be rejected at the publisher
/// boundary, not silently round-robined.
///
/// Baseline behaviour: `validate_publish_key` returns `Ok(None)`; the
/// publisher then skips `record.key(...)` and Kafka's default
/// partitioner round-robins.
///
/// Fixed behaviour: returns `Err(BusError::Publish)` with an
/// operator-readable message naming "missing root".
#[test]
fn validate_publish_key_rejects_rootless_event_book() {
    let book = book_without_root("orders");
    let result = validate_publish_key(&book);
    assert!(
        result.is_err(),
        "root-less EventBook must be rejected at the Kafka publisher \
         boundary (mirrors C-12 on SNS/SQS FIFO); got Ok({:?})",
        result.ok()
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("missing root") || err_msg.contains("non-empty"),
        "error must name the root cause; got: {}",
        err_msg
    );
}

/// H-10 (second variant): a Cover whose `root` ProtoUuid is present but
/// carries an empty byte vector also produces an empty hex string. An
/// empty-string key would partition-hash to a single partition and
/// silently collapse all such events into one ordering group. Same
/// shape as the SNS/SQS empty MessageGroupId case; treat it the same
/// way.
#[test]
fn validate_publish_key_rejects_empty_root_bytes() {
    let book = book_with_empty_root("orders");
    let result = validate_publish_key(&book);
    assert!(
        result.is_err(),
        "EventBook with present-but-empty root must be rejected; \
         got Ok({:?})",
        result.ok()
    );
}

/// Regression guard: a happy-path publish (root present) must return
/// a non-empty key. The fix must not break the existing successful
/// path.
#[test]
fn validate_publish_key_happy_path_returns_root_hex() {
    let root = Uuid::new_v4();
    let book = book_with_root("orders", root);
    let key = validate_publish_key(&book).expect("happy-path publish must succeed");
    assert!(!key.is_empty(), "happy-path key must be non-empty");
    assert_eq!(
        key,
        hex::encode(root.as_bytes()),
        "key must be hex-encoded aggregate root for per-root partitioning"
    );
}

/// Regression guard: distinct roots produce distinct keys so Kafka's
/// hash partitioner spreads aggregates across partitions for
/// parallelism while preserving per-root ordering.
#[test]
fn validate_publish_key_distinct_roots_get_distinct_keys() {
    let root_a = Uuid::new_v4();
    let root_b = Uuid::new_v4();
    let book_a = book_with_root("orders", root_a);
    let book_b = book_with_root("orders", root_b);

    let key_a = validate_publish_key(&book_a).expect("publish A must succeed");
    let key_b = validate_publish_key(&book_b).expect("publish B must succeed");

    assert_ne!(
        key_a, key_b,
        "distinct aggregate roots must produce distinct partition keys"
    );
}
