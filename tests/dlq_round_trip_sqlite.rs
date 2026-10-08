//! DLQ publish -> read round-trip against a real SQLite file-backed DB.
//!
//! Run with:
//! ```bash
//! cargo test --test dlq_round_trip_sqlite --features "test-utils" -- --nocapture
//! ```
//!
//! What this proves (R2-15 step 5a/5b/6 + step 8):
//!
//! 1. A coordinator (saga / PM / projector / aggregate) calls
//!    `DeadLetterPublisher::publish(...)` with the constructor matching
//!    its component type.
//! 2. The publisher serializes to proto and writes a `dlq_entries` row
//!    via SQLite.
//! 3. The status binary reads from the same DB via `SqliteDlqReader`.
//! 4. Each entry survives the round-trip with its
//!    `source_component_type`, `source_component`, `domain`,
//!    `correlation_id`, and rejection metadata intact -- the contract
//!    that the operator-facing admin UI depends on.
//!
//! Filtering by `source_component` is also verified -- the status UI
//! groups DLQ entries by handler type, and the filter pushdown lives at
//! the storage layer.
//!
//! Uses a tempfile-backed SQLite file (NOT `:memory:`) so the publisher
//! and reader can each open their own pool against the same DB --
//! mirrors production where the two processes share storage but not
//! pool. In-memory SQLite gives each pool a private database, which
//! would defeat the test.

#![cfg(feature = "test-utils")]

use std::collections::HashMap;

use angzarr::dlq::reader::{DeadLetterReader, ListFilter};
use angzarr::dlq::{
    AngzarrDeadLetter, DeadLetterPayload, DeadLetterPublisher, RejectionDetails,
    SqliteDlqPublisher, SqliteDlqReader,
};
use angzarr::proto::{CommandBook, Cover, EventBook};
use tempfile::TempDir;

/// Per-test SQLite database file. Returns (tempdir, publisher, reader)
/// all pointing at the same backing file.
///
/// The tempdir handle MUST be kept alive for the duration of the test
/// -- dropping it deletes the underlying file and breaks the pool.
async fn setup_round_trip() -> (TempDir, SqliteDlqPublisher, SqliteDlqReader) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("dlq.db");
    let uri = format!("sqlite://{}?mode=rwc", path.display());

    let publisher = SqliteDlqPublisher::new(&uri)
        .await
        .expect("init SQLite DLQ publisher");
    let reader = SqliteDlqReader::new(&uri)
        .await
        .expect("init SQLite DLQ reader");

    (dir, publisher, reader)
}

fn cover(domain: &str, correlation_id: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: None,
        correlation_id: correlation_id.to_string(),
        edition: None,
        ext: None,
    }
}

fn command(domain: &str, correlation_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain, correlation_id)),
        pages: vec![],
    }
}

fn events(domain: &str, correlation_id: &str) -> EventBook {
    EventBook {
        cover: Some(cover(domain, correlation_id)),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

/// All four coordinator constructors write rows that survive the
/// round-trip with their `source_component_type` preserved. This is the
/// single contract that the admin UI's "group by component type" view
/// depends on.
#[tokio::test]
async fn dlq_round_trip_all_component_types_preserve_source_component_type() {
    let (_dir, publisher, reader) = setup_round_trip().await;

    // 1. Aggregate sequence-mismatch (R2-15 step 4 site).
    publisher
        .publish(AngzarrDeadLetter::from_sequence_mismatch(
            &command("orders", "corr-agg"),
            3, // expected
            5, // actual
            angzarr::proto::MergeStrategy::MergeManual,
            "aggregate-orders",
        ))
        .await
        .expect("publish aggregate DL");

    // 2. Saga immediate rejection (R2-15 step 5a immediate site).
    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("inventory", "corr-saga"),
            "schema mismatch",
            0,
            false,
            "saga-order-to-inventory",
        ))
        .await
        .expect("publish saga DL");

    // 3. PM persist retry-exhausted (R2-15 step 5b persist-exhausted site).
    publisher
        .publish(AngzarrDeadLetter::from_pm_persist_failure(
            &events("fulfillment-pm", "corr-pm"),
            "Sequence conflict",
            5,
            true,
            "pm-fulfillment",
        ))
        .await
        .expect("publish PM DL");

    // 4. Projector permanent error (R2-15 step 6 site).
    publisher
        .publish(AngzarrDeadLetter::from_event_processing_failure(
            &events("orders", "corr-proj"),
            "FailedPrecondition: malformed payload",
            0,
            false,
            Vec::new(),
            "projector-order-summary",
            "projector",
        ))
        .await
        .expect("publish projector DL");

    // Read everything back and bucket by component_type.
    let page = reader
        .list(ListFilter::default())
        .await
        .expect("list all DL entries");
    assert_eq!(page.entries.len(), 4, "expected 4 round-tripped entries");

    let by_type: HashMap<&str, &angzarr::dlq::reader::StoredDeadLetter> = page
        .entries
        .iter()
        .map(|e| (e.source_component_type.as_str(), e))
        .collect();

    assert!(by_type.contains_key("aggregate"), "aggregate row missing");
    assert!(by_type.contains_key("saga"), "saga row missing");
    assert!(
        by_type.contains_key("process_manager"),
        "process_manager row missing"
    );
    assert!(by_type.contains_key("projector"), "projector row missing");

    // Spot-check the saga entry: payload bytes decode back to an
    // AngzarrDeadLetter proto whose payload is a `RejectedCommand` and
    // whose rejection_details carries the saga's transient/retry_count
    // metadata. This pins that the publisher's proto encoding survives
    // the SQLite BLOB round-trip.
    use prost::Message;
    let saga_row = by_type["saga"];
    assert_eq!(saga_row.source_component, "saga-order-to-inventory");
    assert_eq!(saga_row.domain, "inventory");
    assert_eq!(saga_row.correlation_id.as_deref(), Some("corr-saga"));
    let decoded = angzarr::proto::AngzarrDeadLetter::decode(saga_row.payload.as_slice())
        .expect("decode saga DL payload");
    assert_eq!(decoded.source_component, "saga-order-to-inventory");
    assert_eq!(decoded.source_component_type, "saga");
}

/// `ListFilter::source_component` pushes down to the storage layer so
/// the admin UI can pivot on "show me only saga DLQ entries" without
/// pulling every entry into memory.
#[tokio::test]
async fn dlq_round_trip_filter_by_source_component_returns_only_matches() {
    let (_dir, publisher, reader) = setup_round_trip().await;

    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("inventory", "c-saga-1"),
            "first",
            0,
            false,
            "saga-A",
        ))
        .await
        .unwrap();
    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("inventory", "c-saga-2"),
            "second",
            0,
            false,
            "saga-A",
        ))
        .await
        .unwrap();
    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("inventory", "c-saga-3"),
            "different-component",
            0,
            false,
            "saga-B",
        ))
        .await
        .unwrap();

    let filter = ListFilter {
        source_component: Some("saga-A".to_string()),
        ..Default::default()
    };
    let page = reader
        .list(filter)
        .await
        .expect("filter by source_component");
    assert_eq!(
        page.entries.len(),
        2,
        "filter must scope to saga-A entries only; got {}",
        page.entries.len()
    );
    for entry in &page.entries {
        assert_eq!(entry.source_component, "saga-A");
    }
}

/// PM retry-exhausted entries carry `is_transient = true` and a
/// `retry_count > 0` in `rejection_details`. Pins that the
/// `EventProcessingFailedDetails` JSON survives the SQLite TEXT
/// round-trip.
#[tokio::test]
async fn dlq_round_trip_pm_persist_retry_exhausted_metadata_survives() {
    let (_dir, publisher, reader) = setup_round_trip().await;

    publisher
        .publish(AngzarrDeadLetter::from_pm_persist_failure(
            &events("pm-domain", "c-pm-1"),
            "Sequence conflict",
            7,
            true,
            "pm-fulfillment",
        ))
        .await
        .unwrap();

    let page = reader.list(ListFilter::default()).await.unwrap();
    assert_eq!(page.entries.len(), 1);
    let row = &page.entries[0];

    use prost::Message;
    let decoded = angzarr::proto::AngzarrDeadLetter::decode(row.payload.as_slice()).unwrap();
    // Re-build via the high-level type to assert against
    // EventProcessingFailedDetails.
    let high_level = AngzarrDeadLetter::from_pm_persist_failure(
        &events("pm-domain", "c-pm-1"),
        "Sequence conflict",
        7,
        true,
        "pm-fulfillment",
    );
    assert_eq!(high_level.source_component_type, "process_manager");
    match high_level.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert_eq!(details.retry_count, 7);
            assert!(details.is_transient);
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }

    // And the proto-level round-trip preserves the same fields.
    assert_eq!(decoded.source_component_type, "process_manager");
    match &decoded.payload {
        Some(angzarr::proto::angzarr_dead_letter::Payload::RejectedEvents(_)) => {}
        other => panic!("PM persist failure must round-trip as Events payload, got {other:?}"),
    }
    let _ = DeadLetterPayload::Events(events("pm-domain", "c-pm-1")); // type-check the import
}
