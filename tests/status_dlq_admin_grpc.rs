//! Integration test: `DlqAdminHandler` (gRPC layer) -> real
//! `SqliteDlqReader` -> real SQLite DB.
//!
//! Run with:
//! ```bash
//! cargo test --test status_dlq_admin_grpc --features "test-utils" -- --nocapture
//! ```
//!
//! Closes the third Category B drift gap from the R2-15 audit: the
//! operator-facing surface. Phase 8 wired
//! `init_dlq_reader(&config.dlq.audit)` into `angzarr_status.rs`,
//! but no test exercised the actual gRPC handler against a real
//! reader against a real DB. `dlq_round_trip_sqlite.rs` proves
//! publisher -> reader at the storage layer; this test takes the
//! next step and drives `list_dead_letters` / `get_dead_letter` /
//! `delete_dead_letter` through the full `DlqAdminService` trait
//! against the same backing DB the publisher wrote into.
//!
//! What this catches that prior tests don't:
//!
//! - `DlqAdminHandler::list_dead_letters`'s envelope construction
//!   (state oneof, `checked_at`, `source`) against real reader
//!   output -- previously only the handler's `stored_to_proto`
//!   conversion had unit-level coverage.
//! - The AIP-160 filter parsing -> `ListFilter` -> backend WHERE
//!   pushdown end-to-end against a real SQL backend.
//! - `delete_dead_letter` actually removes the row from the
//!   underlying DB (vs. just returning `Ok(deleted=true)`).
//! - `get_dead_letter`'s no-row-matches branch returns
//!   `state.ok` with `entry = None`, not `state.degraded` (the
//!   handler's documented "successful empty query" contract).

#![cfg(feature = "test-utils")]

use std::sync::Arc;

use prost::Message;
use tempfile::TempDir;
use tonic::Request;

use angzarr::dlq::{AngzarrDeadLetter, DeadLetterPublisher, SqliteDlqPublisher, SqliteDlqReader};
use angzarr::proto::status::dlq_admin_service_server::DlqAdminService;
use angzarr::proto::status::{
    delete_dead_letter_response, get_dead_letter_response, list_dead_letters_response,
    DeleteDeadLetterRequest, GetDeadLetterRequest, ListDeadLettersRequest,
};
use angzarr::proto::{CommandBook, Cover, EventBook};
use angzarr::status::handlers::dlq::DlqAdminHandler;

// ============================================================================
// Fixtures
// ============================================================================

/// Per-test SQLite database file. Publisher creates the schema +
/// writes; reader opens its own pool against the same file (mirrors
/// production where the coordinator binary and `angzarr-status`
/// share storage but not pools).
async fn setup() -> (TempDir, SqliteDlqPublisher, DlqAdminHandler) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("dlq.db");
    let uri = format!("sqlite://{}?mode=rwc", path.display());

    let publisher = SqliteDlqPublisher::new(&uri).await.expect("init publisher");
    let reader = SqliteDlqReader::new(&uri).await.expect("init reader");
    let handler = DlqAdminHandler::new(Arc::new(reader));
    (dir, publisher, handler)
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

/// Populate the publisher with one entry per component type so list
/// + filter scenarios have something interesting to inspect.
async fn seed_one_per_component(publisher: &SqliteDlqPublisher) {
    publisher
        .publish(AngzarrDeadLetter::from_sequence_mismatch(
            &command("orders", "corr-agg"),
            3,
            5,
            angzarr::proto::MergeStrategy::MergeManual,
            "aggregate-orders",
        ))
        .await
        .expect("publish aggregate");

    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("inventory", "corr-saga"),
            "schema mismatch",
            0,
            false,
            "saga-order-to-inventory",
        ))
        .await
        .expect("publish saga");

    publisher
        .publish(AngzarrDeadLetter::from_pm_persist_failure(
            &events("fulfillment-pm", "corr-pm"),
            "Sequence conflict",
            5,
            true,
            "pm-fulfillment",
        ))
        .await
        .expect("publish PM");

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
        .expect("publish projector");
}

// ============================================================================
// Tests
// ============================================================================

/// Empty backend: `ListDeadLetters` returns `state.ok` with zero
/// entries (NOT `state.degraded`). Pins the "successful empty query"
/// contract that distinguishes a working backend with no entries
/// from a broken backend.
#[tokio::test]
async fn list_dead_letters_empty_backend_returns_ok_with_zero_entries() {
    let (_dir, _publisher, handler) = setup().await;
    let response = handler
        .list_dead_letters(Request::new(ListDeadLettersRequest::default()))
        .await
        .expect("list rpc");
    let inner = response.into_inner();

    match inner.state {
        Some(list_dead_letters_response::State::Ok(ok)) => {
            assert!(ok.entries.is_empty(), "expected zero entries");
            assert!(
                ok.next_page_token.is_empty(),
                "no continuation when no entries"
            );
        }
        other => panic!("expected state.ok, got {other:?}"),
    }
    assert!(
        inner.checked_at.is_some(),
        "envelope must always carry checked_at"
    );
}

/// `ListDeadLetters` round-trips every published DLQ entry, with
/// each entry's payload bytes decoding back to the original
/// `AngzarrDeadLetter` proto. Pins the publisher -> SQLite ->
/// reader -> gRPC handler -> proto-envelope path end-to-end.
#[tokio::test]
async fn list_dead_letters_returns_all_published_entries_with_correct_envelope() {
    let (_dir, publisher, handler) = setup().await;
    seed_one_per_component(&publisher).await;

    let response = handler
        .list_dead_letters(Request::new(ListDeadLettersRequest::default()))
        .await
        .expect("list rpc");
    let inner = response.into_inner();
    let ok = match inner.state {
        Some(list_dead_letters_response::State::Ok(ok)) => ok,
        other => panic!("expected state.ok, got {other:?}"),
    };
    assert_eq!(ok.entries.len(), 4, "expected 4 round-tripped entries");

    // Envelope checks.
    assert!(inner.checked_at.is_some(), "envelope carries checked_at");
    assert!(!inner.source.is_empty(), "envelope carries source id");

    // Each entry's `payload` field is the proto-encoded
    // AngzarrDeadLetter bytes, exactly as the publisher wrote.
    // Decoding back gives us the source_component_type the
    // coordinator stamped.
    let mut decoded_types = std::collections::HashSet::new();
    for entry in &ok.entries {
        let dl = angzarr::proto::AngzarrDeadLetter::decode(entry.payload.as_slice())
            .expect("decode payload");
        decoded_types.insert(dl.source_component_type);
    }
    for expected in ["aggregate", "saga", "process_manager", "projector"] {
        assert!(
            decoded_types.contains(expected),
            "missing {expected} in decoded payloads: {decoded_types:?}"
        );
    }
}

/// `ListDeadLetters` with an AIP-160 filter pushes the predicate
/// down to the SQL backend; the response contains ONLY matching
/// entries. Filter parsing + SQL WHERE generation is exercised
/// against a real backend, not the mock.
#[tokio::test]
async fn list_dead_letters_filter_scopes_to_matching_entries() {
    let (_dir, publisher, handler) = setup().await;
    seed_one_per_component(&publisher).await;

    let response = handler
        .list_dead_letters(Request::new(ListDeadLettersRequest {
            filter: "source_component = \"saga-order-to-inventory\"".to_string(),
            ..Default::default()
        }))
        .await
        .expect("list rpc");
    let ok = match response.into_inner().state {
        Some(list_dead_letters_response::State::Ok(ok)) => ok,
        other => panic!("expected state.ok, got {other:?}"),
    };
    assert_eq!(
        ok.entries.len(),
        1,
        "filter must scope to one saga entry, got {}",
        ok.entries.len()
    );
    assert_eq!(ok.entries[0].source_component, "saga-order-to-inventory");
}

/// `GetDeadLetter` returns the row matching its id. Round-trips the
/// payload bytes the publisher wrote, so the operator UI can decode
/// the original `AngzarrDeadLetter` proto without an extra
/// round-trip.
#[tokio::test]
async fn get_dead_letter_returns_existing_row() {
    let (_dir, publisher, handler) = setup().await;
    seed_one_per_component(&publisher).await;

    // Find the id of an entry via list, then fetch it.
    let list = handler
        .list_dead_letters(Request::new(ListDeadLettersRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let entries = match list.state {
        Some(list_dead_letters_response::State::Ok(ok)) => ok.entries,
        other => panic!("expected Ok, got {other:?}"),
    };
    let target_id = entries[0].id;
    let target_type = entries[0].source_component_type.clone();

    let get = handler
        .get_dead_letter(Request::new(GetDeadLetterRequest { id: target_id }))
        .await
        .expect("get rpc");
    let ok = match get.into_inner().state {
        Some(get_dead_letter_response::State::Ok(ok)) => ok,
        other => panic!("expected state.ok, got {other:?}"),
    };
    let entry = ok.entry.expect("entry must be Some for an existing id");
    assert_eq!(entry.id, target_id);
    assert_eq!(entry.source_component_type, target_type);
}

/// `GetDeadLetter` for a non-existent id returns `state.ok` with
/// `entry = None` -- the handler's documented "successful empty
/// query" contract that distinguishes "no such id" from "backend
/// down" (the latter would surface as `state.degraded`).
#[tokio::test]
async fn get_dead_letter_missing_id_returns_ok_with_no_entry() {
    let (_dir, _publisher, handler) = setup().await;
    let get = handler
        .get_dead_letter(Request::new(GetDeadLetterRequest { id: 999_999 }))
        .await
        .expect("get rpc");
    match get.into_inner().state {
        Some(get_dead_letter_response::State::Ok(ok)) => {
            assert!(
                ok.entry.is_none(),
                "missing id must return entry = None, got {:?}",
                ok.entry
            );
        }
        other => panic!("expected state.ok, got {other:?}"),
    }
}

/// `DeleteDeadLetter` returns `deleted = true` AND actually removes
/// the row from the backing DB. A subsequent list must NOT return
/// the deleted entry. Pins the destructive-side of the admin gRPC
/// against the real store -- the unit tests don't see this because
/// they end at the handler boundary.
#[tokio::test]
async fn delete_dead_letter_removes_row_from_db() {
    let (_dir, publisher, handler) = setup().await;
    seed_one_per_component(&publisher).await;

    let list_before = handler
        .list_dead_letters(Request::new(ListDeadLettersRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let entries_before = match list_before.state {
        Some(list_dead_letters_response::State::Ok(ok)) => ok.entries,
        other => panic!("expected Ok, got {other:?}"),
    };
    let target_id = entries_before[0].id;

    let delete = handler
        .delete_dead_letter(Request::new(DeleteDeadLetterRequest { id: target_id }))
        .await
        .expect("delete rpc");
    let ok = match delete.into_inner().state {
        Some(delete_dead_letter_response::State::Ok(ok)) => ok,
        other => panic!("expected state.ok, got {other:?}"),
    };
    assert!(ok.deleted, "deleted flag must be true for existing id");

    // Row is actually gone from the backing DB.
    let list_after = handler
        .list_dead_letters(Request::new(ListDeadLettersRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let entries_after = match list_after.state {
        Some(list_dead_letters_response::State::Ok(ok)) => ok.entries,
        other => panic!("expected Ok, got {other:?}"),
    };
    assert_eq!(
        entries_after.len(),
        entries_before.len() - 1,
        "delete must remove exactly one row; before={}, after={}",
        entries_before.len(),
        entries_after.len()
    );
    assert!(
        entries_after.iter().all(|e| e.id != target_id),
        "deleted id {target_id} must not appear in subsequent listing"
    );

    // get of the deleted id now returns entry=None.
    let get = handler
        .get_dead_letter(Request::new(GetDeadLetterRequest { id: target_id }))
        .await
        .unwrap()
        .into_inner();
    match get.state {
        Some(get_dead_letter_response::State::Ok(ok)) => {
            assert!(ok.entry.is_none(), "deleted id must get(None)");
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

/// `DeleteDeadLetter` for an id that doesn't exist returns
/// `state.ok` with `deleted = false`. Idempotent contract: calling
/// delete twice on the same id (or on a missing id) is not an error.
#[tokio::test]
async fn delete_dead_letter_missing_id_returns_deleted_false() {
    let (_dir, _publisher, handler) = setup().await;
    let delete = handler
        .delete_dead_letter(Request::new(DeleteDeadLetterRequest { id: 999_999 }))
        .await
        .expect("delete rpc");
    let ok = match delete.into_inner().state {
        Some(delete_dead_letter_response::State::Ok(ok)) => ok,
        other => panic!("expected state.ok, got {other:?}"),
    };
    assert!(
        !ok.deleted,
        "missing id must return deleted = false, got true"
    );
}
