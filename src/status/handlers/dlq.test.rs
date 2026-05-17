//! Tests for the DlqAdminService handler.
//!
//! WHY: the handler is the contract the SPA keys on. Drift in the
//! envelope shape (state discriminator, source field, checked_at
//! presence) breaks every tile silently. Pinning the envelope here
//! catches it before the SPA does. The Noop-degraded path is
//! exercised explicitly because that's the failure mode operators
//! will see first when they spin up a status console with no
//! DB-backed publisher configured.

use std::sync::Arc;

use chrono::{TimeZone, Utc};

use super::*;
use crate::dlq::reader::{
    DeadLetterPage, DeadLetterReader, ListFilter, NoopDeadLetterReader, StoredDeadLetter,
};
use crate::dlq::DlqError;
use crate::proto::status::{
    delete_dead_letter_response::State as DelState, get_dead_letter_response::State as GetState,
    list_dead_letters_response::State as ListState, DeleteDeadLetterRequest, GetDeadLetterRequest,
    ListDeadLettersRequest, RejectionType,
};

// -------- Test doubles -----------------------------------------------------

/// Reader that returns one well-known row + a next_page_token.
/// Lets us exercise the success path without touching a DB.
struct FixtureReader;

#[async_trait::async_trait]
impl DeadLetterReader for FixtureReader {
    async fn list(&self, _filter: ListFilter) -> Result<DeadLetterPage, DlqError> {
        Ok(DeadLetterPage {
            entries: vec![fixture_row(42)],
            next_page_token: Some("tok-2".to_string()),
        })
    }
    async fn get(&self, id: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
        if id == 42 {
            Ok(Some(fixture_row(42)))
        } else {
            Ok(None)
        }
    }
    async fn delete(&self, id: i64) -> Result<bool, DlqError> {
        Ok(id == 42)
    }
    fn source_id(&self) -> &'static str {
        "fixture"
    }
}

fn fixture_row(id: i64) -> StoredDeadLetter {
    StoredDeadLetter {
        id,
        domain: "player".to_string(),
        correlation_id: Some("corr-1".to_string()),
        payload: vec![0x0a, 0x05, b'h', b'e', b'l', b'l', b'o'],
        rejection_reason: "test".to_string(),
        rejection_type: "sequence_mismatch".to_string(),
        details: Some("{}".to_string()),
        source_component: "aggregate".to_string(),
        source_component_type: "aggregate".to_string(),
        occurred_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 0).unwrap(),
        created_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 1).unwrap(),
    }
}

fn handler_with<R: DeadLetterReader + 'static>(r: R) -> DlqAdminHandler {
    DlqAdminHandler::new(Arc::new(r))
}

// -------- Envelope shape (cross-cutting) -----------------------------------

#[tokio::test]
async fn list_noop_returns_degraded_envelope_with_source_noop() {
    // Plan tolerance contract: NotConfigured backend → degraded state,
    // not a gRPC error. The whole UI stays up; only this tile shows
    // the error.
    let h = handler_with(NoopDeadLetterReader);
    let resp = h
        .list_dead_letters(tonic::Request::new(ListDeadLettersRequest::default()))
        .await
        .expect("handler must not surface gRPC error on backend NotConfigured")
        .into_inner();

    let state = resp.state.expect("envelope state must be populated");
    assert!(matches!(state, ListState::Degraded(_)));
    assert_eq!(resp.source, "noop");
    assert!(resp.checked_at.is_some());
}

#[tokio::test]
async fn list_configured_reader_returns_ok_envelope() {
    let h = handler_with(FixtureReader);
    let resp = h
        .list_dead_letters(tonic::Request::new(ListDeadLettersRequest::default()))
        .await
        .unwrap()
        .into_inner();

    let state = resp.state.expect("envelope state populated");
    match state {
        ListState::Ok(ok) => {
            assert_eq!(ok.entries.len(), 1);
            assert_eq!(ok.entries[0].id, 42);
            assert_eq!(ok.entries[0].domain, "player");
            assert_eq!(
                ok.entries[0].rejection_type,
                RejectionType::SequenceMismatch as i32
            );
            assert_eq!(ok.next_page_token, "tok-2");
        }
        ListState::Degraded(p) => panic!("expected ok, got degraded: {}", p.detail),
    }
    assert_eq!(resp.source, "fixture");
}

// -------- Get --------------------------------------------------------------

#[tokio::test]
async fn get_existing_id_returns_ok_with_entry() {
    let h = handler_with(FixtureReader);
    let resp = h
        .get_dead_letter(tonic::Request::new(GetDeadLetterRequest { id: 42 }))
        .await
        .unwrap()
        .into_inner();

    match resp.state.unwrap() {
        GetState::Ok(ok) => assert_eq!(ok.entry.unwrap().id, 42),
        GetState::Degraded(p) => panic!("expected ok, got {}", p.detail),
    }
}

#[tokio::test]
async fn get_missing_id_returns_ok_with_no_entry_not_degraded() {
    // Critical distinction: "no row matches" is success, not
    // degradation. Mixing them would make the UI show "backend down"
    // for a perfectly healthy empty query.
    let h = handler_with(FixtureReader);
    let resp = h
        .get_dead_letter(tonic::Request::new(GetDeadLetterRequest { id: 999 }))
        .await
        .unwrap()
        .into_inner();

    match resp.state.unwrap() {
        GetState::Ok(ok) => assert!(ok.entry.is_none()),
        GetState::Degraded(p) => panic!("must NOT degrade on missing row: {}", p.detail),
    }
}

#[tokio::test]
async fn get_noop_returns_degraded() {
    let h = handler_with(NoopDeadLetterReader);
    let resp = h
        .get_dead_letter(tonic::Request::new(GetDeadLetterRequest { id: 1 }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(resp.state.unwrap(), GetState::Degraded(_)));
}

// -------- Delete -----------------------------------------------------------

#[tokio::test]
async fn delete_existing_id_returns_deleted_true() {
    let h = handler_with(FixtureReader);
    let resp = h
        .delete_dead_letter(tonic::Request::new(DeleteDeadLetterRequest { id: 42 }))
        .await
        .unwrap()
        .into_inner();
    match resp.state.unwrap() {
        DelState::Ok(ok) => assert!(ok.deleted),
        DelState::Degraded(p) => panic!("expected ok: {}", p.detail),
    }
}

#[tokio::test]
async fn delete_missing_id_returns_deleted_false_not_degraded() {
    // Idempotent contract: deleting a nonexistent row is success, not
    // degradation. Operator can hammer the button safely.
    let h = handler_with(FixtureReader);
    let resp = h
        .delete_dead_letter(tonic::Request::new(DeleteDeadLetterRequest { id: 999 }))
        .await
        .unwrap()
        .into_inner();
    match resp.state.unwrap() {
        DelState::Ok(ok) => assert!(!ok.deleted),
        DelState::Degraded(_) => panic!("must NOT degrade on missing row"),
    }
}

#[tokio::test]
async fn delete_noop_returns_degraded() {
    let h = handler_with(NoopDeadLetterReader);
    let resp = h
        .delete_dead_letter(tonic::Request::new(DeleteDeadLetterRequest { id: 1 }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(resp.state.unwrap(), DelState::Degraded(_)));
}

// -------- ProblemDetails shape --------------------------------------------

#[tokio::test]
async fn noop_problem_details_carries_503_status() {
    // RFC 7807 conformance: ProblemDetails.status mirrors the
    // HTTP-equivalent code so the envoy sidecar can stamp the same
    // status on the wire when it transcodes. NotConfigured ≡ 503
    // Service Unavailable per the handler's mapping table.
    let h = handler_with(NoopDeadLetterReader);
    let resp = h
        .list_dead_letters(tonic::Request::new(ListDeadLettersRequest::default()))
        .await
        .unwrap()
        .into_inner();
    match resp.state.unwrap() {
        ListState::Degraded(p) => {
            assert_eq!(p.status, 503);
            assert!(!p.title.is_empty());
            assert!(p.r#type.starts_with("urn:angzarr:status:dlq:"));
        }
        ListState::Ok(_) => panic!("expected degraded"),
    }
}

// -------- Rejection-type conversion ---------------------------------------

#[test]
fn rejection_type_from_str_known_values() {
    // The publisher writes lowercase discriminators; the proto enum
    // is the wire-stable surface. Pinning the mapping here catches a
    // rename-without-grep on either side.
    assert_eq!(
        super::rejection_type_from_str("sequence_mismatch"),
        RejectionType::SequenceMismatch
    );
    assert_eq!(
        super::rejection_type_from_str("event_processing_failed"),
        RejectionType::EventProcessingFailed
    );
    assert_eq!(
        super::rejection_type_from_str("payload_retrieval_failed"),
        RejectionType::PayloadRetrievalFailed
    );
}

#[test]
fn rejection_type_from_str_unknown_falls_back_to_unspecified() {
    // Tolerance: a future / unknown discriminator string must not
    // crash the handler; it surfaces as Unspecified for the SPA to
    // render as "other".
    assert_eq!(
        super::rejection_type_from_str("brand-new-type-2030"),
        RejectionType::Unspecified
    );
    assert_eq!(
        super::rejection_type_from_str(""),
        RejectionType::Unspecified
    );
}

// ============================================================================
// payload_view round-trip (P1.2.5)
// ============================================================================
//
// Operators need to SEE messages. The handler decodes the proto-encoded
// AngzarrDeadLetter payload to JSON server-side so the SPA gets
// structured content in one round-trip.

/// Reader that returns a row whose `payload` is a real
/// proto-encoded `AngzarrDeadLetter`. Lets us exercise the
/// decode-to-JSON path end-to-end.
struct RealisticPayloadReader;

#[async_trait::async_trait]
impl DeadLetterReader for RealisticPayloadReader {
    async fn list(&self, _filter: ListFilter) -> Result<DeadLetterPage, DlqError> {
        Ok(DeadLetterPage {
            entries: vec![row_with_real_payload()],
            next_page_token: None,
        })
    }
    async fn get(&self, _id: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
        Ok(Some(row_with_real_payload()))
    }
    async fn delete(&self, _id: i64) -> Result<bool, DlqError> {
        Ok(true)
    }
    fn source_id(&self) -> &'static str {
        "realistic"
    }
}

fn row_with_real_payload() -> StoredDeadLetter {
    use prost::Message;
    let dl = crate::proto::AngzarrDeadLetter {
        cover: Some(crate::proto::Cover {
            domain: "player".to_string(),
            root: None,
            correlation_id: "trace-payload-view".to_string(),
            edition: None,
        }),
        rejection_reason: "for the operator's eyes".to_string(),
        source_component: "agg-player".to_string(),
        source_component_type: "aggregate".to_string(),
        ..Default::default()
    };
    StoredDeadLetter {
        id: 1,
        domain: "player".to_string(),
        correlation_id: Some("trace-payload-view".to_string()),
        payload: dl.encode_to_vec(),
        rejection_reason: "for the operator's eyes".to_string(),
        rejection_type: "sequence_mismatch".to_string(),
        details: None,
        source_component: "agg-player".to_string(),
        source_component_type: "aggregate".to_string(),
        occurred_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 0).unwrap(),
        created_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 1).unwrap(),
    }
}

#[tokio::test]
async fn list_populates_payload_view_with_decoded_json() {
    // Ensure pool initialized so payload_view actually decodes.
    let _ = crate::proto_reflect::ensure_initialized();
    let h = handler_with(RealisticPayloadReader);
    let resp = h
        .list_dead_letters(tonic::Request::new(ListDeadLettersRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let entries = match resp.state.unwrap() {
        ListState::Ok(ok) => ok.entries,
        ListState::Degraded(p) => panic!("expected ok, got degraded: {}", p.detail),
    };
    assert_eq!(entries.len(), 1);
    let view = &entries[0].payload_view;
    assert!(!view.is_empty(), "payload_view must be non-empty after decode");
    assert!(
        view.contains("trace-payload-view"),
        "decoded JSON should surface correlation_id: {}",
        view
    );
    assert!(
        view.contains("for the operator's eyes"),
        "decoded JSON should surface rejection_reason: {}",
        view
    );
}

#[tokio::test]
async fn get_populates_payload_view_with_decoded_json() {
    let _ = crate::proto_reflect::ensure_initialized();
    let h = handler_with(RealisticPayloadReader);
    let resp = h
        .get_dead_letter(tonic::Request::new(GetDeadLetterRequest { id: 1 }))
        .await
        .unwrap()
        .into_inner();
    let entry = match resp.state.unwrap() {
        GetState::Ok(ok) => ok.entry.unwrap(),
        GetState::Degraded(p) => panic!("expected ok: {}", p.detail),
    };
    assert!(!entry.payload_view.is_empty());
    assert!(entry.payload_view.contains("player"));
}

// ============================================================================
// Replay path (P1.3)
// ============================================================================
//
// WHY: replay is the operator's primary recovery action. The handler
// must (a) fetch the row, (b) decode the AngzarrDeadLetter, (c) stamp
// a fresh correlation_id + audit metadata, (d) hand the command to the
// publisher, (e) return a Health<T> envelope with new_correlation_id.
// Failure modes — missing row, garbage payload, event-replay-unsupported,
// publisher unconfigured, publisher backend error — must each produce
// a distinguishable degraded response, never a transport-level error.

/// In-memory replay publisher that records the most-recent command it
/// was handed, so tests can assert the handler stamped metadata as
/// expected.
struct RecordingReplayPublisher {
    last: std::sync::Mutex<Option<crate::proto::CommandBook>>,
}

impl RecordingReplayPublisher {
    fn new() -> Self {
        Self {
            last: std::sync::Mutex::new(None),
        }
    }
    fn taken(&self) -> Option<crate::proto::CommandBook> {
        self.last.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::dlq::ReplayPublisher for RecordingReplayPublisher {
    async fn replay(
        &self,
        command: crate::proto::CommandBook,
    ) -> Result<(), crate::dlq::DlqError> {
        *self.last.lock().unwrap() = Some(command);
        Ok(())
    }
    fn source_id(&self) -> &'static str {
        "recording"
    }
}

fn handler_with_replay<R, P>(reader: R, publisher: P) -> DlqAdminHandler
where
    R: DeadLetterReader + 'static,
    P: crate::dlq::ReplayPublisher + 'static,
{
    DlqAdminHandler::new_with_replay(Arc::new(reader), Arc::new(publisher))
}

/// Reader serving a real AngzarrDeadLetter that wraps a real
/// CommandBook — exercises the full decode → stamp → publish chain.
struct CommandReplayReader;

#[async_trait::async_trait]
impl DeadLetterReader for CommandReplayReader {
    async fn list(&self, _: ListFilter) -> Result<DeadLetterPage, DlqError> {
        Ok(DeadLetterPage {
            entries: vec![cmd_dl_row(42, "original-trace")],
            next_page_token: None,
        })
    }
    async fn get(&self, id: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
        if id == 42 {
            Ok(Some(cmd_dl_row(id, "original-trace")))
        } else {
            Ok(None)
        }
    }
    async fn delete(&self, _: i64) -> Result<bool, DlqError> {
        Ok(false)
    }
}

fn cmd_dl_row(id: i64, correlation: &str) -> StoredDeadLetter {
    use crate::proto::{
        command_page, page_header::SequenceType, CommandBook, CommandPage, Cover, MergeStrategy,
        PageHeader,
    };
    let cmd = CommandBook {
        cover: Some(Cover {
            domain: "player".to_string(),
            root: None,
            correlation_id: correlation.to_string(),
            edition: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(7)),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "type.googleapis.com/test.RegisterPlayer".to_string(),
                value: vec![0x0a, 0x05, b'a', b'l', b'i', b'c', b'e'],
            })),
            merge_strategy: MergeStrategy::MergeStrict as i32,
        }],
    };
    let dl = crate::proto::AngzarrDeadLetter {
        cover: Some(crate::proto::Cover {
            domain: "player".to_string(),
            root: None,
            correlation_id: correlation.to_string(),
            edition: None,
        }),
        rejection_reason: "sequence mismatch".to_string(),
        source_component: "agg-player".to_string(),
        source_component_type: "aggregate".to_string(),
        payload: Some(crate::proto::angzarr_dead_letter::Payload::RejectedCommand(cmd)),
        ..Default::default()
    };
    StoredDeadLetter {
        id,
        domain: "player".to_string(),
        correlation_id: Some(correlation.to_string()),
        payload: dl.encode_to_vec(),
        rejection_reason: "sequence mismatch".to_string(),
        rejection_type: "sequence_mismatch".to_string(),
        details: None,
        source_component: "agg-player".to_string(),
        source_component_type: "aggregate".to_string(),
        occurred_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 0).unwrap(),
        created_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 1).unwrap(),
    }
}

#[tokio::test]
async fn replay_happy_path_returns_new_correlation_id() {
    use crate::proto::status::replay_dead_letter_response::State;
    let _ = crate::proto_reflect::ensure_initialized();
    let pubr = Arc::new(RecordingReplayPublisher::new());
    let h = DlqAdminHandler::new_with_replay(
        Arc::new(CommandReplayReader),
        pubr.clone() as Arc<dyn crate::dlq::ReplayPublisher>,
    );
    let resp = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 42,
            replay_mode: crate::proto::status::ReplayMode::FreshSequence as i32,
        }))
        .await
        .unwrap()
        .into_inner();

    let ok = match resp.state.unwrap() {
        State::Ok(o) => o,
        State::Degraded(p) => panic!("expected ok, got degraded: {}", p.detail),
    };
    assert!(!ok.new_correlation_id.is_empty());
    assert_ne!(
        ok.new_correlation_id, "original-trace",
        "new correlation_id MUST differ from original"
    );
    assert_eq!(
        ok.applied_mode,
        crate::proto::status::ReplayMode::FreshSequence as i32
    );
    assert_eq!(resp.source, "recording");
}

#[tokio::test]
async fn replay_rewrites_correlation_id_on_published_command() {
    // The handler stamps a fresh correlation_id on the CommandBook
    // before handing it to the publisher. Catches regressions where
    // a refactor accidentally drops the rewrite step.
    let _ = crate::proto_reflect::ensure_initialized();
    let pubr = Arc::new(RecordingReplayPublisher::new());
    let h = DlqAdminHandler::new_with_replay(
        Arc::new(CommandReplayReader),
        pubr.clone() as Arc<dyn crate::dlq::ReplayPublisher>,
    );
    let _ = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 42,
            replay_mode: crate::proto::status::ReplayMode::AsIs as i32,
        }))
        .await
        .unwrap();

    let captured = pubr.taken().expect("publisher must have received a command");
    let new_corr = captured.cover.unwrap().correlation_id;
    assert!(!new_corr.is_empty());
    assert_ne!(new_corr, "original-trace");
}

#[tokio::test]
async fn replay_missing_id_returns_404_problem_details() {
    // Operator hits replay on a row that's already been deleted by
    // another operator. Surfaced as a clear 404, not a silent
    // success.
    use crate::proto::status::replay_dead_letter_response::State;
    let pubr = RecordingReplayPublisher::new();
    let h = handler_with_replay(CommandReplayReader, pubr);
    let resp = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 99_999,
            replay_mode: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.state.unwrap() {
        State::Degraded(p) => {
            assert_eq!(p.status, 404);
            assert!(p.r#type.contains("not-found"));
        }
        State::Ok(_) => panic!("missing id must surface as degraded 404"),
    }
}

#[tokio::test]
async fn replay_with_noop_publisher_returns_degraded_503() {
    // The bootstrap default — status binary running without
    // replay-publisher wiring. Operators see "not configured", not a
    // silent fake success.
    use crate::proto::status::replay_dead_letter_response::State;
    let h = DlqAdminHandler::new(Arc::new(CommandReplayReader));
    let resp = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 42,
            replay_mode: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.state.unwrap() {
        State::Degraded(p) => assert_eq!(p.status, 503),
        State::Ok(_) => panic!("noop publisher must degrade"),
    }
    assert_eq!(resp.source, "noop");
}

#[tokio::test]
async fn replay_of_event_dead_letter_returns_400_unsupported() {
    // Phase 1.3 ships command-replay only. Event-replay needs a
    // to_handler selector (P1.5). An event row must surface a
    // clear "not yet supported" 400 — not silently no-op, not
    // crash.
    use crate::proto::status::replay_dead_letter_response::State;

    struct EventReader;
    #[async_trait::async_trait]
    impl DeadLetterReader for EventReader {
        async fn list(&self, _: ListFilter) -> Result<DeadLetterPage, DlqError> {
            Ok(DeadLetterPage {
                entries: vec![],
                next_page_token: None,
            })
        }
        async fn get(&self, _: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
            let dl = crate::proto::AngzarrDeadLetter {
                cover: Some(crate::proto::Cover {
                    domain: "player".to_string(),
                    root: None,
                    correlation_id: "ev".to_string(),
                    edition: None,
                }),
                rejection_reason: "event processing failed".to_string(),
                source_component: "saga-x".to_string(),
                source_component_type: "saga".to_string(),
                payload: Some(crate::proto::angzarr_dead_letter::Payload::RejectedEvents(
                    crate::proto::EventBook::default(),
                )),
                ..Default::default()
            };
            Ok(Some(StoredDeadLetter {
                id: 5,
                domain: "player".to_string(),
                correlation_id: Some("ev".to_string()),
                payload: dl.encode_to_vec(),
                rejection_reason: "x".to_string(),
                rejection_type: "event_processing_failed".to_string(),
                details: None,
                source_component: "saga-x".to_string(),
                source_component_type: "saga".to_string(),
                occurred_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 0).unwrap(),
                created_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 1).unwrap(),
            }))
        }
        async fn delete(&self, _: i64) -> Result<bool, DlqError> {
            Ok(false)
        }
    }

    let h = handler_with_replay(EventReader, RecordingReplayPublisher::new());
    let resp = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 5,
            replay_mode: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.state.unwrap() {
        State::Degraded(p) => {
            assert_eq!(p.status, 400);
            assert!(p.r#type.contains("event-replay-unsupported"));
        }
        State::Ok(_) => panic!("event-replay must surface 400 until P1.5"),
    }
}

/// Recording audit writer — captures records in memory so handler
/// tests can assert success/failure paths both flow into audit.
struct RecordingAuditWriter {
    rows: std::sync::Mutex<Vec<crate::dlq::ReplayAuditRecord>>,
}

impl RecordingAuditWriter {
    fn new() -> Self {
        Self {
            rows: std::sync::Mutex::new(vec![]),
        }
    }
    fn rows(&self) -> Vec<crate::dlq::ReplayAuditRecord> {
        self.rows.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::dlq::ReplayAuditWriter for RecordingAuditWriter {
    async fn record(
        &self,
        record: crate::dlq::ReplayAuditRecord,
    ) -> Result<(), crate::dlq::DlqError> {
        self.rows.lock().unwrap().push(record);
        Ok(())
    }
    fn source_id(&self) -> &'static str {
        "recording-audit"
    }
}

#[tokio::test]
async fn replay_success_writes_audit_row_with_success_outcome() {
    let _ = crate::proto_reflect::ensure_initialized();
    let pubr = Arc::new(RecordingReplayPublisher::new());
    let audit = Arc::new(RecordingAuditWriter::new());
    let h = DlqAdminHandler::new_with_audit(
        Arc::new(CommandReplayReader),
        pubr.clone() as Arc<dyn crate::dlq::ReplayPublisher>,
        audit.clone() as Arc<dyn crate::dlq::ReplayAuditWriter>,
    );
    let _ = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 42,
            replay_mode: crate::proto::status::ReplayMode::FreshSequence as i32,
        }))
        .await
        .unwrap();

    let rows = audit.rows();
    assert_eq!(rows.len(), 1, "exactly one audit row per replay");
    assert_eq!(rows[0].dlq_id, 42);
    assert_eq!(rows[0].outcome, crate::dlq::ReplayOutcome::Success);
    assert!(!rows[0].new_correlation_id.is_empty());
    assert_eq!(
        rows[0].original_correlation_id.as_deref(),
        Some("original-trace")
    );
    assert_eq!(rows[0].replay_mode, crate::dlq::ReplayMode::FreshSequence);
    assert!(rows[0].result_message.is_none());
}

#[tokio::test]
async fn replay_failure_writes_audit_row_with_failure_outcome() {
    // Failed replays must audit too — operators need to investigate
    // why a replay didn't take.
    let _ = crate::proto_reflect::ensure_initialized();
    struct AlwaysFailPublisher;
    #[async_trait::async_trait]
    impl crate::dlq::ReplayPublisher for AlwaysFailPublisher {
        async fn replay(
            &self,
            _: crate::proto::CommandBook,
        ) -> Result<(), crate::dlq::DlqError> {
            Err(crate::dlq::DlqError::Connection("nope".to_string()))
        }
        fn source_id(&self) -> &'static str {
            "failing"
        }
    }

    let audit = Arc::new(RecordingAuditWriter::new());
    let h = DlqAdminHandler::new_with_audit(
        Arc::new(CommandReplayReader),
        Arc::new(AlwaysFailPublisher),
        audit.clone() as Arc<dyn crate::dlq::ReplayAuditWriter>,
    );
    let _ = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 42,
            replay_mode: crate::proto::status::ReplayMode::AsIs as i32,
        }))
        .await
        .unwrap();

    let rows = audit.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, crate::dlq::ReplayOutcome::Failure);
    assert!(rows[0].result_message.is_some());
}

#[tokio::test]
async fn replay_does_not_write_audit_when_row_missing() {
    // No replay was performed → no audit row. Avoids polluting the
    // audit log with "operator clicked replay on a deleted row" noise.
    let audit = Arc::new(RecordingAuditWriter::new());
    let h = DlqAdminHandler::new_with_audit(
        Arc::new(CommandReplayReader),
        Arc::new(RecordingReplayPublisher::new()),
        audit.clone() as Arc<dyn crate::dlq::ReplayAuditWriter>,
    );
    let _ = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 99_999,
            replay_mode: 0,
        }))
        .await
        .unwrap();
    assert!(audit.rows().is_empty(), "missing-id replay must not audit");
}

#[tokio::test]
async fn audit_write_failure_does_not_break_replay_response() {
    // The plan's resilience contract: degraded observability is
    // preferable to swallowing a successful replay. If the audit
    // backend is down, the replay still completes and the operator
    // sees the success envelope.
    use crate::proto::status::replay_dead_letter_response::State;
    let _ = crate::proto_reflect::ensure_initialized();

    struct BrokenAuditWriter;
    #[async_trait::async_trait]
    impl crate::dlq::ReplayAuditWriter for BrokenAuditWriter {
        async fn record(
            &self,
            _: crate::dlq::ReplayAuditRecord,
        ) -> Result<(), crate::dlq::DlqError> {
            Err(crate::dlq::DlqError::Connection("audit-down".to_string()))
        }
    }

    let pubr = Arc::new(RecordingReplayPublisher::new());
    let h = DlqAdminHandler::new_with_audit(
        Arc::new(CommandReplayReader),
        pubr.clone() as Arc<dyn crate::dlq::ReplayPublisher>,
        Arc::new(BrokenAuditWriter),
    );
    let resp = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 42,
            replay_mode: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    // Replay still succeeded — audit failure is the lesser harm.
    assert!(matches!(resp.state.unwrap(), State::Ok(_)));
}

#[tokio::test]
async fn replay_garbage_payload_bytes_returns_500_degraded() {
    // Tolerance: a DB row with corrupt bytes must not crash the
    // handler — return a 500-class ProblemDetails so the operator
    // can move on (delete the corrupt row, escalate, etc.).
    use crate::proto::status::replay_dead_letter_response::State;

    struct GarbageRowReader;
    #[async_trait::async_trait]
    impl DeadLetterReader for GarbageRowReader {
        async fn list(&self, _: ListFilter) -> Result<DeadLetterPage, DlqError> {
            Ok(DeadLetterPage {
                entries: vec![],
                next_page_token: None,
            })
        }
        async fn get(&self, _: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
            Ok(Some(StoredDeadLetter {
                id: 1,
                domain: "x".to_string(),
                correlation_id: None,
                payload: vec![0xff; 16],
                rejection_reason: "x".to_string(),
                rejection_type: "sequence_mismatch".to_string(),
                details: None,
                source_component: "x".to_string(),
                source_component_type: "x".to_string(),
                occurred_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            }))
        }
        async fn delete(&self, _: i64) -> Result<bool, DlqError> {
            Ok(false)
        }
    }

    let h = handler_with_replay(GarbageRowReader, RecordingReplayPublisher::new());
    let resp = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 1,
            replay_mode: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.state.unwrap() {
        State::Degraded(p) => assert_eq!(p.status, 500),
        State::Ok(_) => panic!("garbage payload must degrade"),
    }
}

#[tokio::test]
async fn payload_view_empty_when_payload_bytes_are_garbage() {
    // Tolerance contract: a row whose payload doesn't decode
    // must still surface (with payload_view = ""), not crash the
    // whole list call. The operator can still see the row metadata
    // and the raw `payload` bytes; the missing view is a graceful
    // degradation.
    let _ = crate::proto_reflect::ensure_initialized();

    struct GarbageReader;
    #[async_trait::async_trait]
    impl DeadLetterReader for GarbageReader {
        async fn list(&self, _: ListFilter) -> Result<DeadLetterPage, DlqError> {
            Ok(DeadLetterPage {
                entries: vec![StoredDeadLetter {
                    id: 1,
                    domain: "x".to_string(),
                    correlation_id: None,
                    payload: vec![0xff; 32], // not a valid AngzarrDeadLetter
                    rejection_reason: "garbage".to_string(),
                    rejection_type: "sequence_mismatch".to_string(),
                    details: None,
                    source_component: "x".to_string(),
                    source_component_type: "x".to_string(),
                    occurred_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                    created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                }],
                next_page_token: None,
            })
        }
        async fn get(&self, _: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
            Ok(None)
        }
        async fn delete(&self, _: i64) -> Result<bool, DlqError> {
            Ok(false)
        }
    }

    let h = handler_with(GarbageReader);
    let resp = h
        .list_dead_letters(tonic::Request::new(ListDeadLettersRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let entries = match resp.state.unwrap() {
        ListState::Ok(ok) => ok.entries,
        ListState::Degraded(p) => panic!("must NOT degrade on bad payload bytes: {}", p.detail),
    };
    assert_eq!(entries.len(), 1, "row still surfaces despite decode failure");
    assert_eq!(entries[0].payload_view, "", "payload_view empty on decode failure");
    assert!(!entries[0].payload.is_empty(), "raw payload bytes still available");
}
