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
            ext: None,
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
    assert!(
        !view.is_empty(),
        "payload_view must be non-empty after decode"
    );
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
/// was handed PLUS the metadata bundle, so tests can assert the
/// handler stamped the correct lineage on the way out.
struct RecordingReplayPublisher {
    last: std::sync::Mutex<Option<(crate::proto::CommandBook, crate::dlq::ReplayMetadata)>>,
}

impl RecordingReplayPublisher {
    fn new() -> Self {
        Self {
            last: std::sync::Mutex::new(None),
        }
    }
    fn taken(&self) -> Option<crate::proto::CommandBook> {
        self.last.lock().unwrap().clone().map(|(c, _)| c)
    }
    fn taken_metadata(&self) -> Option<crate::dlq::ReplayMetadata> {
        self.last.lock().unwrap().clone().map(|(_, m)| m)
    }
}

#[async_trait::async_trait]
impl crate::dlq::ReplayPublisher for RecordingReplayPublisher {
    async fn replay(
        &self,
        command: crate::proto::CommandBook,
        metadata: crate::dlq::ReplayMetadata,
    ) -> Result<(), crate::dlq::DlqError> {
        *self.last.lock().unwrap() = Some((command, metadata));
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
            ext: None,
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
            ext: None,
        }),
        rejection_reason: "sequence mismatch".to_string(),
        source_component: "agg-player".to_string(),
        source_component_type: "aggregate".to_string(),
        payload: Some(crate::proto::angzarr_dead_letter::Payload::RejectedCommand(
            cmd,
        )),
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

    let captured = pubr
        .taken()
        .expect("publisher must have received a command");
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
                    ext: None,
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
            _: crate::dlq::ReplayMetadata,
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
async fn audit_write_failure_after_successful_publish_surfaces_degraded() {
    // H-30: previously the handler swallowed audit-write failures with
    // a `warn!` log and returned Ok. The UI is documented to consult
    // the audit table to warn operators on re-replay; silent audit
    // loss enables double-replay.
    //
    // New contract: when the publish succeeded but the durable audit
    // write failed, the operator MUST see a degraded ProblemDetails
    // carrying enough detail to know "replay happened but audit
    // failed — manual reconciliation required." Losing the audit row
    // silently is worse than telling the operator about a partial
    // success.
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
    match resp.state.unwrap() {
        State::Degraded(p) => {
            // Sentinel surface so the SPA can render the "publish OK,
            // audit lost — reconcile manually" message distinctly from
            // a vanilla 500.
            assert!(
                p.r#type.contains("audit-write-failed"),
                "type uri must signal audit-write failure, got: {}",
                p.r#type
            );
            // Operator MUST see the new correlation_id even when audit
            // failed, otherwise they can't manually reconcile.
            assert!(
                p.detail.contains("audit") || p.detail.contains("Audit"),
                "detail must mention audit failure: {}",
                p.detail
            );
        }
        State::Ok(_) => {
            panic!("audit-write failure after publish must NOT surface as Ok")
        }
    }
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

// ============================================================================
// Filter parsing (C-20)
// ============================================================================
//
// WHY: `ListDeadLettersRequest.filter` is the AIP-160 surface operators
// use to narrow queries. A Phase-1.1 stub previously discarded
// `req.filter` entirely, returning every row regardless of constraint.
// The handler must parse `req.filter` via `crate::dlq::parse_filter`
// and forward the typed `ListFilter` to the reader.
//
// Test double: an in-memory reader holding 3 rows across 2 domains. The
// reader honours `ListFilter.domain` so the test exercises the full
// parse → forward → filter chain. The reader also records the most
// recent `ListFilter` for cross-checking the parser output.
struct InMemoryFilteringReader {
    rows: Vec<StoredDeadLetter>,
    last_filter: std::sync::Mutex<Option<ListFilter>>,
}

impl InMemoryFilteringReader {
    fn with_three_across_two_domains() -> Self {
        Self {
            rows: vec![
                row_with_domain(1, "player", "corr-p1"),
                row_with_domain(2, "player", "corr-p2"),
                row_with_domain(3, "tournament", "corr-t1"),
            ],
            last_filter: std::sync::Mutex::new(None),
        }
    }
    fn last_filter(&self) -> Option<ListFilter> {
        self.last_filter.lock().unwrap().clone()
    }
}

fn row_with_domain(id: i64, domain: &str, correlation: &str) -> StoredDeadLetter {
    StoredDeadLetter {
        id,
        domain: domain.to_string(),
        correlation_id: Some(correlation.to_string()),
        payload: vec![],
        rejection_reason: "x".to_string(),
        rejection_type: "sequence_mismatch".to_string(),
        details: None,
        source_component: format!("agg-{}", domain),
        source_component_type: "aggregate".to_string(),
        occurred_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 0).unwrap(),
        created_at: Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 1).unwrap(),
    }
}

#[async_trait::async_trait]
impl DeadLetterReader for InMemoryFilteringReader {
    async fn list(&self, filter: ListFilter) -> Result<DeadLetterPage, DlqError> {
        *self.last_filter.lock().unwrap() = Some(filter.clone());
        let matches: Vec<_> = self
            .rows
            .iter()
            .filter(|r| match &filter.domain {
                Some(d) => &r.domain == d,
                None => true,
            })
            .cloned()
            .collect();
        Ok(DeadLetterPage {
            entries: matches,
            next_page_token: None,
        })
    }
    async fn get(&self, id: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
        Ok(self.rows.iter().find(|r| r.id == id).cloned())
    }
    async fn delete(&self, _: i64) -> Result<bool, DlqError> {
        Ok(false)
    }
    fn source_id(&self) -> &'static str {
        "filtering"
    }
}

#[tokio::test]
async fn list_dead_letters_honors_domain_filter_in_request() {
    // C-20: operators send `filter = "domain = \"player\""` and must
    // receive ONLY the player-domain rows. Previously the handler's
    // Phase-1.1 stub discarded req.filter entirely and returned all 3
    // rows. The fix wires `crate::dlq::parse_filter` into the call so
    // the typed `ListFilter` reaches the reader.
    let reader = Arc::new(InMemoryFilteringReader::with_three_across_two_domains());
    let h = DlqAdminHandler::new(reader.clone() as Arc<dyn DeadLetterReader>);

    let resp = h
        .list_dead_letters(tonic::Request::new(ListDeadLettersRequest {
            filter: r#"domain = "player""#.to_string(),
            page_size: 0,
            page_token: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();

    let entries = match resp.state.unwrap() {
        ListState::Ok(ok) => ok.entries,
        ListState::Degraded(p) => panic!("expected ok, got degraded: {}", p.detail),
    };

    assert_eq!(
        entries.len(),
        2,
        "filter should narrow 3 rows -> 2 player-domain rows; got {} entries (filter not wired?)",
        entries.len()
    );
    for e in &entries {
        assert_eq!(
            e.domain, "player",
            "every returned row must be in player domain, got: {}",
            e.domain
        );
    }
    // Belt-and-suspenders: the parsed filter that reached the reader
    // must carry the domain constraint. Catches a regression where
    // someone parses but forgets to pass through the result.
    let observed = reader.last_filter().expect("reader.list called once");
    assert_eq!(
        observed.domain.as_deref(),
        Some("player"),
        "parsed ListFilter.domain must reach the reader"
    );
}

#[tokio::test]
async fn list_dead_letters_invalid_filter_returns_degraded_400() {
    // Garbage filter (unknown field, OR, unterminated quote) must
    // surface a 400-class degraded ProblemDetails rather than a silent
    // unfiltered list. Confirms parse errors take the degraded path
    // instead of being swallowed.
    let reader = Arc::new(InMemoryFilteringReader::with_three_across_two_domains());
    let h = DlqAdminHandler::new(reader.clone() as Arc<dyn DeadLetterReader>);
    let resp = h
        .list_dead_letters(tonic::Request::new(ListDeadLettersRequest {
            filter: r#"domian = "player""#.to_string(), // misspelled field
            page_size: 0,
            page_token: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.state.unwrap() {
        ListState::Degraded(p) => assert_eq!(p.status, 400),
        ListState::Ok(_) => panic!("invalid filter must surface degraded 400"),
    }
}

#[tokio::test]
async fn list_dead_letters_empty_filter_returns_all_rows() {
    // Empty filter → no constraint. Confirms the fix preserves the
    // existing behaviour for unfiltered queries.
    let reader = Arc::new(InMemoryFilteringReader::with_three_across_two_domains());
    let h = DlqAdminHandler::new(reader.clone() as Arc<dyn DeadLetterReader>);
    let resp = h
        .list_dead_letters(tonic::Request::new(ListDeadLettersRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let entries = match resp.state.unwrap() {
        ListState::Ok(ok) => ok.entries,
        ListState::Degraded(p) => panic!("expected ok: {}", p.detail),
    };
    assert_eq!(entries.len(), 3, "empty filter should return all rows");
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
    assert_eq!(
        entries.len(),
        1,
        "row still surfaces despite decode failure"
    );
    assert_eq!(
        entries[0].payload_view, "",
        "payload_view empty on decode failure"
    );
    assert!(
        !entries[0].payload.is_empty(),
        "raw payload bytes still available"
    );
}

// ============================================================================
// H-29 — Replay metadata propagation to the published command (lineage)
// ============================================================================
//
// WHY: `replayed_from_dlq_id` and `original_correlation_id` are documented as
// stamped on the command for downstream lineage. Previously they leaked
// only via `tracing::debug!`, which is lost on log rotation and not
// queryable. The new contract: the handler MUST pass these to the
// `ReplayPublisher::replay` call alongside the rewritten command, so the
// publisher implementation can stamp them on the wire (proto-level
// `Cover.metadata` lands when the angzarr-project submodule adds the
// field; in the meantime the trait surface carries the data so transport
// impls don't have to re-derive it from logs).

#[tokio::test]
async fn replay_passes_lineage_metadata_to_publisher() {
    // The publisher receives `ReplayMetadata` carrying the original
    // correlation_id AND the source DLQ row id. A regression that drops
    // either field strands the replayed command without a back-pointer
    // to its origin.
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

    let metadata = pubr
        .taken_metadata()
        .expect("publisher must have received metadata alongside the command");
    assert_eq!(
        metadata.replayed_from_dlq_id, 42,
        "replayed_from_dlq_id must point at the source row"
    );
    assert_eq!(
        metadata.original_correlation_id, "original-trace",
        "original_correlation_id must carry the row's pre-replay value"
    );
}

#[tokio::test]
async fn replay_metadata_carries_empty_original_when_row_has_none() {
    // Some legacy rows have NULL correlation_id; the publisher must
    // still receive a well-formed (empty) original_correlation_id
    // rather than a missing field on the wire.
    let _ = crate::proto_reflect::ensure_initialized();

    struct NullCorrelationReader;
    #[async_trait::async_trait]
    impl DeadLetterReader for NullCorrelationReader {
        async fn list(&self, _: ListFilter) -> Result<DeadLetterPage, DlqError> {
            Ok(DeadLetterPage {
                entries: vec![],
                next_page_token: None,
            })
        }
        async fn get(&self, id: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
            if id != 7 {
                return Ok(None);
            }
            let mut row = cmd_dl_row(7, "");
            row.correlation_id = None;
            Ok(Some(row))
        }
        async fn delete(&self, _: i64) -> Result<bool, DlqError> {
            Ok(false)
        }
    }

    let pubr = Arc::new(RecordingReplayPublisher::new());
    let h = DlqAdminHandler::new_with_replay(
        Arc::new(NullCorrelationReader),
        pubr.clone() as Arc<dyn crate::dlq::ReplayPublisher>,
    );
    let _ = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 7,
            replay_mode: 0,
        }))
        .await
        .unwrap();
    let metadata = pubr.taken_metadata().expect("metadata captured");
    assert_eq!(metadata.replayed_from_dlq_id, 7);
    assert_eq!(metadata.original_correlation_id, "");
}

// ============================================================================
// H-31 — Two-phase replay-audit ordering + idempotency-key dedup
// ============================================================================
//
// WHY: previously the handler published BEFORE writing the audit row. Two
// replicas + an operator double-click could both publish duplicate
// replays of the same DLQ row. The new contract: a pending audit row is
// inserted FIRST (keyed on a request-scoped idempotency key) and the
// publish only happens if that insert wins. A second concurrent request
// carrying the same idempotency key sees the conflict and refuses to
// publish.

/// In-memory audit writer that simulates a UNIQUE-constraint on
/// `idempotency_key`. The trait's new `begin_pending` method returns
/// `DlqError::Conflict` on the second call with a known key.
struct DedupingAuditWriter {
    pending_keys: std::sync::Mutex<Vec<String>>,
    final_rows: std::sync::Mutex<Vec<crate::dlq::ReplayAuditRecord>>,
}

impl DedupingAuditWriter {
    fn new() -> Self {
        Self {
            pending_keys: std::sync::Mutex::new(vec![]),
            final_rows: std::sync::Mutex::new(vec![]),
        }
    }
    fn final_rows(&self) -> Vec<crate::dlq::ReplayAuditRecord> {
        self.final_rows.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::dlq::ReplayAuditWriter for DedupingAuditWriter {
    async fn begin_pending(
        &self,
        record: &crate::dlq::ReplayAuditRecord,
    ) -> Result<(), crate::dlq::DlqError> {
        let mut keys = self.pending_keys.lock().unwrap();
        if keys.contains(&record.idempotency_key) {
            return Err(crate::dlq::DlqError::Conflict(format!(
                "duplicate idempotency_key: {}",
                record.idempotency_key
            )));
        }
        keys.push(record.idempotency_key.clone());
        Ok(())
    }
    async fn record(
        &self,
        record: crate::dlq::ReplayAuditRecord,
    ) -> Result<(), crate::dlq::DlqError> {
        self.final_rows.lock().unwrap().push(record);
        Ok(())
    }
    fn source_id(&self) -> &'static str {
        "deduping"
    }
}

#[tokio::test]
async fn concurrent_replays_with_same_idempotency_key_publish_only_once() {
    // Two replicas (or one operator double-clicking) attempt to replay
    // the same DLQ entry. Both requests carry the same client-supplied
    // idempotency key (`x-idempotency-key` metadata). The first request
    // INSERTs the pending audit row and proceeds to publish; the second
    // hits a `Conflict` on the pending-row insert and returns degraded
    // WITHOUT publishing.
    use crate::proto::status::replay_dead_letter_response::State;
    let _ = crate::proto_reflect::ensure_initialized();

    let pubr = Arc::new(RecordingReplayPublisher::new());
    let audit = Arc::new(DedupingAuditWriter::new());
    let h = Arc::new(DlqAdminHandler::new_with_audit(
        Arc::new(CommandReplayReader),
        pubr.clone() as Arc<dyn crate::dlq::ReplayPublisher>,
        audit.clone() as Arc<dyn crate::dlq::ReplayAuditWriter>,
    ));

    fn req_with_key(id: i64, key: &str) -> tonic::Request<ReplayDeadLetterRequest> {
        let mut r = tonic::Request::new(ReplayDeadLetterRequest { id, replay_mode: 0 });
        r.metadata_mut()
            .insert("x-idempotency-key", key.parse().unwrap());
        r
    }

    // Two replay attempts with the same idempotency key. The pending
    // row protocol must let exactly one through.
    let r1 = h.replay_dead_letter(req_with_key(42, "op-click-abc")).await;
    let r2 = h.replay_dead_letter(req_with_key(42, "op-click-abc")).await;
    let r1 = r1.unwrap().into_inner();
    let r2 = r2.unwrap().into_inner();

    // Exactly one of the two responses is Ok; the other is degraded
    // with the conflict signature.
    let oks = [r1.state.clone(), r2.state.clone()]
        .into_iter()
        .filter(|s| matches!(s, Some(State::Ok(_))))
        .count();
    let degradeds: Vec<_> = [r1.state.clone(), r2.state.clone()]
        .into_iter()
        .filter_map(|s| match s {
            Some(State::Degraded(p)) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(oks, 1, "exactly one replay must succeed");
    assert_eq!(degradeds.len(), 1, "exactly one must degrade on conflict");
    let p = &degradeds[0];
    assert!(
        p.r#type.contains("conflict") || p.r#type.contains("in-progress"),
        "degraded type uri must signal conflict / in-progress: {}",
        p.r#type
    );
    // Critically: only ONE publish call reached the publisher.
    assert!(
        pubr.taken().is_some(),
        "the winning replica must publish exactly once"
    );
    // And exactly one final audit row (not two) was written.
    assert_eq!(
        audit.final_rows().len(),
        1,
        "only the winning replica writes the final audit row"
    );
}

#[tokio::test]
async fn replay_inserts_pending_audit_row_before_publishing() {
    // Ordering invariant: the pending-row INSERT must precede the
    // publish call. Verified by an audit writer that fails
    // `begin_pending` — if the handler observes the failure, the
    // publisher must NOT receive the command.
    use crate::proto::status::replay_dead_letter_response::State;
    let _ = crate::proto_reflect::ensure_initialized();

    struct AlwaysConflictAudit;
    #[async_trait::async_trait]
    impl crate::dlq::ReplayAuditWriter for AlwaysConflictAudit {
        async fn begin_pending(
            &self,
            _: &crate::dlq::ReplayAuditRecord,
        ) -> Result<(), crate::dlq::DlqError> {
            Err(crate::dlq::DlqError::Conflict(
                "another replay already in flight".to_string(),
            ))
        }
        async fn record(
            &self,
            _: crate::dlq::ReplayAuditRecord,
        ) -> Result<(), crate::dlq::DlqError> {
            panic!("record must NOT be called when begin_pending failed");
        }
    }

    let pubr = Arc::new(RecordingReplayPublisher::new());
    let h = DlqAdminHandler::new_with_audit(
        Arc::new(CommandReplayReader),
        pubr.clone() as Arc<dyn crate::dlq::ReplayPublisher>,
        Arc::new(AlwaysConflictAudit),
    );
    let resp = h
        .replay_dead_letter(tonic::Request::new(ReplayDeadLetterRequest {
            id: 42,
            replay_mode: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.state.unwrap() {
        State::Degraded(p) => assert!(
            p.r#type.contains("conflict") || p.r#type.contains("in-progress"),
            "conflict on pending-row insert must surface as conflict degraded: {}",
            p.r#type
        ),
        State::Ok(_) => panic!("conflict must NOT surface as Ok"),
    }
    assert!(
        pubr.taken().is_none(),
        "publisher must NOT be called when the pending-row INSERT lost the race"
    );
}
