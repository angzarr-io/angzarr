//! DLQ admin gRPC handler.
//!
//! Implements [`DlqAdminService`] by delegating to a
//! [`DeadLetterReader`]. The handler owns the proto/domain mapping
//! and the `Health<T>` envelope wrapping; the reader owns the
//! storage-specific query path.
//!
//! ## Filter parsing
//!
//! `ListDeadLettersRequest.filter` is the AIP-160 string surface,
//! parsed by [`crate::dlq::parse_filter`] into a typed [`ListFilter`].
//! The handler forwards the parsed filter (plus AIP-158 pagination)
//! to the backing [`DeadLetterReader`]. Empty filter strings pass
//! through as an unconstrained [`ListFilter::default`]; parse
//! errors map to a 400-class degraded [`ProblemDetails`] response.
//!
//! ## Resilience contract
//!
//! Every method returns `Ok(Response::new(_))` (never a gRPC error)
//! when the backend is misconfigured. The `state.degraded` field
//! carries the `ProblemDetails`. Per-tile UI fetches see "this tile
//! is unavailable" without other tiles failing — see the plan's
//! degradation contract.
//!
//! The exception is true unrecoverable errors (panics, internal
//! invariants violated) which still surface as gRPC `Internal`.

use std::sync::Arc;

use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::dlq::{
    DeadLetterReader, DlqError, ListFilter, NoopReplayAuditWriter, NoopReplayPublisher,
    ReplayAuditRecord, ReplayAuditWriter, ReplayMode, ReplayOutcome, ReplayPublisher,
    StoredDeadLetter,
};
use crate::proto::status::dlq_admin_service_server::DlqAdminService;
use crate::proto::status::{
    delete_dead_letter_response, get_dead_letter_response, list_dead_letters_response,
    replay_dead_letter_response, DeleteDeadLetterOk, DeleteDeadLetterRequest,
    DeleteDeadLetterResponse, GetDeadLetterOk, GetDeadLetterRequest, GetDeadLetterResponse,
    ListDeadLettersOk, ListDeadLettersRequest, ListDeadLettersResponse, ProblemDetails,
    RejectionType, ReplayDeadLetterOk, ReplayDeadLetterRequest, ReplayDeadLetterResponse,
    StoredDeadLetter as ProtoStoredDeadLetter,
};

/// Handler for [`DlqAdminService`]. Owns no storage of its own; the
/// reader is the single source of truth.
///
/// `Arc<dyn>` rather than a generic so the same gRPC server instance
/// can be wired against different backends at deploy-time (Noop in
/// Phase 1.1 / SQLite locally / Postgres in production) without
/// changing the type at compile time.
pub struct DlqAdminHandler {
    reader: Arc<dyn DeadLetterReader>,
    replay: Arc<dyn ReplayPublisher>,
    audit: Arc<dyn ReplayAuditWriter>,
}

impl DlqAdminHandler {
    /// Build a handler with `Noop` replay publisher AND audit
    /// writer. Used when only read operations are needed
    /// (Phase 1.1 / 1.2 wiring).
    pub fn new(reader: Arc<dyn DeadLetterReader>) -> Self {
        Self {
            reader,
            replay: Arc::new(NoopReplayPublisher),
            audit: Arc::new(NoopReplayAuditWriter),
        }
    }

    /// Build a handler with a reader + replay publisher; audit
    /// stays no-op. Convenience for tests / dev paths that don't
    /// need durable audit.
    pub fn new_with_replay(
        reader: Arc<dyn DeadLetterReader>,
        replay: Arc<dyn ReplayPublisher>,
    ) -> Self {
        Self {
            reader,
            replay,
            audit: Arc::new(NoopReplayAuditWriter),
        }
    }

    /// Production wiring: reader + replay publisher + durable audit
    /// writer. Used by `angzarr-status` once P1.4 schema is applied.
    pub fn new_with_audit(
        reader: Arc<dyn DeadLetterReader>,
        replay: Arc<dyn ReplayPublisher>,
        audit: Arc<dyn ReplayAuditWriter>,
    ) -> Self {
        Self {
            reader,
            replay,
            audit,
        }
    }

    /// Build the `checked_at` + `source` envelope fields. Pulled into
    /// a helper so every response method shapes them identically.
    fn envelope_fields(&self) -> (prost_types::Timestamp, String) {
        (current_timestamp(), self.reader.source_id().to_string())
    }
}

/// Wall-clock now as a proto Timestamp (seconds resolution is
/// sufficient for the operator-facing `checked_at` field).
fn current_timestamp() -> prost_types::Timestamp {
    let now_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    prost_types::Timestamp {
        seconds: now_seconds,
        nanos: 0,
    }
}

/// Extract a replayable `CommandBook` from a stored row.
///
/// Phase 1.3 ships command-replay only. Event-replay (where
/// `rejected_events` is set instead of `rejected_command`) lands in
/// P1.5 — that path needs a `to_handler` selector to avoid re-fanning
/// to every subscriber, which is almost never what the operator
/// wants. Until then, an event-only row returns a 400-class
/// `ProblemDetails` so the operator sees a clear "not yet supported"
/// signal instead of a silent failure.
fn extract_command_for_replay(
    stored: &StoredDeadLetter,
) -> Result<crate::proto::CommandBook, ProblemDetails> {
    let dl = crate::proto::AngzarrDeadLetter::decode(stored.payload.as_slice()).map_err(|e| {
        ProblemDetails {
            r#type: "urn:angzarr:status:dlq:payload-decode".to_string(),
            title: "Cannot decode DLQ payload".to_string(),
            status: 500,
            detail: format!(
                "stored payload bytes are not a valid AngzarrDeadLetter: {}",
                e
            ),
            instance: String::new(),
        }
    })?;
    match dl.payload {
        Some(crate::proto::angzarr_dead_letter::Payload::RejectedCommand(cmd)) => Ok(cmd),
        Some(crate::proto::angzarr_dead_letter::Payload::RejectedEvents(_)) => {
            Err(ProblemDetails {
                r#type: "urn:angzarr:status:dlq:event-replay-unsupported".to_string(),
                title: "Event replay is not yet supported".to_string(),
                status: 400,
                detail:
                    "this DLQ entry is an event (saga/projector failure); event replay lands in P1.5"
                        .to_string(),
                instance: String::new(),
            })
        }
        None => Err(ProblemDetails {
            r#type: "urn:angzarr:status:dlq:no-payload".to_string(),
            title: "DLQ entry has no rejected payload".to_string(),
            status: 500,
            detail: "AngzarrDeadLetter.payload oneof is unset".to_string(),
            instance: String::new(),
        }),
    }
}

/// Stamp the replay audit-trail metadata on a `CommandBook` and
/// rewrite the cover's `correlation_id` to a fresh one.
///
/// The `replayed_from_dlq_id` + `original_correlation_id` pair lets
/// downstream observers (logs, traces, DLQ tap consumer, audit
/// queries) connect the replayed command back to its origin without
/// the new correlation_id colliding with anything still in-flight.
///
/// Plan reference: P5 replay-semantics decision.
fn stamp_replay_metadata(
    mut command: crate::proto::CommandBook,
    dlq_id: i64,
    original_correlation_id: &str,
    new_correlation_id: &str,
) -> crate::proto::CommandBook {
    if let Some(cover) = command.cover.as_mut() {
        cover.correlation_id = new_correlation_id.to_string();
    }
    // We do NOT modify `command.pages` for FRESH_SEQUENCE here —
    // that's the publisher's contract, since it needs the live
    // EventQueryService client to look up the current next_sequence.
    // The metadata pointers below ride on the publisher's choice of
    // transport (whatever the bus impl supports).
    //
    // For Phase 1.3 the metadata is logged via tracing on success.
    // The dlq_replay_audit table (P1.4) will persist it durably.
    tracing::debug!(
        replayed_from_dlq_id = dlq_id,
        original_correlation_id = original_correlation_id,
        new_correlation_id = new_correlation_id,
        "stamping replay metadata"
    );
    command
}

#[tonic::async_trait]
impl DlqAdminService for DlqAdminHandler {
    async fn list_dead_letters(
        &self,
        request: Request<ListDeadLettersRequest>,
    ) -> Result<Response<ListDeadLettersResponse>, Status> {
        let req = request.into_inner();
        let (checked_at, source) = self.envelope_fields();

        // AIP-160 filter parsing — see module docs. Parse errors are
        // surfaced to the operator as 400-class degraded, never a
        // silent unfiltered result.
        let filter = match parse_list_filter(&req.filter, req.page_size, req.page_token) {
            Ok(f) => f,
            Err(e) => {
                return Ok(Response::new(ListDeadLettersResponse {
                    state: Some(list_dead_letters_response::State::Degraded(
                        problem_details_for(&e),
                    )),
                    checked_at: Some(checked_at),
                    source,
                }));
            }
        };

        match self.reader.list(filter).await {
            Ok(page) => Ok(Response::new(ListDeadLettersResponse {
                state: Some(list_dead_letters_response::State::Ok(ListDeadLettersOk {
                    entries: page.entries.into_iter().map(stored_to_proto).collect(),
                    next_page_token: page.next_page_token.unwrap_or_default(),
                })),
                checked_at: Some(checked_at),
                source,
            })),
            Err(e) => {
                warn!(error = %e, "ListDeadLetters backend error — returning degraded");
                Ok(Response::new(ListDeadLettersResponse {
                    state: Some(list_dead_letters_response::State::Degraded(
                        problem_details_for(&e),
                    )),
                    checked_at: Some(checked_at),
                    source,
                }))
            }
        }
    }

    async fn get_dead_letter(
        &self,
        request: Request<GetDeadLetterRequest>,
    ) -> Result<Response<GetDeadLetterResponse>, Status> {
        let req = request.into_inner();
        let (checked_at, source) = self.envelope_fields();

        match self.reader.get(req.id).await {
            Ok(Some(entry)) => Ok(Response::new(GetDeadLetterResponse {
                state: Some(get_dead_letter_response::State::Ok(GetDeadLetterOk {
                    entry: Some(stored_to_proto(entry)),
                })),
                checked_at: Some(checked_at),
                source,
            })),
            Ok(None) => {
                // No-row-matches is a successful query that returned
                // empty. Surfaced as `state.ok` with `entry = None` so
                // the caller can distinguish "no such id" from
                // "backend down". gRPC NOT_FOUND would conflate them.
                Ok(Response::new(GetDeadLetterResponse {
                    state: Some(get_dead_letter_response::State::Ok(GetDeadLetterOk {
                        entry: None,
                    })),
                    checked_at: Some(checked_at),
                    source,
                }))
            }
            Err(e) => {
                warn!(error = %e, id = %req.id, "GetDeadLetter backend error — degraded");
                Ok(Response::new(GetDeadLetterResponse {
                    state: Some(get_dead_letter_response::State::Degraded(
                        problem_details_for(&e),
                    )),
                    checked_at: Some(checked_at),
                    source,
                }))
            }
        }
    }

    async fn delete_dead_letter(
        &self,
        request: Request<DeleteDeadLetterRequest>,
    ) -> Result<Response<DeleteDeadLetterResponse>, Status> {
        let req = request.into_inner();
        let (checked_at, source) = self.envelope_fields();

        match self.reader.delete(req.id).await {
            Ok(deleted) => Ok(Response::new(DeleteDeadLetterResponse {
                state: Some(delete_dead_letter_response::State::Ok(DeleteDeadLetterOk {
                    deleted,
                })),
                checked_at: Some(checked_at),
                source,
            })),
            Err(e) => {
                warn!(error = %e, id = %req.id, "DeleteDeadLetter backend error — degraded");
                Ok(Response::new(DeleteDeadLetterResponse {
                    state: Some(delete_dead_letter_response::State::Degraded(
                        problem_details_for(&e),
                    )),
                    checked_at: Some(checked_at),
                    source,
                }))
            }
        }
    }

    async fn replay_dead_letter(
        &self,
        request: Request<ReplayDeadLetterRequest>,
    ) -> Result<Response<ReplayDeadLetterResponse>, Status> {
        let req = request.into_inner();
        let mode = ReplayMode::from_proto(req.replay_mode);
        let now_ts = current_timestamp();
        // Envelope source identifies the publisher (not the reader) —
        // operators care which bus actually performed the replay.
        let source = self.replay.source_id().to_string();

        // 1. Fetch the dead-letter row.
        let stored = match self.reader.get(req.id).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                return Ok(Response::new(ReplayDeadLetterResponse {
                    state: Some(replay_dead_letter_response::State::Degraded(
                        ProblemDetails {
                            r#type: "urn:angzarr:status:dlq:not-found".to_string(),
                            title: "Dead letter not found".to_string(),
                            status: 404,
                            detail: format!("no dlq_entries row with id={}", req.id),
                            instance: String::new(),
                        },
                    )),
                    checked_at: Some(now_ts),
                    source,
                }));
            }
            Err(e) => {
                warn!(error = %e, id = %req.id, "ReplayDeadLetter reader error — degraded");
                return Ok(Response::new(ReplayDeadLetterResponse {
                    state: Some(replay_dead_letter_response::State::Degraded(
                        problem_details_for(&e),
                    )),
                    checked_at: Some(now_ts),
                    source,
                }));
            }
        };

        // 2. Decode the AngzarrDeadLetter and extract the command.
        let command = match extract_command_for_replay(&stored) {
            Ok(c) => c,
            Err(p) => {
                return Ok(Response::new(ReplayDeadLetterResponse {
                    state: Some(replay_dead_letter_response::State::Degraded(p)),
                    checked_at: Some(now_ts),
                    source,
                }));
            }
        };

        // 3. Stamp metadata + new correlation_id. Sequence rewriting
        //    for FRESH_SEQUENCE is the publisher's concern (it needs
        //    the EventQueryService client) and lands when the real
        //    publisher impl is wired. The handler always stamps the
        //    audit-trail metadata; if mode == FreshSequence, the
        //    sequence rewrite is the publisher's contract.
        let new_correlation_id = uuid::Uuid::new_v4().to_string();
        let rewritten = stamp_replay_metadata(
            command,
            stored.id,
            stored.correlation_id.as_deref().unwrap_or(""),
            &new_correlation_id,
        );

        // 4. Publish. Audit BOTH success and failure paths so
        //    operators can investigate either outcome. Audit write
        //    failures are themselves logged but do not fail the
        //    replay response — degrading the audit log is preferable
        //    to swallowing the replay result. Per plan resilience
        //    contract (fail-soft on observability surfaces).
        match self.replay.replay(rewritten).await {
            Ok(()) => {
                info!(
                    dlq_id = stored.id,
                    new_correlation_id = %new_correlation_id,
                    mode = ?mode,
                    "DLQ entry replayed"
                );
                self.audit_replay(
                    &stored,
                    mode,
                    &new_correlation_id,
                    ReplayOutcome::Success,
                    None,
                )
                .await;
                Ok(Response::new(ReplayDeadLetterResponse {
                    state: Some(replay_dead_letter_response::State::Ok(ReplayDeadLetterOk {
                        new_correlation_id,
                        replayed_at: Some(now_ts),
                        applied_mode: mode.to_proto() as i32,
                    })),
                    checked_at: Some(now_ts),
                    source,
                }))
            }
            Err(e) => {
                warn!(error = %e, id = %stored.id, "ReplayDeadLetter publisher error — degraded");
                self.audit_replay(
                    &stored,
                    mode,
                    &new_correlation_id,
                    ReplayOutcome::Failure,
                    Some(e.to_string()),
                )
                .await;
                Ok(Response::new(ReplayDeadLetterResponse {
                    state: Some(replay_dead_letter_response::State::Degraded(
                        problem_details_for(&e),
                    )),
                    checked_at: Some(now_ts),
                    source,
                }))
            }
        }
    }
}

impl DlqAdminHandler {
    /// Persist (or log-fallback) an audit row for the replay.
    ///
    /// Failure of the audit write itself is `warn!`-logged but
    /// silently swallowed — the replay's outcome (success or
    /// publisher-failure) is already conveyed to the operator via
    /// the gRPC response. Losing the audit row is the lesser harm
    /// vs. surfacing an audit error for what was a real successful
    /// replay.
    async fn audit_replay(
        &self,
        stored: &StoredDeadLetter,
        mode: ReplayMode,
        new_correlation_id: &str,
        outcome: ReplayOutcome,
        result_message: Option<String>,
    ) {
        let record = ReplayAuditRecord {
            dlq_id: stored.id,
            replayed_at: chrono::Utc::now(),
            replay_mode: mode,
            new_correlation_id: new_correlation_id.to_string(),
            original_correlation_id: stored.correlation_id.clone(),
            outcome,
            result_message,
        };
        if let Err(e) = self.audit.record(record).await {
            warn!(
                error = %e,
                dlq_id = stored.id,
                "replay-audit write failed; replay outcome unaffected"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

/// Fully-qualified proto type name of `AngzarrDeadLetter` —
/// pinned here so a future rename surfaces as a build/test failure
/// rather than silently producing empty `payload_view` strings.
const ANGZARR_DEAD_LETTER_TYPE_NAME: &str = "angzarr_client.proto.angzarr.AngzarrDeadLetter";

/// Map [`StoredDeadLetter`] (domain row) → proto wire type.
///
/// `payload_view` is the descriptor-decoded JSON form of `payload`,
/// best-effort: if the descriptor pool isn't initialized or the
/// bytes don't decode, the field is left empty and the operator
/// falls back to the raw `payload` bytes. Per the plan's tolerance
/// contract.
fn stored_to_proto(s: StoredDeadLetter) -> ProtoStoredDeadLetter {
    let payload_view =
        crate::proto_reflect::decode_to_json(ANGZARR_DEAD_LETTER_TYPE_NAME, &s.payload);
    ProtoStoredDeadLetter {
        id: s.id,
        domain: s.domain,
        correlation_id: s.correlation_id.unwrap_or_default(),
        payload: s.payload,
        rejection_reason: s.rejection_reason,
        rejection_type: rejection_type_from_str(&s.rejection_type) as i32,
        details: s.details.unwrap_or_default(),
        source_component: s.source_component,
        source_component_type: s.source_component_type,
        occurred_at: Some(prost_types::Timestamp {
            seconds: s.occurred_at.timestamp(),
            nanos: s.occurred_at.timestamp_subsec_nanos() as i32,
        }),
        created_at: Some(prost_types::Timestamp {
            seconds: s.created_at.timestamp(),
            nanos: s.created_at.timestamp_subsec_nanos() as i32,
        }),
        payload_view,
    }
}

/// Map the publisher's stringly-typed `rejection_type` discriminator
/// into the proto enum. Unknown / future values fall back to
/// `UNSPECIFIED` rather than failing the call.
fn rejection_type_from_str(s: &str) -> RejectionType {
    match s {
        "sequence_mismatch" => RejectionType::SequenceMismatch,
        "event_processing_failed" => RejectionType::EventProcessingFailed,
        "payload_retrieval_failed" => RejectionType::PayloadRetrievalFailed,
        _ => RejectionType::Unspecified,
    }
}

/// Parse a `ListDeadLettersRequest` into a typed [`ListFilter`].
///
/// Combines the AIP-160 filter grammar (delegated to
/// [`crate::dlq::parse_filter`], which understands `domain`,
/// `correlation_id`, `rejection_type`, `source_component`,
/// `occurred_after`, `occurred_before` AND-joined) with the AIP-158
/// pagination fields, which live on the request rather than in the
/// filter string.
///
/// Errors propagate as [`DlqError::InvalidArgument`] so the handler
/// can surface a 400-class degraded `ProblemDetails`.
fn parse_list_filter(
    filter: &str,
    page_size: i32,
    page_token: String,
) -> Result<ListFilter, DlqError> {
    let mut f = crate::dlq::parse_filter(filter)?;
    f.page_size = page_size.max(0) as u32;
    f.page_token = if page_token.is_empty() {
        None
    } else {
        Some(page_token)
    };
    Ok(f)
}

/// Build an RFC 7807-style `ProblemDetails` for a backend error.
fn problem_details_for(err: &DlqError) -> ProblemDetails {
    use crate::dlq::error::errmsg;
    let (type_uri, title, status_hint) = match err {
        DlqError::NotConfigured => (
            "urn:angzarr:status:dlq:not-configured",
            errmsg::NOT_CONFIGURED,
            503, // Service Unavailable
        ),
        DlqError::Connection(_) => (
            "urn:angzarr:status:dlq:connection",
            "DLQ backend connection error",
            503,
        ),
        DlqError::QueryFailed(_) => (
            "urn:angzarr:status:dlq:query-failed",
            "DLQ query failed",
            500,
        ),
        DlqError::InvalidArgument(_) => (
            "urn:angzarr:status:dlq:invalid-argument",
            "Invalid DLQ query argument",
            400,
        ),
        _ => ("urn:angzarr:status:dlq:error", "DLQ admin error", 500),
    };
    ProblemDetails {
        r#type: type_uri.to_string(),
        title: title.to_string(),
        status: status_hint,
        detail: err.to_string(),
        instance: String::new(),
    }
}

#[cfg(test)]
#[path = "dlq.test.rs"]
mod tests;
