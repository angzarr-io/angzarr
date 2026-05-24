//! Saga compensation handling.
//!
//! DOC: This file is referenced in docs/docs/operations/error-recovery.mdx
//!      Update documentation when making changes to compensation patterns.
//!
//! Provides utilities for handling saga command rejections, including:
//! - Building Notification messages with RejectionNotification payload
//! - Emitting SagaCompensationFailed events
//! - Escalation via configurable handlers (EventBus, webhook, etc.)
//!
//! # Provenance Model
//!
//! Command provenance is stored in each page's `PageHeader.angzarr_deferred`:
//! - `source`: Cover identifying the source aggregate (domain + root)
//! - `source_seq`: Sequence of the triggering event
//!
//! The compensation flow extracts source info from the first command page's header.

use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use sha2::{Digest, Sha256};
use tracing::{debug, error, info, warn};
// `Uuid` is unused in production code after H-37 — the compensation root is
// derived deterministically via SHA-256 — but the test module (loaded via
// `#[path = "mod.test.rs"]`) still imports `Uuid::new_v4()` for fixture
// construction. The `#[cfg(test)]` attribute keeps the production binary
// free of the dependency edge.
#[cfg(test)]
use uuid::Uuid;

use crate::bus::EventBus;
use crate::config::SagaCompensationConfig;
use crate::proto::{
    business_response, page_header::SequenceType, AngzarrDeferredSequence, BusinessResponse,
    CommandBook, Cover, EventBook, EventPage, MergeStrategy, Notification, PageHeader,
    RejectionNotification, RevocationResponse, SagaCompensationFailed, Uuid as ProtoUuid,
};
use crate::proto_ext::type_url;
use crate::proto_ext::CoverExt;

/// Minimal clock abstraction for the compensation builder.
///
/// Production callers use [`SystemClock`] which reads `SystemTime::now()` on
/// every call. Tests (and any replay/idempotency harness that needs a
/// byte-identical EventBook) inject a fixed clock so the page's
/// `created_at` is deterministic.
///
/// The clock only affects `EventBook.pages[*].created_at`. The aggregate
/// root is derived purely from the `CompensationContext` + reason, so even
/// with a real wall-clock the root remains stable across redeliveries.
pub trait Clock: Send + Sync {
    /// Returns the current instant. Implementations must be cheap and
    /// non-blocking — this is called on the synchronous build path.
    fn now(&self) -> std::time::SystemTime;
}

/// Wall-clock implementation of [`Clock`].
///
/// Used by all production call sites. Tests can substitute a fixed-instant
/// clock to assert byte-equality of the compensation EventBook.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
}

/// Result type for compensation operations.
pub type Result<T> = std::result::Result<T, CompensationError>;

/// Error message constants for compensation operations.
pub mod errmsg {
    pub const MISSING_PROVENANCE: &str =
        "Command missing angzarr_deferred provenance - not a saga/PM command";
    pub const MISSING_SOURCE: &str = "Missing source Cover in angzarr_deferred";
    pub const ABORTED: &str = "Compensation aborted: ";
    pub const ESCALATION_FAILED: &str = "Escalation failed: ";
    pub const EVENT_STORE_ERROR: &str = "Event store error: ";
}

/// Errors that can occur during saga compensation.
#[derive(Debug, thiserror::Error)]
pub enum CompensationError {
    #[error("{}", errmsg::MISSING_PROVENANCE)]
    MissingProvenance,

    #[error("{}", errmsg::MISSING_SOURCE)]
    MissingSource,

    #[error("{}{}", errmsg::ABORTED, .0)]
    Aborted(String),

    #[error("{}{}", errmsg::ESCALATION_FAILED, .0)]
    EscalationFailed(String),

    #[error("{}{}", errmsg::EVENT_STORE_ERROR, .0)]
    EventStore(String),
}

/// Trait for handling escalation actions during saga compensation.
///
/// Two distinct concerns:
/// - Quarantine: isolate failed messages for later reprocessing (operational)
/// - Notify: inform operators of failures requiring attention (observability)
///
/// These are intentionally separate methods because callers may want one without
/// the other (e.g., quarantine for replay without alerting, or alerting without quarantine).
#[async_trait]
pub trait EscalationHandler: Send + Sync {
    /// Quarantine a compensation failure for later reprocessing.
    ///
    /// Called when `send_to_dead_letter_queue` flag is set. Implementations
    /// should preserve the full context for later replay/investigation.
    async fn quarantine(
        &self,
        context: &CompensationContext,
        reason: &str,
    ) -> std::result::Result<(), CompensationError>;

    /// Notify operators of a compensation failure.
    ///
    /// Called when `escalate` flag is set. Implementations should alert
    /// operators for manual review and resolution.
    async fn notify(
        &self,
        context: &CompensationContext,
        reason: &str,
    ) -> std::result::Result<(), CompensationError>;
}

/// Default escalation handler that routes based on configuration.
///
/// - `quarantine`: If `dead_letter_queue_url` configured → publishes to fallback domain via EventBus
/// - `notify`: If `escalation_webhook_url` configured → calls webhook with retry
pub struct DefaultEscalationHandler {
    event_bus: Arc<dyn EventBus>,
    config: SagaCompensationConfig,
    http_client: reqwest::Client,
}

impl DefaultEscalationHandler {
    /// Create a new default escalation handler.
    pub fn new(event_bus: Arc<dyn EventBus>, config: SagaCompensationConfig) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client for webhook escalation");
        Self {
            event_bus,
            config,
            http_client,
        }
    }
}

#[async_trait]
impl EscalationHandler for DefaultEscalationHandler {
    async fn quarantine(
        &self,
        context: &CompensationContext,
        reason: &str,
    ) -> std::result::Result<(), CompensationError> {
        let Some(ref dlq_url) = self.config.dead_letter_queue_url else {
            warn!(
                source_domain = %context.source.source.as_ref().map(|c| c.domain.as_str()).unwrap_or("?"),
                "Quarantine requested but dead_letter_queue_url not configured"
            );
            return Ok(());
        };

        info!(
            source_domain = %context.source.source.as_ref().map(|c| c.domain.as_str()).unwrap_or("?"),
            source_seq = context.source.source_seq,
            dlq_url = %dlq_url,
            reason = %reason,
            "Quarantining compensation failure"
        );

        let event_book = build_compensation_failed_event_book(context, reason, &self.config);
        self.event_bus
            .publish(Arc::new(event_book))
            .await
            .map_err(|e| {
                CompensationError::EscalationFailed(format!("Quarantine failed: {}", e))
            })?;

        Ok(())
    }

    async fn notify(
        &self,
        context: &CompensationContext,
        reason: &str,
    ) -> std::result::Result<(), CompensationError> {
        // Always log at ERROR for notifications
        let source_cover = context.source.source.as_ref();
        error!(
            source_domain = %source_cover.map(|c| c.domain.as_str()).unwrap_or("?"),
            source_seq = context.source.source_seq,
            rejection_reason = %context.rejection_reason,
            compensation_reason = %reason,
            "NOTIFY: Saga/PM compensation failed"
        );

        let Some(ref webhook_url) = self.config.escalation_webhook_url else {
            warn!(
                source_domain = %source_cover.map(|c| c.domain.as_str()).unwrap_or("?"),
                "Notification requested but escalation_webhook_url not configured"
            );
            return Ok(());
        };

        // Build webhook payload
        let payload = serde_json::json!({
            "source_domain": source_cover.map(|c| &c.domain),
            "source_root_id": source_cover
                .and_then(|c| c.root.as_ref())
                .map(|u| hex::encode(&u.value)),
            "source_seq": context.source.source_seq,
            "rejection_reason": context.rejection_reason,
            "compensation_reason": reason,
            "correlation_id": context.correlation_id,
            "occurred_at": chrono::Utc::now().to_rfc3339(),
        });

        // Retry with exponential backoff: 100ms -> 1s, max 3 attempts
        let max_attempts = 3;
        let mut attempt = 0;
        let mut last_error = None;

        while attempt < max_attempts {
            attempt += 1;

            match self
                .http_client
                .post(webhook_url)
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    info!(
                        source_domain = %source_cover.map(|c| c.domain.as_str()).unwrap_or("?"),
                        webhook = %webhook_url,
                        status = %response.status(),
                        attempt,
                        "Webhook notification sent successfully"
                    );
                    return Ok(());
                }
                Ok(response) if response.status().is_server_error() => {
                    // Server error (5xx) - retry
                    last_error = Some(format!("HTTP {}", response.status()));
                    warn!(
                        source_domain = %source_cover.map(|c| c.domain.as_str()).unwrap_or("?"),
                        webhook = %webhook_url,
                        status = %response.status(),
                        attempt,
                        max_attempts,
                        "Webhook returned server error, will retry"
                    );
                }
                Ok(response) => {
                    // Client error (4xx) - don't retry, log and return
                    warn!(
                        source_domain = %source_cover.map(|c| c.domain.as_str()).unwrap_or("?"),
                        webhook = %webhook_url,
                        status = %response.status(),
                        "Webhook returned client error (not retrying)"
                    );
                    return Ok(());
                }
                Err(e) => {
                    // Network error - retry
                    last_error = Some(e.to_string());
                    warn!(
                        source_domain = %source_cover.map(|c| c.domain.as_str()).unwrap_or("?"),
                        webhook = %webhook_url,
                        error = %e,
                        attempt,
                        max_attempts,
                        "Webhook request failed, will retry"
                    );
                }
            }

            // Exponential backoff: 100ms, 200ms, 400ms...
            if attempt < max_attempts {
                let delay = std::time::Duration::from_millis(100 * (1 << (attempt - 1)));
                tokio::time::sleep(delay).await;
            }
        }

        // All retries exhausted
        error!(
            source_domain = %source_cover.map(|c| c.domain.as_str()).unwrap_or("?"),
            webhook = %webhook_url,
            last_error = ?last_error,
            attempts = max_attempts,
            "Webhook notification failed after all retries"
        );

        Ok(())
    }
}

/// No-op escalation handler that only logs.
///
/// Useful for tests or when escalation is disabled.
pub struct NoopEscalationHandler;

#[async_trait]
impl EscalationHandler for NoopEscalationHandler {
    async fn quarantine(
        &self,
        context: &CompensationContext,
        reason: &str,
    ) -> std::result::Result<(), CompensationError> {
        warn!(
            source_domain = %context.source.source.as_ref().map(|c| c.domain.as_str()).unwrap_or("?"),
            source_seq = context.source.source_seq,
            reason = %reason,
            "Quarantine requested but using NoopEscalationHandler (logging only)"
        );
        Ok(())
    }

    async fn notify(
        &self,
        context: &CompensationContext,
        reason: &str,
    ) -> std::result::Result<(), CompensationError> {
        warn!(
            source_domain = %context.source.source.as_ref().map(|c| c.domain.as_str()).unwrap_or("?"),
            source_seq = context.source.source_seq,
            reason = %reason,
            "Notification requested but using NoopEscalationHandler (logging only)"
        );
        Ok(())
    }
}

/// Context for compensation operations.
///
/// Contains all information needed to build compensation events
/// and route failures.
#[derive(Debug, Clone)]
pub struct CompensationContext {
    /// The source provenance from the rejected command's page header.
    /// Identifies which aggregate/event triggered this command.
    pub source: AngzarrDeferredSequence,
    /// Why the command was rejected.
    pub rejection_reason: String,
    /// The rejected command.
    pub rejected_command: CommandBook,
    /// Correlation ID for tracing.
    pub correlation_id: String,
}

impl CompensationContext {
    /// Create a new compensation context from a rejected command.
    ///
    /// Returns None if the command doesn't have angzarr_deferred provenance
    /// in its page headers (indicating it's not a saga/PM-issued command).
    pub fn from_rejected_command(command: &CommandBook, rejection_reason: String) -> Option<Self> {
        // Extract angzarr_deferred from first page's header
        let source = command.pages.first().and_then(|page| {
            page.header.as_ref().and_then(|h| match &h.sequence_type {
                Some(SequenceType::AngzarrDeferred(ad)) => Some(ad.clone()),
                _ => None,
            })
        })?;

        let correlation_id = command.correlation_id().to_string();

        Some(Self {
            source,
            rejection_reason,
            rejected_command: command.clone(),
            correlation_id,
        })
    }
}

/// Build a RejectionNotification for a rejected saga/PM command.
///
/// This is the payload for the Notification sent to the source aggregate
/// (identified by angzarr_deferred.source), allowing it to emit compensation events.
///
/// The new RejectionNotification structure is simpler:
/// - `rejected_command`: The command that was rejected
/// - `rejection_reason`: Why it was rejected
///
/// Source provenance is already in the rejected_command's page headers.
pub fn build_rejection_notification(context: &CompensationContext) -> RejectionNotification {
    RejectionNotification {
        rejected_command: Some(context.rejected_command.clone()),
        rejection_reason: context.rejection_reason.clone(),
    }
}

/// Build a Notification wrapping a RejectionNotification.
///
/// This is the pattern for compensation - Notification with typed payload.
/// Routes to the source aggregate identified in angzarr_deferred.
pub fn build_notification(context: &CompensationContext) -> Notification {
    let rejection = build_rejection_notification(context);

    // Build cover from source (the aggregate that triggered the saga/PM)
    let cover = context.source.source.clone();

    Notification {
        cover,
        payload: Some(prost_types::Any {
            type_url: type_url::REJECTION_NOTIFICATION.to_string(),
            value: rejection.encode_to_vec(),
        }),
        sent_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
    }
}

/// Build a CommandBook to send the Notification to the source aggregate.
pub fn build_notification_command_book(context: &CompensationContext) -> Result<CommandBook> {
    let source_aggregate = context
        .source
        .source
        .as_ref()
        .ok_or(CompensationError::MissingSource)?;

    let notification = build_notification(context);

    // Clone source aggregate and set correlation_id on cover
    let mut cover = source_aggregate.clone();
    if cover.correlation_id.is_empty() {
        cover.correlation_id = context.correlation_id.clone();
    }

    Ok(CommandBook {
        cover: Some(cover),
        pages: vec![crate::proto::CommandPage {
            // Notifications use deferred sequence - the aggregate will stamp on receipt
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                    // Source is the same aggregate receiving this notification
                    // (compensation loops back to source)
                    source: Some(source_aggregate.clone()),
                    source_seq: context.source.source_seq,
                })),
            }),
            payload: Some(crate::proto::command_page::Payload::Command(
                prost_types::Any {
                    type_url: type_url::NOTIFICATION.to_string(),
                    value: notification.encode_to_vec(),
                },
            )),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    })
}

/// Build a SagaCompensationFailed event.
///
/// This is emitted when client logic cannot handle the revocation
/// or explicitly requests system revocation.
///
/// The `occurred_at` timestamp is sourced from the injected [`Clock`]. In
/// production, `&SystemClock` reproduces the historical `SystemTime::now()`
/// behavior; tests and replay paths can pin a fixed clock for determinism.
pub fn build_compensation_failed_event(
    context: &CompensationContext,
    compensation_failure_reason: &str,
    clock: &dyn Clock,
) -> SagaCompensationFailed {
    SagaCompensationFailed {
        triggering_aggregate: context.source.source.clone(),
        triggering_event_sequence: context.source.source_seq,
        rejection_reason: context.rejection_reason.clone(),
        compensation_failure_reason: compensation_failure_reason.to_string(),
        rejected_command: Some(context.rejected_command.clone()),
        occurred_at: Some(prost_types::Timestamp::from(clock.now())),
    }
}

/// Derive the compensation EventBook's aggregate root deterministically.
///
/// H-37 fix. Previously a fresh `Uuid::new_v4()` was minted on every call,
/// so every redelivery of the same failed saga command (e.g., AMQP/NATS
/// redelivery, crash + replay) wrote a new fallback-domain aggregate,
/// breaking idempotency and the "snapshot derivable from events alone"
/// invariant.
///
/// The root is now SHA-256 over a separator-delimited concatenation of:
///   - source aggregate's domain (UTF-8 bytes)
///   - source aggregate's root UUID bytes (empty if absent)
///   - source_seq (little-endian u64)
///   - rejected command's encoded proto bytes
///   - compensation failure reason (UTF-8 bytes)
///
/// **Determinism caveat**: protobuf wire format is NOT canonical by spec.
/// Prost's encoder is deterministic within a single binary version, so the
/// derived root is stable across replays of the same process and across
/// recompilations of the same `prost` major. A prost upgrade, a switch to a
/// different proto runtime, or a change to the proto schema's field numbering
/// can shift the bytes and therefore the derived root — at which point a
/// re-delivery of the *same logical* failed saga command would land on a
/// different fallback-domain aggregate (the idempotency invariant the fix
/// pins would silently break). Callers depending on cross-version stability
/// should hash a stable projection (sorted field numbers + length-delimited
/// values) instead of the wire bytes.
///
/// We slice the digest to the first 16 bytes to fit a UUID. Different
/// logical compensation events still discriminate (SHA-256 collisions on
/// 128-bit truncation are vanishingly improbable), and identical inputs
/// always produce the same root — which is exactly the idempotency
/// invariant the bug violated.
fn derive_compensation_root_bytes(
    context: &CompensationContext,
    compensation_failure_reason: &str,
) -> Vec<u8> {
    let mut hasher = Sha256::new();
    if let Some(src) = context.source.source.as_ref() {
        hasher.update(src.domain.as_bytes());
        // Field separator so domain "ab" + empty root can't collide with
        // domain "a" + root "b".
        hasher.update(b"\x00");
        if let Some(root) = src.root.as_ref() {
            hasher.update(&root.value);
        }
        hasher.update(b"\x00");
    } else {
        // No source provenance — still emit a stable separator so the
        // remaining fields drive the digest.
        hasher.update(b"\x00\x00");
    }
    hasher.update(context.source.source_seq.to_le_bytes());
    hasher.update(b"\x00");
    hasher.update(context.rejected_command.encode_to_vec());
    hasher.update(b"\x00");
    hasher.update(compensation_failure_reason.as_bytes());

    let digest = hasher.finalize();
    digest[..16].to_vec()
}

/// Build an EventBook containing the SagaCompensationFailed event.
///
/// Uses the fallback domain from config as the target domain. The
/// `EventBook.cover.root` is derived deterministically from the
/// (context, reason) pair (see [`derive_compensation_root_bytes`]) so that
/// re-deliveries of the same failed saga command land on the same
/// fallback-domain aggregate. The page timestamp is taken from
/// `SystemClock`; callers needing deterministic timestamps should use
/// [`build_compensation_failed_event_book_with_clock`].
pub fn build_compensation_failed_event_book(
    context: &CompensationContext,
    compensation_failure_reason: &str,
    config: &SagaCompensationConfig,
) -> EventBook {
    build_compensation_failed_event_book_with_clock(
        context,
        compensation_failure_reason,
        config,
        &SystemClock,
    )
}

/// Clock-injected variant of [`build_compensation_failed_event_book`].
///
/// Behavior is identical except the `created_at` and `occurred_at`
/// timestamps come from `clock.now()` rather than `SystemTime::now()`.
/// Used in tests to assert byte-equality of the produced EventBook; the
/// public clock-less function delegates here with `&SystemClock`.
pub fn build_compensation_failed_event_book_with_clock(
    context: &CompensationContext,
    compensation_failure_reason: &str,
    config: &SagaCompensationConfig,
    clock: &dyn Clock,
) -> EventBook {
    let event = build_compensation_failed_event(context, compensation_failure_reason, clock);
    let root_bytes = derive_compensation_root_bytes(context, compensation_failure_reason);
    let created_at = prost_types::Timestamp::from(clock.now());

    EventBook {
        cover: Some(Cover {
            domain: config.fallback_domain.clone(),
            root: Some(ProtoUuid { value: root_bytes }),
            correlation_id: context.correlation_id.clone(),
            edition: None,
            ext: None,
        }),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(0)),
            }),
            created_at: Some(created_at),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: type_url::SAGA_COMPENSATION_FAILED.to_string(),
                value: event.encode_to_vec(),
            })),
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    }
}

/// Outcome of handling a business response to a rejection Notification.
#[derive(Debug)]
pub enum CompensationOutcome {
    /// Business provided compensation events - use them.
    Events(EventBook),
    /// Emit SagaCompensationFailed event to fallback domain.
    EmitSystemRevocation(EventBook),
    /// Compensation declined, just log.
    Declined { reason: String },
    /// Abort saga chain, propagate error to caller.
    Aborted { reason: String },
}

/// Handle a BusinessResponse to a rejection Notification.
///
/// Routes compensation responses through a priority-based decision tree:
///
/// # Response Priority
///
/// 1. **Explicit events** - Business provided compensation events directly.
///    These are used as-is (business knows best how to compensate).
///
/// 2. **RevocationResponse** - Business returned flags indicating desired
///    escalation behavior. Flags are processed by `process_revocation_flags`.
///
/// 3. **Empty/Error fallback** - If business returns empty response or gRPC
///    fails, we use config-based fallback flags. This ensures compensation
///    always has a defined behavior even when business logic is unavailable.
///
/// # Fallback Strategy
///
/// The fallback behavior exists because saga compensation must complete even
/// if the source aggregate's revocation handler fails. Config flags determine
/// whether to emit system events, quarantine, or escalate by default.
///
/// Importantly, fallback never sets `abort = true` - we don't want network
/// failures to halt saga processing entirely.
pub async fn handle_business_response(
    response: std::result::Result<BusinessResponse, tonic::Status>,
    context: &CompensationContext,
    config: &SagaCompensationConfig,
    escalation_handler: &dyn EscalationHandler,
) -> Result<CompensationOutcome> {
    let source_domain = context
        .source
        .source
        .as_ref()
        .map(|c| c.domain.as_str())
        .unwrap_or("?");

    let revocation = match response {
        Ok(BusinessResponse {
            result: Some(business_response::Result::Events(book)),
        }) if !book.pages.is_empty() => {
            // Business provided compensation events - use them
            info!(
                source_domain = %source_domain,
                source_seq = context.source.source_seq,
                events = book.pages.len(),
                "Business provided compensation events"
            );
            #[cfg(feature = "otel")]
            {
                use crate::advice::metrics::{self, SAGA_COMPENSATION_TOTAL};
                SAGA_COMPENSATION_TOTAL.add(1, &[metrics::name_attr(source_domain)]);
            }
            return Ok(CompensationOutcome::Events(book));
        }
        Ok(BusinessResponse {
            result: Some(business_response::Result::Revocation(r)),
        }) => r,
        Ok(_) => {
            // Empty events → use config-based fallback flags
            warn!(
                source_domain = %source_domain,
                "Business returned empty response, using fallback"
            );
            RevocationResponse {
                emit_system_revocation: config.fallback_emit_system_revocation,
                send_to_dead_letter_queue: config.fallback_send_to_dlq,
                escalate: config.fallback_escalate,
                abort: false, // Don't abort on fallback
                reason: "client logic returned empty response".to_string(),
            }
        }
        Err(status) => {
            // gRPC error → use config-based fallback flags
            error!(
                source_domain = %source_domain,
                error = %status,
                "gRPC error from client logic, using fallback"
            );
            RevocationResponse {
                emit_system_revocation: config.fallback_emit_system_revocation,
                send_to_dead_letter_queue: config.fallback_send_to_dlq,
                escalate: config.fallback_escalate,
                abort: false, // Don't abort on fallback
                reason: format!("gRPC error: {}", status),
            }
        }
    };

    // Process revocation flags
    process_revocation_flags(&revocation, context, config, escalation_handler).await
}

/// Process RevocationResponse flags and take appropriate actions.
///
/// # Flag Processing Order
///
/// Flags are processed in a specific order to ensure proper escalation:
///
/// 1. **quarantine** (`send_to_dead_letter_queue`) - Preserves context for
///    later replay/investigation. Runs first so failure data is captured
///    even if subsequent steps fail.
///
/// 2. **notify** (`escalate`) - Alerts operators via webhook. Runs second
///    so humans are notified after quarantine succeeds.
///
/// 3. **abort** - Stops the saga chain entirely. Checked after escalation
///    so operators are notified before the chain halts.
///
/// 4. **emit_system_revocation** - Emits SagaCompensationFailed event to
///    fallback domain. Only runs if abort is false.
///
/// # Error Isolation
///
/// Escalation handler errors (quarantine/notify failures) are logged but
/// don't prevent other flags from processing. This ensures partial success:
/// if quarantine succeeds but webhook fails, the data is still preserved.
///
/// # Outcome Mapping
///
/// | Flags Set | Outcome |
/// |-----------|---------|
/// | abort=true | `Err(Aborted)` |
/// | emit_system_revocation=true | `EmitSystemRevocation` |
/// | only escalation flags | `Declined` (escalation already happened) |
/// | none | `Declined` |
async fn process_revocation_flags(
    revocation: &RevocationResponse,
    context: &CompensationContext,
    config: &SagaCompensationConfig,
    escalation_handler: &dyn EscalationHandler,
) -> Result<CompensationOutcome> {
    let source_domain = context
        .source
        .source
        .as_ref()
        .map(|c| c.domain.as_str())
        .unwrap_or("?");

    info!(
        source_domain = %source_domain,
        source_seq = context.source.source_seq,
        emit = revocation.emit_system_revocation,
        dlq = revocation.send_to_dead_letter_queue,
        escalate = revocation.escalate,
        abort = revocation.abort,
        reason = %revocation.reason,
        "Processing revocation response"
    );

    // Quarantine if requested (for later reprocessing)
    if revocation.send_to_dead_letter_queue {
        if let Err(e) = escalation_handler
            .quarantine(context, &revocation.reason)
            .await
        {
            error!(error = %e, "Failed to quarantine");
            // Continue processing other flags even if quarantine fails
        }
    }

    // Notify if requested (for human intervention)
    if revocation.escalate {
        if let Err(e) = escalation_handler.notify(context, &revocation.reason).await {
            error!(error = %e, "Failed to notify");
            // Continue processing other flags even if notification fails
        }
    }

    // Check abort flag - it takes precedence over other outcomes
    if revocation.abort {
        return Err(CompensationError::Aborted(revocation.reason.clone()));
    }

    // Emit system revocation event if requested
    if revocation.emit_system_revocation {
        let event_book = build_compensation_failed_event_book(context, &revocation.reason, config);
        #[cfg(feature = "otel")]
        {
            use crate::advice::metrics::{self, SAGA_COMPENSATION_TOTAL};
            SAGA_COMPENSATION_TOTAL.add(1, &[metrics::name_attr(source_domain)]);
        }
        return Ok(CompensationOutcome::EmitSystemRevocation(event_book));
    }

    // No flags set - declined, just log
    Ok(CompensationOutcome::Declined {
        reason: revocation.reason.clone(),
    })
}

/// Process compensation response from coordinator and handle all outcomes.
///
/// This is the shared entry point for saga compensation - both gRPC and local
/// modes call this after getting the BusinessResponse from the coordinator.
///
/// # Outcome Handling
///
/// | Outcome | Action |
/// |---------|--------|
/// | `Events` | Log success - events already persisted by HandleCompensation |
/// | `EmitSystemRevocation` | Publish SagaCompensationFailed to event bus |
/// | `Declined` | Debug log - business chose not to compensate |
/// | `Aborted` (outcome) | Error log - business explicitly stopped chain |
/// | `Aborted` (error) | Error log - abort flag was set during processing |
/// | Other errors | Error log - unexpected failure |
///
/// # Aborted Variants
///
/// There are two paths to "aborted":
/// - `CompensationOutcome::Aborted` - Business explicitly returned abort in events
/// - `CompensationError::Aborted` - The abort flag was set in RevocationResponse
///
/// Both halt the saga chain, but the distinction helps with debugging whether
/// the abort came from explicit business logic or flag-based processing.
pub async fn process_compensation_response(
    response: std::result::Result<crate::proto::BusinessResponse, tonic::Status>,
    context: &CompensationContext,
    config: &SagaCompensationConfig,
    event_bus: &std::sync::Arc<dyn crate::bus::EventBus>,
    saga_name: &str,
    triggering_domain: &str,
) {
    let escalation_handler = DefaultEscalationHandler::new(event_bus.clone(), config.clone());

    let outcome = handle_business_response(response, context, config, &escalation_handler).await;

    match outcome {
        Ok(CompensationOutcome::Events(events)) => {
            // Business provided compensation events - already persisted by HandleCompensation
            info!(
                saga = %saga_name,
                triggering_domain = %triggering_domain,
                events = events.pages.len(),
                "Compensation events recorded successfully"
            );
        }
        Ok(CompensationOutcome::EmitSystemRevocation(event_book)) => {
            info!(
                saga = %saga_name,
                triggering_domain = %triggering_domain,
                "Emitting system revocation event"
            );
            if let Err(e) = event_bus.publish(std::sync::Arc::new(event_book)).await {
                error!(
                    saga = %saga_name,
                    error = %e,
                    "Failed to publish system revocation event"
                );
            }
        }
        Ok(CompensationOutcome::Declined { reason }) => {
            debug!(
                saga = %saga_name,
                triggering_domain = %triggering_domain,
                reason = %reason,
                "Compensation declined by business logic"
            );
        }
        Ok(CompensationOutcome::Aborted { reason }) => {
            error!(
                saga = %saga_name,
                triggering_domain = %triggering_domain,
                reason = %reason,
                "Compensation aborted by business logic - saga chain stopped"
            );
        }
        Err(CompensationError::Aborted(reason)) => {
            error!(
                saga = %saga_name,
                triggering_domain = %triggering_domain,
                reason = %reason,
                "Compensation aborted - saga chain stopped"
            );
        }
        Err(e) => {
            error!(
                saga = %saga_name,
                triggering_domain = %triggering_domain,
                error = %e,
                "Compensation failed"
            );
        }
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
