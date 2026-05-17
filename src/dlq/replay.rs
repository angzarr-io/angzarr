//! Replay publisher — the bus-facing side of DLQ replay.
//!
//! When the DLQ admin handler replays a dead letter, it does three
//! things:
//!   1. Fetches the row via [`crate::dlq::DeadLetterReader`].
//!   2. Decodes the stored `AngzarrDeadLetter`, extracts the
//!      rejected `CommandBook`.
//!   3. Stamps a fresh correlation_id + `replayed_from_dlq_id` and
//!      `original_correlation_id` metadata pointers on the command,
//!      optionally rewrites the sequence to the aggregate's current
//!      `next_sequence` (when [`ReplayMode::FreshSequence`]), then
//!      hands the rewritten command to a [`ReplayPublisher`].
//!
//! The publisher is the abstraction over "how does this command
//! actually reach the right coordinator." Production impl will hold
//! a [`crate::discovery::ServiceDiscovery`] + per-domain gRPC client
//! pool and call `CommandHandlerCoordinatorService::HandleCommand`.
//! For Phase 1.3 we ship the trait + a [`NoopReplayPublisher`] so the
//! handler can be tested end-to-end against the Health<T> envelope
//! without dragging in service-discovery wiring. The real impl lands
//! when the cross-domain publisher work is scheduled.
//!
//! Plan reference: P1.3 / P5 in `plans/virtual-spinning-flute.md`.

use async_trait::async_trait;

use super::error::DlqError;
use crate::proto::CommandBook;

/// The two replay shapes the operator chooses between. Mirrors the
/// `ReplayMode` proto enum (`crate::proto::status::ReplayMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayMode {
    /// Preserve the original command's sequence on the page header.
    /// Likely rejected as `FAILED_PRECONDITION` if state has moved;
    /// the diagnostic mode operators use to reproduce a failure.
    AsIs,
    /// Rewrite the sequence to the aggregate's current
    /// `next_sequence` before publishing. Best chance of success
    /// but the payload may depend on stale state.
    FreshSequence,
}

impl ReplayMode {
    /// Convert from the wire proto enum value, defaulting
    /// `UNSPECIFIED` → `FreshSequence` (the safer default per the
    /// plan's resilience contract — operators who didn't specify
    /// probably want the command to succeed).
    pub fn from_proto(value: i32) -> Self {
        use crate::proto::status::ReplayMode as Proto;
        match Proto::try_from(value).unwrap_or(Proto::Unspecified) {
            Proto::AsIs => ReplayMode::AsIs,
            // Default for UNSPECIFIED + FRESH_SEQUENCE both → fresh.
            _ => ReplayMode::FreshSequence,
        }
    }

    /// Convert to the wire proto enum value.
    pub fn to_proto(self) -> crate::proto::status::ReplayMode {
        use crate::proto::status::ReplayMode as Proto;
        match self {
            ReplayMode::AsIs => Proto::AsIs,
            ReplayMode::FreshSequence => Proto::FreshSequence,
        }
    }
}

/// The bus-facing replay surface. Implementations route a
/// (possibly-rewritten) `CommandBook` to the coordinator that owns
/// the target aggregate.
///
/// Returning `Ok(())` means the publish succeeded at the transport
/// layer — NOT that the command was accepted by the aggregate. The
/// aggregate's accept/reject flows back through the normal
/// observability path (events, DLQ on second-time-rejection, etc.).
#[async_trait]
pub trait ReplayPublisher: Send + Sync {
    /// Publish the (already-rewritten) command on the appropriate
    /// bus. The command's metadata is expected to already carry
    /// `replayed_from_dlq_id` and `original_correlation_id` —
    /// the handler stamps those before calling.
    async fn replay(&self, command: CommandBook) -> Result<(), DlqError>;

    /// Whether this publisher can actually route. Mirrors the
    /// pattern on the reader/publisher traits — the noop returns
    /// `false` so the handler emits a degraded `ProblemDetails`
    /// instead of pretending the replay succeeded.
    fn is_configured(&self) -> bool {
        true
    }

    /// Identifier surfaced in the Health<T> envelope `source` field.
    fn source_id(&self) -> &'static str {
        "unknown"
    }
}

/// No-op publisher for the bootstrap path (status binary running
/// without cross-domain publisher wiring). Every call returns
/// [`DlqError::NotConfigured`] so the handler returns a `degraded`
/// envelope to the operator.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopReplayPublisher;

#[async_trait]
impl ReplayPublisher for NoopReplayPublisher {
    async fn replay(&self, _command: CommandBook) -> Result<(), DlqError> {
        Err(DlqError::NotConfigured)
    }

    fn is_configured(&self) -> bool {
        false
    }

    fn source_id(&self) -> &'static str {
        "noop"
    }
}

#[cfg(test)]
#[path = "replay.test.rs"]
mod tests;
