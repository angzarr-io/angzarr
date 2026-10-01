//! Orchestration layer for unified in-process and distributed execution.
//!
//! Each sub-module defines a trait (interface) in `mod.rs` with shared orchestration logic.
//! Transport-specific implementations live in `local/` (in-process) and `grpc/` (remote)
//! subdirectories, named by their key differentiating factor.

use async_trait::async_trait;

use crate::proto::EventBook;

pub mod aggregate;
pub mod command;
pub mod correlation;
pub mod destination;
pub mod fact;
pub mod process_manager;
pub mod projector;
pub mod saga;
pub mod shared;

// ============================================================================
// Fact Injection
// ============================================================================

/// Error message constants for orchestration operations.
pub mod errmsg {
    // Fact injection errors
    pub const AGGREGATE_NOT_FOUND: &str = "Target aggregate not found: ";
    pub const REJECTED: &str = "Fact handler rejected: ";
    pub const INTERNAL: &str = "Fact injection failed: ";

    // Command/Event book validation errors
    pub const COMMAND_BOOK_MISSING_COVER: &str = "CommandBook must have a cover";
    pub const EVENT_BOOK_MISSING_COVER: &str = "EventBook must have a cover";
    pub const COVER_MISSING_ROOT: &str = "Cover must have a root UUID";
    pub const REPLAY_MISSING_STATE: &str = "Replay response missing state";

    // Aggregate command pipeline errors
    pub const INVALID_UUID: &str = "Invalid UUID: ";
    pub const SPECULATIVE_REQUIRES_TEMPORAL: &str =
        "Speculative requires either as_of_sequence or as_of_timestamp";
    /// Prefix shared by every merge-gate sequence-mismatch message. Callers
    /// treat it as "refresh state and resubmit" (see `utils::retry`).
    pub const SEQUENCE_MISMATCH_CLASS: &str = "Sequence mismatch:";
    pub const SEQUENCE_MISMATCH: &str = "Sequence mismatch: command expects ";
    pub const SEQUENCE_MISMATCH_OVERLAP: &str =
        "Sequence mismatch: overlapping fields, command expects ";
    pub const SEQUENCE_MISMATCH_DLQ_SUFFIX: &str = ". Sent to DLQ for manual review.";
    pub const FACT_EVENTS_MISSING_MARKER: &str =
        "Fact events must have ExternalDeferredSequence markers";
}

/// Error type for fact injection failures.
#[derive(Debug, thiserror::Error)]
pub enum FactInjectionError {
    /// Target aggregate not found for the fact's domain.
    #[error("{}{domain}", errmsg::AGGREGATE_NOT_FOUND)]
    AggregateNotFound { domain: String },

    /// Fact handler rejected the fact.
    #[error("{}{reason}", errmsg::REJECTED)]
    Rejected { reason: String },

    /// Storage or transport error during fact injection.
    #[error("{}{}", errmsg::INTERNAL, .0)]
    Internal(String),
}

/// Executor for injecting facts (events) into target aggregates.
///
/// Facts are events emitted by sagas or process managers that are injected
/// directly into target aggregates, bypassing command handling. The coordinator
/// stamps the sequence number on receipt based on the aggregate's current state.
///
/// Facts must have `external_id` set in their Cover for idempotent handling.
#[async_trait]
pub trait FactExecutor: Send + Sync {
    /// Inject a fact into the target aggregate specified by the fact's cover.
    ///
    /// The coordinator:
    /// 1. Looks up the aggregate by domain from the fact's cover
    /// 2. Stamps sequence numbers on the fact's pages
    /// 3. Optionally routes through the aggregate's `handle_fact()` handler
    /// 4. Persists the events
    ///
    /// # Errors
    /// Returns `FactInjectionError` if:
    /// - Target aggregate is not found
    /// - Fact handler rejects the fact
    /// - Storage/transport failure
    async fn inject(&self, fact: EventBook) -> Result<(), FactInjectionError>;
}
