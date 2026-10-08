//! Type definitions for aggregate command pipeline.
//!
//! Contains enums and simple structs used across the aggregate orchestration module.

use crate::proto::{EventBook, Projection};

/// How to load prior events.
#[derive(Debug, Clone)]
pub enum TemporalQuery {
    /// Current state (latest events, snapshot-optimized).
    Current,
    /// Events up to a specific sequence number (inclusive).
    AsOfSequence(u32),
    /// Events up to a specific timestamp.
    ///
    /// C10: carries the typed `prost_types::Timestamp` end to end. It was
    /// previously a `String` that each layer parsed/reformatted, which is
    /// exactly the round-trip the storage `until: &str` footgun lived in —
    /// keeping the typed value means the single normalization point is the
    /// repository/storage boundary, not every intermediate hop.
    AsOfTimestamp(prost_types::Timestamp),
}

/// Pipeline execution mode.
#[derive(Debug, Clone)]
pub enum PipelineMode {
    /// Normal execution: validate → invoke → persist → post-persist.
    Execute,
    /// Speculative: load temporal state → invoke → return (no persist/publish).
    Speculative {
        as_of_sequence: Option<u32>,
        as_of_timestamp: Option<prost_types::Timestamp>,
    },
}

/// Context for fact event handling.
///
/// Contains the fact events to record and the aggregate's prior events.
#[derive(Debug, Clone)]
pub struct FactContext {
    /// The fact events to record (with ExternalDeferredSequence markers in PageHeader).
    pub facts: EventBook,
    /// Prior events for this aggregate root (for state reconstruction).
    pub prior_events: Option<EventBook>,
}

/// Response from fact injection.
#[derive(Debug, Clone)]
pub struct FactResponse {
    /// The persisted events (with real sequence numbers).
    pub events: EventBook,
    /// Projections from sync projectors.
    pub projections: Vec<Projection>,
    /// True if this was a duplicate request (external_id already processed).
    pub already_processed: bool,
}
