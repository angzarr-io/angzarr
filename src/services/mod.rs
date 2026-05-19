//! gRPC service implementations.

use std::sync::Arc;
use tonic::Status;
use tracing::error;

pub mod aggregate;
pub mod event_query;
pub mod gap_fill;
pub mod pm_coord;
pub mod projector_coord;
pub mod saga_coord;
pub mod snapshot_handler;
pub mod upcaster;

use crate::proto::EventBook;

/// Fill gaps in an EventBook if a gap filler is available.
///
/// This is a common pattern used by saga and PM coordinators to ensure
/// EventBooks are complete before processing.
///
/// # Arguments
/// * `gap_filler` - Optional gap filler instance
/// * `book` - The EventBook to potentially fill
///
/// # Returns
/// The filled EventBook, or the original if no gap filler is available.
pub(crate) async fn fill_gaps_if_needed(
    gap_filler: Option<
        &Arc<gap_fill::GapFiller<gap_fill::NoOpPositionStore, gap_fill::RemoteEventSource>>,
    >,
    book: EventBook,
) -> Result<EventBook, Status> {
    match gap_filler {
        Some(filler) => filler.fill_if_needed(book).await.map_err(|e| {
            error!(error = %e, "Failed to fill EventBook gaps");
            Status::internal(format!("{}{}", errmsg::REPAIR_EVENTBOOK_FAILED, e))
        }),
        None => Ok(book),
    }
}

/// Error message constants for gRPC services.
pub mod errmsg {
    /// CommandRequest missing command.
    pub const COMMAND_REQUEST_MISSING_COMMAND: &str = "CommandRequest must have a command";
    /// SpeculateAggregateRequest missing command.
    pub const SPECULATE_AGG_MISSING_COMMAND: &str = "SpeculateAggregateRequest must have a command";
    /// EventRequest missing events.
    pub const EVENT_REQUEST_MISSING_EVENTS: &str = "EventRequest must have events";
    /// SpeculateProjectorRequest missing events.
    pub const SPECULATE_PROJ_MISSING_EVENTS: &str = "SpeculateProjectorRequest requires events";
    /// Query missing cover with domain/root or correlation.
    pub const QUERY_MISSING_COVER_OR_CORRELATION: &str =
        "Query must have a cover with domain/root or correlation_id";
    /// Query missing root UUID or correlation.
    pub const QUERY_MISSING_ROOT_OR_CORRELATION: &str =
        "Query must have a root UUID or correlation_id";
    /// Query missing cover.
    pub const QUERY_MISSING_COVER: &str = "Query must have a cover";
    /// Query missing root UUID.
    pub const QUERY_MISSING_ROOT: &str = "Query must have a root UUID";
    /// Invalid UUID prefix.
    pub const INVALID_UUID: &str = "Invalid UUID: ";
    /// Temporal query missing point in time.
    pub const TEMPORAL_QUERY_MISSING_POINT: &str =
        "TemporalQuery must specify as_of_time or as_of_sequence";
    /// EventBook gap-fill failure prefix.
    pub const REPAIR_EVENTBOOK_FAILED: &str = "Failed to fill EventBook gaps: ";
    /// Projector failure suffix (appears after projector name).
    pub const PROJECTOR_FAILED: &str = " failed: ";
}

pub use crate::utils::saga_compensation::{
    build_compensation_failed_event, build_compensation_failed_event_book,
    build_compensation_failed_event_book_with_clock, build_notification_command_book,
    build_rejection_notification, handle_business_response, Clock, CompensationContext,
    CompensationError, CompensationOutcome, DefaultEscalationHandler, EscalationHandler,
    NoopEscalationHandler, SystemClock,
};
pub use aggregate::AggregateService;
pub use event_query::EventQueryService;
pub use gap_fill::{
    GapFiller, LocalEventSource, NoOpPositionStore, PositionStoreAdapter, RemoteEventSource,
};
pub use pm_coord::PmCoord;
pub use projector_coord::ProjectorCoord;
pub use saga_coord::SagaCoord;
pub use upcaster::{Upcaster, UpcasterConfig};
