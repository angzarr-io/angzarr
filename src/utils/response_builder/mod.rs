//! Response building logic for AggregateService.
//!
//! Handles EventBook response building, event page construction,
//! and correlation ID propagation.

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use tonic::{Response, Status};

use crate::bus::EventBus;
use crate::bus::PublishResult;
use crate::proto::{business_response, BusinessResponse, CommandResponse, EventBook};

/// Extracts events from a BusinessResponse, handling revocation and empty responses.
///
/// # Arguments
/// * `response` - The client logic response
/// * `correlation_id` - The correlation ID to propagate to events
///
/// # Returns
/// The EventBook from the response with correlation ID set, or an error for revocation.
pub fn extract_events_from_response(
    response: BusinessResponse,
    correlation_id: &str,
) -> Result<EventBook, Status> {
    let mut events = match response.result {
        Some(business_response::Result::Events(events)) => events,
        Some(business_response::Result::Revocation(revocation)) => {
            // client logic explicitly requested framework handling
            return Err(Status::failed_precondition(format!(
                "Command revoked: {}",
                revocation.reason
            )));
        }
        Some(business_response::Result::Notification(notification)) => {
            // client logic wants to forward rejection upstream
            // Return empty EventBook - notification forwarding handled separately
            tracing::debug!(
                message = "Rejection handler returned notification for upstream propagation",
                cover = ?notification.cover,
            );
            EventBook {
                cover: None,
                pages: vec![],
                snapshot: None,
                ..Default::default()
            }
        }
        None => {
            // Empty response - return empty EventBook
            EventBook {
                cover: None,
                pages: vec![],
                snapshot: None,
                ..Default::default()
            }
        }
    };

    // Propagate correlation ID from command to events via cover
    if let Some(ref mut cover) = events.cover {
        if cover.correlation_id.is_empty() {
            cover.correlation_id = correlation_id.to_owned();
        }
    }

    Ok(events)
}

/// Publishes events and builds the final CommandResponse.
///
/// # Arguments
/// * `event_bus` - The event bus to publish to
/// * `event_book` - The events to publish
///
/// # Returns
/// A CommandResponse containing the events and projection results.
pub async fn publish_and_build_response(
    event_bus: &Arc<dyn EventBus>,
    event_book: EventBook,
) -> Result<Response<CommandResponse>, Status> {
    // Wrap in Arc for immutable distribution
    let event_book = Arc::new(event_book);

    // Notify event bus (projectors, sagas)
    let publish_result = event_bus
        .publish(Arc::clone(&event_book))
        .await
        .map_err(|e| Status::internal(format!("Failed to publish events: {e}")))?;

    Ok(Response::new(build_command_response(
        event_book,
        publish_result,
    )))
}

/// Builds a CommandResponse from an EventBook and publish result.
///
/// # Arguments
/// * `event_book` - The events (wrapped in Arc)
/// * `_publish_result` - The result from publishing to the event bus. Kept
///   in the signature so callers still thread the bus's return value
///   through (in case a future transport reports something via
///   `PublishResult`), but currently carries no data — see `PublishResult`'s
///   docs in `bus::traits`. `projections` is always empty from this path;
///   the real per-command sync projector output is computed directly in
///   `orchestration::aggregate::grpc::call_sync_projectors`, not via the bus.
///
/// # Returns
/// A CommandResponse with the events and (always empty, from this path) projections.
pub fn build_command_response(
    event_book: Arc<EventBook>,
    _publish_result: PublishResult,
) -> CommandResponse {
    CommandResponse {
        events: Some(Arc::try_unwrap(event_book).unwrap_or_else(|arc| (*arc).clone())),
        projections: Vec::new(),
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
