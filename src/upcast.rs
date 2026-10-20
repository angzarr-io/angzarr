//! Upcasting stored events to their current shapes.
//!
//! The coordinator upcasts on every read and publish path of its stream (the
//! load for a command, query responses, and the books it publishes), so
//! consumers see only current shapes; stored events are never rewritten.
//! The upcaster is part of the component (decision 14.22): the coordinator
//! calls its own business process's `UpcasterService`.

use async_trait::async_trait;
use tonic::Status;

use crate::proto::EventPage;

/// Brings a domain's stored pages up to their current shapes, in order.
#[async_trait]
pub trait EventUpcaster: Send + Sync {
    async fn upcast(&self, domain: &str, pages: Vec<EventPage>) -> Result<Vec<EventPage>, Status>;
}
