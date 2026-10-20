//! gRPC service implementations.

pub mod aggregate;
pub mod event_query;
pub mod projector_coord;
pub mod upcaster;

pub use aggregate::AggregateService;
pub use event_query::EventQueryService;
pub use projector_coord::ProjectorCoord;
pub use upcaster::{Upcaster, UpcasterConfig};
