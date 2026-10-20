//! gRPC service implementations.

pub mod aggregate;
pub mod projector_coord;
pub mod upcaster;

pub use aggregate::AggregateService;
pub use projector_coord::ProjectorCoord;
pub use upcaster::{Upcaster, UpcasterConfig};
