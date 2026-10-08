//! Core angzarr sidecar handlers.
//!
//! These handlers receive events from buses and forward them to client logic
//! coordinators (projectors, sagas, process managers) via gRPC.

pub mod process_manager;
pub mod projector;
pub mod saga;

pub use process_manager::ProcessManagerEventHandler;
pub use projector::ProjectorEventHandler;
pub use saga::SagaEventHandler;
