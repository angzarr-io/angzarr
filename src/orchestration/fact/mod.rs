//! Fact injection abstraction.
//!
//! `FactExecutor` injects facts (external events) into target aggregates
//! via `grpc/`'s remote `CommandHandlerCoordinatorServiceClient::handle_event`.

pub mod grpc;
