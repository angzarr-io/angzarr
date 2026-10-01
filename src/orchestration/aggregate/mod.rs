//! Aggregate command execution pipeline abstraction.
//!
//! This module implements the core command processing flow for event-sourced
//! aggregates. Commands flow through: parse → load → validate → invoke →
//! persist → publish.
//!
//! # Sequence Validation and Merge Strategies
//!
//! Commands include an `expected_sequence` indicating what aggregate state they
//! were prepared against. When this doesn't match the current `actual_sequence`,
//! a concurrent write occurred. How we handle this depends on the merge strategy:
//!
//! | Strategy | Behavior | Use Case |
//! |----------|----------|----------|
//! | **COMMUTATIVE** (default) | Merge when the command's fields are disjoint from the window's writes; overlap → retryable FAILED_PRECONDITION | Concurrent writes to independent fields |
//! | **STRICT** | Retryable FAILED_PRECONDITION; the caller refreshes and resubmits | Operations that must see the latest state |
//! | **MANUAL** | Merge when disjoint; overlap → DLQ + ABORTED for human review | Conflict-sensitive operations |
//! | **AGGREGATE_HANDLES** | No coordinator check; the aggregate decides | Custom concurrency control |
//!
//! Mismatch statuses carry the current EventBook in their details. The field
//! comparison replays state through the client's `Replay` and diffs it by
//! field (descriptor pool when the type is known, protobuf wire tags
//! otherwise); when replay is unavailable COMMUTATIVE answers as STRICT and
//! MANUAL dead-letters.
//!
//! # Architecture
//!
//! - `AggregateContext`: Storage access, publish and sync fan-out hooks
//! - `ClientLogic`: Business logic invocation (gRPC client to aggregate handler)
//! - `execute_command_pipeline`: The main execution flow
//! - `merge`: Field-level conflict detection for COMMUTATIVE / MANUAL
//!
//! # Module Structure
//!
//! - `grpc/`: the production context (storage, bus, service discovery)
//! - `types`: Enums and structs (TemporalQuery, PipelineMode, FactContext, FactResponse)
//! - `traits`: Trait definitions (AggregateContext, ClientLogic)
//! - `client`: gRPC client logic implementation (GrpcBusinessLogic)
//! - `parsing`: Cover/sequence extraction and validation
//! - `merge`: Commutative merge field-overlap detection
//! - `pipeline`: Command and fact execution pipelines

// tonic::Status is large by design - it carries error details for gRPC
#![allow(clippy::result_large_err)]

// Submodule implementations
pub mod grpc;

// Internal modules
mod client;
mod merge;
mod parsing;
mod pipeline;
mod sync_policy;
mod traits;
mod types;

// Re-exports: types
pub use types::{FactContext, FactResponse, PipelineMode, TemporalQuery};

// Re-exports: traits
pub use traits::{AggregateContext, ClientLogic, PersistOutcome, SyncFanout};

// Re-exports: client
pub use client::GrpcBusinessLogic;

// Re-exports: parsing
pub(crate) use parsing::deferred_source_info;
pub use parsing::{edition_key, extract_command_sequence, parse_command_cover, parse_event_cover};

// Re-exports: pipeline
pub use pipeline::{
    execute_command_pipeline, execute_command_with_retry, execute_compensation_pipeline,
    execute_fact_pipeline,
};

// Re-export default edition constant
pub use crate::proto_ext::constants::DEFAULT_EDITION;

#[cfg(test)]
mod tests;
