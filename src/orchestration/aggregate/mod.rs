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
//! | **STRICT** | Return FAILED_PRECONDITION, retry with fresh state | Default, most operations |
//! | **COMMUTATIVE** | Check field overlap; proceed if disjoint, else retry | High-concurrency aggregates |
//! | **MANUAL** | Send to DLQ for human review | Conflict-sensitive operations |
//! | **AGGREGATE_HANDLES** | Skip validation, let aggregate decide | Custom concurrency control |
//!
//! STRICT is the safest default: always retry on mismatch. COMMUTATIVE optimizes
//! for throughput when concurrent writes often touch different fields (e.g.,
//! counters, independent properties). MANUAL is for operations where automatic
//! retry could cause business problems (e.g., financial transactions).
//!
//! # Architecture
//!
//! - `AggregateContext`: Storage access and post-persist hooks (local vs gRPC impl)
//! - `ClientLogic`: Business logic invocation (gRPC client to aggregate handler)
//! - `execute_command_pipeline`: The main execution flow
//! - `try_commutative_merge`: Field-level conflict detection for COMMUTATIVE mode
//!
//! # Module Structure
//!
//! - `local/`: SQLite-backed storage with static service discovery
//! - `grpc/`: Remote storage with K8s service discovery
//! - `types`: Enums and structs (TemporalQuery, PipelineMode, FactContext, FactResponse)
//! - `traits`: Trait definitions (AggregateContext, ClientLogic, AggregateContextFactory)
//! - `client`: gRPC client logic implementation (GrpcBusinessLogic)
//! - `parsing`: Cover/sequence extraction and validation
//! - `merge`: Commutative merge field-overlap detection
//! - `pipeline`: Command and fact execution pipelines

// tonic::Status is large by design - it carries error details for gRPC
#![allow(clippy::result_large_err)]

// Submodule implementations
pub mod grpc;
// Local module always compiled (sqlite always on)
pub mod local;

// Internal modules
mod client;
mod merge;
mod parsing;
mod pipeline;
mod sync_policy;
mod traits;
pub mod two_phase;
mod types;

// Re-exports: types
pub use types::{FactContext, FactResponse, PipelineMode, TemporalQuery};

// Re-exports: traits
pub use traits::{AggregateContext, AggregateContextFactory, ClientLogic, PersistOutcome};

// Re-exports: client
pub use client::GrpcBusinessLogic;

// Re-exports: parsing
pub use parsing::{extract_command_sequence, parse_command_cover, parse_event_cover};

// Re-exports: pipeline
pub use pipeline::{execute_command_pipeline, execute_command_with_retry, execute_fact_pipeline};

// Re-exports: two_phase
pub use two_phase::{transform_for_two_phase, TwoPhaseContext, TwoPhaseResult};

// Re-export default edition constant
pub use crate::proto_ext::constants::DEFAULT_EDITION;

#[cfg(test)]
mod tests;
