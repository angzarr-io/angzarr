// Enable coverage(off) attribute on nightly when coverage_nightly feature is active
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Angzarr - CQRS/ES Framework
//!
//! The coordinators of the angzarr framework: the aggregate coordinator
//! (commands in, events out) and the projector coordinator (events to read
//! models), over the latest `io.angzarr.v1` wire contract.

pub mod advice;
pub mod alerts;
pub mod bus;
pub mod config;
pub mod descriptor;
pub mod discovery;
pub mod dlq;
pub mod grpc;
pub mod handlers;
pub mod orchestration;
pub mod process;
pub mod proto;
pub mod proto_ext;
pub mod registration;
pub mod registry;
pub mod services;
pub mod storage;
pub mod transport;
pub mod utils;

/// Test doubles for the coordinators (unit tests and the contract harness).
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

// Re-export extension traits for convenient imports
pub use proto_ext::{CoverExt, EventBookExt};

// Re-export trivial_delegation macro for marking functions excluded from unit testing
pub use trivial_delegation::trivial_delegation;
