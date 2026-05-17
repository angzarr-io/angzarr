// Enable coverage(off) attribute on nightly when coverage_nightly feature is active
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Angzarr - CQRS/ES Framework
//!
//! A Rust implementation of the angzarr framework for building
//! event-sourced applications with CQRS architecture.

pub mod advice;
pub mod bus;
pub mod cascade;
pub mod client_traits;
pub mod config;
pub mod descriptor;
pub mod discovery;
pub mod dlq;
pub mod edition;
pub mod grpc;
pub mod handlers;
pub mod orchestration;
pub mod payload_store;
pub mod process;
pub mod proto;
pub mod proto_ext;
pub mod proto_reflect;
pub mod registration;
pub mod repository;
pub mod services;
pub mod status;
pub mod storage;
pub mod transport;
pub mod utils;
pub mod validation;

/// Test utilities for bus and storage integration tests.
///
/// Provides reusable test handlers and fixture builders.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

// Re-export extension traits for convenient imports
pub use proto_ext::{
    CommandBookExt, CommandPageExt, CoverExt, EditionExt, EventBookExt, EventPageExt, ProtoUuidExt,
    UuidExt,
};

// Re-export trivial_delegation macro for marking functions excluded from unit testing
pub use trivial_delegation::trivial_delegation;
