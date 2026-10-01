//! Extension traits for proto types.
//!
//! Provides convenient accessor methods for common patterns like extracting
//! domain, correlation_id, and root_id from Cover-bearing types.
//!
//! ## Module Organization
//!
//! - [`constants`] - Shared constants (domain names, type URLs, headers)
//! - [`cover`] - CoverExt trait for accessing cover fields
//! - [`edition`] - EditionExt trait and Edition constructors
//! - [`enums`] - SyncModeExt and MergeStrategyExt wire-value resolution
//! - [`uuid`] - UUID conversion traits
//! - [`pages`] - EventPageExt and CommandPageExt traits
//! - [`books`] - EventBookExt, CommandBookExt, and sequence helpers
//! - [`grpc`] - gRPC utilities for correlation and tracing
//! - [`type_url`] - Type URL constants and helpers for angzarr types

pub mod books;
pub mod constants;
pub mod cover;
pub mod edition;
pub mod enums;
pub mod grpc;
pub mod pages;
pub mod type_url;
pub mod uuid;

// Re-export all public items for convenient imports
pub use books::{calculate_next_sequence, calculate_set_next_seq, CommandBookExt, EventBookExt};
pub use constants::{
    CORRELATION_ID_HEADER, DEFAULT_EDITION, META_ANGZARR_DOMAIN, PROJECTION_DOMAIN_PREFIX,
    PROJECTION_TYPE_URL, UNKNOWN_DOMAIN, WILDCARD_DOMAIN,
};
pub use cover::CoverExt;
pub use edition::EditionExt;
pub use enums::{CascadeErrorModeExt, MergeStrategyExt, SyncModeExt};
pub use grpc::correlated_request;
pub use pages::{AngzarrDeferredSequenceExt, CommandPageExt, EventPageExt, PageHeaderExt};
pub use uuid::{ProtoUuidExt, UuidExt};
