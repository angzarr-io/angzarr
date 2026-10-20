//! Extension traits on the angzarr-proto wire types.
//!
//! Wire types come from `angzarr-proto` (see `crate::proto`); behaviour core
//! needs on them lives here as extension traits, never as free functions on
//! raw fields (CLAUDE.md, "Proto").

pub mod books;
pub mod constants;
pub mod cover;
pub mod grpc;
pub mod type_url;

pub use books::EventBookExt;
pub use constants::{CORRELATION_ID_HEADER, UNKNOWN_DOMAIN, WILDCARD_DOMAIN};
pub use cover::CoverExt;
pub use grpc::correlated_request;
