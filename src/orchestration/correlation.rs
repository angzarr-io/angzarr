//! Shared correlation ID extraction.
//!
//! Correlation IDs are client-provided for cross-domain workflows.
//! If not provided, the ID remains empty and process managers won't trigger.

#![allow(clippy::result_large_err)]

use std::sync::LazyLock;

use tonic::Status;

use crate::proto_ext::CoverExt;
use crate::validation;

/// Angzarr UUID namespace derived from DNS-based UUIDv5.
///
/// Used for deterministic UUID generation (e.g., component name to root UUID).
pub static ANGZARR_UUID_NAMESPACE: LazyLock<uuid::Uuid> =
    LazyLock::new(|| uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, b"angzarr.dev"));

/// Extract and validate the correlation ID of a command or event book.
///
/// Correlation IDs are client-provided for cross-domain workflows.
/// Returns empty string if not provided—this is intentional:
/// - Process managers require correlation_id and will skip events without one
/// - This enables opt-in cross-domain tracking without polluting single-domain flows
///
/// Validates format if non-empty.
pub fn extract_correlation_id(book: &impl CoverExt) -> Result<String, Status> {
    let id = book.correlation_id().to_string();
    validation::validate_correlation_id(&id)?;
    Ok(id)
}

#[cfg(test)]
#[path = "correlation.test.rs"]
mod tests;
