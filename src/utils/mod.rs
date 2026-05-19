//! Pure utility functions.
//!
//! These are stateless helper functions used across the codebase.

pub mod bootstrap;
pub mod response_builder;
pub mod retry;
pub mod saga_compensation;
pub mod sidecar;
pub mod single_sequence_check;
pub mod tracing;

/// Box an error into `Box<dyn std::error::Error + Send + Sync>`.
///
/// Avoids the verbose `.map_err(|e| Box::new(e) as Box<...>)` pattern
/// and type inference issues with `Into::into` when multiple From impls exist.
pub fn box_err<E: std::error::Error + Send + Sync + 'static>(
    e: E,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(e)
}
