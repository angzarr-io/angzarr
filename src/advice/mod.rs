//! Aspect-oriented advice for cross-cutting concerns.
//!
//! This module provides wrapper types that add orthogonal behavior
//! (metrics, tracing, retries, fault injection) without polluting core logic.
//!
//! # Architecture
//!
//! Advice is applied at service composition time, not in implementations:
//!
//! ```ignore
//! // Core implementation - pure client logic
//! let store = SqliteEventStore::new(pool);
//!
//! // Apply advice layers
//! let store = Instrumented::new(store, "sqlite");
//!
//! // Use as normal - metrics are transparent
//! store.add(domain, root, events, correlation_id).await?;
//! ```
//!
//! # Available Advice
//!
//! - [`Instrumented`] - Adds metrics (counters, histograms) for storage operations
//! - [`InstrumentedBus`] - Adds metrics for event bus operations
//!
//! # Metrics
//!
//! All metrics are feature-gated behind `otel`. When disabled, wrappers pass
//! through with no overhead. See [`metrics`] module for metric definitions.

mod instrumented;
mod instrumented_bus;
pub mod metrics;

pub use instrumented::Instrumented;
pub use instrumented_bus::{InstrumentedBus, InstrumentedDynBus};
