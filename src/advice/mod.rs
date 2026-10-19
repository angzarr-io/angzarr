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
mod instrumented_handlers;
pub mod metrics;

pub use instrumented::Instrumented;
pub use instrumented_bus::{InstrumentedBus, InstrumentedDynBus};
pub use instrumented_handlers::{
    InstrumentedPMHandler, InstrumentedProjectorHandler, InstrumentedSagaHandler,
};

// Re-export metric constants for external dashboards/alerting
pub use metrics::{
    // Special values
    DOMAIN_CORRELATION_QUERY,
    // Operation names
    OP_EVENT_ADD,
    OP_EVENT_GET,
    OP_EVENT_GET_BY_CORRELATION,
    OP_EVENT_GET_FROM,
    OP_EVENT_GET_FROM_TO,
    OP_EVENT_GET_NEXT_SEQUENCE,
    OP_EVENT_LIST_DOMAINS,
    OP_EVENT_LIST_ROOTS,
    OP_POSITION_GET,
    OP_POSITION_PUT,
    OP_SNAPSHOT_DELETE,
    OP_SNAPSHOT_GET,
    OP_SNAPSHOT_PUT,
};
