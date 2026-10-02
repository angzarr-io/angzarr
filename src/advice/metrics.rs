//! Centralized OTel metric definitions for the advice module.
//!
//! All metrics are feature-gated behind `otel`. When disabled, the wrapper
//! types still function but emit no metrics (pass-through only).
//!
//! Naming follows OTel semantic conventions (dot-separated).

#[cfg(feature = "otel")]
use std::sync::LazyLock;

#[cfg(feature = "otel")]
use opentelemetry::metrics::{Counter, Histogram, Meter};
#[cfg(feature = "otel")]
use opentelemetry::{global, KeyValue};

#[cfg(feature = "otel")]
static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("angzarr"));

// ============================================================================
// Storage Metrics
// ============================================================================

/// Duration of storage operations (event store, snapshot store, position store).
#[cfg(feature = "otel")]
pub static STORAGE_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("angzarr.storage.duration")
        .with_description("Storage operation duration")
        .with_unit("s")
        .build()
});

/// Total events stored.
#[cfg(feature = "otel")]
pub static EVENTS_STORED_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.storage.events.stored")
        .with_description("Total events stored")
        .build()
});

/// Total events loaded.
#[cfg(feature = "otel")]
pub static EVENTS_LOADED_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.storage.events.loaded")
        .with_description("Total events loaded")
        .build()
});

/// Total snapshots stored.
#[cfg(feature = "otel")]
pub static SNAPSHOTS_STORED_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.storage.snapshots.stored")
        .with_description("Total snapshots stored")
        .build()
});

/// Total snapshots loaded.
#[cfg(feature = "otel")]
pub static SNAPSHOTS_LOADED_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.storage.snapshots.loaded")
        .with_description("Total snapshots loaded")
        .build()
});

/// Total position updates.
#[cfg(feature = "otel")]
pub static POSITIONS_UPDATED_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.storage.positions.updated")
        .with_description("Total position updates")
        .build()
});

// ============================================================================
// Event Bus Metrics
// ============================================================================

/// Duration of event bus publish operations.
#[cfg(feature = "otel")]
pub static BUS_PUBLISH_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("angzarr.bus.publish.duration")
        .with_description("Event bus publish duration")
        .with_unit("s")
        .build()
});

/// Total event bus publish operations.
#[cfg(feature = "otel")]
pub static BUS_PUBLISH_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.bus.publish.total")
        .with_description("Total event bus publish operations")
        .build()
});

// ============================================================================
// Command Pipeline Metrics
// ============================================================================

/// Duration of command handling (handle, handle_sync, dry_run).
#[cfg(feature = "otel")]
pub static COMMAND_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("angzarr.command.duration")
        .with_description("Command handling duration")
        .with_unit("s")
        .build()
});

/// Total commands processed.
#[cfg(feature = "otel")]
pub static COMMAND_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.command.total")
        .with_description("Total commands processed")
        .build()
});

// ============================================================================
// Saga Metrics
// ============================================================================

/// Duration of saga orchestration.
#[cfg(feature = "otel")]
pub static SAGA_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("angzarr.saga.duration")
        .with_description("Saga orchestration duration")
        .with_unit("s")
        .build()
});

/// Total saga retry attempts.
#[cfg(feature = "otel")]
pub static SAGA_RETRY_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.saga.retry.total")
        .with_description("Total saga retry attempts")
        .build()
});

/// Total saga compensations triggered.
#[cfg(feature = "otel")]
pub static SAGA_COMPENSATION_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.saga.compensation.total")
        .with_description("Total saga compensations triggered")
        .build()
});

// ============================================================================
// Process Manager Metrics
// ============================================================================

/// Duration of process manager orchestration.
#[cfg(feature = "otel")]
pub static PM_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("angzarr.pm.duration")
        .with_description("Process manager orchestration duration")
        .with_unit("s")
        .build()
});

// ============================================================================
// Projector Metrics
// ============================================================================

/// Duration of projector event handling.
#[cfg(feature = "otel")]
pub static PROJECTOR_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("angzarr.projector.duration")
        .with_description("Projector event handling duration")
        .with_unit("s")
        .build()
});

// ============================================================================
// Dead Letter Queue Metrics
// ============================================================================

/// Total DLQ publish operations.
#[cfg(feature = "otel")]
pub static DLQ_PUBLISH_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("angzarr.dlq.publish.total")
        .with_description("Total DLQ publish operations")
        .build()
});

/// Duration of DLQ publish operations.
#[cfg(feature = "otel")]
pub static DLQ_PUBLISH_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("angzarr.dlq.publish.duration")
        .with_description("DLQ publish duration")
        .with_unit("s")
        .build()
});

// ============================================================================
// Attribute Helpers
// ============================================================================

/// Create a domain attribute.
#[cfg(feature = "otel")]
pub fn domain_attr(domain: &str) -> KeyValue {
    KeyValue::new("domain", domain.to_string())
}

/// Create an outcome attribute (success, failure, etc.).
#[cfg(feature = "otel")]
pub fn outcome_attr(outcome: &str) -> KeyValue {
    KeyValue::new("outcome", outcome.to_string())
}

/// Create a bus_type attribute (channel, amqp, kafka, etc.).
#[cfg(feature = "otel")]
pub fn bus_type_attr(bus_type: &str) -> KeyValue {
    KeyValue::new("bus_type", bus_type.to_string())
}

/// Create a component attribute (aggregate, projector, saga, process_manager).
#[cfg(feature = "otel")]
pub fn component_attr(component: &str) -> KeyValue {
    KeyValue::new("component", component.to_string())
}

/// Create a name attribute (specific component instance name).
#[cfg(feature = "otel")]
pub fn name_attr(name: &str) -> KeyValue {
    KeyValue::new("name", name.to_string())
}

/// Create an operation attribute (event_add, snapshot_get, etc.).
#[cfg(feature = "otel")]
pub fn operation_attr(operation: &str) -> KeyValue {
    KeyValue::new("operation", operation.to_string())
}

/// Create a storage_type attribute (sqlite, postgres, redis, etc.).
#[cfg(feature = "otel")]
pub fn storage_type_attr(storage_type: &str) -> KeyValue {
    KeyValue::new("storage_type", storage_type.to_string())
}

/// Create a namespace attribute (for snapshots).
#[cfg(feature = "otel")]
pub fn namespace_attr(namespace: &str) -> KeyValue {
    KeyValue::new("namespace", namespace.to_string())
}

/// Create a handler attribute (for position stores).
#[cfg(feature = "otel")]
pub fn handler_attr(handler: &str) -> KeyValue {
    KeyValue::new("handler", handler.to_string())
}

/// Create a reason_type attribute for DLQ entries.
#[cfg(feature = "otel")]
pub fn reason_type_attr(reason_type: &str) -> KeyValue {
    KeyValue::new("reason_type", reason_type.to_string())
}

/// Create a backend attribute for DLQ entries.
#[cfg(feature = "otel")]
pub fn backend_attr(backend: &str) -> KeyValue {
    KeyValue::new("backend", backend.to_string())
}

// ============================================================================
// Operation Constants
// ============================================================================

/// Operation: add events to store.
pub const OP_EVENT_ADD: &str = "event_add";
/// Operation: get events by root.
pub const OP_EVENT_GET: &str = "event_get";
/// Operation: get events from sequence.
pub const OP_EVENT_GET_FROM: &str = "event_get_from";
/// Operation: get events in sequence range.
pub const OP_EVENT_GET_FROM_TO: &str = "event_get_from_to";
/// Operation: get events for an edition branched at an explicit divergence.
pub const OP_EVENT_GET_WITH_DIVERGENCE: &str = "event_get_with_divergence";
/// Operation: list aggregate roots.
pub const OP_EVENT_LIST_ROOTS: &str = "event_list_roots";
/// Operation: list domains.
pub const OP_EVENT_LIST_DOMAINS: &str = "event_list_domains";
/// Operation: get next sequence number.
pub const OP_EVENT_GET_NEXT_SEQUENCE: &str = "event_get_next_sequence";
/// Operation: get events by correlation ID.
pub const OP_EVENT_GET_BY_CORRELATION: &str = "event_get_by_correlation";
/// Operation: get events until timestamp.
pub const OP_EVENT_GET_UNTIL_TIMESTAMP: &str = "event_get_until_timestamp";
/// Operation: delete edition events.
pub const OP_EVENT_DELETE_EDITION: &str = "event_delete_edition";
/// Operation: get snapshot.
pub const OP_SNAPSHOT_GET: &str = "snapshot_get";
/// Operation: get snapshot at sequence.
pub const OP_SNAPSHOT_GET_AT_SEQ: &str = "snapshot_get_at_seq";
/// Operation: put snapshot.
pub const OP_SNAPSHOT_PUT: &str = "snapshot_put";
/// Operation: delete snapshot.
pub const OP_SNAPSHOT_DELETE: &str = "snapshot_delete";
/// Operation: get position.
pub const OP_POSITION_GET: &str = "position_get";
/// Operation: put position.
pub const OP_POSITION_PUT: &str = "position_put";

/// Placeholder domain for correlation queries spanning multiple domains.
pub const DOMAIN_CORRELATION_QUERY: &str = "correlation_query";

#[cfg(test)]
#[path = "metrics.test.rs"]
mod tests;
