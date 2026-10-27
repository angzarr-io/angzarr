//! Tests for OTel metric definitions and attribute helpers.
//!
//! The metrics module provides centralized OpenTelemetry metric definitions
//! for the entire advice module. Attribute helpers create consistent KeyValue
//! pairs for metric labeling.
//!
//! Why this matters: Consistent metric labeling enables proper aggregation
//! and querying in observability backends. These helpers ensure all components
//! use the same attribute names and formats.
//!
//! Tests are feature-gated: only compiled when `otel` feature is enabled.

// ============================================================================
// Attribute Helper Tests
// ============================================================================

/// domain_attr creates a KeyValue with "domain" key.
///
/// Domain labels enable per-domain metric aggregation (e.g., orders vs inventory).
#[test]
#[cfg(feature = "otel")]
fn test_domain_attr_creates_keyvalue() {
    use super::domain_attr;

    let kv = domain_attr("orders");
    assert_eq!(kv.key.as_str(), "domain");
    assert_eq!(kv.value.as_str(), "orders");
}

/// outcome_attr creates a KeyValue with "outcome" key.
///
/// Outcome labels distinguish success/failure metrics.
#[test]
#[cfg(feature = "otel")]
fn test_outcome_attr_creates_keyvalue() {
    use super::outcome_attr;

    let kv = outcome_attr("success");
    assert_eq!(kv.key.as_str(), "outcome");
    assert_eq!(kv.value.as_str(), "success");
}

/// bus_type_attr creates a KeyValue with "bus_type" key.
///
/// Bus type labels distinguish metrics by transport (channel, amqp, kafka).
#[test]
#[cfg(feature = "otel")]
fn test_bus_type_attr_creates_keyvalue() {
    use super::bus_type_attr;

    let kv = bus_type_attr("kafka");
    assert_eq!(kv.key.as_str(), "bus_type");
    assert_eq!(kv.value.as_str(), "kafka");
}

/// component_attr creates a KeyValue with "component" key.
///
/// Component labels identify the type (aggregate, saga, projector, pm).
#[test]
#[cfg(feature = "otel")]
fn test_component_attr_creates_keyvalue() {
    use super::component_attr;

    let kv = component_attr("saga");
    assert_eq!(kv.key.as_str(), "component");
    assert_eq!(kv.value.as_str(), "saga");
}

/// name_attr creates a KeyValue with "name" key.
///
/// Name labels identify specific component instances.
#[test]
#[cfg(feature = "otel")]
fn test_name_attr_creates_keyvalue() {
    use super::name_attr;

    let kv = name_attr("saga-order-fulfillment");
    assert_eq!(kv.key.as_str(), "name");
    assert_eq!(kv.value.as_str(), "saga-order-fulfillment");
}

/// operation_attr creates a KeyValue with "operation" key.
///
/// Operation labels distinguish storage operations (event_add, snapshot_get).
#[test]
#[cfg(feature = "otel")]
fn test_operation_attr_creates_keyvalue() {
    use super::operation_attr;
    use super::OP_EVENT_ADD;

    let kv = operation_attr(OP_EVENT_ADD);
    assert_eq!(kv.key.as_str(), "operation");
    assert_eq!(kv.value.as_str(), "event_add");
}

/// storage_type_attr creates a KeyValue with "storage_type" key.
///
/// Storage type labels distinguish backend implementations (sqlite, postgres).
#[test]
#[cfg(feature = "otel")]
fn test_storage_type_attr_creates_keyvalue() {
    use super::storage_type_attr;

    let kv = storage_type_attr("postgres");
    assert_eq!(kv.key.as_str(), "storage_type");
    assert_eq!(kv.value.as_str(), "postgres");
}

/// namespace_attr creates a KeyValue with "namespace" key.
///
/// Namespace labels are used for snapshot stores.
#[test]
#[cfg(feature = "otel")]
fn test_namespace_attr_creates_keyvalue() {
    use super::namespace_attr;

    let kv = namespace_attr("aggregate-state");
    assert_eq!(kv.key.as_str(), "namespace");
    assert_eq!(kv.value.as_str(), "aggregate-state");
}

/// handler_attr creates a KeyValue with "handler" key.
///
/// Handler labels identify position store handlers.
#[test]
#[cfg(feature = "otel")]
fn test_handler_attr_creates_keyvalue() {
    use super::handler_attr;

    let kv = handler_attr("projector-inventory-stock");
    assert_eq!(kv.key.as_str(), "handler");
    assert_eq!(kv.value.as_str(), "projector-inventory-stock");
}

/// reason_type_attr creates a KeyValue with "reason_type" key.
///
/// Reason type labels categorize DLQ entries.
#[test]
#[cfg(feature = "otel")]
fn test_reason_type_attr_creates_keyvalue() {
    use super::reason_type_attr;

    let kv = reason_type_attr("validation_failed");
    assert_eq!(kv.key.as_str(), "reason_type");
    assert_eq!(kv.value.as_str(), "validation_failed");
}

/// backend_attr creates a KeyValue with "backend" key.
///
/// Backend labels identify DLQ backend implementations.
#[test]
#[cfg(feature = "otel")]
fn test_backend_attr_creates_keyvalue() {
    use super::backend_attr;

    let kv = backend_attr("kafka");
    assert_eq!(kv.key.as_str(), "backend");
    assert_eq!(kv.value.as_str(), "kafka");
}

// ============================================================================
// Operation Constant Tests
// ============================================================================

/// Operation constants have expected values.
///
/// Ensures metric queries using these constants match actual metric labels.
#[test]
fn test_operation_constants() {
    use super::*;

    assert_eq!(OP_EVENT_ADD, "event_add");
    assert_eq!(OP_EVENT_GET, "event_get");
    assert_eq!(OP_EVENT_GET_FROM, "event_get_from");
    assert_eq!(OP_EVENT_GET_FROM_TO, "event_get_from_to");
    assert_eq!(OP_EVENT_LIST_ROOTS, "event_list_roots");
    assert_eq!(OP_EVENT_LIST_DOMAINS, "event_list_domains");
    assert_eq!(OP_EVENT_GET_NEXT_SEQUENCE, "event_get_next_sequence");
    assert_eq!(OP_EVENT_GET_BY_CORRELATION, "event_get_by_correlation");
    assert_eq!(OP_EVENT_GET_UNTIL_TIMESTAMP, "event_get_until_timestamp");
    assert_eq!(OP_EVENT_DELETE_EDITION, "event_delete_edition");
    assert_eq!(OP_SNAPSHOT_GET, "snapshot_get");
    assert_eq!(OP_SNAPSHOT_GET_AT_SEQ, "snapshot_get_at_seq");
    assert_eq!(OP_SNAPSHOT_PUT, "snapshot_put");
    assert_eq!(OP_SNAPSHOT_DELETE, "snapshot_delete");
    assert_eq!(OP_POSITION_GET, "position_get");
    assert_eq!(OP_POSITION_PUT, "position_put");
}

/// DOMAIN_CORRELATION_QUERY constant has expected value.
///
/// This placeholder domain is used for correlation queries spanning multiple domains.
#[test]
fn test_domain_correlation_query_constant() {
    use super::DOMAIN_CORRELATION_QUERY;

    assert_eq!(DOMAIN_CORRELATION_QUERY, "correlation_query");
}

// ============================================================================
// Static Metric Initialization Tests (feature-gated)
// ============================================================================

/// Static metrics are lazily initialized without panic.
///
/// Accessing the lazy statics should trigger initialization and not crash.
/// Note: These don't test actual metric recording (requires OTel runtime).
#[test]
#[cfg(feature = "otel")]
fn test_static_metrics_initialize() {
    use super::*;

    // Accessing these statics forces initialization
    // They should not panic even without an OTel runtime configured
    let _ = &*STORAGE_DURATION;
    let _ = &*EVENTS_STORED_TOTAL;
    let _ = &*EVENTS_LOADED_TOTAL;
    let _ = &*SNAPSHOTS_STORED_TOTAL;
    let _ = &*SNAPSHOTS_LOADED_TOTAL;
    let _ = &*POSITIONS_UPDATED_TOTAL;
    let _ = &*BUS_PUBLISH_DURATION;
    let _ = &*BUS_PUBLISH_TOTAL;
    let _ = &*COMMAND_DURATION;
    let _ = &*COMMAND_TOTAL;
    let _ = &*SAGA_DURATION;
    let _ = &*SAGA_RETRY_TOTAL;
    let _ = &*PM_DURATION;
    let _ = &*PROJECTOR_DURATION;
    let _ = &*DLQ_PUBLISH_TOTAL;
    let _ = &*DLQ_PUBLISH_DURATION;
}
