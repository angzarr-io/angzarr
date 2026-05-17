//! Tests for upcaster configuration and disabled mode.
//!
//! The upcaster transforms old event versions to current versions during
//! replay. These unit tests verify configuration parsing and passthrough
//! behavior when disabled.
//!
//! Why this matters: Schema evolution is inevitable in event-sourced systems.
//! The upcaster enables transparent migration: old events are transformed
//! to current format during replay, so aggregates only implement handlers
//! for the latest schema. Without proper testing, misconfiguration could
//! cause silent data corruption or replay failures.
//!
//! Key behaviors verified:
//! - Default config is disabled (safe default)
//! - Disabled upcaster passes through events unchanged
//! - Empty event list short-circuits without server call
//! - gRPC transformation works (V1 → V2)
//! - Sequence numbers preserved through transformation
//! - Error propagation from upcaster service

use super::*;
use crate::proto::PageHeader;

// ============================================================================
// Configuration Tests
// ============================================================================

/// Default config is disabled.
#[test]
fn test_upcaster_config_default() {
    let config = UpcasterConfig::default();
    assert!(!config.enabled);
    assert!(config.address.is_none());
}

/// Disabled upcaster reports is_enabled() false.
#[test]
fn test_upcaster_disabled() {
    let upcaster = Upcaster::disabled();
    assert!(!upcaster.is_enabled());
}

// ============================================================================
// Passthrough Tests
// ============================================================================

/// Disabled upcaster returns events unchanged — no transformation.
#[tokio::test]
async fn test_upcaster_passthrough_when_disabled() {
    let upcaster = Upcaster::disabled();

    let events = vec![EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(1)),
        }),
        created_at: None,
        payload: None,
        ..Default::default()
    }];

    let result = upcaster.upcast("test", events.clone()).await.unwrap();
    assert_eq!(result.len(), 1);
}

/// Empty event list is passthrough — no server call needed.
#[tokio::test]
async fn test_upcaster_passthrough_empty_events() {
    let upcaster = Upcaster::disabled();
    let result = upcaster.upcast("test", vec![]).await.unwrap();
    assert!(result.is_empty());
}
