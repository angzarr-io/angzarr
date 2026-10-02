//! Tests for gRPC correlation ID utilities.
//!
//! The `correlated_request` function wraps tonic requests with correlation ID
//! metadata. This enables trace correlation across service boundaries.
//!
//! Why this matters: Without correlation IDs in gRPC metadata, distributed
//! traces cannot be correlated across services. The server-side tower
//! middleware extracts this header to create linked spans.

use super::*;
use crate::proto_ext::constants::CORRELATION_ID_HEADER;

/// correlated_request with valid ID inserts header.
///
/// The primary use case: saga/PM passes correlation ID from event,
/// which should appear in gRPC metadata for the outbound command.
#[test]
fn test_correlated_request_with_valid_id() {
    let correlation_id = "test-correlation-123";
    let request = correlated_request("test message", correlation_id);

    let header_value = request
        .metadata()
        .get(CORRELATION_ID_HEADER)
        .expect("should have correlation header");

    assert_eq!(header_value.to_str().unwrap(), correlation_id);
}

/// correlated_request with empty ID skips header.
///
/// Commands without correlation IDs (e.g., user-initiated actions without
/// existing trace context) should not insert an empty header.
#[test]
fn test_correlated_request_with_empty_id_skips_header() {
    let request = correlated_request("test message", "");

    let header_value = request.metadata().get(CORRELATION_ID_HEADER);

    assert!(header_value.is_none(), "empty ID should not insert header");
}

/// correlated_request preserves the message.
///
/// The wrapped message should be accessible unchanged.
#[test]
fn test_correlated_request_preserves_message() {
    let message = "the payload";
    let request = correlated_request(message, "some-id");

    assert_eq!(*request.get_ref(), message);
}

/// correlated_request with special characters in ID.
///
/// Correlation IDs may contain various characters (UUIDs, custom formats).
/// Valid ASCII values should work.
#[test]
fn test_correlated_request_with_uuid_format() {
    let uuid_id = "550e8400-e29b-41d4-a716-446655440000";
    let request = correlated_request("test", uuid_id);

    let header_value = request
        .metadata()
        .get(CORRELATION_ID_HEADER)
        .expect("should have header");

    assert_eq!(header_value.to_str().unwrap(), uuid_id);
}

/// correlated_request with alphanumeric + dash + underscore.
///
/// Common ID formats should all parse correctly.
#[test]
fn test_correlated_request_with_common_id_formats() {
    // Test various common ID formats
    for id in &["abc123", "order-12345", "order_12345", "ABC-123-xyz"] {
        let request = correlated_request("test", id);
        let header = request.metadata().get(CORRELATION_ID_HEADER);
        assert!(header.is_some(), "ID '{}' should be valid", id);
    }
}

// ============================================================================
// OTel Feature Tests
// ============================================================================

/// correlated_request with otel feature enabled injects trace context.
///
/// When the `otel` feature is enabled, `correlated_request` also calls
/// `inject_trace_context` to add W3C trace headers to the gRPC metadata.
/// Without an active OTel span, no traceparent header is injected.
#[test]
#[cfg(feature = "otel")]
fn test_correlated_request_calls_inject_trace_context() {
    // Without an active OTel span, traceparent won't be present
    // This test verifies the code path compiles and doesn't panic
    let request = correlated_request("test message", "test-correlation-id");

    // Correlation ID should still be set regardless of otel
    let corr_header = request.metadata().get(CORRELATION_ID_HEADER);
    assert!(corr_header.is_some());

    // Without OTel context, traceparent should NOT be present
    // (would require active span with OTel runtime initialized)
    let traceparent = request.metadata().get("traceparent");
    assert!(
        traceparent.is_none(),
        "Without active OTel span, traceparent should not be present"
    );
}

/// correlated_request doesn't panic without OTel runtime.
///
/// Even when the otel feature is enabled, the request should be created
/// successfully even if no OTel runtime (exporter, propagator) is configured.
#[test]
#[cfg(feature = "otel")]
fn test_correlated_request_graceful_without_otel_runtime() {
    // This simulates the common case: otel feature is compiled in but
    // the application hasn't configured an OTel runtime yet
    let request = correlated_request("payload", "corr-123");

    // Request should be created without panic
    assert_eq!(*request.get_ref(), "payload");
}

// ----- StatusExt::error_info_reason ------------------------------------------

/// A status whose details are a google.rpc.Status carrying an ErrorInfo.
fn status_with_error_info(reason: &str, type_url: &str) -> tonic::Status {
    use prost::Message;
    let info = ErrorInfo {
        reason: reason.to_string(),
        domain: "inventory".to_string(),
        metadata: Default::default(),
    };
    let details = RpcStatus {
        code: tonic::Code::FailedPrecondition as i32,
        message: "out of stock".to_string(),
        details: vec![
            prost_types::Any {
                type_url: "type.googleapis.com/google.rpc.DebugInfo".to_string(),
                value: vec![],
            },
            prost_types::Any {
                type_url: type_url.to_string(),
                value: info.encode_to_vec(),
            },
        ],
    };
    tonic::Status::with_details(
        tonic::Code::FailedPrecondition,
        "out of stock",
        details.encode_to_vec().into(),
    )
}

/// The machine code is ErrorInfo.reason from the status details, whatever
/// the type URL prefix (C-0505).
#[test]
fn error_info_reason_is_read_from_status_details() {
    let status = status_with_error_info("OUT_OF_STOCK", "type.googleapis.com/google.rpc.ErrorInfo");
    assert_eq!(status.error_info_reason(), "OUT_OF_STOCK");
    let bare = status_with_error_info("CARD_DECLINED", "/google.rpc.ErrorInfo");
    assert_eq!(bare.error_info_reason(), "CARD_DECLINED");
}

/// No ErrorInfo — no details, undecodable details, or details of another
/// type — yields an empty code (C-0506).
#[test]
fn error_info_reason_is_empty_without_error_info() {
    assert_eq!(
        tonic::Status::failed_precondition("x").error_info_reason(),
        ""
    );
    let garbage = tonic::Status::with_details(tonic::Code::Aborted, "x", vec![0xff, 0x01].into());
    assert_eq!(garbage.error_info_reason(), "");
    let other = status_with_error_info("NOPE", "type.googleapis.com/google.rpc.ErrorInfoX");
    assert_eq!(other.error_info_reason(), "");
}
