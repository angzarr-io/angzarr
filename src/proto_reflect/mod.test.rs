//! Tests for protobuf reflection utilities.
//!
//! Proto reflection enables runtime inspection of Any-packed messages:
//! - Type URL parsing extracts message type from "type.googleapis.com/pkg.Type"
//! - Field diffing identifies changed fields between message versions
//! - Disjoint field detection enables commutative merge optimization
//!
//! Why this matters:
//! - State diff: Detect conflicting concurrent updates (optimistic locking)
//! - Logging: Human-readable event/state representation
//! - Debug tooling: Inspect Any-packed messages without static type knowledge
//!
//! Key behaviors verified:
//! - Type URL parsing handles various prefix formats
//! - Field disjointness correctly identifies non-overlapping changes
//! - Map fields use keyed paths like "field[key]" for granular diff
//!
//! Note: Full diff_fields tests require integration tests with real
//! descriptor sets. Unit tests cover parsing and set operations.

use super::*;

// ============================================================================
// Type URL Parsing Tests
// ============================================================================

/// Extract type name from googleapis.com format.
///
/// Standard protobuf Any type URL format.
#[test]
fn test_extract_type_name_googleapis() {
    let type_url = "type.googleapis.com/examples.PlayerState";
    let result = extract_type_name(type_url).unwrap();
    assert_eq!(result, "examples.PlayerState");
}

/// Extract type name from angzarr.io format.
///
/// Custom type URLs used for angzarr-specific messages.
#[test]
fn test_extract_type_name_angzarr() {
    use crate::proto_ext::type_url;
    let result = extract_type_name(type_url::SAGA_COMPENSATION_FAILED).unwrap();
    assert_eq!(result, "angzarr.SagaCompensationFailed");
}

/// Edge case: bare type name without prefix still works.
///
/// Handles malformed or simplified type URLs gracefully.
#[test]
fn test_extract_type_name_just_name() {
    // Edge case: no prefix
    let type_url = "examples.PlayerState";
    let result = extract_type_name(type_url).unwrap();
    assert_eq!(result, "examples.PlayerState");
}

// ============================================================================
// Field Disjointness Tests
// ============================================================================
//
// Disjoint fields enable commutative merge: if two concurrent updates
// touch different fields, they can be applied in any order.

/// Empty field sets are trivially disjoint.
#[test]
fn test_fields_are_disjoint_empty() {
    let a: HashSet<String> = HashSet::new();
    let b: HashSet<String> = HashSet::new();
    assert!(fields_are_disjoint(&a, &b));
}

/// Different scalar fields are disjoint (can merge).
///
/// Example: One update changes "bankroll", another changes "name".
/// No conflict; both can be applied.
#[test]
fn test_fields_are_disjoint_different_fields() {
    let a: HashSet<String> = ["bankroll".to_string()].into_iter().collect();
    let b: HashSet<String> = ["name".to_string()].into_iter().collect();
    assert!(fields_are_disjoint(&a, &b));
}

/// Same field in both sets → conflict (cannot merge).
///
/// Example: Both updates change "bankroll". Last-write-wins or reject.
#[test]
fn test_fields_are_disjoint_same_field() {
    let a: HashSet<String> = ["bankroll".to_string()].into_iter().collect();
    let b: HashSet<String> = ["bankroll".to_string()].into_iter().collect();
    assert!(!fields_are_disjoint(&a, &b));
}

/// Different keys in same map → disjoint (key-level granularity).
///
/// Map fields track changes per-key: seats[1] and seats[2] don't conflict.
#[test]
fn test_fields_are_disjoint_keyed_different_keys() {
    // Different keys in same map → disjoint
    let a: HashSet<String> = ["seats[1]".to_string()].into_iter().collect();
    let b: HashSet<String> = ["seats[2]".to_string()].into_iter().collect();
    assert!(fields_are_disjoint(&a, &b));
}

/// Same key in same map → conflict.
///
/// Both updates modify seats[1]; conflict detected.
#[test]
fn test_fields_are_disjoint_keyed_same_key() {
    // Same key → overlap
    let a: HashSet<String> = ["seats[1]".to_string()].into_iter().collect();
    let b: HashSet<String> = ["seats[1]".to_string()].into_iter().collect();
    assert!(!fields_are_disjoint(&a, &b));
}

/// Mixed scalar and keyed fields: all different → disjoint.
#[test]
fn test_fields_are_disjoint_mixed() {
    let a: HashSet<String> = ["bankroll".to_string(), "seats[1]".to_string()]
        .into_iter()
        .collect();
    let b: HashSet<String> = ["name".to_string(), "seats[2]".to_string()]
        .into_iter()
        .collect();
    assert!(fields_are_disjoint(&a, &b));
}

/// Mixed scalar and keyed fields: one overlap → conflict.
#[test]
fn test_fields_are_disjoint_mixed_overlap() {
    let a: HashSet<String> = ["bankroll".to_string(), "seats[1]".to_string()]
        .into_iter()
        .collect();
    let b: HashSet<String> = ["seats[1]".to_string(), "name".to_string()]
        .into_iter()
        .collect();
    assert!(!fields_are_disjoint(&a, &b));
}

// ============================================================================
// Map Key Formatting Tests
// ============================================================================

/// String map keys format as-is.
#[test]
fn test_format_map_key_string() {
    use prost_reflect::MapKey;
    let key = MapKey::String("table_a".to_string());
    assert_eq!(format_map_key(&key), "table_a");
}

/// Integer map keys format as decimal strings.
#[test]
fn test_format_map_key_i32() {
    use prost_reflect::MapKey;
    let key = MapKey::I32(42);
    assert_eq!(format_map_key(&key), "42");
}

/// Unsigned 64-bit map keys format as decimal strings.
#[test]
fn test_format_map_key_u64() {
    use prost_reflect::MapKey;
    let key = MapKey::U64(123456);
    assert_eq!(format_map_key(&key), "123456");
}

// ============================================================================
// Pool Initialization Tests
// ============================================================================
//
// Note: These tests verify error types, not actual initialization.
// Global static makes initialization tests unreliable in parallel test runs.
// Full reflection tests belong in integration tests with descriptor sets.

/// NotInitialized error has correct message.
#[test]
fn test_pool_not_initialized_error() {
    // In a fresh test process where pool isn't initialized,
    // this would return NotInitialized. However, other tests
    // may have initialized it. We test the error type exists.
    let err = ReflectError::NotInitialized;
    assert_eq!(err.to_string(), errmsg::NOT_INITIALIZED);
}

/// AlreadyInitialized error has correct message.
#[test]
fn test_already_initialized_error() {
    let err = ReflectError::AlreadyInitialized;
    assert_eq!(err.to_string(), errmsg::ALREADY_INITIALIZED);
}

/// UnknownType error includes the type name.
///
/// Diagnostic: Helps identify which proto type is missing from descriptors.
#[test]
fn test_unknown_type_error() {
    let err = ReflectError::UnknownType("foo.Bar".to_string());
    assert_eq!(err.to_string(), format!("{}foo.Bar", errmsg::UNKNOWN_TYPE));
}

// ============================================================================
// decode_to_json tolerance tests
// ============================================================================
//
// WHY: payload rendering for the DLQ admin surface (and later the
// GraphQL gateway) calls `decode_to_json` per row. Failure modes —
// pool not initialized, unknown type, bad bytes — must produce an
// empty string (the agreed-upon "I couldn't decode" signal), NOT
// panic, NOT crash the response. The plan's resilience contract
// pins this behavior; these tests pin it in code.

/// Unknown type returns empty string even when the pool IS initialized.
/// Catches the easy bug where we accidentally surface a panic from
/// `decode` on a missing descriptor lookup.
#[test]
fn decode_to_json_unknown_type_returns_empty() {
    let _ = ensure_initialized();
    let result = decode_to_json("definitely.not.a.real.Type", b"\x08\x42");
    assert_eq!(result, "");
}

/// Bad bytes against a known type return empty string.
#[test]
fn decode_to_json_garbage_bytes_returns_empty() {
    let _ = ensure_initialized();
    let result = decode_to_json(
        "angzarr_client.proto.angzarr.AngzarrDeadLetter",
        &[0xff, 0xff, 0xff, 0xff],
    );
    assert_eq!(result, "");
}

/// Round-trip: encode an `AngzarrDeadLetter`, decode_to_json returns
/// a non-empty JSON string containing the expected fields.
#[test]
fn decode_to_json_roundtrip_angzarr_dead_letter() {
    use prost::Message;
    let _ = ensure_initialized();
    let dl = crate::proto::AngzarrDeadLetter {
        cover: Some(crate::proto::Cover {
            domain: "player".to_string(),
            root: None,
            correlation_id: "trace-xyz".to_string(),
            edition: None,
        }),
        rejection_reason: "test failure".to_string(),
        source_component: "agg-player".to_string(),
        source_component_type: "aggregate".to_string(),
        ..Default::default()
    };
    let bytes = dl.encode_to_vec();
    let json = decode_to_json("angzarr_client.proto.angzarr.AngzarrDeadLetter", &bytes);

    assert!(!json.is_empty(), "json must be non-empty on happy path");
    // Field-name spot checks against the proto3 JSON encoding.
    assert!(
        json.contains("\"player\""),
        "decoded JSON should contain domain value: {}",
        json
    );
    assert!(
        json.contains("\"trace-xyz\""),
        "decoded JSON should contain correlation_id: {}",
        json
    );
    assert!(
        json.contains("\"test failure\""),
        "decoded JSON should contain rejection_reason: {}",
        json
    );
}

/// `ensure_initialized` is idempotent — repeat calls succeed.
#[test]
fn ensure_initialized_is_idempotent() {
    let r1 = ensure_initialized();
    let r2 = ensure_initialized();
    let r3 = ensure_initialized();
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert!(r3.is_ok());
}

// ============================================================================
// Any-in-hand path (decode_any_to_json) — reusable across the framework
// ============================================================================
//
// The DLQ admin handler decodes a typed AngzarrDeadLetter via
// decode_to_json. The event-store browser, GraphQL gateway, and
// future projection viewers will work with bare `Any` payloads
// (EventPage.event, CommandPage.command, etc.) — these tests pin
// the Any-in-hand entry point that the same primitive serves.

/// Happy path: encode a known framework message in an Any, decode
/// back to JSON. Catches a regression in the type_url → DescriptorPool
/// lookup chain (which is what makes the reusable surface work).
#[test]
fn decode_any_to_json_roundtrip_known_type() {
    use prost::Message;
    use prost_types::Any;

    let _ = ensure_initialized();
    let cover = crate::proto::Cover {
        domain: "any-roundtrip".to_string(),
        root: None,
        correlation_id: "trace-cover".to_string(),
        edition: None,
    };
    let any = Any {
        type_url: "type.googleapis.com/angzarr_client.proto.angzarr.Cover".to_string(),
        value: cover.encode_to_vec(),
    };
    let json = decode_any_to_json(&any);
    assert!(!json.is_empty(), "Any decode should produce JSON: {}", json);
    assert!(
        json.contains("any-roundtrip"),
        "JSON should contain cover.domain: {}",
        json
    );
}

/// Unknown type_url returns empty string (tolerance contract for
/// the Any-in-hand path).
#[test]
fn decode_any_to_json_unknown_type_returns_empty() {
    use prost_types::Any;
    let _ = ensure_initialized();
    let any = Any {
        type_url: "type.googleapis.com/never.heard.of.It".to_string(),
        value: vec![0x08, 0x42],
    };
    assert_eq!(decode_any_to_json(&any), "");
}

/// Bad bytes against a real type_url return empty string.
#[test]
fn decode_any_to_json_garbage_bytes_returns_empty() {
    use prost_types::Any;
    let _ = ensure_initialized();
    let any = Any {
        type_url: "type.googleapis.com/angzarr_client.proto.angzarr.Cover".to_string(),
        value: vec![0xff; 16],
    };
    assert_eq!(decode_any_to_json(&any), "");
}

/// Symmetry: encode the same message, decode via either entry point —
/// outputs match. Confirms the bytes-path and Any-path are not
/// silently drifting from each other.
#[test]
fn decode_any_to_json_matches_decode_to_json() {
    use prost::Message;
    use prost_types::Any;

    let _ = ensure_initialized();
    let cover = crate::proto::Cover {
        domain: "symmetry-test".to_string(),
        root: None,
        correlation_id: "trace-sym".to_string(),
        edition: None,
    };
    let bytes = cover.encode_to_vec();
    let from_bytes = decode_to_json("angzarr_client.proto.angzarr.Cover", &bytes);
    let from_any = decode_any_to_json(&Any {
        type_url: "type.googleapis.com/angzarr_client.proto.angzarr.Cover".to_string(),
        value: bytes,
    });
    assert!(!from_bytes.is_empty());
    assert_eq!(from_bytes, from_any);
}

// ============================================================================
// Integration Test Scaffolding
// ============================================================================
//
// Full diff_fields tests require:
// 1. Generated descriptor.bin from protoc
// 2. Test proto messages with known field structures
//
// These will be added as integration tests in tests/standalone_integration/state_diff.rs
