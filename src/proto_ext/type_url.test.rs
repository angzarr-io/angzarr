//! Tests for type URL utilities.
//!
//! Type URLs identify protobuf message types in Any fields.
//! angzarr's canonical form is bare: "/{package}.{MessageType}".
//!
//! Key behaviors verified:
//! - for_type() builds correct canonical URLs
//! - fqn() extracts the FQN regardless of resolver prefix (recognition)

use super::*;

/// for_type() prepends the bare canonical prefix.
#[test]
fn test_for_type() {
    assert_eq!(
        for_type("io.angzarr.examples.v1.OrderCreated"),
        "/io.angzarr.examples.v1.OrderCreated"
    );
    // Round-trips with the framework constants.
    assert_eq!(for_type("io.angzarr.v1.Notification"), NOTIFICATION);
}

/// fqn() yields the absolute proto name regardless of resolver prefix — the
/// basis for recognizing inbound framework/client events.
#[test]
fn test_fqn_is_prefix_agnostic() {
    // angzarr's bare canonical form.
    assert_eq!(fqn(NOTIFICATION), "io.angzarr.v1.Notification");
    // Other-language Any.Pack() default.
    assert_eq!(
        fqn("type.googleapis.com/io.angzarr.v1.Compensate"),
        "io.angzarr.v1.Compensate"
    );
    // No `/` at all — whole string is the name.
    assert_eq!(
        fqn("io.angzarr.v1.Notification"),
        "io.angzarr.v1.Notification"
    );
}

/// The canonical constants are the bare form: a leading `/` then the FQN,
/// no resolver host and no `angzarr` org duplication.
#[test]
fn test_constants_are_bare_canonical() {
    for url in [
        NOTIFICATION,
        REJECTION_NOTIFICATION,
        SAGA_COMPENSATION_FAILED,
        COMPENSATE,
        COMMAND_BOOK,
    ] {
        assert!(
            url.starts_with("/io.angzarr.v1."),
            "{url} must be a bare canonical URL"
        );
        assert!(
            !url.contains("googleapis") && !url.contains("angzarr.io"),
            "{url} must not home in a resolver host"
        );
    }
}
