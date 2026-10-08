//! Tests for connection-string redaction.

use super::*;

/// The password never reaches the log; user and host stay for debugging.
#[test]
fn redacts_password_keeps_user_and_host() {
    assert_eq!(
        redact_uri("postgres://angzarr:s3cr3t@db:5432/angzarr"),
        "postgres://angzarr:***@db:5432/angzarr"
    );
    assert_eq!(
        redact_uri("amqp://guest:guest@rabbit:5672/%2f"),
        "amqp://guest:***@rabbit:5672/%2f"
    );
}

/// A password containing `@` is still fully hidden (the last `@` ends the
/// userinfo).
#[test]
fn redacts_password_containing_at_sign() {
    assert_eq!(
        redact_uri("redis://u:p@ss@cache:6379"),
        "redis://u:***@cache:6379"
    );
}

/// An `@` after the authority (path/query) is not userinfo.
#[test]
fn ignores_at_sign_outside_authority() {
    assert_eq!(redact_uri("sqlite://data/x@y.db"), "sqlite://data/x@y.db");
    assert_eq!(
        redact_uri("postgres://db/angzarr?user=a@b"),
        "postgres://db/angzarr?user=a@b"
    );
}

/// No credentials, nothing to redact.
#[test]
fn leaves_uris_without_password_unchanged() {
    assert_eq!(redact_uri("amqp://rabbit:5672"), "amqp://rabbit:5672");
    assert_eq!(redact_uri("postgres://user@db/x"), "postgres://user@db/x");
    assert_eq!(redact_uri("sqlite::memory:"), "sqlite::memory:");
}
