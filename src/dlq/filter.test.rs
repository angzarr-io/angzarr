//! Tests for the AIP-160 filter parser.
//!
//! WHY: the parser is the operator-facing surface for narrowing DLQ
//! queries. Drift in grammar (mistakenly accepting `OR`, silently
//! tolerating malformed timestamps, accepting unquoted values) would
//! return wrong-rows-with-no-warning. Pinning the grammar tight here
//! means a future contributor can't accidentally widen it without
//! also breaking tests.

use chrono::{TimeZone, Utc};

use super::*;

#[test]
fn empty_filter_returns_default_no_constraints() {
    let f = parse_filter("").unwrap();
    assert_eq!(f, ListFilter::default());
}

#[test]
fn whitespace_only_filter_returns_default() {
    let f = parse_filter("   \t\n  ").unwrap();
    assert_eq!(f, ListFilter::default());
}

#[test]
fn single_domain_clause() {
    let f = parse_filter(r#"domain = "player""#).unwrap();
    assert_eq!(f.domain.as_deref(), Some("player"));
    assert!(f.rejection_type.is_none());
}

#[test]
fn three_clauses_anded() {
    let f = parse_filter(
        r#"domain = "player" AND rejection_type = "sequence_mismatch" AND correlation_id = "corr-1""#,
    )
    .unwrap();
    assert_eq!(f.domain.as_deref(), Some("player"));
    assert_eq!(f.rejection_type.as_deref(), Some("sequence_mismatch"));
    assert_eq!(f.correlation_id.as_deref(), Some("corr-1"));
}

#[test]
fn and_is_case_insensitive() {
    // 'and' / 'And' / 'AND' all the same per the grammar — operators
    // should not have to remember caps.
    let f1 = parse_filter(r#"domain = "a" and source_component = "b""#).unwrap();
    let f2 = parse_filter(r#"domain = "a" And source_component = "b""#).unwrap();
    let f3 = parse_filter(r#"domain = "a" AND source_component = "b""#).unwrap();
    assert_eq!(f1, f2);
    assert_eq!(f2, f3);
}

#[test]
fn occurred_after_parses_rfc3339() {
    let f = parse_filter(r#"occurred_after = "2026-05-15T12:00:00Z""#).unwrap();
    assert_eq!(
        f.occurred_after,
        Some(Utc.with_ymd_and_hms(2026, 5, 15, 12, 0, 0).unwrap())
    );
}

#[test]
fn occurred_after_rejects_malformed_timestamp() {
    // Catches the easy bug where we accept "yesterday" or
    // "2026-05-15" (no time) and silently produce a default UTC.
    let err = parse_filter(r#"occurred_after = "not-a-date""#).unwrap_err();
    assert!(matches!(err, DlqError::InvalidArgument(_)));
}

#[test]
fn occurred_before_parses_rfc3339() {
    let f = parse_filter(r#"occurred_before = "2026-05-15T23:59:59Z""#).unwrap();
    assert_eq!(
        f.occurred_before,
        Some(Utc.with_ymd_and_hms(2026, 5, 15, 23, 59, 59).unwrap())
    );
}

#[test]
fn unknown_field_is_rejected() {
    // Strict whitelist — a typo like `domian = "x"` must error rather
    // than silently match nothing (or worse: match everything).
    let err = parse_filter(r#"domian = "x""#).unwrap_err();
    assert!(matches!(err, DlqError::InvalidArgument(_)));
    if let DlqError::InvalidArgument(msg) = err {
        assert!(
            msg.contains("domian"),
            "msg should name the bad field: {}",
            msg
        );
    }
}

#[test]
fn unquoted_value_is_rejected() {
    // Pins the contract that values must be double-quoted. Catches a
    // future "convenience" that accepts bare identifiers — operators
    // would have no consistent way to escape a value containing
    // spaces.
    let err = parse_filter(r#"domain = player"#).unwrap_err();
    assert!(matches!(err, DlqError::InvalidArgument(_)));
}

#[test]
fn unterminated_quote_is_rejected() {
    let err = parse_filter(r#"domain = "player"#).unwrap_err();
    assert!(matches!(err, DlqError::InvalidArgument(_)));
}

#[test]
fn missing_equals_is_rejected() {
    let err = parse_filter(r#"domain "player""#).unwrap_err();
    assert!(matches!(err, DlqError::InvalidArgument(_)));
}

#[test]
fn repeated_field_is_rejected() {
    // Last-wins would silently lose the operator's first constraint.
    // Pinning rejection here forces them to fix the ambiguous query.
    let err = parse_filter(r#"domain = "a" AND domain = "b""#).unwrap_err();
    assert!(matches!(err, DlqError::InvalidArgument(_)));
}

#[test]
fn trailing_and_is_rejected() {
    let err = parse_filter(r#"domain = "a" AND"#).unwrap_err();
    assert!(matches!(err, DlqError::InvalidArgument(_)));
}

#[test]
fn or_is_rejected_as_unsupported() {
    // Explicit forbid-list for v1 — future extension is safe because
    // current callers don't expect `OR` to work today.
    let err = parse_filter(r#"domain = "a" OR domain = "b""#).unwrap_err();
    assert!(matches!(err, DlqError::InvalidArgument(_)));
}

#[test]
fn andless_concatenation_is_rejected() {
    // No implicit AND — clauses MUST be joined with AND.
    let err = parse_filter(r#"domain = "a" rejection_type = "b""#).unwrap_err();
    assert!(matches!(err, DlqError::InvalidArgument(_)));
}

#[test]
fn page_size_and_token_are_not_touched_by_parser() {
    // Pagination lives on the request, not the filter string.
    // Confirming the parser doesn't accidentally set these.
    let f = parse_filter(r#"domain = "player""#).unwrap();
    assert_eq!(f.page_size, 0);
    assert!(f.page_token.is_none());
}

#[test]
fn extra_whitespace_around_tokens_is_tolerated() {
    // Operators may format for readability; the grammar tolerates
    // extra whitespace except inside quoted values.
    let f =
        parse_filter(r#"  domain   =   "player"   AND   source_component  =  "saga"  "#).unwrap();
    assert_eq!(f.domain.as_deref(), Some("player"));
    assert_eq!(f.source_component.as_deref(), Some("saga"));
}

#[test]
fn quoted_value_with_spaces_is_preserved() {
    // Values are read verbatim — a value with internal whitespace is
    // intentional (e.g. matching a free-form `details` if we ever
    // expose it).
    let f = parse_filter(r#"correlation_id = "trace 42 with spaces""#).unwrap();
    assert_eq!(f.correlation_id.as_deref(), Some("trace 42 with spaces"));
}
