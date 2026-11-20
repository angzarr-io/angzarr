use angzarr_coordinator_base::{Code, Refusal};
use angzarr_diagnostic::Diagnostic;

use super::*;

/// An operation the endpoint does not take is UNIMPLEMENTED under the
/// edge's own key, with the operation as its arg.
#[test]
fn an_unimplemented_operation_is_keyed_with_its_name() {
    let coded = Unimplemented {
        operation: "deliver",
    }
    .coded();
    assert_eq!(coded.code, Code::Unimplemented);
    assert_eq!(coded.key, "angzarr-coordinator-unimplemented");
    assert_eq!(
        coded.args.get("operation").map(String::as_str),
        Some("deliver")
    );
    assert_eq!(coded.args.len(), 1);
    assert_eq!(coded.coded(), coded);
}

/// A bad credential has its own code and names nothing.
#[test]
fn unauthenticated_is_its_own_code_and_names_nothing() {
    assert_eq!(
        Unauthenticated.code(),
        "angzarr-coordinator-unauthenticated"
    );
    assert!(Unauthenticated.args().is_empty());
    assert_eq!(
        Unauthenticated.to_string(),
        "angzarr-coordinator-unauthenticated"
    );
}

/// A bad credential is UNAUTHENTICATED, and its coded form names nothing.
#[test]
fn unauthenticated_is_coded_unauthenticated() {
    let coded = Unauthenticated.coded();
    assert_eq!(coded.code, Code::Unauthenticated);
    assert_eq!(coded.key, "angzarr-coordinator-unauthenticated");
    assert!(coded.args.is_empty());
}

/// A client calling a framework-only operation is PERMISSION_DENIED, keyed,
/// with the operation.
#[test]
fn framework_only_is_permission_denied_with_its_operation() {
    let coded = FrameworkOnly {
        operation: "deliver",
    }
    .coded();
    assert_eq!(coded.code, Code::PermissionDenied);
    assert_eq!(coded.key, "angzarr-coordinator-framework-only");
    assert_eq!(coded.args.len(), 1);
    assert_eq!(
        coded.args.get("operation").map(String::as_str),
        Some("deliver")
    );
}
