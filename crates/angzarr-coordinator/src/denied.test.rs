use angzarr_diagnostic::Diagnostic;

use super::*;

/// Every variant has the audit log's stable reason, its own code, and the
/// args it names.
#[test]
fn each_denial_has_its_reason_code_and_args() {
    let hidden = Hidden {
        domain: "player".to_string(),
        arrival: Arrival::Ip,
        reach: Reach::None,
    };
    let cases = [
        (
            Denied::NotExposed(hidden),
            "not_exposed",
            "angzarr-coordinator-denied-not-exposed",
            3,
        ),
        (
            Denied::TypeNotAllowed {
                type_url: "x/Y".to_string(),
            },
            "type_not_allowed",
            "angzarr-coordinator-denied-type-not-allowed",
            1,
        ),
        (
            Denied::BodyKindNotAllowed { kind: "fact" },
            "body_kind_not_allowed",
            "angzarr-coordinator-denied-body-kind-not-allowed",
            1,
        ),
        (
            Denied::NotOwner {
                domain: "d".to_string(),
            },
            "not_owner",
            "angzarr-coordinator-denied-not-owner",
            1,
        ),
        (
            Denied::SelectionNotAllowed {
                operation: "follow",
            },
            "selection_not_allowed",
            "angzarr-coordinator-denied-selection-not-allowed",
            1,
        ),
    ];
    for (denied, reason, code, args) in cases {
        assert_eq!(denied.reason(), reason);
        assert_eq!(denied.code(), code);
        assert_eq!(denied.args().len(), args, "{code}");
    }
}

/// A denial names what the request named and nothing of a credential.
#[test]
fn a_denial_names_the_request_and_no_credential() {
    let denied = Denied::NotExposed(Hidden {
        domain: "player".to_string(),
        arrival: Arrival::Ip,
        reach: Reach::Loopback,
    });
    let names: Vec<&str> = denied.args().iter().map(|(name, _)| *name).collect();
    assert_eq!(names, ["domain", "arrival", "reach"]);
    let owner = Denied::NotOwner {
        domain: "player".to_string(),
    };
    assert_eq!(owner.args()[0].0, "domain");
    assert!(!owner.to_string().to_lowercase().contains("token"));
}
