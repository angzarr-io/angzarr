use angzarr_diagnostic::{Arg, Report};
use angzarr_proto::v1::AngzarrReason;

use super::*;

fn args<const N: usize>(pairs: [(&str, &str); N]) -> Vec<(String, String)> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

/// A refusal is a status, a key and an argument map, and this crate links
/// no tonic to say so (`tests/dependencies.rs` pins the closure).
#[test]
fn coded_has_key_args_and_code_without_tonic() {
    let coded = Coded::new(
        Code::Unavailable,
        "ANGZARR_REASON_UPCASTER_UNAVAILABLE",
        args([("domain", "player"), ("cause", "annals-io")]),
    );
    assert_eq!(coded.code, Code::Unavailable);
    assert_eq!(coded.key, "ANGZARR_REASON_UPCASTER_UNAVAILABLE");
    assert_eq!(coded.args.get("domain").map(String::as_str), Some("player"));
    assert_eq!(
        coded.args.get("cause").map(String::as_str),
        Some("annals-io")
    );
    assert_eq!(coded.args.len(), 2);
    // The status message IS the key (decision 14.37).
    assert_eq!(coded.status_message(), coded.key);
    // A Coded is a Refusal of itself.
    assert_eq!(Refusal::coded(&coded), coded);
}

/// A spec key goes out under the status its `(reason)` option declares
/// (decision 14.65), for EVERY key: the oracle is the generated surface.
#[test]
fn a_spec_key_takes_the_status_its_option_declares() {
    let mut seen = 0;
    for number in 0..200 {
        let Ok(reason) = AngzarrReason::try_from(number) else {
            continue;
        };
        seen += 1;
        let coded = Coded::spec(reason, args([("k", "v")]));
        assert_eq!(coded.key, reason.as_str_name());
        assert_eq!(coded.args.get("k").map(String::as_str), Some("v"));
        let expected = match reason.surface() {
            Some(angzarr_proto::ReasonSurface::GrpcStatus(status)) => {
                Code::try_from(status).expect("a key's status is one of the fifteen")
            }
            Some(angzarr_proto::ReasonSurface::Readiness) | None => Code::Internal,
        };
        assert_eq!(coded.code, expected, "{}", reason.as_str_name());
    }
    assert!(seen > 40, "every key of the enum is walked: {seen}");
}

/// Independent of the generated surface: a few keys, each under the status
/// reasons.proto's comment gives it.
#[test]
fn spec_keys_spot_checked_against_the_spec() {
    let status = |reason| Coded::spec(reason, []).code;
    assert_eq!(
        status(AngzarrReason::UpcasterUnavailable),
        Code::Unavailable
    );
    assert_eq!(status(AngzarrReason::StoreUnavailable), Code::Unavailable);
    assert_eq!(
        status(AngzarrReason::EditionNotForked),
        Code::FailedPrecondition
    );
    assert_eq!(status(AngzarrReason::PersistedEventCorrupt), Code::DataLoss);
    assert_eq!(
        status(AngzarrReason::NoComponentOfKind),
        Code::Unimplemented
    );
    assert_eq!(
        status(AngzarrReason::SyncNoProjectors),
        Code::InvalidArgument
    );
    // A readiness key never travels as a status: INTERNAL, the bug of
    // answering it.
    assert_eq!(status(AngzarrReason::MissingRulesVersion), Code::Internal);
    // The zero value is never answered either.
    assert_eq!(status(AngzarrReason::Unspecified), Code::Internal);
}

/// A diagnostic's args flatten as angzarr carries them: by name, a wrapped
/// cause as its code under the name and each of its args under
/// `<name>.<arg>`, at any depth.
#[test]
fn a_diagnostics_args_flatten_with_a_nested_cause() {
    let root = Report::new("annals-io").arg("errno", 5u64);
    let middle = Report::new("annals-wrapped")
        .arg("path", "a b")
        .arg("cause", Arg::nested(&root));
    let top = Report::new("angzarr-x-top")
        .arg("count", 3u64)
        .arg("delta", -4i64)
        .arg("name", "player")
        .arg("cause", Arg::nested(&middle));
    assert_eq!(
        flat_args(&top),
        args([
            ("count", "3"),
            ("delta", "-4"),
            ("name", "player"),
            ("cause", "annals-wrapped"),
            ("cause.path", "a b"),
            ("cause.cause", "annals-io"),
            ("cause.cause.errno", "5"),
        ])
    );
    assert_eq!(flat_args(&Report::new("angzarr-x-bare")), []);
}

#[test]
fn a_diagnostic_is_a_coded_under_its_own_code() {
    let report = Report::new("angzarr-x-top").arg("count", 3u64).arg(
        "cause",
        Arg::nested(&Report::new("annals-io").arg("errno", 5u64)),
    );
    let coded = Coded::from_diagnostic(Code::Aborted, &report);
    assert_eq!(coded.code, Code::Aborted);
    assert_eq!(coded.key, "angzarr-x-top");
    assert_eq!(
        coded.args.into_iter().collect::<Vec<_>>(),
        args([("cause", "annals-io"), ("cause.errno", "5"), ("count", "3")])
    );
}

/// What a log shows: the key, then its args, ` name=value` each, in name
/// order; never English.
#[test]
fn a_coded_displays_as_its_key_then_its_args() {
    let coded = Coded::new(Code::Internal, "K", args([("b", "2"), ("a", "1")]));
    assert_eq!(coded.to_string(), "K a=1 b=2");
    assert_eq!(Coded::new(Code::Internal, "K", []).to_string(), "K");
}

/// On the wire a Coded is types.proto's `Diagnostic`: its key as the code
/// and its args by name.
#[test]
fn a_coded_is_the_wire_diagnostic() {
    let coded = Coded::new(Code::Internal, "K", args([("a", "1"), ("b", "2")]));
    let wire = angzarr_proto::v1::Diagnostic::from(&coded);
    assert_eq!(wire.code, "K");
    assert_eq!(wire.args.len(), 2);
    assert_eq!(wire.args.get("a").map(String::as_str), Some("1"));
    assert_eq!(wire.args.get("b").map(String::as_str), Some("2"));
}
