//! Errors carry a key and typed args, never rendered English (decision
//! 14.37, amended 2026-11-24): `Diagnostic { code, typed_args }`, with its
//! numbers. A dead letter's why (a typed refusal, a reason or a
//! transport's why) is tested in `roundtrip.rs`; `PayloadRetrievalFailed`
//! is the distributed transport's (decision 14.64), tested in
//! angzarr-proto-transport-distributed.

use angzarr_proto::v1::diagnostic_arg::Value;
use angzarr_proto::v1::{Diagnostic, DiagnosticArg};
use prost::{Message, Name};

fn round_trip<M: Message + Default>(m: &M) -> M {
    M::decode(&*m.encode_to_vec()).expect("decodes")
}

fn arg(name: &str, value: Value) -> DiagnosticArg {
    DiagnosticArg {
        name: name.into(),
        value: Some(value),
    }
}

/// Every arg type, a nested cause with args and one with none.
fn typed() -> Diagnostic {
    Diagnostic {
        code: "angzarr-ports-store-unavailable".into(),
        typed_args: vec![
            arg("stream", Value::Text("inventory".into())),
            arg("errno", Value::Int(-10)),
            arg("attempts", Value::Uint(u64::MAX)),
            arg(
                "cause",
                Value::Nested(Diagnostic::with_text_args(
                    "annals-store-sql-io",
                    [("detail".to_string(), "refused".to_string())],
                )),
            ),
            arg(
                "bare",
                Value::Nested(Diagnostic {
                    code: "annals-store-closed".into(),
                    typed_args: vec![],
                }),
            ),
        ],
    }
}

#[test]
fn a_diagnostic_is_a_code_and_its_typed_args_in_fields_1_and_3() {
    assert_eq!(Diagnostic::full_name(), "io.angzarr.v1.Diagnostic");
    assert_eq!(DiagnosticArg::full_name(), "io.angzarr.v1.DiagnosticArg");
    let bytes = Diagnostic {
        code: "X".into(),
        typed_args: vec![arg("k", Value::Text("v".into()))],
    }
    .encode_to_vec();
    assert_eq!(
        bytes,
        [
            (1 << 3) | 2,
            1,
            b'X',
            (3 << 3) | 2,
            6, // name (1+1+1) and text (1+1+1)
            (1 << 3) | 2,
            1,
            b'k',
            (2 << 3) | 2,
            1,
            b'v'
        ]
    );
}

/// An arg keeps its type through the wire: a number is not text, and a
/// cause with no args of its own is still a cause (nested), not its code
/// as text.
#[test]
fn typed_args_round_trip_with_their_types_and_order() {
    let d = typed();
    let back = round_trip(&d);
    assert_eq!(back, d);
    let names: Vec<&str> = back.typed_args.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, ["stream", "errno", "attempts", "cause", "bare"]);
    assert!(matches!(
        &back.typed_args[4].value,
        Some(Value::Nested(cause)) if cause.code == "annals-store-closed" && cause.typed_args.is_empty()
    ));
}

/// One fact must have one byte form: event bytes are compared and hashed.
/// The args are a list in the diagnostic's own order, so the same value
/// always encodes to the same bytes.
#[test]
fn a_diagnostic_encodes_to_the_same_bytes_every_time() {
    assert_eq!(typed().encode_to_vec(), typed().encode_to_vec());
    assert_eq!(
        typed().encode_to_vec(),
        round_trip(&typed()).encode_to_vec()
    );
}

/// Old data carried `map<string, string> args` in field 2. That field is
/// retired and there is no legacy decode: a reader yields the code with no
/// args.
#[test]
fn old_map_bytes_decode_to_the_code_with_no_args() {
    let mut old = vec![(1 << 3) | 2, 3];
    old.extend_from_slice(b"old");
    // One map entry, field 2: key "k", value "v".
    old.extend_from_slice(&[
        (2 << 3) | 2,
        6,
        (1 << 3) | 2,
        1,
        b'k',
        (2 << 3) | 2,
        1,
        b'v',
    ]);
    let d = Diagnostic::decode(&*old).expect("old bytes decode");
    assert_eq!(d.code, "old");
    assert!(d.typed_args.is_empty());
}

#[test]
fn flat_args_are_the_text_pairs_a_flat_transport_carries() {
    assert_eq!(
        typed().flat_args(),
        [
            ("stream", "inventory"),
            ("errno", "-10"),
            ("attempts", "18446744073709551615"),
            ("cause", "annals-store-sql-io"),
            ("cause.detail", "refused"),
            ("bare", "annals-store-closed"),
        ]
        .map(|(k, v)| (k.to_string(), v.to_string()))
    );
}
