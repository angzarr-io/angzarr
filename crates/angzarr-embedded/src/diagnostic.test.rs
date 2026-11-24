use std::fmt;

use angzarr_proto::v1::diagnostic_arg::Value;
use coded_diagnostic::{Arg, Diagnostic, Report};
use prost::Message;

use super::{DiagnosticToWire, WireDiagnosticExt};

/// A hand-written error holding every kind of arg, so the conversion is
/// proved on the trait and not only on `Report`.
#[derive(Debug)]
struct Many;

impl fmt::Display for Many {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        coded_diagnostic::display(self, f)
    }
}

impl Diagnostic for Many {
    fn code(&self) -> &str {
        "angzarr-embedded-test-many"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![
            ("name", Arg::Str("n".into())),
            ("delta", Arg::Int(-4)),
            ("count", Arg::Uint(u64::MAX)),
            (
                "cause",
                Arg::Nested(Box::new(
                    Report::new("angzarr-embedded-test-cause").arg("errno", 10_u32),
                )),
            ),
            (
                "bare",
                Arg::Nested(Box::new(Report::new("angzarr-embedded-test-bare"))),
            ),
        ]
    }
}

/// Each arg keeps its type on the wire, in the diagnostic's own order: a
/// number is not text, and a cause with no args is still nested.
#[test]
fn a_diagnostics_args_are_typed_one_to_one_in_order() {
    let wire = Many.to_wire();
    assert_eq!(wire.code, "angzarr-embedded-test-many");
    let shape: Vec<(&str, &str)> = wire
        .typed_args
        .iter()
        .map(|a| {
            let kind = match a.value.as_ref().expect("a value") {
                Value::Text(_) => "text",
                Value::Int(_) => "int",
                Value::Uint(_) => "uint",
                Value::Nested(_) => "nested",
            };
            (a.name.as_str(), kind)
        })
        .collect();
    assert_eq!(
        shape,
        [
            ("name", "text"),
            ("delta", "int"),
            ("count", "uint"),
            ("cause", "nested"),
            ("bare", "nested"),
        ]
    );
}

/// The conversion works on a `&dyn Diagnostic`, as route and the dead
/// letters hold it.
#[test]
fn a_dyn_diagnostic_converts_to_the_wire() {
    let held: &dyn Diagnostic = &Many;
    assert_eq!(held.to_wire(), Many.to_wire());
}

/// Wire and back loses nothing: the report holds the same code, the same
/// typed args, the argless cause as a cause, across the bytes too.
#[test]
fn wire_and_back_is_the_same_report() {
    let report = Report::of(&Many);
    let wire = Many.to_wire();
    let over_the_wire = angzarr_proto::v1::Diagnostic::decode(&*wire.encode_to_vec()).unwrap();
    assert_eq!(over_the_wire.to_report(), report);
    assert_eq!(report.to_wire(), wire);
}

/// An arg whose value was never set has nothing to hold: it is dropped,
/// the rest kept.
#[test]
fn an_arg_with_no_value_is_dropped() {
    let mut wire = Many.to_wire();
    wire.typed_args[1].value = None;
    let names: Vec<String> = wire
        .to_report()
        .args()
        .iter()
        .map(|(n, _)| n.to_string())
        .collect();
    assert_eq!(names, ["name", "count", "cause", "bare"]);
}
