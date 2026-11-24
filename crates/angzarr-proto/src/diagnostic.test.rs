use super::*;

/// A transport's flat metadata and a dead-letter line say one thing for
/// every arg type: numbers as decimal text, a cause as its code with its
/// own args under `<name>.<arg>`, however deep, an argless cause as its
/// code alone.
#[test]
fn flat_args_flattens_every_arg_type_and_nested_causes_recursively() {
    let deep = Diagnostic::with_text_args("deep", [("why".to_string(), "x".to_string())]);
    let mid = Diagnostic {
        code: "mid".into(),
        typed_args: vec![
            DiagnosticArg::nested("root", deep),
            DiagnosticArg::nested("bare", Diagnostic::with_text_args("bare-code", [])),
        ],
    };
    let d = Diagnostic {
        code: "top".into(),
        typed_args: vec![
            DiagnosticArg::text("name", "n"),
            DiagnosticArg {
                name: "delta".into(),
                value: Some(Value::Int(-3)),
            },
            DiagnosticArg {
                name: "count".into(),
                value: Some(Value::Uint(u64::MAX)),
            },
            DiagnosticArg::nested("cause", mid),
            DiagnosticArg {
                name: "empty".into(),
                value: None,
            },
        ],
    };
    assert_eq!(
        d.flat_args(),
        [
            ("name", "n"),
            ("delta", "-3"),
            ("count", "18446744073709551615"),
            ("cause", "mid"),
            ("cause.root", "deep"),
            ("cause.root.why", "x"),
            ("cause.bare", "bare-code"),
        ]
        .map(|(k, v)| (k.to_string(), v.to_string()))
    );
}
