//! The error's English and the catalog.

use angzarr_diagnostic::{Arg, Diagnostic};

angzarr_presentation::english_for!(&[crate::EN_US.1]);

use std::io;
use std::path::PathBuf;

use super::{diagnostic_samples, CwdError, TargetExists};

fn arg_names(d: &dyn Diagnostic) -> Vec<&str> {
    d.args().into_iter().map(|(name, _)| name).collect()
}

#[test]
fn test_a_cwd_error_says_why_the_working_directory_cannot_be_read() {
    let error = CwdError {
        reason: "gone".to_string(),
    };
    assert_eq!(
        error.english(),
        "the working directory cannot be read: gone"
    );
    assert_eq!(error.code(), angzarr_diagnostic::code!("no-cwd"));
    assert!(
        matches!(&error.args()[..], [("reason", Arg::Str(r))] if r == "gone"),
        "{error:?}"
    );
}

#[test]
fn test_a_target_that_exists_names_it_and_says_it_is_left() {
    let error = TargetExists {
        path: PathBuf::from("/work/to"),
    };
    assert_eq!(
        error.english(),
        "/work/to already exists, and is left as it is"
    );
    assert_eq!(error.code(), angzarr_diagnostic::code!("target-exists"));
    assert!(
        matches!(&error.args()[..], [("path", Arg::Str(p))] if p == "/work/to"),
        "{error:?}"
    );
}

#[test]
fn test_an_already_exists_becomes_the_target_and_nothing_else_does() {
    let to = PathBuf::from("/work/to");
    let exists = TargetExists::at(io::ErrorKind::AlreadyExists.into(), &to);
    assert_eq!(exists.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(
        TargetExists::of(&exists),
        Some(&TargetExists { path: to.clone() })
    );
    let missing = TargetExists::at(io::ErrorKind::NotFound.into(), &to);
    assert_eq!(missing.kind(), io::ErrorKind::NotFound);
    assert_eq!(TargetExists::of(&missing), None);
}

#[test]
fn test_an_error_carrying_something_else_is_no_target() {
    let other = io::Error::new(io::ErrorKind::AlreadyExists, "elsewhere");
    assert_eq!(TargetExists::of(&other), None);
    assert_eq!(TargetExists::of(&io::ErrorKind::AlreadyExists.into()), None);
}

#[test]
fn test_every_error_has_a_catalog_message_with_exactly_its_args() {
    use fluent_syntax::ast::{Entry, Expression, InlineExpression, PatternElement};

    let resource = fluent_syntax::parser::parse(crate::EN_US.1).expect("the catalog parses");
    let samples = diagnostic_samples();
    let mut catalog: Vec<&str> = resource
        .body
        .iter()
        .filter_map(|e| match e {
            Entry::Message(m) => Some(m.id.name),
            _ => None,
        })
        .collect();
    catalog.sort_unstable();
    let mut sampled: Vec<&str> = samples.iter().map(|s| s.code()).collect();
    sampled.sort_unstable();
    assert_eq!(catalog, sampled, "catalog messages and samples");
    for sample in samples {
        // Display is the key and args, never English (decision 14.37).
        assert_eq!(
            sample.to_string(),
            angzarr_diagnostic::Report::of(&*sample).to_string(),
            "{}",
            sample.code()
        );
        let message = resource
            .body
            .iter()
            .find_map(|e| match e {
                Entry::Message(m) if m.id.name == sample.code() => Some(m),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no message for {}", sample.code()));
        let mut used: Vec<&str> = message
            .value
            .iter()
            .flat_map(|p| &p.elements)
            .filter_map(|el| match el {
                PatternElement::Placeable {
                    expression: Expression::Inline(InlineExpression::VariableReference { id }),
                } => Some(id.name),
                _ => None,
            })
            .collect();
        used.sort_unstable();
        used.dedup();
        let mut args = arg_names(&*sample);
        args.sort_unstable();
        assert_eq!(used, args, "{}", sample.code());
    }
}

/// The catalog is exported under the crate's package name, and every code
/// is under that name, with its message using exactly its args.
#[test]
fn test_every_code_is_under_the_crates_name_with_its_message() {
    let (name, text) = crate::EN_US;
    assert_eq!(name, env!("CARGO_PKG_NAME"));
    assert_eq!(
        angzarr_diagnostic::catalog::check(name, text, &diagnostic_samples()),
        []
    );
}
