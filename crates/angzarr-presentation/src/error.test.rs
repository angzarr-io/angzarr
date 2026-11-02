//! The errors' English, and the catalog.

use angzarr_diagnostic::Diagnostic;

crate::english_for!(&[crate::EN_US.1]);

use super::diagnostic_samples;
use crate::Catalog;

fn arg_names(d: &dyn Diagnostic) -> Vec<&str> {
    d.args().into_iter().map(|(name, _)| name).collect()
}

#[test]
fn test_each_error_says_what_failed_in_its_catalog_english() {
    let catalog = Catalog::en_us(&[]).expect("the catalog loads");
    let english: Vec<String> = diagnostic_samples().iter().map(|d| d.english()).collect();
    assert_eq!(
        english,
        [
            "the format yaml is not one of text or json",
            "the tasks message catalog cannot be loaded: Expected a value",
            "no catalog has a message for x-busy",
            "the message for x-busy cannot be filled in: unknown variable: $attempts",
        ]
    );
    for sample in diagnostic_samples() {
        let message = catalog.render(&*sample).expect("a catalog message");
        assert!(!message.is_empty(), "{}", sample.code());
        assert!(
            sample.to_string().starts_with(sample.code()),
            "Display is the key and args, never English: {sample}"
        );
    }
}

#[test]
fn test_every_error_has_a_catalog_message_with_exactly_its_args() {
    use fluent_syntax::ast::{Entry, Expression, InlineExpression, PatternElement};

    let resource = fluent_syntax::parser::parse(super::EN_US.1).expect("the catalog parses");
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

/// The crate offers its catalogs by locale under the same name, built from
/// `locales/` with no list of its own: en-US alone, the same text as
/// `EN_US`.
#[test]
fn test_its_locales_are_every_catalog_under_locales_en_us_alone() {
    let (name, table) = crate::LOCALES;
    assert_eq!(name, crate::EN_US.0);
    assert_eq!(table, &[("en-US", crate::EN_US.1)]);
}
