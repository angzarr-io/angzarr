//! The errors' English and the catalog.

use angzarr_diagnostic::Diagnostic;
use angzarr_ports::{Arg as PortArg, Cause, StoreError};

angzarr_presentation::english_for!(&[crate::EN_US.1]);

use super::{diagnostic_samples, Failure, OpenError, TornTail};

fn arg_names(d: &dyn Diagnostic) -> Vec<&str> {
    d.args().into_iter().map(|(name, _)| name).collect()
}

#[test]
fn test_a_journal_in_use_says_so_and_what_to_do() {
    let e = OpenError::InUse {
        path: "/h/data/store.jsonl".to_string(),
    };
    assert_eq!(
        e.english(),
        "the journal /h/data/store.jsonl is held by another store: a journal has one store at a time, in one process; close that store first, or use a store that several processes can share"
    );
    assert_eq!(e.code(), angzarr_diagnostic::code!("in-use"));
    assert_eq!(arg_names(&e), ["path"]);
}

#[test]
fn test_a_corrupt_line_names_the_line_and_leaves_the_journal() {
    let e = OpenError::Corrupt {
        path: "/j".to_string(),
        line: 7,
        reason: "why".to_string(),
    };
    assert_eq!(
        e.english(),
        "line 7 of the journal /j is not an entry of this format (why); the journal is left as it is: repair or restore it"
    );
    assert_eq!(arg_names(&e), ["path", "line", "reason"]);
}

#[test]
fn test_an_unsupported_format_names_both_formats() {
    let e = OpenError::UnsupportedFormat {
        path: "/j".to_string(),
        found: 3,
        supported: 2,
    };
    assert_eq!(
        e.english(),
        "the journal /j is in format 3; this build reads formats up to 2"
    );
    assert_eq!(arg_names(&e), ["path", "found", "supported"]);
}

#[test]
fn test_a_torn_tail_notice_says_what_was_removed() {
    let notice = TornTail {
        path: "/j".into(),
        line: 31,
        bytes: 118,
    };
    assert_eq!(
        notice.english(),
        "the last line of the journal /j (line 31, 118 bytes) was cut short, so it never completed; it was removed and nothing before it was touched"
    );
    assert_eq!(
        notice.code(),
        angzarr_diagnostic::code!("torn-tail-truncated")
    );
    assert_eq!(arg_names(&notice), ["path", "line", "bytes"]);
}

#[test]
fn test_each_failure_is_the_stores_cause_with_its_code_and_args() {
    let append = Failure::AppendFailed {
        ext_err_txt: "disk full".to_string(),
    };
    assert_eq!(
        append.clone().into_store_error(),
        StoreError::Unavailable(Cause::new(
            angzarr_diagnostic::code!("append-failed"),
            [("ext_err_txt", PortArg::Str("disk full".to_string()))]
        ))
    );
    assert_eq!(
        Failure::Poisoned.into_store_error(),
        StoreError::Unavailable(Cause::new(angzarr_diagnostic::code!("poisoned"), []))
    );
    assert_eq!(
        Failure::Suspended.into_store_error(),
        StoreError::Unavailable(Cause::new(angzarr_diagnostic::code!("suspended"), []))
    );
    assert_eq!(
        Failure::EncodeFailed {
            ext_err_txt: "x".to_string()
        }
        .into_store_error(),
        StoreError::Unavailable(Cause::new(
            angzarr_diagnostic::code!("encode-failed"),
            [("ext_err_txt", PortArg::Str("x".to_string()))]
        ))
    );
    assert_eq!(
        append.english(),
        "the operation could not be appended to the journal, so whether it is there is unknown: disk full; the store answers nothing more: open the journal again to see what it holds"
    );
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
