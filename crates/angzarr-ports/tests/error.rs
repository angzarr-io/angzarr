//! One error, stable codes (RULED(14) in contract.rs). The codes, args,
//! recovery, its display (the code and args, never English: decision
//! 14.37), and its size.

use std::collections::BTreeSet;

use angzarr_ports::angzarr_proto::v1::{self, Cover};
use angzarr_ports::{Arg, Cause, Recovery, StoreError};

fn stream() -> Cover {
    Cover {
        domain: "account".to_string(),
        root: Some(v1::Uuid::from_bytes([7; 16])),
        edition: None,
    }
}

fn other() -> Cover {
    Cover {
        root: Some(v1::Uuid::from_bytes([8; 16])),
        ..stream()
    }
}

fn stream_in(edition: &str) -> Cover {
    Cover {
        edition: Some(edition.to_string()),
        ..stream()
    }
}

/// An adapter's cause: its own code and its driver's errno as an arg.
fn io() -> Cause {
    Cause::new("annals-store-sql-io", [("errno", Arg::Uint(10))])
}

/// What a log shows for `code` and `args`, by the rule (decision 14.37):
/// the code, then ` name=value` per arg in order; a nested cause as
/// ` name=<its code>`, then its args as ` name.<arg>=value`. A text value
/// that would not parse back (empty, or holding whitespace, `=`, `"` or
/// `\`) is double-quoted, its `"` and `\` escaped with a backslash.
fn rendered(code: &str, args: &[(&'static str, Arg)]) -> String {
    fn text(s: &str) -> String {
        let ambiguous = s.is_empty()
            || s.chars()
                .any(|c| c.is_whitespace() || matches!(c, '=' | '"' | '\\'));
        if !ambiguous {
            return s.to_string();
        }
        let escaped: String = s
            .chars()
            .flat_map(|c| match c {
                '"' | '\\' => vec!['\\', c],
                c => vec![c],
            })
            .collect();
        format!("\"{escaped}\"")
    }
    fn push(out: &mut String, prefix: &str, args: &[(&'static str, Arg)]) {
        for (name, arg) in args {
            let name = format!("{prefix}{name}");
            match arg {
                Arg::Uint(n) => out.push_str(&format!(" {name}={n}")),
                Arg::Str(s) => out.push_str(&format!(" {name}={}", text(s))),
                Arg::Cause(cause) => {
                    out.push_str(&format!(" {name}={}", cause.code));
                    push(out, &format!("{name}."), &cause.args);
                }
            }
        }
    }
    let mut out = code.to_string();
    push(&mut out, "", args);
    out
}

/// One of every variant, with its pinned code and recovery.
fn every() -> Vec<(StoreError, &'static str, Recovery)> {
    use Recovery::*;
    let name = || "what-if".to_string();
    vec![
        (
            StoreError::conflict(stream(), 1, 2),
            "angzarr-ports-store-conflict",
            Retry,
        ),
        (
            StoreError::discontinuous(stream(), 2, 3),
            "angzarr-ports-store-discontinuous",
            Bug,
        ),
        (
            StoreError::malformed_stream(stream()),
            "angzarr-ports-store-malformed-stream",
            Invalid,
        ),
        (
            StoreError::malformed_creation(stream_in("what-if"), 3, 5),
            "angzarr-ports-store-malformed-creation",
            Invalid,
        ),
        (
            StoreError::Unavailable(io()),
            "angzarr-ports-store-unavailable",
            Retry,
        ),
        (
            StoreError::unreadable(stream(), 4, io()),
            "angzarr-ports-store-unreadable",
            Bug,
        ),
        (
            StoreError::unknown_edition(stream()),
            "angzarr-ports-store-unknown-edition",
            Invalid,
        ),
        (
            StoreError::EditionNameRequired,
            "angzarr-ports-edition-name-required",
            Invalid,
        ),
        (
            StoreError::EditionNotFound { name: name() },
            "angzarr-ports-edition-not-found",
            Invalid,
        ),
        (
            StoreError::edition_not_deleted(stream_in("what-if")),
            "angzarr-ports-store-edition-not-deleted",
            Bug,
        ),
        (
            StoreError::edition_not_propagated(stream_in("what-if")),
            "angzarr-ports-store-edition-not-propagated",
            Retry,
        ),
        (
            StoreError::edition_not_forked(stream_in("what-if")),
            "angzarr-ports-store-edition-not-forked",
            Invalid,
        ),
        (
            StoreError::edition_already_forked(stream_in("what-if")),
            "angzarr-ports-store-edition-already-forked",
            Invalid,
        ),
    ]
}

/// A code is stable: these exact strings, kebab-case, prefixed with the
/// crate's name, each used once. A code whose meaning changes gets a new one.
#[test]
fn every_error_has_its_stable_code() {
    let mut seen = BTreeSet::new();
    for (error, code, _) in every() {
        assert_eq!(error.code(), code, "{error:?}");
        assert!(code.starts_with("angzarr-ports-"), "{code}");
        assert!(
            code.chars().all(|c| c.is_ascii_lowercase() || c == '-'),
            "{code} is kebab-case"
        );
        assert!(seen.insert(code), "{code} used twice");
    }
}

/// A conflict and an outage are retried; a gap and an unreadable event
/// are bugs; a bad name or stream is the caller's to fix.
#[test]
fn every_error_says_how_to_recover() {
    for (error, _, recovery) in every() {
        assert_eq!(error.recovery(), recovery, "{error:?}");
    }
}

/// Args carry data, never English: the numbers as numbers, the stream as
/// its text form, a cause nested whole.
#[test]
fn args_carry_the_data_by_name() {
    let conflict = StoreError::conflict(stream(), 1, 2);
    assert_eq!(
        conflict.args(),
        vec![
            (
                "stream",
                Arg::Str("account/07070707-0707-0707-0707-070707070707".to_string())
            ),
            ("expected", Arg::Uint(1)),
            ("actual", Arg::Uint(2)),
        ]
    );
    assert_eq!(
        StoreError::Unavailable(io()).args(),
        vec![("cause", Arg::Cause(io()))]
    );
    assert_eq!(StoreError::EditionNameRequired.args(), vec![]);
}

/// 14.37: a store error's display is its code, then its args, never
/// English: what a log shows is the key and the map, and text is rendered
/// only where it is displayed, from a catalog keyed by the code.
#[test]
fn the_display_is_the_code_then_its_args() {
    assert_eq!(
        StoreError::conflict(stream(), 1, 2).to_string(),
        "angzarr-ports-store-conflict stream=account/07070707-0707-0707-0707-070707070707 expected=1 actual=2"
    );
    assert_eq!(
        StoreError::EditionNameRequired.to_string(),
        "angzarr-ports-edition-name-required"
    );
}

/// 14.37: no store error carries rendered text: every variant displays
/// exactly its code and args, so nothing but the key and the map reaches a
/// log or crosses a boundary.
#[test]
fn no_store_error_carries_rendered_text() {
    for (error, code, _) in every() {
        assert_eq!(
            error.to_string(),
            rendered(code, &error.args()),
            "{error:?}"
        );
    }
}

/// 14.37: a cause is a code and args, never its driver's text, and a
/// wrapped one is displayed flattened under its arg's name.
#[test]
fn a_cause_is_a_code_and_args_and_displays_flattened() {
    let cause = io();
    assert_eq!(cause.code, "annals-store-sql-io");
    assert_eq!(cause.args, vec![("errno", Arg::Uint(10))]);
    assert_eq!(
        StoreError::Unavailable(io()).to_string(),
        "angzarr-ports-store-unavailable cause=annals-store-sql-io cause.errno=10"
    );
    assert_eq!(
        StoreError::unreadable(stream(), 4, io()).to_string(),
        "angzarr-ports-store-unreadable stream=account/07070707-0707-0707-0707-070707070707 sequence=4 cause=annals-store-sql-io cause.errno=10"
    );
}

/// A text value that would not parse back is quoted (decision 14.37's
/// display rule): one empty, or holding whitespace, `=`, `"` or `\`, is
/// written double-quoted, its `"` and `\` escaped with a backslash; any
/// other value stays bare.
#[test]
fn a_value_that_would_not_parse_back_is_quoted() {
    let text = |s: &str| Arg::Str(s.to_string());
    let cause = Cause::new(
        "annals-store-sql-io",
        [
            ("ext_err_txt", text("database is locked")),
            ("empty", text("")),
            ("pair", text("k=v")),
            ("said", text("say \"hi\"")),
            ("path", text("C:\\dir")),
            ("tab", text("a\tb")),
            ("bare", text("account/7-x_y.z")),
        ],
    );
    assert_eq!(
        StoreError::Unavailable(cause).to_string(),
        "angzarr-ports-store-unavailable cause=annals-store-sql-io \
         cause.ext_err_txt=\"database is locked\" cause.empty=\"\" cause.pair=\"k=v\" \
         cause.said=\"say \\\"hi\\\"\" cause.path=\"C:\\\\dir\" cause.tab=\"a\tb\" \
         cause.bare=account/7-x_y.z"
    );
}

/// The editions' refusals carry their streams, as args and so in the
/// display.
#[test]
fn the_new_refusals_name_their_streams() {
    let root = |n: u8| format!("{n:02x}").repeat(4);
    let text = |n: u8| {
        let r = root(n);
        format!(
            "{}-{}-{}-{}-{}",
            r,
            &r[..4],
            &r[..4],
            &r[..4],
            r.clone() + &r[..4]
        )
    };
    let s = format!("account/{}", text(7));
    let e = format!("{s}@what-if");
    for refusal in [
        StoreError::edition_not_forked(stream_in("what-if")),
        StoreError::edition_already_forked(stream_in("what-if")),
        StoreError::edition_not_deleted(stream_in("what-if")),
        StoreError::edition_not_propagated(stream_in("what-if")),
    ] {
        assert_eq!(refusal.args(), vec![("stream", Arg::Str(e.clone()))]);
        assert!(refusal.to_string().contains(&e), "{refusal}");
    }
}

/// The streams are boxed (owner ruling, 2026-10-28) so the error stays
/// small: every `Result<_, StoreError>` carries it. The bound is the boxed
/// layout's: the largest variant is `Unreadable` (a box, a sequence and a
/// `Cause` of a `&'static str` and a `Vec` of args: 8 + 8 + 16 + 24 = 56
/// bytes),
/// whose niche holds the discriminant, so 56; 64 leaves a word to spare and
/// stops it silently growing back.
#[test]
fn the_error_stays_small() {
    assert!(
        std::mem::size_of::<StoreError>() <= 64,
        "StoreError is {} bytes",
        std::mem::size_of::<StoreError>()
    );
}

/// The constructors and the accessor hide the box.
#[test]
fn the_stream_is_read_without_the_box() {
    assert_eq!(
        StoreError::conflict(stream(), 1, 2).stream(),
        Some(&stream())
    );
    assert_eq!(
        StoreError::edition_not_propagated(other()).stream(),
        Some(&other())
    );
    assert_eq!(StoreError::EditionNameRequired.stream(), None);
}
