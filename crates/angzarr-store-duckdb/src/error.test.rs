use angzarr_ports::angzarr_diagnostic::code;
use angzarr_ports::{Arg, StoreError};

use super::{causes, errors, held, opening, unavailable, DuckError};

/// An error's `Display` is its code, then its args (` name=value` each),
/// never English: text is rendered only where it is shown, from the
/// catalog. A third party's text (DuckDB's) is carried verbatim as
/// `ext_err_txt`, quoted where it would not parse back.
#[test]
fn an_error_displays_its_code_and_args_never_english() {
    let failed = DuckError::Unavailable {
        detail: "disk I/O error".to_string(),
    }
    .to_string();
    assert_eq!(
        failed,
        concat!(code!("unavailable"), " ext_err_txt=\"disk I/O error\"")
    );
    assert!(!failed.contains("the database failed"), "{failed}");
    assert_eq!(
        DuckError::BadRoot { length: 3 }.to_string(),
        concat!(code!("bad-root"), " length=3")
    );
    assert_eq!(
        DuckError::NewerFormat {
            found: 9,
            supported: 1
        }
        .to_string(),
        concat!(code!("newer-format"), " found=9 supported=1")
    );
}

/// Every error has a code of its own under the crate's name, and the
/// causes the catalog test is given are one per error: a code reused for
/// two meanings, or an error left out of the list, would leave a message
/// unchecked against its args.
#[test]
fn every_error_has_its_own_code_and_a_cause() {
    let all = errors();
    let mut codes: Vec<&str> = all.iter().map(DuckError::code).collect();
    assert!(codes
        .iter()
        .all(|code| code.starts_with("angzarr-store-duckdb-")));
    codes.sort_unstable();
    codes.dedup();
    assert_eq!(codes.len(), all.len());
    assert_eq!(causes().len(), all.len());
    for (error, cause) in all.iter().zip(causes()) {
        assert_eq!(cause.code, error.code());
        assert_eq!(cause.args, error.args());
    }
}

/// The args of each error are what its message names, by name and kind.
#[test]
fn an_errors_args_are_named_for_its_message() {
    let place = DuckError::BadDeadLetterPlace { kind: 1, length: 4 };
    assert_eq!(
        place.args(),
        vec![("kind", Arg::Uint(1)), ("length", Arg::Uint(4))]
    );
    let text = |detail: &str| vec![("ext_err_txt", Arg::Str(detail.to_string()))];
    for error in [
        DuckError::Unavailable { detail: "d".into() },
        DuckError::HeldElsewhere { detail: "d".into() },
        DuckError::Undecodable { detail: "d".into() },
        DuckError::NotAStore { detail: "d".into() },
    ] {
        assert_eq!(error.args(), text("d"), "{error}");
    }
}

/// A lock another owner holds is told from the database failing, so a
/// caller can say "already open" and not "broken": DuckDB's wording
/// of it, when another process holds the file.
#[test]
fn a_held_lock_is_told_from_a_failure() {
    assert!(held(
        "IO Error: Could not set lock on file \"x\": Conflicting lock"
    ));
    assert!(!held("IO Error: disk full"));
}

/// An open's failure keeps DuckDB's text and the cause that fits it.
#[test]
fn an_open_failure_carries_its_cause() {
    let failure = duckdb::Error::DuckDBFailure(
        duckdb::ffi::Error::new(1),
        Some("Could not set lock on file".to_string()),
    );
    let StoreError::Unavailable(cause) = opening(failure) else {
        panic!("an open that fails is unavailable");
    };
    assert_eq!(cause.code, code!("held-elsewhere"));
    let other = duckdb::Error::DuckDBFailure(duckdb::ffi::Error::new(1), Some("disk full".into()));
    let StoreError::Unavailable(cause) = unavailable(other) else {
        panic!("a failure is unavailable");
    };
    assert_eq!(cause.code, code!("unavailable"));
}
