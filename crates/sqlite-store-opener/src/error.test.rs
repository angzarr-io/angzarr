//! The errors' codes and args, the catalog, and the error as the store's.

use angzarr_diagnostic::{code, Arg, Diagnostic};
use angzarr_ports::{self, StoreError};

use super::{diagnostic_samples, OpenError, EN_US};

/// The samples whose code this crate owns: the store's own error is rendered by the
/// store's catalog.
fn own() -> Vec<Box<dyn Diagnostic>> {
    diagnostic_samples()
        .into_iter()
        .filter(|d| d.code().starts_with(env!("CARGO_PKG_NAME")))
        .collect()
}

/// The catalog is exported under the crate's package name, and every code
/// is under that name, with its message using exactly its args.
#[test]
fn every_code_is_under_the_crates_name_with_its_message() {
    let (name, text) = EN_US;
    assert_eq!(name, "sqlite-store-opener");
    let own = own();
    assert_eq!(
        own.len(),
        4,
        "create-dir, unplaced, predates-embedded, restrict"
    );
    assert_eq!(angzarr_diagnostic::catalog::check(name, text, &own), []);
}

#[test]
fn each_error_displays_its_code_and_args_never_english() {
    let displayed: Vec<String> = diagnostic_samples().iter().map(|d| d.to_string()).collect();
    assert_eq!(
        displayed,
        [
            concat!(
                code!("create-dir"),
                r#" path=/home/u/.demo/projects/jolly-mute-magma ext_err_txt="permission denied""#
            ),
            concat!(
                code!("unplaced"),
                r#" path=/home/u/.demo/projects/jolly-mute-magma/store.db ext_err_txt="not an absolute path: C:store.db""#
            ),
            concat!(
                code!("predates-embedded"),
                " path=/home/u/.demo/projects/jolly-mute-magma/store.db \
                 backup=/home/u/.demo/projects/jolly-mute-magma/store.db.pre-embedded"
            ),
            concat!(
                code!("restrict"),
                r#" path=/home/u/.demo/projects/jolly-mute-magma/store.db-wal ext_err_txt="operation not permitted""#
            ),
            "angzarr-ports-store-unavailable cause=angzarr-store-sqlite-unavailable cause.ext_err_txt=locked",
        ]
        .map(str::to_string)
    );
}

#[test]
fn the_codes_are_stable() {
    assert_eq!(code!("create-dir"), "sqlite-store-opener-create-dir");
    assert_eq!(code!("unplaced"), "sqlite-store-opener-unplaced");
    assert_eq!(
        code!("predates-embedded"),
        "sqlite-store-opener-predates-embedded"
    );
    assert_eq!(code!("restrict"), "sqlite-store-opener-restrict");
}

#[test]
fn each_error_gives_its_args_values() {
    let error = OpenError::CreateDir {
        path: "p".to_string(),
        detail: "d".to_string(),
    };
    assert!(
        matches!(&error.args()[..], [("path", Arg::Str(p)), ("ext_err_txt", Arg::Str(d))] if p == "p" && d == "d"),
        "{error:?}"
    );
    let error = OpenError::Unplaced {
        path: "p".to_string(),
        detail: "d".to_string(),
    };
    assert!(
        matches!(&error.args()[..], [("path", Arg::Str(p)), ("ext_err_txt", Arg::Str(d))] if p == "p" && d == "d"),
        "{error:?}"
    );
    let error = OpenError::PredatesEmbedded {
        path: "p".to_string(),
        backup: "b".to_string(),
    };
    assert!(
        matches!(&error.args()[..], [("path", Arg::Str(p)), ("backup", Arg::Str(b))] if p == "p" && b == "b"),
        "{error:?}"
    );
    let error = OpenError::Restrict {
        path: "p".to_string(),
        detail: "d".to_string(),
    };
    assert!(
        matches!(&error.args()[..], [("path", Arg::Str(p)), ("ext_err_txt", Arg::Str(d))] if p == "p" && d == "d"),
        "{error:?}"
    );
}

#[test]
fn the_stores_error_has_its_own_code_and_args() {
    let store = StoreError::NewerThanBuild {
        applied: 99,
        build: 6,
    };
    let wrapped = OpenError::Store(store.clone());
    assert_eq!(wrapped.code(), Diagnostic::code(&store));
    assert_eq!(
        format!("{:?}", wrapped.args()),
        format!("{:?}", Diagnostic::args(&store))
    );
}

#[test]
fn each_error_is_carried_in_a_store_error_by_its_code_and_args() {
    let cause = |error: OpenError| match error.into_store_error() {
        StoreError::Unavailable(cause) => cause,
        other => panic!("not Unavailable: {other:?}"),
    };
    let s = |t: &str| angzarr_ports::Arg::Str(t.to_string());
    let c = cause(OpenError::CreateDir {
        path: "p".to_string(),
        detail: "d".to_string(),
    });
    assert_eq!(c.code, code!("create-dir"));
    assert_eq!(c.args, [("path", s("p")), ("ext_err_txt", s("d"))]);
    let c = cause(OpenError::Unplaced {
        path: "p".to_string(),
        detail: "d".to_string(),
    });
    assert_eq!(c.code, code!("unplaced"));
    assert_eq!(c.args, [("path", s("p")), ("ext_err_txt", s("d"))]);
    let c = cause(OpenError::PredatesEmbedded {
        path: "p".to_string(),
        backup: "b".to_string(),
    });
    assert_eq!(c.code, code!("predates-embedded"));
    assert_eq!(c.args, [("path", s("p")), ("backup", s("b"))]);
    let c = cause(OpenError::Restrict {
        path: "p".to_string(),
        detail: "d".to_string(),
    });
    assert_eq!(c.code, code!("restrict"));
    assert_eq!(c.args, [("path", s("p")), ("ext_err_txt", s("d"))]);
}

#[test]
fn the_stores_own_error_passes_through_as_it_was() {
    let store = StoreError::NewerThanBuild {
        applied: 99,
        build: 6,
    };
    assert_eq!(OpenError::Store(store.clone()).into_store_error(), store);
}
