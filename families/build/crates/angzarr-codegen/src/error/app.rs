//! An application's host config (`angzarr.yaml`) or manifest
//! (`Cargo.toml`) that `angzarr scaffold app` cannot generate from.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic, code};

use super::{text, texts};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AppError {
    /// A host config that does not load: `cause` is the reader's error,
    /// naming for a decode the key path where it goes wrong
    /// (`component[1].kind`).
    Config { cause: angzarr_diagnostic::Report },
    /// The host config has lints ([`crate::AppLint`]), each reported on its
    /// own.
    Refused { lints: Vec<crate::AppLint> },
    /// A manifest that does not read as one, or whose
    /// `[package.metadata.angzarr.domain]` is malformed.
    Manifest { detail: String },
    /// A domain crate's path, or its wire types', that is no Rust path.
    DomainPath { path: String },
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for AppError {}

impl Diagnostic for AppError {
    fn code(&self) -> &'static str {
        match self {
            AppError::Config { .. } => code!("app-config"),
            AppError::Refused { .. } => code!("app-refused"),
            AppError::Manifest { .. } => code!("app-manifest"),
            AppError::DomainPath { .. } => code!("app-domain-path"),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            AppError::Config { cause } => vec![("cause", Arg::nested(cause))],
            AppError::Refused { lints } => {
                vec![(
                    "count",
                    i64::try_from(lints.len()).unwrap_or(i64::MAX).into(),
                )]
            }
            AppError::Manifest { detail } => texts![detail],
            AppError::DomainPath { path } => texts![path],
        }
    }
}

/// One value of every [`AppError`].
pub(super) fn samples() -> Vec<AppError> {
    vec![
        // The reader's own error, so no other crate's code is spelled here.
        crate::app::read("[]").expect_err("a sequence is no host config"),
        AppError::Refused { lints: Vec::new() },
        AppError::Manifest {
            detail: text("detail"),
        },
        AppError::DomainPath { path: text("path") },
    ]
}
