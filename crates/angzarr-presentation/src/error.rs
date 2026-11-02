//! This crate's own errors, and its catalog.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic};

use crate::Source;

/// This crate's en-US messages; every [`Catalog`](crate::Catalog) holds
/// them.
pub const EN_US: Source = (
    "presentation",
    include_str!("../locales/en-US/presentation.ftl"),
);

/// A `--format` that is neither `text` nor `json`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UnknownFormat {
    pub value: String,
}

/// A crate's catalog that cannot be loaded: it does not parse, or it
/// repeats a message another catalog holds.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CatalogError {
    /// The catalog's crate.
    pub source: String,
    pub detail: String,
}

/// Why a diagnostic has no catalog message.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Unrendered {
    /// No catalog holds its code.
    Unknown { code: String },
    /// Its message cannot be filled from its args (one is missing, say).
    Unformattable { code: String, detail: String },
}

impl fmt::Display for UnknownFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl fmt::Display for Unrendered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for UnknownFormat {}
impl std::error::Error for CatalogError {}
impl std::error::Error for Unrendered {}

impl Diagnostic for UnknownFormat {
    fn code(&self) -> &str {
        "presentation-unknown-format"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("value", self.value.as_str().into())]
    }
}

impl Diagnostic for CatalogError {
    fn code(&self) -> &str {
        "presentation-catalog-invalid"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![
            ("source", self.source.as_str().into()),
            ("ext_err_txt", self.detail.as_str().into()),
        ]
    }
}

impl Diagnostic for Unrendered {
    fn code(&self) -> &str {
        match self {
            Unrendered::Unknown { .. } => "presentation-unknown-code",
            Unrendered::Unformattable { .. } => "presentation-unformattable",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            Unrendered::Unknown { code } => vec![("code", code.as_str().into())],
            Unrendered::Unformattable { code, detail } => vec![
                ("code", code.as_str().into()),
                ("ext_err_txt", detail.as_str().into()),
            ],
        }
    }
}

/// One value of every error this crate defines, for the catalog test.
#[cfg(any(test, feature = "test-support"))]
pub fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    vec![
        Box::new(UnknownFormat {
            value: "yaml".to_string(),
        }),
        Box::new(CatalogError {
            source: "tasks".to_string(),
            detail: "Expected a value".to_string(),
        }),
        Box::new(Unrendered::Unknown {
            code: "x-busy".to_string(),
        }),
        Box::new(Unrendered::Unformattable {
            code: "x-busy".to_string(),
            detail: "unknown variable: $attempts".to_string(),
        }),
    ]
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
