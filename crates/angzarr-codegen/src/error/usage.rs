//! A use of the protos they cannot serve: what a caller asks of them (a
//! file, the wire path of a package's prost types, the Python domain), or a
//! user's file read back to complete that does not match them. Each names
//! what was asked for, or the user's file.

use std::fmt;

use coded_diagnostic::{Arg, Diagnostic, code};

use super::{text, texts};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum UseError {
    /// A file asked for is not in the descriptor set.
    FileNotInSet { file: String },
    /// A package with domain types but no Rust path for its prost types.
    NoWirePath { package: String },
    /// A package whose types `message`, of another package, holds, but no
    /// Rust path for its domain layer: its types are referenced there,
    /// never duplicated.
    NoDomainPath { package: String, message: String },
    /// The protos declare no component of the domain, or of the name,
    /// asked for.
    PythonNoDomain { domain: String },
    /// The user's file for a component holds no decorated impl of it, so
    /// `--missing` has nowhere to add stubs; the file is left as it is.
    ScaffoldImplNotFound { path: String, component: String },
    /// The user's file takes an input as a command (`cmd`) where the proto
    /// declares a fact (`fact`), or the reverse. `expected` and `found` are
    /// the parameters' names.
    InputMismatch {
        path: String,
        message: String,
        expected: String,
        found: String,
    },
    /// The scaffold's configuration names a component the protos declare
    /// as a policy or a projector, which holds no state to snapshot.
    ScaffoldNoComponent { component: String },
}

impl fmt::Display for UseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        coded_diagnostic::display(self, f)
    }
}

impl std::error::Error for UseError {}

impl Diagnostic for UseError {
    fn code(&self) -> &'static str {
        match self {
            UseError::FileNotInSet { .. } => code!("file-not-in-set"),
            UseError::NoWirePath { .. } => code!("no-wire-path"),
            UseError::NoDomainPath { .. } => code!("no-domain-path"),
            UseError::PythonNoDomain { .. } => code!("python-no-domain"),
            UseError::ScaffoldImplNotFound { .. } => code!("scaffold-impl-not-found"),
            UseError::InputMismatch { .. } => code!("input-mismatch"),
            UseError::ScaffoldNoComponent { .. } => code!("scaffold-no-component"),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        use UseError as E;
        match self {
            E::FileNotInSet { file } => texts![file],
            E::NoWirePath { package } => texts![package],
            E::NoDomainPath { package, message } => texts![package, message],
            E::PythonNoDomain { domain } => texts![domain],
            E::ScaffoldImplNotFound { path, component } => texts![path, component],
            E::InputMismatch {
                path,
                message,
                expected,
                found,
            } => texts![path, message, expected, found],
            E::ScaffoldNoComponent { component } => texts![component],
        }
    }
}

/// One value of every [`UseError`].
pub(super) fn samples() -> Vec<UseError> {
    use UseError as E;
    vec![
        E::FileNotInSet { file: text("file") },
        E::NoWirePath {
            package: text("package"),
        },
        E::NoDomainPath {
            package: text("package"),
            message: text("message"),
        },
        E::PythonNoDomain {
            domain: text("domain"),
        },
        E::ScaffoldImplNotFound {
            path: text("path"),
            component: text("component"),
        },
        E::InputMismatch {
            path: text("path"),
            message: text("message"),
            expected: text("expected"),
            found: text("found"),
        },
        E::ScaffoldNoComponent {
            component: text("component"),
        },
    ]
}
