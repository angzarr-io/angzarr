//! What a read refuses with: [`Invalid`] (this crate's own refusals, each a
//! catalog message) and [`ReadError`], which also carries a store's and an
//! upcaster's.

use std::fmt;

use angzarr_coordinator_base::{Code, Coded, Refusal};
use angzarr_diagnostic::{code, Arg, Diagnostic};
use angzarr_ports::StoreError;

/// A request this crate refuses, whatever the store holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Invalid {
    /// The cover names no domain.
    NoDomain,
    /// The cover's root is unset or not 16 bytes.
    BadRoot,
    /// An edition is named and its name is empty.
    EmptyEdition,
    /// A correlation id is unset where one is required, or not 16 bytes.
    BadCorrelation,
    /// A query of a domain names neither a root nor a correlation.
    NoTarget,
    /// A unary read was asked for one book and `streams` streams answer.
    ManyStreams { streams: u64 },
    /// A unary read needs more than one book.
    TooLarge {
        books: u64,
        pages: u64,
        max_pages: u64,
        max_bytes: u64,
    },
}

impl Invalid {
    /// The status this refusal travels with.
    pub fn status(&self) -> Code {
        match self {
            Invalid::TooLarge { .. } => Code::ResourceExhausted,
            _ => Code::InvalidArgument,
        }
    }
}

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for Invalid {
    fn code(&self) -> &str {
        match self {
            Invalid::NoDomain => code!("no-domain"),
            Invalid::BadRoot => code!("bad-root"),
            Invalid::EmptyEdition => code!("empty-edition"),
            Invalid::BadCorrelation => code!("bad-correlation"),
            Invalid::NoTarget => code!("no-target"),
            Invalid::ManyStreams { .. } => code!("many-streams"),
            Invalid::TooLarge { .. } => code!("too-large"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            Invalid::NoDomain
            | Invalid::BadRoot
            | Invalid::EmptyEdition
            | Invalid::BadCorrelation
            | Invalid::NoTarget => Vec::new(),
            Invalid::ManyStreams { streams } => vec![("streams", Arg::Uint(*streams))],
            Invalid::TooLarge {
                books,
                pages,
                max_pages,
                max_bytes,
            } => vec![
                ("books", Arg::Uint(*books)),
                ("pages", Arg::Uint(*pages)),
                ("max_pages", Arg::Uint(*max_pages)),
                ("max_bytes", Arg::Uint(*max_bytes)),
            ],
        }
    }
}

impl std::error::Error for Invalid {}

/// Why a read failed.
#[derive(Clone, Debug, PartialEq)]
pub enum ReadError {
    /// The request is refused.
    Invalid(Invalid),
    /// The store failed, or names the edition unknown.
    Store(StoreError),
    /// The upcaster failed, as [`angzarr_coordinator_base::upcast_pages`]
    /// answers it.
    Upcast(Coded),
}

impl From<Invalid> for ReadError {
    fn from(invalid: Invalid) -> ReadError {
        ReadError::Invalid(invalid)
    }
}

impl From<StoreError> for ReadError {
    fn from(error: StoreError) -> ReadError {
        ReadError::Store(error)
    }
}

impl From<Coded> for ReadError {
    fn from(coded: Coded) -> ReadError {
        ReadError::Upcast(coded)
    }
}

impl Refusal for ReadError {
    fn coded(&self) -> Coded {
        match self {
            ReadError::Invalid(invalid) => Coded::from_diagnostic(invalid.status(), invalid),
            ReadError::Store(error) => Coded::from(error),
            ReadError::Upcast(coded) => coded.clone(),
        }
    }
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for ReadError {
    fn code(&self) -> &str {
        match self {
            ReadError::Invalid(invalid) => invalid.code(),
            ReadError::Store(error) => Diagnostic::code(error),
            ReadError::Upcast(coded) => &coded.key,
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            ReadError::Invalid(invalid) => invalid.args(),
            ReadError::Store(error) => Diagnostic::args(error),
            ReadError::Upcast(coded) => coded
                .args
                .iter()
                .map(|(name, value)| (name.as_str(), Arg::Str(value.clone())))
                .collect(),
        }
    }
}

impl std::error::Error for ReadError {}

/// One of every [`Invalid`] variant, each once: [`crate::diagnostic_samples`].
pub(crate) fn samples() -> Vec<Invalid> {
    vec![
        Invalid::NoDomain,
        Invalid::BadRoot,
        Invalid::EmptyEdition,
        Invalid::BadCorrelation,
        Invalid::NoTarget,
        Invalid::ManyStreams { streams: 2 },
        Invalid::TooLarge {
            books: 3,
            pages: 2500,
            max_pages: 1000,
            max_bytes: 3 << 20,
        },
    ]
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
