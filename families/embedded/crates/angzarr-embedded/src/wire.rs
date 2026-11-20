//! Why a wire message is not a valid domain value: what a generated
//! conversion from a prost message to its domain type returns
//! (`angzarr_build`, backlog CR-4). The payload or snapshot it came from is
//! then refused as [`PayloadError::Invalid`](super::PayloadError).

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};

/// A field of a decoded message that its domain type cannot hold.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum WireError {
    /// A field the domain type requires is absent: a message field, which
    /// the domain layer holds without an `Option`.
    Missing {
        message: &'static str,
        field: &'static str,
    },
    /// An enum field holds a value the domain does not define. The zero
    /// value is one: `*_UNSPECIFIED` is the wire's absence, never a domain
    /// value.
    UnknownEnum {
        message: &'static str,
        field: &'static str,
        value: i32,
    },
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for WireError {
    fn code(&self) -> &str {
        match self {
            WireError::Missing { .. } => code!("wire-missing"),
            WireError::UnknownEnum { .. } => code!("wire-unknown-enum"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            WireError::Missing { message, field } => {
                vec![("message", (*message).into()), ("field", (*field).into())]
            }
            WireError::UnknownEnum {
                message,
                field,
                value,
            } => vec![
                ("message", (*message).into()),
                ("field", (*field).into()),
                ("value", (*value).into()),
            ],
        }
    }
}
