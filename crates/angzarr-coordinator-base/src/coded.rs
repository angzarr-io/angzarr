//! [`Coded`]: a status, a key and an argument map; [`Refusal`]: what
//! answers as one.

use std::collections::BTreeMap;
use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_proto::v1::AngzarrReason;
use angzarr_proto::ReasonSurface;

use crate::Code;

/// A coded refusal or failure: the form every one takes before it crosses a
/// boundary, a KEY and an ARGUMENT MAP (decision 14.37), no English. `key`
/// is the cross-language identifier (`ANGZARR_REASON_UPCASTER_UNAVAILABLE`, or
/// a crate's kebab-case code: `angzarr-ports-store-conflict`); it is the
/// `ErrorInfo` reason and the status message alike. `args` are the
/// `ErrorInfo` metadata, a wrapped cause flattened into them as `cause` and
/// `cause.<arg>`. `code` is the status it travels with. This is the
/// router's `CodedError` without its typed refusal and without tonic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Coded {
    pub code: Code,
    pub key: String,
    pub args: BTreeMap<String, String>,
}

impl Coded {
    /// `key` under `code`, with `args`.
    pub fn new(
        code: Code,
        key: impl Into<String>,
        args: impl IntoIterator<Item = (String, String)>,
    ) -> Coded {
        Coded {
            code,
            key: key.into(),
            args: args.into_iter().collect(),
        }
    }

    /// The spec key `reason` with its args, under the status its option
    /// declares (decision 14.65). A readiness key is never answered so; it is
    /// INTERNAL here, the status of the bug answering it would be.
    pub fn spec(reason: AngzarrReason, args: impl IntoIterator<Item = (String, String)>) -> Coded {
        let code = match reason.surface() {
            Some(ReasonSurface::GrpcStatus(status)) => {
                Code::try_from(status).unwrap_or(Code::Internal)
            }
            Some(ReasonSurface::Readiness) | None => Code::Internal,
        };
        Coded::new(code, reason.as_str_name(), args)
    }

    /// The gRPC status message: the key itself.
    pub fn status_message(&self) -> &str {
        &self.key
    }

    /// `diagnostic` under `code`: its code as the key, its args flattened
    /// ([`flat_args`]).
    pub fn from_diagnostic(code: Code, diagnostic: &dyn Diagnostic) -> Coded {
        Coded::new(code, diagnostic.code(), flat_args(diagnostic))
    }
}

/// A diagnostic's args, flattened as angzarr carries them (decision 14.37):
/// each by its name, a wrapped cause as its code under the arg's name and
/// each of its args under `<name>.<arg>`.
pub fn flat_args(diagnostic: &dyn Diagnostic) -> Vec<(String, String)> {
    let mut flat = Vec::new();
    for (name, arg) in diagnostic.args() {
        match arg {
            Arg::Int(n) => flat.push((name.to_string(), n.to_string())),
            Arg::Uint(n) => flat.push((name.to_string(), n.to_string())),
            Arg::Str(s) => flat.push((name.to_string(), s)),
            Arg::Nested(cause) => {
                flat.push((name.to_string(), cause.code().to_string()));
                flat.extend(
                    flat_args(cause.as_ref())
                        .into_iter()
                        .map(|(inner, value)| (format!("{name}.{inner}"), value)),
                );
            }
        }
    }
    flat
}

/// The key, then its args, ` name=value` each; never English.
impl fmt::Display for Coded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key)?;
        self.args
            .iter()
            .try_for_each(|(name, value)| write!(f, " {name}={}", angzarr_diagnostic::quote(value)))
    }
}

impl std::error::Error for Coded {}

/// This error as a key and args on the wire (types.proto's `Diagnostic`).
impl From<&Coded> for angzarr_proto::v1::Diagnostic {
    fn from(coded: &Coded) -> Self {
        angzarr_proto::v1::Diagnostic {
            code: coded.key.clone(),
            args: coded.args.clone().into_iter().collect(),
        }
    }
}

/// What answers as a [`Coded`]: a refusal or failure with a key, args and a
/// status of its own, however it is held (a store's error, an upcaster's, a
/// reads engine's). What a frontend maps to its transport's status, and
/// nothing else of it.
pub trait Refusal: fmt::Debug {
    /// This refusal as the coordinator answers it.
    fn coded(&self) -> Coded;
}

impl Refusal for Coded {
    fn coded(&self) -> Coded {
        self.clone()
    }
}

#[cfg(test)]
#[path = "coded.test.rs"]
mod tests;
