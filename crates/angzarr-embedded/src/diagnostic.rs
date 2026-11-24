//! A diagnostic on the wire and back: the two conversions between
//! `coded_diagnostic`'s errors and angzarr's `Diagnostic`, so no
//! downstream crate re-implements them.
//!
//! - [`DiagnosticToWire::to_wire`]: any `Diagnostic` (a `&dyn Diagnostic`
//!   too) as the wire message, its args typed one to one (`Str` as `text`,
//!   `Int` as `int`, `Uint` as `uint`, `Nested` as `nested`, a cause with
//!   no args of its own still `nested`), in the diagnostic's own order.
//! - [`WireDiagnosticExt::to_report`]: the wire message as a [`Report`],
//!   the same typing back. An arg with no value is dropped.
//!
//! Both are extension traits on the types they convert, so the wire type
//! is still angzarr's own `Diagnostic`.

use angzarr_proto::v1::diagnostic_arg::Value;
use angzarr_proto::v1::{Diagnostic as Wire, DiagnosticArg};
use coded_diagnostic::{Arg, Diagnostic, Report};

/// A `coded_diagnostic::Diagnostic` as angzarr's wire `Diagnostic`.
pub trait DiagnosticToWire {
    /// Its code and its args, typed and in order.
    fn to_wire(&self) -> Wire;
}

impl<D: Diagnostic + ?Sized> DiagnosticToWire for D {
    fn to_wire(&self) -> Wire {
        Wire {
            code: self.code().to_string(),
            typed_args: self
                .args()
                .into_iter()
                .map(|(name, arg)| DiagnosticArg {
                    name: name.to_string(),
                    value: Some(match arg {
                        Arg::Str(text) => Value::Text(text),
                        Arg::Int(n) => Value::Int(n),
                        Arg::Uint(n) => Value::Uint(n),
                        Arg::Nested(cause) => Value::Nested(cause.to_wire()),
                    }),
                })
                .collect(),
        }
    }
}

/// angzarr's wire `Diagnostic` as a `coded_diagnostic::Report`.
pub trait WireDiagnosticExt {
    /// Its code and its args, typed as they were written.
    fn to_report(&self) -> Report;
}

impl WireDiagnosticExt for Wire {
    fn to_report(&self) -> Report {
        self.typed_args
            .iter()
            .fold(Report::new(self.code.clone()), |report, arg| {
                match &arg.value {
                    Some(Value::Text(text)) => report.arg(arg.name.clone(), text.as_str()),
                    Some(Value::Int(n)) => report.arg(arg.name.clone(), *n),
                    Some(Value::Uint(n)) => report.arg(arg.name.clone(), *n),
                    Some(Value::Nested(cause)) => {
                        report.arg(arg.name.clone(), Arg::Nested(Box::new(cause.to_report())))
                    }
                    None => report,
                }
            })
    }
}

#[cfg(test)]
#[path = "diagnostic.test.rs"]
mod tests;
