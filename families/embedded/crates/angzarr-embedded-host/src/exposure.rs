//! The exposure seam: what the host's listener (later, one gRPC server
//! serving `CommandHandlerCoordinatorService`, routed by `cover.domain`)
//! calls for each request, [`App::serve`](crate::App::serve). It takes the
//! request's book and where the request arrived from, and answers as
//! `CommandHandlerCoordinatorService.Handle` does, in core's messages (the
//! kernel's [`Answered`](angzarr_embedded::Answered), from which the listener
//! builds the distributed transport's `CommandResponse`; decision 14.64), or
//! the kernel's refusal. A domain is served only when its `component` entry's
//! `expose` admits the arrival ([`Expose`]): a request on loopback reaches
//! a component exposed `"loopback"` or `"ip"`, one from the network only a
//! component exposed `"ip"`. A domain not exposed to the arrival is
//! answered exactly as an unknown domain is (the kernel's
//! `Malformed::UnknownDomain`), so a caller cannot tell a hidden domain
//! from a missing one; the real reason ([`NotExposed`]) goes back to the
//! listener beside that answer ([`Unserved::Hidden`]), for it to log
//! locally: the host writes to no terminal of its own.

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};
use angzarr_embedded::{Failure, Rejected};

use crate::config::Expose;

/// Where a request to the host's listener arrived from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arrival {
    /// This machine: a loopback address.
    Loopback,
    /// The network: any other address.
    Ip,
}

impl Arrival {
    /// As an arg: `"loopback"` or `"ip"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Arrival::Loopback => "loopback",
            Arrival::Ip => "ip",
        }
    }

    /// Whether a component exposed `expose` is served to a request from
    /// here: loopback is admitted by `"loopback"` and `"ip"`, the network
    /// by `"ip"` alone, and `"none"` admits nothing.
    pub fn admitted_by(&self, expose: Expose) -> bool {
        match self {
            Arrival::Loopback => expose >= Expose::Loopback,
            Arrival::Ip => expose == Expose::Ip,
        }
    }
}

/// Why a request for `domain` was answered as an unknown domain: its
/// component is exposed `expose`, which does not admit `arrival`. The
/// host's local log line, never the caller's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotExposed {
    pub domain: String,
    pub arrival: Arrival,
    pub expose: Expose,
}

impl fmt::Display for NotExposed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for NotExposed {
    fn code(&self) -> &str {
        code!("not-exposed")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![
            ("domain", self.domain.as_str().into()),
            ("arrival", self.arrival.as_str().into()),
            ("expose", self.expose.as_str().into()),
        ]
    }
}

/// Why a request to the host's listener was not served
/// ([`App::serve_in`](crate::App::serve_in)): what its caller is answered,
/// and, for a hidden domain, why, which only the host's own log may say.
#[derive(Clone, PartialEq, Debug)]
pub enum Unserved {
    /// Its domain is not exposed to where it arrived from: answered
    /// `answer`, exactly as an unknown domain is; `hidden` is the real
    /// reason, the listener's to log locally, never the caller's.
    Hidden {
        answer: Failure<Rejected>,
        hidden: NotExposed,
    },
    /// The kernel's own failure, answered as it is.
    Failed(Failure<Rejected>),
}

impl Unserved {
    /// What the caller is answered.
    pub fn answer(&self) -> &Failure<Rejected> {
        match self {
            Unserved::Hidden { answer, .. } | Unserved::Failed(answer) => answer,
        }
    }

    /// Why the domain was hidden, for the host's own log: `None` for the
    /// kernel's own failure.
    pub fn hidden(&self) -> Option<&NotExposed> {
        match self {
            Unserved::Hidden { hidden, .. } => Some(hidden),
            Unserved::Failed(_) => None,
        }
    }
}

/// One value of every error this module defines, for the catalog parity
/// test.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    vec![Box::new(NotExposed {
        domain: "player".to_string(),
        arrival: Arrival::Ip,
        expose: Expose::Loopback,
    })]
}

#[cfg(test)]
#[path = "exposure.test.rs"]
mod tests;
