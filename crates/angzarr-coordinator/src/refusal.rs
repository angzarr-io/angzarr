//! The edge's own refusals: a call to an operation the endpoint does not
//! implement (answered as a `Coded`), a credential that is not (or no
//! longer) good (UNAUTHENTICATED), and a call to an operation only a framework
//! component may call (PERMISSION_DENIED).

use std::fmt;

use angzarr_coordinator_base::{Code, Coded};
use angzarr_diagnostic::{code, Arg, Diagnostic};

/// The endpoint does not implement `operation` (`handle_async`,
/// `handle_sync`, `deliver`): UNIMPLEMENTED, keyed, with the operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unimplemented {
    pub operation: &'static str,
}

impl Unimplemented {
    /// This refusal as the coordinator answers it.
    pub fn coded(&self) -> Coded {
        Coded::from_diagnostic(Code::Unimplemented, self)
    }
}

impl fmt::Display for Unimplemented {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for Unimplemented {
    fn code(&self) -> &str {
        code!("unimplemented")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("operation", self.operation.into())]
    }
}

impl std::error::Error for Unimplemented {}

/// The credential is missing, stale or revoked: shown to the caller as it
/// is, never answered as a hidden domain. It names nothing (no domain, no
/// arrival, no reach), so it gives a caller nothing to probe with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unauthenticated;

impl Unauthenticated {
    /// This refusal as the coordinator answers it: UNAUTHENTICATED.
    pub fn coded(&self) -> Coded {
        Coded::from_diagnostic(Code::Unauthenticated, self)
    }
}

impl fmt::Display for Unauthenticated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for Unauthenticated {
    fn code(&self) -> &str {
        code!("unauthenticated")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        Vec::new()
    }
}

impl std::error::Error for Unauthenticated {}

/// A client called an operation only a framework component may call:
/// PERMISSION_DENIED, shown to the caller (the operation exists; only who
/// may call it is refused), never answered as a hidden domain. The spec
/// declares no key for it, so it has the edge's own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameworkOnly {
    pub operation: &'static str,
}

impl FrameworkOnly {
    /// This refusal as the coordinator answers it: PERMISSION_DENIED.
    pub fn coded(&self) -> Coded {
        Coded::from_diagnostic(Code::PermissionDenied, self)
    }
}

impl fmt::Display for FrameworkOnly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for FrameworkOnly {
    fn code(&self) -> &str {
        code!("framework-only")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("operation", self.operation.into())]
    }
}

impl std::error::Error for FrameworkOnly {}

/// One value of every error this module defines, for the catalog parity
/// test.
pub(crate) fn samples() -> Vec<Box<dyn Diagnostic>> {
    vec![
        Box::new(Unimplemented {
            operation: "handle_async",
        }),
        Box::new(Unauthenticated),
        Box::new(FrameworkOnly {
            operation: "deliver",
        }),
    ]
}

#[cfg(test)]
#[path = "refusal.test.rs"]
mod tests;
