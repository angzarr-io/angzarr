//! [`Denied`]: the real reason a request was refused after the caller was
//! identified. It is the audit log's, never the caller's: the caller is
//! answered exactly as a missing domain is answered.

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};

use crate::{Arrival, Hidden, Reach};

/// Why the gate refused a request: one variant per step that can fail after
/// `identify`. Carries what the request named (a domain, a type URL), never
/// a credential: nothing here has one to carry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Denied {
    /// The domain is not served as far as the request arrived from.
    NotExposed(Hidden),
    /// The body kind is allowed, the type is not.
    TypeNotAllowed { type_url: String },
    /// No body of this kind is allowed (`kind` is `"none"` for a request
    /// with no body).
    BodyKindNotAllowed { kind: &'static str },
    /// The principal does not own a stream the request names, in `domain`.
    NotOwner { domain: String },
    /// The principal may not use this read operation.
    SelectionNotAllowed { operation: &'static str },
}

impl Denied {
    /// The audit log's reason, one stable word: `not_exposed`,
    /// `type_not_allowed`, `body_kind_not_allowed`, `not_owner` or
    /// `selection_not_allowed`.
    pub fn reason(&self) -> &'static str {
        match self {
            Denied::NotExposed(_) => "not_exposed",
            Denied::TypeNotAllowed { .. } => "type_not_allowed",
            Denied::BodyKindNotAllowed { .. } => "body_kind_not_allowed",
            Denied::NotOwner { .. } => "not_owner",
            Denied::SelectionNotAllowed { .. } => "selection_not_allowed",
        }
    }
}

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for Denied {
    fn code(&self) -> &str {
        match self {
            Denied::NotExposed(_) => code!("denied-not-exposed"),
            Denied::TypeNotAllowed { .. } => code!("denied-type-not-allowed"),
            Denied::BodyKindNotAllowed { .. } => code!("denied-body-kind-not-allowed"),
            Denied::NotOwner { .. } => code!("denied-not-owner"),
            Denied::SelectionNotAllowed { .. } => code!("denied-selection-not-allowed"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            Denied::NotExposed(hidden) => vec![
                ("domain", hidden.domain.as_str().into()),
                ("arrival", hidden.arrival.as_str().into()),
                ("reach", hidden.reach.as_str().into()),
            ],
            Denied::TypeNotAllowed { type_url } => vec![("type_url", type_url.as_str().into())],
            Denied::BodyKindNotAllowed { kind } => vec![("kind", (*kind).into())],
            Denied::NotOwner { domain } => vec![("domain", domain.as_str().into())],
            Denied::SelectionNotAllowed { operation } => vec![("operation", (*operation).into())],
        }
    }
}

impl std::error::Error for Denied {}

/// One value of every variant, for the catalog parity test.
pub(crate) fn samples() -> Vec<Box<dyn Diagnostic>> {
    let variants = [
        Denied::NotExposed(Hidden {
            domain: "player".to_string(),
            arrival: Arrival::Ip,
            reach: Reach::Loopback,
        }),
        Denied::TypeNotAllowed {
            type_url: "type.example/Deal".to_string(),
        },
        Denied::BodyKindNotAllowed { kind: "fact" },
        Denied::NotOwner {
            domain: "player".to_string(),
        },
        Denied::SelectionNotAllowed {
            operation: "follow",
        },
    ];
    variants
        .into_iter()
        .map(|d| Box::new(d) as Box<dyn Diagnostic>)
        .collect()
}

#[cfg(test)]
#[path = "denied.test.rs"]
mod tests;
