//! The bearer credential, read from a request's metadata.
//!
//! [`Bearer`] is opaque: its `Debug` never prints the secret, it has no
//! `Display`, and nothing here logs a request's metadata. Turning it into
//! the gate's credential, and comparing it, are the edge's (a later slice);
//! this crate only extracts it.

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};
use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::status::unauthenticated;

/// The secret of an `authorization: Bearer <secret>` header.
#[derive(Clone, PartialEq, Eq)]
pub struct Bearer(String);

impl Bearer {
    /// The secret itself, for the one place that must hold it (the gate's
    /// credential).
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Bearer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Bearer(<redacted>)")
    }
}

/// Why a request's authorization header is not a bearer credential. The
/// header's text is never in the error: it may be a secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationError {
    /// More than one `authorization` header.
    Repeated,
    /// A value that is not visible ASCII.
    NotText,
    /// A scheme other than `Bearer`.
    NotBearer,
    /// `Bearer` with nothing after it.
    Empty,
}

impl fmt::Display for AuthorizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for AuthorizationError {
    fn code(&self) -> &str {
        match self {
            AuthorizationError::Repeated => code!("authorization-repeated"),
            AuthorizationError::NotText => code!("authorization-not-text"),
            AuthorizationError::NotBearer => code!("authorization-not-bearer"),
            AuthorizationError::Empty => code!("authorization-empty"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        Vec::new()
    }
}

impl std::error::Error for AuthorizationError {}

/// A malformed credential is UNAUTHENTICATED, under the error's code.
impl From<AuthorizationError> for Status {
    fn from(error: AuthorizationError) -> Status {
        unauthenticated(error.code(), [])
    }
}

/// The bearer credential in `metadata`: `None` when the request carries no
/// `authorization` header (whether that is allowed is the gate's to say), an
/// error when it carries one that is not `Bearer <secret>` (the scheme in
/// any case, one or more spaces).
pub fn bearer(metadata: &MetadataMap) -> Result<Option<Bearer>, AuthorizationError> {
    let mut values = metadata.get_all("authorization").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AuthorizationError::Repeated);
    }
    let text = value.to_str().map_err(|_| AuthorizationError::NotText)?;
    let (scheme, rest) = text.split_once(' ').unwrap_or((text, ""));
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(AuthorizationError::NotBearer);
    }
    let secret = rest.trim_start_matches(' ');
    if secret.is_empty() {
        return Err(AuthorizationError::Empty);
    }
    Ok(Some(Bearer(secret.to_string())))
}

/// One of every variant, each once: [`crate::diagnostic_samples`].
pub(crate) fn samples() -> Vec<AuthorizationError> {
    vec![
        AuthorizationError::Repeated,
        AuthorizationError::NotText,
        AuthorizationError::NotBearer,
        AuthorizationError::Empty,
    ]
}

#[cfg(test)]
#[path = "bearer.test.rs"]
mod tests;
