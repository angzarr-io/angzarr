//! [`Credential`]: what a caller presents, opaque to the platform. The
//! platform never mints, stores or compares credentials (the application's
//! [`Gate`](crate::Gate) decides what one means); it only carries one to
//! `identify`, and keeps it out of every log and every `Debug`.

use std::collections::BTreeMap;
use std::fmt;

/// How a credential was presented.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Scheme {
    /// A bearer token (an `authorization` header's value).
    Bearer,
    /// A peer certificate, as the distributed transport's mTLS gives it.
    PeerCertificate,
}

/// A secret and the pairs that travel with it, as a transport presented
/// them. Opaque: `Debug` prints the scheme and the metadata's NAMES, never
/// a secret or a value; it has no `Display`, no `PartialEq` (a credential is
/// compared with [`Credential::secret_matches`], in constant time) and no
/// serialization. The platform never reads the metadata to authorize
/// anything: the application's `identify` does, and what a caller may do
/// comes from the [`Principal`](crate::Principal) it returns alone.
#[derive(Clone)]
#[non_exhaustive]
pub struct Credential {
    scheme: Scheme,
    secret: Vec<u8>,
    metadata: BTreeMap<String, String>,
}

impl Credential {
    /// A bearer token.
    pub fn bearer(secret: impl Into<Vec<u8>>) -> Credential {
        Credential::new(Scheme::Bearer, secret)
    }

    /// A peer certificate's bytes.
    pub fn peer_certificate(certificate: impl Into<Vec<u8>>) -> Credential {
        Credential::new(Scheme::PeerCertificate, certificate)
    }

    fn new(scheme: Scheme, secret: impl Into<Vec<u8>>) -> Credential {
        Credential {
            scheme,
            secret: secret.into(),
            metadata: BTreeMap::new(),
        }
    }

    /// With the pair `name` and `value` (a second pair of the same name
    /// replaces the first).
    pub fn with_metadata(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Credential {
        self.metadata.insert(name.into(), value.into());
        self
    }

    /// How it was presented.
    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    /// The secret itself, for the application's own `identify` to resolve
    /// (hash, look up, verify). Named so a use is visible in review.
    pub fn expose_secret(&self) -> &[u8] {
        &self.secret
    }

    /// The value of the pair named `name`.
    pub fn metadata(&self, name: &str) -> Option<&str> {
        self.metadata.get(name).map(String::as_str)
    }

    /// Whether the secret equals `expected`, in time that does not depend on
    /// where they differ ([`constant_time_eq`]).
    pub fn secret_matches(&self, expected: &[u8]) -> bool {
        constant_time_eq(&self.secret, expected)
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("scheme", &self.scheme)
            .field("secret", &"<redacted>")
            .field("metadata", &self.metadata.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Whether `a` and `b` are equal, without stopping at the first difference:
/// every byte of the longer is visited, and the lengths are folded into the
/// result rather than compared first. (Hand-written: no third-party crate;
/// `black_box` keeps the optimizer from short-circuiting the fold.)
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    std::hint::black_box(diff) == 0
}

#[cfg(test)]
#[path = "credential.test.rs"]
mod tests;
