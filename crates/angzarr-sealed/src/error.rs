//! The crate's errors: a code, args and an en-US message each.

use std::fmt;

use coded_diagnostic::{code, Arg, Diagnostic};

/// Why a [`Sealed`](crate::Sealed) was not made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealError {
    /// The recipient key is a low-order point, so the Diffie-Hellman shared
    /// secret is all zero (RFC 7748 section 6.1, RFC 9180 section 7.1.4).
    WeakSharedSecret,
    /// The system random source failed.
    RngFailed,
    /// The data key was destroyed.
    KeyDestroyed,
    /// The AEAD refused to encrypt: the plaintext is beyond its limit, or an
    /// HPKE context's sequence numbers are exhausted.
    EncryptionFailed,
    /// A `key_id` or the caller's `data` is longer than `u32::MAX` bytes,
    /// which the associated data's 4-byte length prefix cannot say.
    TooLong {
        /// The field: `key_id` or `data`.
        field: &'static str,
    },
}

/// Why a [`Sealed`](crate::Sealed) was not opened. Every cryptographic
/// failure is the one `Failed`: it says nothing of which field was wrong.
///
/// A service that relays errors to an untrusted party should map
/// [`OpenError::UnknownKey`] and [`OpenError::Failed`] to one failure:
/// `UnknownKey` is for the key holder (it tells her a rotation has not
/// reached her or the value is for another consumer); told to a sender it
/// would say which key ids the holder has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenError {
    /// The authentication failed: the value was altered (any field), was
    /// made for another key, or was sealed for other data.
    Failed,
    /// The algorithm is not the one suite this crate implements.
    UnsupportedAlgorithm,
    /// A field is the wrong size or missing.
    Malformed {
        /// The field's name.
        field: &'static str,
        /// What is wrong with it.
        why: &'static str,
    },
    /// The data key was destroyed.
    KeyDestroyed,
    /// [`open_with`](crate::open_with): the key set holds no key named by the
    /// value's `key_id`. Distinct from `Failed` so a holder can tell "not my
    /// key" (a rotation not yet distributed, a value for another consumer)
    /// from "my key, but altered". The id is the value's own clear text, so
    /// naming it tells the holder nothing it did not already hold; the error
    /// is for the holder, and a service that relays errors to an untrusted
    /// sender should answer both as one failure.
    UnknownKey {
        /// The `key_id` the value names.
        key_id: String,
    },
}

/// Why a [`KeySet`](crate::KeySet) was not changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySetError {
    /// No key of that id is in the set.
    NoSuchKey {
        /// The id asked for.
        key_id: String,
    },
    /// The current key cannot be removed: mark another current first.
    CurrentKey {
        /// The current key's id.
        key_id: String,
    },
    /// A key of that id is already in the set: replacing a key silently
    /// would orphan everything sealed to it, so it is refused.
    AlreadyHeld {
        /// The id.
        key_id: String,
    },
}

/// Why a data key was not wrapped or unwrapped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WrapError {
    /// The authentication failed: altered, another key-encryption key, or
    /// another subject.
    Failed,
    /// A field is the wrong size or missing.
    Malformed {
        /// The field's name.
        field: &'static str,
        /// What is wrong with it.
        why: &'static str,
    },
}

/// The args of a `Malformed` variant: its field and why.
trait Malformed {
    fn malformed(&self) -> Option<(&'static str, &'static str)>;
}
impl Malformed for OpenError {
    fn malformed(&self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::Malformed { field, why } => Some((field, why)),
            _ => None,
        }
    }
}
impl Malformed for WrapError {
    fn malformed(&self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::Malformed { field, why } => Some((field, why)),
            _ => None,
        }
    }
}
fn malformed_args<E: Malformed>(e: &E) -> Vec<(&str, Arg)> {
    e.malformed()
        .map(|(field, why)| vec![("field", Arg::from(field)), ("why", Arg::from(why))])
        .unwrap_or_default()
}

macro_rules! diagnostic {
    ($type:ty { $($variant:pat => $code:expr),+ $(,)? }, $args:expr) => {
        impl fmt::Display for $type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                coded_diagnostic::display(self, f)
            }
        }
        impl std::error::Error for $type {}
        impl Diagnostic for $type {
            fn code(&self) -> &str {
                match self { $($variant => $code),+ }
            }
            fn args(&self) -> Vec<(&str, Arg)> {
                $args(self)
            }
        }
    };
}

diagnostic!(SealError {
    Self::WeakSharedSecret => code!("seal-weak-shared-secret"),
    Self::RngFailed => code!("seal-rng-failed"),
    Self::KeyDestroyed => code!("seal-key-destroyed"),
    Self::EncryptionFailed => code!("seal-encryption-failed"),
    Self::TooLong { .. } => code!("seal-too-long"),
}, |e: &SealError| match e {
    SealError::TooLong { field } => vec![("field", Arg::from(*field))],
    _ => Vec::new(),
});

diagnostic!(OpenError {
    Self::Failed => code!("open-failed"),
    Self::UnsupportedAlgorithm => code!("open-unsupported-algorithm"),
    Self::Malformed { .. } => code!("open-malformed"),
    Self::KeyDestroyed => code!("open-key-destroyed"),
    Self::UnknownKey { .. } => code!("open-unknown-key"),
}, open_args);

diagnostic!(KeySetError {
    Self::NoSuchKey { .. } => code!("key-set-no-such-key"),
    Self::CurrentKey { .. } => code!("key-set-current-key"),
    Self::AlreadyHeld { .. } => code!("key-set-already-held"),
}, |e: &KeySetError| match e {
    KeySetError::NoSuchKey { key_id }
    | KeySetError::CurrentKey { key_id }
    | KeySetError::AlreadyHeld { key_id } => {
        vec![("key_id", Arg::from(key_id.as_str()))]
    }
});

fn open_args(e: &OpenError) -> Vec<(&str, Arg)> {
    match e {
        OpenError::UnknownKey { key_id } => vec![("key_id", Arg::from(key_id.as_str()))],
        other => malformed_args(other),
    }
}

diagnostic!(WrapError {
    Self::Failed => code!("wrap-failed"),
    Self::Malformed { .. } => code!("wrap-malformed"),
}, malformed_args::<WrapError>);

/// One value of every error, each variant once.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    vec![
        Box::new(SealError::WeakSharedSecret),
        Box::new(SealError::RngFailed),
        Box::new(SealError::KeyDestroyed),
        Box::new(SealError::EncryptionFailed),
        Box::new(SealError::TooLong { field: "data" }),
        Box::new(OpenError::Failed),
        Box::new(OpenError::UnsupportedAlgorithm),
        Box::new(OpenError::Malformed {
            field: "ciphertext",
            why: "empty",
        }),
        Box::new(OpenError::KeyDestroyed),
        Box::new(OpenError::UnknownKey {
            key_id: "k1".into(),
        }),
        Box::new(KeySetError::NoSuchKey {
            key_id: "k1".into(),
        }),
        Box::new(KeySetError::CurrentKey {
            key_id: "k1".into(),
        }),
        Box::new(KeySetError::AlreadyHeld {
            key_id: "k1".into(),
        }),
        Box::new(WrapError::Failed),
        Box::new(WrapError::Malformed {
            field: "nonce",
            why: "not 12 bytes",
        }),
    ]
}
