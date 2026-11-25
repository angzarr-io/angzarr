//! Sealed fields: a message's secret, encrypted to the public key of the
//! process that will use it.
//!
//! A message may carry a secret only as the exception (a reference, such as
//! a variable's name, is preferred). Where one must travel, it travels as a
//! [`Sealed`]: encrypted with RFC 9180 HPKE in base mode, to the consumer's
//! [`RecipientPublicKey`], so the producer and the consumer may be different
//! processes on different machines and the store, the log and every reader in
//! between see ciphertext. Only the holder of the [`RecipientPrivateKey`]
//! [opens](open) it.
//!
//! The suite is one, not negotiable: DHKEM(X25519, HKDF-SHA256),
//! HKDF-SHA256, ChaCha20-Poly1305 (RFC 9180 section 7: kem_id 0x0020,
//! kdf_id 0x0001, aead_id 0x0003). [`rfc9180`] holds its steps, held to the
//! RFC's Appendix A.2 test vectors. The `key_id`, the algorithm and the
//! field's own `data` are bound as associated data ([`associated_data`]),
//! so a value cannot be moved to another field, another key or another
//! suite.
//!
//! Crypto-shredding is the data key's: a [`DataKey`] per subject seals that
//! subject's fields ([`DataKey::seal`]), and is itself stored only as a
//! [`WrappedKey`], encrypted under a [`KeyEncryptionKey`] ([`wrap`],
//! [`unwrap`]). Destroying the wrapped key (or the key-encryption key)
//! destroys everything sealed under the data key.
//!
//! Primitives are `ring`'s (HKDF's HMAC, the AEAD, the random source) and
//! `x25519-dalek`'s (X25519); nothing here implements one.

#![forbid(unsafe_code)]

mod error;
mod key_set;
mod keys;
mod seal;
mod wrap;

pub mod rfc9180;

/// The generated `io.angzarr.sealed.v1` messages: [`Sealed`], [`WrappedKey`]
/// and their algorithm enums.
pub mod proto {
    #![allow(missing_docs, clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/io.angzarr.sealed.v1.rs"));
}

pub use error::{KeySetError, OpenError, SealError, WrapError};
pub use key_set::{open_with, KeySet};
pub use keys::{KeyId, RecipientPrivateKey, RecipientPublicKey};
pub use proto::{Aead, Algorithm, Kdf, Kem, Sealed, WrappedKey};
#[doc(hidden)]
pub use seal::seal_with_ephemeral;
pub use seal::{associated_data, open, seal, suite, AAD_LABEL, INFO};
pub use wrap::{unwrap, wrap, DataKey, KeyEncryptionKey};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics.
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// This crate's catalogs by locale, under the same name.
pub const LOCALES: (&str, &[(&str, &str)]) = (
    env!("CARGO_PKG_NAME"),
    include!(concat!(env!("OUT_DIR"), "/locales.rs")),
);

/// One value of every error this crate defines, for the catalog parity test.
pub fn diagnostic_samples() -> Vec<Box<dyn coded_diagnostic::Diagnostic>> {
    let mut samples = Vec::new();
    samples.extend(error::diagnostic_samples());
    samples
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;

#[cfg(test)]
#[path = "rfc9180_a2.test.rs"]
mod rfc9180_a2;

#[cfg(test)]
#[path = "testkit.test.rs"]
mod testkit;
