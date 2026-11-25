//! Sealed fields: a message's secret, encrypted to the public key of the
//! process that will use it.
//!
//! A message may carry a secret only as the exception (a reference, such as
//! a variable's name, is preferred). Where one must travel, it travels as a
//! [`Sealed`]: encrypted with RFC 9180 HPKE in base mode, to the consumer's
//! [`RecipientPublicKey`], so the producer and the consumer may be different
//! processes on different machines and the store, the log and every reader in
//! between see ciphertext. Only the holder of the [`RecipientPrivateKey`]
//! [opens](open) it, and the opened bytes are a [`Plaintext`], wiped on drop.
//!
//! The suite is one, not negotiable: DHKEM(X25519, HKDF-SHA256),
//! HKDF-SHA256, ChaCha20-Poly1305 (RFC 9180 section 7: kem_id 0x0020,
//! kdf_id 0x0001, aead_id 0x0003). Its steps are crate-private and held, by
//! the tests, to the RFC's Appendix A.2.1 vectors. The `key_id`, the
//! algorithm and the caller's `data` are bound as associated data
//! ([`associated_data`]), so a value cannot be moved to another field,
//! another key or another suite.
//!
//! Rotation is the [`KeySet`]: several private keys by id, one current for
//! sealing, [`open_with`] choosing by the value's `key_id`.
//!
//! # Security properties
//!
//! Guaranteed (given the recipient's private key stays secret):
//! - Confidentiality of the plaintext against anyone who sees the
//!   [`Sealed`], including the store and the log.
//! - Integrity: a changed ciphertext, encapsulated key, `key_id`,
//!   algorithm or `data` does not open.
//! - A fresh ephemeral key per seal: sealing one secret twice gives two
//!   unrelated values.
//!
//! NOT guaranteed:
//! - **No sender authentication.** HPKE base mode authenticates nobody:
//!   anyone holding the recipient's public key can produce a valid [`Sealed`]
//!   for any `key_id` and `data`. The binding stops a value being moved
//!   between fields, not substituted by a forger. Where the origin matters,
//!   sign the event that contains the value, or use HPKE's authenticated
//!   mode (not offered here).
//! - **Not key-committing.** ChaCha20-Poly1305 does not commit to its key:
//!   a ciphertext can be crafted to open under two keys. Do not use a
//!   [`Sealed`] as proof of which key made it.
//! - **Length is visible.** The ciphertext is the plaintext's length plus 16
//!   bytes; the `key_id` and algorithm are in the clear.
//! - **No forward secrecy for the recipient's key.** Whoever later obtains
//!   the private key opens every value sealed to it that they still hold.
//! - **Wiping is best effort.** Key types, [`Plaintext`] and the HPKE
//!   schedule are wiped on drop, but a copy the caller made, or one the
//!   platform's X25519 call left on the stack, is not.
//!
//! # Crypto-shredding
//!
//! A [`DataKey`] per subject seals that subject's fields
//! ([`DataKey::seal`]); the data key is itself stored only as a
//! [`WrappedKey`], encrypted under a [`KeyEncryptionKey`] ([`wrap`],
//! [`unwrap`]). Everything sealed under the data key is destroyed when
//! every copy of its wrapped key is erased or the key-encryption key is
//! destroyed. [`DataKey::destroy`] wipes one in-memory instance only and
//! shreds nothing by itself. Wrapped keys therefore belong in a mutable key
//! store, never in an append-only event stream, where they could not be
//! erased. Random 96-bit nonces bound each data key and each
//! key-encryption key to 2^32 uses (NIST SP 800-38D).
//!
//! Primitives are `ring`'s (HKDF's HMAC, the AEAD, the random source) and
//! `x25519-dalek`'s (X25519); nothing here implements one.

#![forbid(unsafe_code)]

mod error;
mod key_set;
mod keys;
mod plaintext;
mod seal;
mod wrap;

mod rfc9180;

/// The generated `io.angzarr.sealed.v1` messages: [`Sealed`], [`WrappedKey`]
/// and their algorithm enums.
pub mod proto {
    #![allow(missing_docs, clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/io.angzarr.sealed.v1.rs"));
}

pub use error::{KeySetError, OpenError, SealError, WrapError};
pub use key_set::{open_with, KeySet};
pub use keys::{KeyId, RecipientPrivateKey, RecipientPublicKey};
pub use plaintext::Plaintext;
pub use proto::{Aead, Algorithm, Kdf, Kem, Sealed, WrappedKey};
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
