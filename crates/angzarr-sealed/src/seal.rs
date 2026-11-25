//! Seal and open a field.

use crate::{
    Algorithm, KeyId, OpenError, RecipientPrivateKey, RecipientPublicKey, SealError, Sealed,
};

/// The one suite: DHKEM(X25519, HKDF-SHA256), HKDF-SHA256,
/// ChaCha20-Poly1305.
pub fn suite() -> Algorithm {
    todo!("the three enum values")
}

/// The associated data a Sealed is authenticated with: the key id, the
/// algorithm and the field's `context`, each length-prefixed (u32 big-endian)
/// so no two triples encode alike, behind a fixed domain-separation label.
///
/// `context` names where the value lives (for example the message type and
/// field, and the subject), so a value cannot be moved to another field.
pub fn associated_data(_key_id: &KeyId, _algorithm: &Algorithm, _context: &[u8]) -> Vec<u8> {
    todo!("label || len(key_id) || key_id || kem || kdf || aead || len(context) || context")
}

/// Seals `plaintext` to `recipient`: RFC 9180 `SealBase` with a fresh
/// ephemeral key, `info` this crate's fixed label, and
/// [`associated_data`] as the AEAD's aad.
///
/// # Errors
/// [`SealError::WeakSharedSecret`] when `recipient` is a low-order point
/// (an all-zero shared secret is refused, RFC 9180 section 7.1.4);
/// [`SealError::RngFailed`].
pub fn seal(
    _recipient: &RecipientPublicKey,
    _key_id: &KeyId,
    _context: &[u8],
    _plaintext: &[u8],
) -> Result<Sealed, SealError> {
    todo!("SetupBaseS then Seal")
}

/// Opens a [`Sealed`] with the recipient's private key: RFC 9180 `OpenBase`.
/// The `context` must be the one it was sealed with.
///
/// # Errors
/// [`OpenError::UnsupportedAlgorithm`], [`OpenError::Malformed`] (a wrong
/// size), and [`OpenError::Failed`] for every authentication failure: a
/// changed ciphertext, encapsulated key, key id, algorithm or context, or
/// another key.
pub fn open(
    _private: &RecipientPrivateKey,
    _sealed: &Sealed,
    _context: &[u8],
) -> Result<Vec<u8>, OpenError> {
    todo!("DecapBase then Open")
}

/// Seals with a caller-chosen ephemeral key: for held-to-the-RFC tests of
/// the whole path. Never use it in production: a repeated ephemeral key is a
/// repeated key.
#[doc(hidden)]
pub fn seal_with_ephemeral(
    _ephemeral: &RecipientPrivateKey,
    _recipient: &RecipientPublicKey,
    _key_id: &KeyId,
    _context: &[u8],
    _plaintext: &[u8],
) -> Result<Sealed, SealError> {
    todo!("SetupBaseS with the given ephemeral key")
}

#[cfg(test)]
#[path = "seal.test.rs"]
mod tests;
