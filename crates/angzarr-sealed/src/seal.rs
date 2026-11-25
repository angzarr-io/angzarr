//! Seal and open a field.

use crate::{
    Algorithm, KeyId, OpenError, RecipientPrivateKey, RecipientPublicKey, SealError, Sealed,
};

/// The one suite: DHKEM(X25519, HKDF-SHA256), HKDF-SHA256,
/// ChaCha20-Poly1305.
pub fn suite() -> Algorithm {
    todo!("the three enum values")
}

/// HPKE's `info` (RFC 9180 section 5.1) for every seal: the ASCII text
/// `angzarr-sealed v1`, 17 bytes, the same for every value. It is the
/// domain separation of this crate's use of HPKE; nothing per-value is in
/// it, because everything per-value is authenticated as the aad
/// ([`associated_data`]) where it is length-prefixed.
pub const INFO: &[u8] = b"angzarr-sealed v1";

/// The fixed label that starts every [`associated_data`]: the ASCII text
/// `angzarr-sealed v1 aad`, 21 bytes.
pub const AAD_LABEL: &[u8] = b"angzarr-sealed v1 aad";

/// The associated data a Sealed is authenticated with: the key id, the
/// algorithm and the field's `context`, each length-prefixed (u32 big-endian)
/// so no two triples encode alike, behind a fixed domain-separation label.
///
/// Layout (every integer is a u32, 4 bytes, big-endian):
///
/// ```text
/// AAD_LABEL (21 bytes, constant)
/// u32 len(key_id)   key_id (UTF-8 bytes)
/// u32 kem           u32 kdf          u32 aead      (the protobuf enum numbers)
/// u32 len(context)  context
/// ```
///
/// Worked example: `key_id` `"k1"`, the suite (kem 1, kdf 1, aead 1),
/// `context` `b"session:abc"`: 54 bytes.
///
/// ```text
/// 616e677a6172722d7365616c656420763120616164   AAD_LABEL "angzarr-sealed v1 aad" (21)
/// 00000002                                     len(key_id) = 2
/// 6b31                                         "k1"
/// 00000001                                     kem  = KEM_DHKEM_X25519_HKDF_SHA256 (1)
/// 00000001                                     kdf  = KDF_HKDF_SHA256 (1)
/// 00000001                                     aead = AEAD_CHACHA20_POLY1305 (1)
/// 0000000b                                     len(context) = 11
/// 73657373696f6e3a616263                       "session:abc"
/// ```
///
/// Unambiguous because the label has a fixed length, the three enum
/// numbers have a fixed width, and each variable field (`key_id`,
/// `context`) is preceded by its own length, so no byte of one field can be
/// read as part of another, and two different triples never encode to the
/// same bytes (`("ab", "c")` and `("a", "bc")` differ in their length
/// words). The label separates these bytes from any other use of the same
/// key. A `context` longer than `u32::MAX` bytes is refused at seal.
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
