//! Seal and open a field.

use zeroize::Zeroizing;

use crate::{
    rfc9180, Aead, Algorithm, Kdf, Kem, KeyId, OpenError, Plaintext, RecipientPrivateKey,
    RecipientPublicKey, SealError, Sealed,
};

/// The one suite: DHKEM(X25519, HKDF-SHA256), HKDF-SHA256,
/// ChaCha20-Poly1305.
pub fn suite() -> Algorithm {
    Algorithm {
        kem: Kem::DhkemX25519HkdfSha256.into(),
        kdf: Kdf::HkdfSha256.into(),
        aead: Aead::Chacha20Poly1305.into(),
    }
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

/// A 4-byte big-endian length word for `field`'s `len` bytes: the one place
/// the `u32::MAX` bound is judged, taking the length (not the bytes) so a
/// test reaches it without a 4 GiB buffer.
///
/// # Errors
/// [`SealError::TooLong`] when `len` is over `u32::MAX`.
pub(crate) fn length_word(field: &'static str, len: usize) -> Result<[u8; 4], SealError> {
    u32::try_from(len)
        .map(u32::to_be_bytes)
        .map_err(|_| SealError::TooLong { field })
}

/// The associated data a Sealed is authenticated with: the key id, the
/// algorithm and the field's `data`, each length-prefixed (u32 big-endian)
/// so no two triples encode alike, behind a fixed domain-separation label.
///
/// Layout (every integer is a u32, 4 bytes, big-endian):
///
/// ```text
/// AAD_LABEL (21 bytes, constant)
/// u32 len(key_id)   key_id (UTF-8 bytes)
/// u32 kem           u32 kdf          u32 aead      (the protobuf enum numbers)
/// u32 len(data)  data
/// ```
///
/// Worked example: `key_id` `"k1"`, the suite (kem 1, kdf 1, aead 1),
/// `data` `b"session:abc"`: 54 bytes.
///
/// ```text
/// 616e677a6172722d7365616c656420763120616164   AAD_LABEL "angzarr-sealed v1 aad" (21)
/// 00000002                                     len(key_id) = 2
/// 6b31                                         "k1"
/// 00000001                                     kem  = KEM_DHKEM_X25519_HKDF_SHA256 (1)
/// 00000001                                     kdf  = KDF_HKDF_SHA256 (1)
/// 00000001                                     aead = AEAD_CHACHA20_POLY1305 (1)
/// 0000000b                                     len(data) = 11
/// 73657373696f6e3a616263                       "session:abc"
/// ```
///
/// Unambiguous because the label has a fixed length, the three enum
/// numbers have a fixed width, and each variable field (`key_id`,
/// `data`) is preceded by its own length, so no byte of one field can be
/// read as part of another, and two different triples never encode to the
/// same bytes (`("ab", "c")` and `("a", "bc")` differ in their length
/// words). The label separates these bytes from any other use of the same
/// key. A `key_id` or `data` longer than `u32::MAX` bytes is refused
/// ([`SealError::TooLong`]).
///
/// `data` names where the value lives (for example the message type and
/// field, and the subject), so a value cannot be moved to another field.
///
/// # Errors
/// [`SealError::TooLong`] for a `key_id` or `data` over `u32::MAX` bytes.
pub fn associated_data(
    key_id: &KeyId,
    algorithm: &Algorithm,
    data: &[u8],
) -> Result<Vec<u8>, SealError> {
    let key_id = key_id.as_str().as_bytes();
    let mut out = Vec::with_capacity(AAD_LABEL.len() + 24 + key_id.len() + data.len());
    out.extend_from_slice(AAD_LABEL);
    out.extend_from_slice(&length_word("key_id", key_id.len())?);
    out.extend_from_slice(key_id);
    for number in [algorithm.kem, algorithm.kdf, algorithm.aead] {
        out.extend_from_slice(&number.to_be_bytes());
    }
    out.extend_from_slice(&length_word("data", data.len())?);
    out.extend_from_slice(data);
    Ok(out)
}

/// Seals `plaintext` to `recipient`: RFC 9180 `SealBase` with a fresh
/// ephemeral key, `info` this crate's fixed label, and
/// [`associated_data`] as the AEAD's aad.
///
/// # Errors
/// [`SealError::WeakSharedSecret`] when `recipient` is a low-order point
/// (an all-zero shared secret is refused, RFC 9180 section 7.1.4);
/// [`SealError::RngFailed`]; [`SealError::EncryptionFailed`];
/// [`SealError::TooLong`].
///
/// Base mode authenticates no sender: anyone holding the recipient's public
/// key can make a Sealed for any `key_id` and `data` (see the crate's
/// "Security properties").
pub fn seal(
    recipient: &RecipientPublicKey,
    key_id: &KeyId,
    data: &[u8],
    plaintext: &[u8],
) -> Result<Sealed, SealError> {
    let ephemeral = RecipientPrivateKey::generate()?;
    seal_with_ephemeral(&ephemeral, recipient, key_id, data, plaintext)
}

/// Opens a [`Sealed`] with the recipient's private key: RFC 9180 `OpenBase`.
/// The `data` must be the one it was sealed with.
///
/// # Errors
/// [`OpenError::UnsupportedAlgorithm`], [`OpenError::Malformed`] (a wrong
/// size), and [`OpenError::Failed`] for every authentication failure: a
/// changed ciphertext, encapsulated key, key id, algorithm or data, or
/// another key. A non-empty `nonce` is [`OpenError::Malformed`]: an HPKE
/// value has none, and ignoring it would give one value two encodings.
pub fn open(
    private: &RecipientPrivateKey,
    sealed: &Sealed,
    data: &[u8],
) -> Result<Plaintext, OpenError> {
    let algorithm = sealed
        .algorithm
        .as_ref()
        .ok_or(OpenError::UnsupportedAlgorithm)?;
    if *algorithm != suite() {
        return Err(OpenError::UnsupportedAlgorithm);
    }
    if !sealed.nonce.is_empty() {
        return Err(OpenError::Malformed {
            field: "nonce",
            why: "not empty",
        });
    }
    let enc: [u8; 32] =
        sealed
            .encapsulated_key
            .as_ref()
            .try_into()
            .map_err(|_| OpenError::Malformed {
                field: "encapsulated_key",
                why: "not 32 bytes",
            })?;
    let key_id = KeyId::new(sealed.key_id.as_str());
    let aad = associated_data(&key_id, algorithm, data).map_err(too_long)?;
    let shared = Zeroizing::new(rfc9180::decap(&enc, private)?);
    let mut schedule = rfc9180::key_schedule(&shared, INFO);
    schedule.open(&aad, &sealed.ciphertext).map(Plaintext::from)
}

/// A [`SealError`] of the associated data, seen from an open: the field
/// that is too long is named as it is.
pub(crate) fn too_long(error: SealError) -> OpenError {
    match error {
        SealError::TooLong { field } => OpenError::Malformed {
            field,
            why: "longer than 4294967295 bytes",
        },
        _ => OpenError::Failed,
    }
}

/// Seals with a caller-chosen ephemeral key: the one path `seal` and the
/// tests (the crate's known answer) share. Private: a repeated ephemeral key
/// is a repeated key.
fn seal_with_ephemeral(
    ephemeral: &RecipientPrivateKey,
    recipient: &RecipientPublicKey,
    key_id: &KeyId,
    data: &[u8],
    plaintext: &[u8],
) -> Result<Sealed, SealError> {
    let algorithm = suite();
    let aad = associated_data(key_id, &algorithm, data)?;
    let (shared, enc) = rfc9180::encap(recipient, ephemeral)?;
    let shared = Zeroizing::new(shared);
    let mut schedule = rfc9180::key_schedule(&shared, INFO);
    let ciphertext = schedule.seal(&aad, plaintext)?;
    Ok(Sealed {
        key_id: key_id.as_str().to_string(),
        algorithm: Some(algorithm),
        encapsulated_key: enc.to_vec().into(),
        nonce: Default::default(),
        ciphertext: ciphertext.into(),
    })
}

#[cfg(test)]
#[path = "seal.test.rs"]
mod tests;
