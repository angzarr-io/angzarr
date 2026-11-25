//! Per-subject data keys, wrapped under a key-encryption key.

use std::fmt;

use zeroize::Zeroize;

use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use zeroize::Zeroizing;

use crate::keys::{random, random_boxed};
use crate::seal::{associated_data, length_word, too_long};
use crate::{
    Aead, Algorithm, KeyId, OpenError, Plaintext, SealError, Sealed, WrapError, WrappedKey,
};

/// The ChaCha20-Poly1305 nonce size.
const NONCE_LEN: usize = 12;

/// The label that starts every wrapped key's associated data: the ASCII
/// text `angzarr-sealed v1 wrap`, 22 bytes. Different from the seal's label,
/// so a wrapped key and a sealed field never share aad bytes.
pub const WRAP_LABEL: &[u8] = b"angzarr-sealed v1 wrap";

/// The algorithm a data-key seal names: no kem, no kdf, ChaCha20-Poly1305.
fn data_key_algorithm() -> Algorithm {
    Algorithm {
        kem: 0,
        kdf: 0,
        aead: Aead::Chacha20Poly1305.into(),
    }
}

/// The AEAD key of 32 bytes.
fn aead_key(bytes: &[u8; 32]) -> LessSafeKey {
    LessSafeKey::new(
        UnboundKey::new(&aead::CHACHA20_POLY1305, bytes).expect("a 32-byte ChaCha20-Poly1305 key"),
    )
}

/// Encrypts `plaintext` under `key` with a fresh random nonce, returning
/// `(nonce, ciphertext || tag)`.
fn encrypt(
    key: &[u8; 32],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<([u8; NONCE_LEN], Vec<u8>), SealError> {
    let nonce_bytes: [u8; NONCE_LEN] = random()?;
    let mut out = plaintext.to_vec();
    aead_key(key)
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce_bytes),
            Aad::from(aad),
            &mut out,
        )
        .map_err(|_| SealError::EncryptionFailed)?;
    Ok((nonce_bytes, out))
}

/// Decrypts `ciphertext || tag` under `key`; `None` on any failure.
fn decrypt(
    key: &[u8; 32],
    nonce: &[u8; NONCE_LEN],
    aad: &[u8],
    ciphertext: &[u8],
) -> Option<Vec<u8>> {
    let mut buffer = ciphertext.to_vec();
    let len = aead_key(key)
        .open_in_place(
            Nonce::assume_unique_for_key(*nonce),
            Aad::from(aad),
            &mut buffer,
        )
        .ok()?
        .len();
    buffer.truncate(len);
    Some(buffer)
}

/// The associated data of a wrapped key: `WRAP_LABEL`, u32 length and bytes
/// of `key_id`, the aead number (u32), u32 length and bytes of `subject`,
/// every integer 4 bytes big-endian.
fn wrap_aad(key_id: &KeyId, aead: i32, subject: &[u8]) -> Result<Vec<u8>, SealError> {
    let id = key_id.as_str().as_bytes();
    let mut out = WRAP_LABEL.to_vec();
    out.extend_from_slice(&length_word("key_id", id.len())?);
    out.extend_from_slice(id);
    out.extend_from_slice(&aead.to_be_bytes());
    out.extend_from_slice(&length_word("subject", subject.len())?);
    out.extend_from_slice(subject);
    Ok(out)
}

/// A symmetric key (256 bits) for one subject's fields. Wiped when dropped
/// or [destroyed](DataKey::destroy); `Debug` renders no byte.
///
/// Every value sealed under it uses a fresh random 96-bit nonce, so a key
/// must seal at most 2^32 values (NIST SP 800-38D's bound for random
/// nonces); a subject's fields are far below it, and a key that could reach
/// it should be replaced by a new data key.
///
/// `destroy` wipes THIS instance only. Shredding what was sealed under the
/// key needs every copy of its [`WrappedKey`] erased, or the
/// [`KeyEncryptionKey`] destroyed: keep wrapped keys in a mutable key store,
/// never in an append-only event stream, where they cannot be erased.
pub struct DataKey {
    bytes: Box<[u8; 32]>,
    destroyed: bool,
}

impl DataKey {
    /// A fresh data key from the system random source.
    pub fn generate() -> Result<Self, SealError> {
        Ok(Self {
            bytes: random_boxed()?,
            destroyed: false,
        })
    }

    /// The key of these bytes (copied; the caller's array stays hers to wipe).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self {
            bytes: Box::new(bytes),
            destroyed: false,
        }
    }

    /// Wipes the key. It seals and opens nothing afterwards
    /// ([`SealError::KeyDestroyed`], [`OpenError::KeyDestroyed`]).
    pub fn destroy(&mut self) {
        self.bytes.as_mut().zeroize();
        self.destroyed = true;
    }

    /// Whether the key was destroyed.
    pub fn is_destroyed(&self) -> bool {
        self.destroyed
    }

    /// Seals `plaintext` under this key with ChaCha20-Poly1305 and a fresh
    /// random nonce: the result has no `encapsulated_key` and carries its
    /// `nonce`; `key_id`, the algorithm (its kem and kdf unspecified, its aead
    /// ChaCha20-Poly1305) and `data` are the aad
    /// ([`associated_data`](crate::associated_data)).
    pub fn seal(&self, key_id: &KeyId, data: &[u8], plaintext: &[u8]) -> Result<Sealed, SealError> {
        if self.destroyed {
            return Err(SealError::KeyDestroyed);
        }
        let algorithm = data_key_algorithm();
        let aad = associated_data(key_id, &algorithm, data)?;
        let (nonce, ciphertext) = encrypt(&self.bytes, &aad, plaintext)?;
        Ok(Sealed {
            key_id: key_id.as_str().to_string(),
            algorithm: Some(algorithm),
            encapsulated_key: Default::default(),
            nonce: nonce.to_vec().into(),
            ciphertext: ciphertext.into(),
        })
    }

    /// Opens what [`DataKey::seal`] sealed.
    ///
    /// # Errors
    /// A non-empty `encapsulated_key` is [`OpenError::Malformed`]: a
    /// data-key value has none, and ignoring it would give one value two
    /// encodings.
    pub fn open(&self, sealed: &Sealed, data: &[u8]) -> Result<Plaintext, OpenError> {
        if self.destroyed {
            return Err(OpenError::KeyDestroyed);
        }
        let algorithm = data_key_algorithm();
        if sealed.algorithm.as_ref() != Some(&algorithm) {
            return Err(OpenError::UnsupportedAlgorithm);
        }
        if !sealed.encapsulated_key.is_empty() {
            return Err(OpenError::Malformed {
                field: "encapsulated_key",
                why: "not empty",
            });
        }
        let nonce: [u8; NONCE_LEN] =
            sealed
                .nonce
                .as_ref()
                .try_into()
                .map_err(|_| OpenError::Malformed {
                    field: "nonce",
                    why: "not 12 bytes",
                })?;
        let aad = associated_data(&KeyId::new(sealed.key_id.as_str()), &algorithm, data)
            .map_err(too_long)?;
        decrypt(&self.bytes, &nonce, &aad, &sealed.ciphertext)
            .map(Plaintext::from)
            .ok_or(OpenError::Failed)
    }
}

impl fmt::Debug for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataKey")
            .field("destroyed", &self.destroyed)
            .finish_non_exhaustive()
    }
}

impl Drop for DataKey {
    fn drop(&mut self) {
        self.bytes.as_mut().zeroize();
    }
}

/// The long-lived symmetric key (256 bits) that wraps data keys. Wiped on
/// drop; `Debug` renders no byte.
///
/// Each wrap uses a fresh random 96-bit nonce, so a key-encryption key must
/// wrap at most 2^32 data keys (NIST SP 800-38D's bound for random nonces):
/// rotate it, by a new key under a new id, long before.
pub struct KeyEncryptionKey {
    bytes: Box<[u8; 32]>,
}

impl KeyEncryptionKey {
    /// A fresh key from the system random source.
    pub fn generate() -> Result<Self, SealError> {
        Ok(Self {
            bytes: random_boxed()?,
        })
    }

    /// The key of these bytes (copied; the caller's array stays hers to wipe).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self {
            bytes: Box::new(bytes),
        }
    }
}

impl fmt::Debug for KeyEncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyEncryptionKey").finish_non_exhaustive()
    }
}

impl Drop for KeyEncryptionKey {
    fn drop(&mut self) {
        self.bytes.as_mut().zeroize();
    }
}

/// Wraps a data key under `kek` for one subject: ChaCha20-Poly1305, a fresh
/// random nonce, `key_id` and `subject` as the aad, so a wrapped key cannot
/// be moved to another subject or another key id.
pub fn wrap(
    kek: &KeyEncryptionKey,
    key_id: &KeyId,
    subject: &[u8],
    data_key: &DataKey,
) -> Result<WrappedKey, SealError> {
    if data_key.destroyed {
        return Err(SealError::KeyDestroyed);
    }
    let aead = i32::from(Aead::Chacha20Poly1305);
    let aad = wrap_aad(key_id, aead, subject)?;
    let (nonce, ciphertext) = encrypt(&kek.bytes, &aad, data_key.bytes.as_ref())?;
    Ok(WrappedKey {
        key_id: key_id.as_str().to_string(),
        aead,
        nonce: nonce.to_vec().into(),
        ciphertext: ciphertext.into(),
    })
}

/// Unwraps a data key.
///
/// # Errors
/// [`WrapError::Failed`] for every authentication failure (altered record,
/// another key-encryption key, another subject);
/// [`WrapError::Malformed`] for a wrong size.
pub fn unwrap(
    kek: &KeyEncryptionKey,
    wrapped: &WrappedKey,
    subject: &[u8],
) -> Result<DataKey, WrapError> {
    if wrapped.aead != i32::from(Aead::Chacha20Poly1305) {
        return Err(WrapError::Malformed {
            field: "aead",
            why: "not ChaCha20-Poly1305",
        });
    }
    let nonce: [u8; NONCE_LEN] =
        wrapped
            .nonce
            .as_ref()
            .try_into()
            .map_err(|_| WrapError::Malformed {
                field: "nonce",
                why: "not 12 bytes",
            })?;
    let aad =
        wrap_aad(&KeyId::new(wrapped.key_id.as_str()), wrapped.aead, subject).map_err(|error| {
            match error {
                SealError::TooLong { field } => WrapError::Malformed {
                    field,
                    why: "longer than 4294967295 bytes",
                },
                _ => WrapError::Failed,
            }
        })?;
    let plain = Zeroizing::new(
        decrypt(&kek.bytes, &nonce, &aad, &wrapped.ciphertext).ok_or(WrapError::Failed)?,
    );
    let mut key = DataKey::from_bytes([0u8; 32]);
    if plain.len() != 32 {
        return Err(WrapError::Malformed {
            field: "ciphertext",
            why: "not a 32-byte key",
        });
    }
    key.bytes.copy_from_slice(&plain);
    Ok(key)
}

#[cfg(test)]
#[path = "wrap.test.rs"]
mod tests;
