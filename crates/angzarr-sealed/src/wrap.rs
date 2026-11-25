//! Per-subject data keys, wrapped under a key-encryption key.

use std::fmt;

use zeroize::Zeroize;

use crate::{KeyId, OpenError, SealError, Sealed, WrapError, WrappedKey};

/// A symmetric key (256 bits) for one subject's fields. Wiped when dropped
/// or [destroyed](DataKey::destroy); `Debug` renders no byte.
pub struct DataKey {
    bytes: [u8; 32],
    destroyed: bool,
}

impl DataKey {
    /// A fresh data key from the system random source.
    pub fn generate() -> Result<Self, SealError> {
        todo!("32 random bytes")
    }

    /// The key of these bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self {
            bytes,
            destroyed: false,
        }
    }

    /// Wipes the key. It seals and opens nothing afterwards
    /// ([`SealError::KeyDestroyed`], [`OpenError::KeyDestroyed`]).
    pub fn destroy(&mut self) {
        todo!("zeroize the bytes and mark it destroyed")
    }

    /// Whether the key was destroyed.
    pub fn is_destroyed(&self) -> bool {
        self.destroyed
    }

    /// Seals `plaintext` under this key with ChaCha20-Poly1305 and a fresh
    /// random nonce: the result has no `encapsulated_key` and carries its
    /// `nonce`; `key_id`, the algorithm (its kem and kdf unspecified, its aead
    /// ChaCha20-Poly1305) and `context` are the aad
    /// ([`associated_data`](crate::associated_data)).
    pub fn seal(
        &self,
        _key_id: &KeyId,
        _context: &[u8],
        _plaintext: &[u8],
    ) -> Result<Sealed, SealError> {
        todo!("AEAD seal")
    }

    /// Opens what [`DataKey::seal`] sealed.
    pub fn open(&self, _sealed: &Sealed, _context: &[u8]) -> Result<Vec<u8>, OpenError> {
        todo!("AEAD open")
    }
}

impl fmt::Debug for DataKey {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("name the type and whether it is destroyed, no byte")
    }
}

impl Drop for DataKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

/// The long-lived symmetric key (256 bits) that wraps data keys. Wiped on
/// drop; `Debug` renders no byte.
pub struct KeyEncryptionKey {
    bytes: [u8; 32],
}

impl KeyEncryptionKey {
    /// A fresh key from the system random source.
    pub fn generate() -> Result<Self, SealError> {
        todo!("32 random bytes")
    }

    /// The key of these bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }
}

impl fmt::Debug for KeyEncryptionKey {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("name the type, no byte")
    }
}

impl Drop for KeyEncryptionKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

/// Wraps a data key under `kek` for one subject: ChaCha20-Poly1305, a fresh
/// random nonce, `key_id` and `subject` as the aad, so a wrapped key cannot
/// be moved to another subject or another key id.
pub fn wrap(
    _kek: &KeyEncryptionKey,
    _key_id: &KeyId,
    _subject: &[u8],
    _data_key: &DataKey,
) -> Result<WrappedKey, SealError> {
    todo!("AEAD seal of the 32 bytes")
}

/// Unwraps a data key.
///
/// # Errors
/// [`WrapError::Failed`] for every authentication failure (altered record,
/// another key-encryption key, another subject);
/// [`WrapError::Malformed`] for a wrong size.
pub fn unwrap(
    _kek: &KeyEncryptionKey,
    _wrapped: &WrappedKey,
    _subject: &[u8],
) -> Result<DataKey, WrapError> {
    todo!("AEAD open")
}

#[cfg(test)]
#[path = "wrap.test.rs"]
mod tests;
