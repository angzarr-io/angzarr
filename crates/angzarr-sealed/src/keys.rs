//! The keys of a sealed field's consumer, and the name that identifies one.

use std::fmt;

use zeroize::Zeroize;

use ring::rand::{SecureRandom, SystemRandom};

use crate::SealError;

/// `N` bytes from the system random source.
pub(crate) fn random<const N: usize>() -> Result<[u8; N], SealError> {
    let mut bytes = [0u8; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| SealError::RngFailed)?;
    Ok(bytes)
}

/// 32 random bytes written straight into a heap allocation, so the key
/// never exists as a copy on the stack.
pub(crate) fn random_boxed() -> Result<Box<[u8; 32]>, SealError> {
    let mut bytes = Box::new([0u8; 32]);
    SystemRandom::new()
        .fill(bytes.as_mut())
        .map_err(|_| SealError::RngFailed)?;
    Ok(bytes)
}

/// Names a recipient key (or a key-encryption key). It is the owner's
/// choice of text, carried in the clear and bound as associated data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyId(String);

impl KeyId {
    /// A key id of the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A recipient's X25519 public key (32 bytes, RFC 7748): what a producer
/// seals to. Not secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecipientPublicKey([u8; 32]);

impl RecipientPublicKey {
    /// The key of these 32 bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The 32 bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A recipient's X25519 private key (32 bytes): what opens a sealed value.
/// Its bytes are wiped when it is dropped, and neither `Debug` nor any other
/// formatting renders them. No `Clone`, no `Display`, no serialization.
pub struct RecipientPrivateKey {
    // Boxed: the key's one home is this allocation, which is wiped on drop;
    // moving the key moves a pointer, not 32 bytes of secret.
    bytes: Box<[u8; 32]>,
}

impl RecipientPrivateKey {
    /// A fresh key from the system random source.
    pub fn generate() -> Result<Self, SealError> {
        Ok(Self {
            bytes: random_boxed()?,
        })
    }

    /// The key of these 32 bytes (X25519 clamps them on use). The caller's
    /// array is copied, and stays the caller's to wipe.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self {
            bytes: Box::new(bytes),
        }
    }

    /// The matching public key.
    pub fn public_key(&self) -> RecipientPublicKey {
        RecipientPublicKey(x25519_dalek::x25519(
            *self.bytes,
            x25519_dalek::X25519_BASEPOINT_BYTES,
        ))
    }

    /// The raw bytes, for the key's owner to store; the name says it.
    pub fn expose_secret(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl fmt::Debug for RecipientPrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecipientPrivateKey")
            .finish_non_exhaustive()
    }
}

impl Drop for RecipientPrivateKey {
    fn drop(&mut self) {
        self.bytes.as_mut().zeroize();
    }
}

#[cfg(test)]
#[path = "keys.test.rs"]
mod tests;
