//! The keys of a sealed field's consumer, and the name that identifies one.

use std::fmt;

use zeroize::Zeroize;

use crate::SealError;

/// Names a recipient key (or a key-encryption key). It is the owner's
/// choice of text, carried in the clear and bound as associated data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    bytes: [u8; 32],
}

impl RecipientPrivateKey {
    /// A fresh key from the system random source.
    pub fn generate() -> Result<Self, SealError> {
        todo!("fill 32 bytes from ring::rand::SystemRandom")
    }

    /// The key of these 32 bytes (X25519 clamps them on use).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    /// The matching public key.
    pub fn public_key(&self) -> RecipientPublicKey {
        todo!("x25519(sk, basepoint)")
    }

    /// The raw bytes, for the key's owner to store; the name says it.
    pub fn expose_secret(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl fmt::Debug for RecipientPrivateKey {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("print the type's name and nothing of the bytes")
    }
}

impl Drop for RecipientPrivateKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

#[cfg(test)]
#[path = "keys.test.rs"]
mod tests;
