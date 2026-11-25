//! The plaintext an open returns: a secret, wiped when dropped.

use std::fmt;
use std::ops::Deref;

use zeroize::Zeroizing;

/// What [`open`](crate::open), [`open_with`](crate::open_with) and
/// [`DataKey::open`](crate::DataKey::open) return: the opened secret's bytes,
/// overwritten when this value is dropped. It dereferences to `[u8]`; `Debug`
/// renders its length alone, never a byte. Copying the bytes out (`to_vec`,
/// `into_vec`) leaves a copy the caller must wipe.
pub struct Plaintext(Zeroizing<Vec<u8>>);

impl Plaintext {
    /// The bytes, as an owned vector the caller now owns and must wipe.
    pub fn into_vec(mut self) -> Vec<u8> {
        std::mem::take(&mut *self.0)
    }
}

impl From<Vec<u8>> for Plaintext {
    fn from(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }
}

impl From<&[u8]> for Plaintext {
    fn from(bytes: &[u8]) -> Self {
        Self::from(bytes.to_vec())
    }
}

impl Deref for Plaintext {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl PartialEq for Plaintext {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_slice() == other.0.as_slice()
    }
}

impl Eq for Plaintext {}

impl fmt::Debug for Plaintext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Plaintext({} bytes)", self.0.len())
    }
}

#[cfg(test)]
#[path = "plaintext.test.rs"]
mod tests;
