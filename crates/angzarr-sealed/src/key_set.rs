//! A set of recipient private keys by id, one current: rotation.

use std::collections::BTreeMap;
use std::fmt;

use crate::{
    KeyId, KeySetError, OpenError, RecipientPrivateKey, RecipientPublicKey, SealError, Sealed,
};

/// The private keys a consumer holds, by [`KeyId`], one of them current.
///
/// New values are sealed to the current key's public key; a value sealed
/// to any held key still opens, so a key is rotated by adding the new one,
/// marking it current, distributing its public key, and removing the old
/// one once nothing sealed to it remains. `Debug` lists the ids and the
/// current one, never a key.
pub struct KeySet {
    keys: BTreeMap<KeyId, RecipientPrivateKey>,
    current: KeyId,
}

impl KeySet {
    /// A set of one key, which is current.
    pub fn new(id: KeyId, key: RecipientPrivateKey) -> Self {
        Self {
            keys: BTreeMap::from([(id.clone(), key)]),
            current: id,
        }
    }

    /// Adds (or replaces) a key. It is not made current.
    pub fn insert(&mut self, _id: KeyId, _key: RecipientPrivateKey) {
        todo!("insert")
    }

    /// Marks a held key current.
    ///
    /// # Errors
    /// [`KeySetError::NoSuchKey`].
    pub fn set_current(&mut self, _id: &KeyId) -> Result<(), KeySetError> {
        todo!("check held, set")
    }

    /// Removes a key: values sealed to it no longer open.
    ///
    /// # Errors
    /// [`KeySetError::NoSuchKey`]; [`KeySetError::CurrentKey`] for the
    /// current key.
    pub fn remove(&mut self, _id: &KeyId) -> Result<(), KeySetError> {
        todo!("refuse the current, remove")
    }

    /// The current key's id.
    pub fn current_id(&self) -> &KeyId {
        &self.current
    }

    /// The current key's public key: what a producer seals to.
    pub fn current_public_key(&self) -> RecipientPublicKey {
        todo!("derive from the current private key")
    }

    /// The ids held, in order.
    pub fn ids(&self) -> Vec<&KeyId> {
        self.keys.keys().collect()
    }

    /// Seals to the current key, named by its id.
    pub fn seal(&self, _context: &[u8], _plaintext: &[u8]) -> Result<Sealed, SealError> {
        todo!("crate::seal to the current key")
    }
}

impl fmt::Debug for KeySet {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("ids and the current id, no key")
    }
}

/// Opens a [`Sealed`] with the key of the set its `key_id` names.
///
/// # Errors
/// [`OpenError::UnknownKey`] when the set holds no such key (the value is
/// not authenticated yet: nothing was tried); otherwise as
/// [`open`](crate::open).
pub fn open_with(_set: &KeySet, _sealed: &Sealed, _context: &[u8]) -> Result<Vec<u8>, OpenError> {
    todo!("lookup by sealed.key_id, then open")
}

#[cfg(test)]
#[path = "key_set.test.rs"]
mod tests;
