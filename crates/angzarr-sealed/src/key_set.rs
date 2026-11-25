//! A set of recipient private keys by id, one current: rotation.

use std::collections::BTreeMap;
use std::fmt;

use crate::{
    KeyId, KeySetError, OpenError, Plaintext, RecipientPrivateKey, RecipientPublicKey, SealError,
    Sealed,
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

    /// Adds a key. It is not made current.
    ///
    /// # Errors
    /// [`KeySetError::AlreadyHeld`] when the id is held: replacing a key
    /// would silently orphan everything sealed to it (the current key
    /// included), so it is refused; remove it first.
    pub fn insert(&mut self, id: KeyId, key: RecipientPrivateKey) -> Result<(), KeySetError> {
        if self.keys.contains_key(&id) {
            return Err(KeySetError::AlreadyHeld {
                key_id: id.as_str().to_string(),
            });
        }
        self.keys.insert(id, key);
        Ok(())
    }

    /// Marks a held key current.
    ///
    /// # Errors
    /// [`KeySetError::NoSuchKey`].
    pub fn set_current(&mut self, id: &KeyId) -> Result<(), KeySetError> {
        if !self.keys.contains_key(id) {
            return Err(KeySetError::NoSuchKey {
                key_id: id.as_str().to_string(),
            });
        }
        self.current = id.clone();
        Ok(())
    }

    /// Removes a key: values sealed to it no longer open.
    ///
    /// # Errors
    /// [`KeySetError::NoSuchKey`]; [`KeySetError::CurrentKey`] for the
    /// current key.
    pub fn remove(&mut self, id: &KeyId) -> Result<(), KeySetError> {
        if *id == self.current {
            return Err(KeySetError::CurrentKey {
                key_id: id.as_str().to_string(),
            });
        }
        self.keys
            .remove(id)
            .map(drop)
            .ok_or_else(|| KeySetError::NoSuchKey {
                key_id: id.as_str().to_string(),
            })
    }

    /// The current key's id.
    pub fn current_id(&self) -> &KeyId {
        &self.current
    }

    /// The current key's public key: what a producer seals to.
    pub fn current_public_key(&self) -> RecipientPublicKey {
        self.keys[&self.current].public_key()
    }

    /// The ids held, in order.
    pub fn ids(&self) -> Vec<&KeyId> {
        self.keys.keys().collect()
    }

    /// Seals to the current key, named by its id.
    pub fn seal(&self, data: &[u8], plaintext: &[u8]) -> Result<Sealed, SealError> {
        crate::seal(&self.current_public_key(), &self.current, data, plaintext)
    }
}

impl fmt::Debug for KeySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeySet")
            .field(
                "ids",
                &self.keys.keys().map(KeyId::as_str).collect::<Vec<_>>(),
            )
            .field("current", &self.current.as_str())
            .finish_non_exhaustive()
    }
}

/// Opens a [`Sealed`] with the key of the set its `key_id` names.
///
/// # Errors
/// [`OpenError::UnknownKey`] when the set holds no such key (the value is
/// not authenticated yet: nothing was tried); otherwise as
/// [`open`](crate::open).
pub fn open_with(set: &KeySet, sealed: &Sealed, data: &[u8]) -> Result<Plaintext, OpenError> {
    match set.keys.get(&KeyId::new(sealed.key_id.as_str())) {
        Some(private) => crate::open(private, sealed, data),
        None => Err(OpenError::UnknownKey {
            key_id: sealed.key_id.clone(),
        }),
    }
}

#[cfg(test)]
#[path = "key_set.test.rs"]
mod tests;
