//! Rotation: a set of keys by id, one current. Why: a key must be
//! replaceable without losing what was sealed to its predecessor, and an
//! id the holder does not hold must be told apart from tampering.

use super::*;
use crate::testkit::{DATA, SECRET};
use crate::{open, KeyId, RecipientPrivateKey};

fn id(name: &str) -> KeyId {
    KeyId::new(name)
}

fn key(byte: u8) -> RecipientPrivateKey {
    RecipientPrivateKey::from_bytes([byte; 32])
}

fn set() -> KeySet {
    KeySet::new(id("2026-11"), key(1))
}

#[test]
fn a_new_set_holds_one_key_which_is_current() {
    let set = set();
    assert_eq!(set.current_id(), &id("2026-11"));
    assert_eq!(set.ids(), vec![&id("2026-11")]);
}

#[test]
fn the_current_public_key_is_the_current_private_keys() {
    assert_eq!(set().current_public_key(), key(1).public_key());
}

#[test]
fn a_seal_is_to_the_current_key_named_by_its_id_and_opens_with_the_set() {
    let set = set();
    let sealed = set.seal(DATA, SECRET).expect("seal");
    assert_eq!(sealed.key_id, "2026-11");
    assert_eq!(open_with(&set, &sealed, DATA), Ok(SECRET.to_vec()));
    assert_eq!(open(&key(1), &sealed, DATA), Ok(SECRET.to_vec()));
}

#[test]
fn a_value_sealed_before_a_rotation_still_opens_after_it() {
    let mut set = set();
    let old = set.seal(DATA, SECRET).expect("seal");
    set.insert(id("2026-12"), key(2));
    set.set_current(&id("2026-12")).expect("set current");
    let new = set.seal(DATA, SECRET).expect("seal");
    assert_eq!(new.key_id, "2026-12");
    assert_eq!(open_with(&set, &old, DATA), Ok(SECRET.to_vec()));
    assert_eq!(open_with(&set, &new, DATA), Ok(SECRET.to_vec()));
}

#[test]
fn inserting_a_key_does_not_make_it_current() {
    let mut set = set();
    set.insert(id("2026-12"), key(2));
    assert_eq!(set.current_id(), &id("2026-11"));
    assert_eq!(set.ids().len(), 2);
}

#[test]
fn a_removed_key_no_longer_opens_what_was_sealed_to_it() {
    let mut set = set();
    let old = set.seal(DATA, SECRET).expect("seal");
    set.insert(id("2026-12"), key(2));
    set.set_current(&id("2026-12")).expect("set current");
    set.remove(&id("2026-11")).expect("remove");
    assert_eq!(
        open_with(&set, &old, DATA),
        Err(OpenError::UnknownKey {
            key_id: "2026-11".to_string()
        })
    );
}

#[test]
fn an_unknown_key_id_is_its_own_error_not_a_failure() {
    let set = set();
    let mut sealed = set.seal(DATA, SECRET).expect("seal");
    sealed.key_id = "elsewhere".to_string();
    assert_eq!(
        open_with(&set, &sealed, DATA),
        Err(OpenError::UnknownKey {
            key_id: "elsewhere".to_string()
        })
    );
}

#[test]
fn a_known_id_with_an_altered_value_is_a_failure_not_an_unknown_key() {
    let set = set();
    let mut sealed = set.seal(DATA, SECRET).expect("seal");
    let mut bytes = sealed.ciphertext.to_vec();
    bytes[0] ^= 1;
    sealed.ciphertext = bytes.into();
    assert_eq!(open_with(&set, &sealed, DATA), Err(OpenError::Failed));
}

#[test]
fn a_known_id_whose_key_is_not_the_one_sealed_to_is_a_failure() {
    let mine = set();
    let theirs = KeySet::new(id("2026-11"), key(9));
    let sealed = theirs.seal(DATA, SECRET).expect("seal");
    assert_eq!(open_with(&mine, &sealed, DATA), Err(OpenError::Failed));
}

#[test]
fn the_current_key_cannot_be_removed_and_an_unknown_one_cannot_be_current() {
    let mut set = set();
    assert_eq!(
        set.remove(&id("2026-11")),
        Err(KeySetError::CurrentKey {
            key_id: "2026-11".to_string()
        })
    );
    assert_eq!(
        set.set_current(&id("nope")),
        Err(KeySetError::NoSuchKey {
            key_id: "nope".to_string()
        })
    );
    assert_eq!(
        set.remove(&id("nope")),
        Err(KeySetError::NoSuchKey {
            key_id: "nope".to_string()
        })
    );
}

#[test]
fn the_debug_names_the_ids_and_the_current_and_renders_no_key() {
    let mut set = KeySet::new(id("2026-11"), RecipientPrivateKey::from_bytes([0xab; 32]));
    set.insert(id("2026-12"), RecipientPrivateKey::from_bytes([0xcd; 32]));
    let shown = format!("{set:?}");
    assert!(
        shown.contains("2026-11") && shown.contains("2026-12"),
        "{shown}"
    );
    assert!(
        !shown.contains("abab") && !shown.contains("cdcd"),
        "{shown}"
    );
    assert!(
        !shown.contains("171") && !shown.contains("205"),
        "no decimal bytes: {shown}"
    );
}
