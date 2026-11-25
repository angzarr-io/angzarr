//! Data keys: sealing under one, wrapping it under a key-encryption key, and
//! shredding. Why: crypto-shredding is only real if destroying the key (or
//! its wrapped record) makes what it sealed unrecoverable, and a wrapped key
//! must not move to another subject.

use super::*;
use crate::testkit::{key_id, DATA, SECRET};

const SUBJECT: &[u8] = b"projects/p/sessions/s";

fn kek() -> KeyEncryptionKey {
    KeyEncryptionKey::from_bytes([3; 32])
}

#[test]
fn a_wrapped_data_key_unwraps_to_a_key_that_opens_what_the_original_sealed() {
    let data = DataKey::generate().expect("generate");
    let sealed = data.seal(&key_id(), DATA, SECRET).expect("seal");
    let wrapped = wrap(&kek(), &key_id(), SUBJECT, &data).expect("wrap");
    let again = unwrap(&kek(), &wrapped, SUBJECT).expect("unwrap");
    assert_eq!(again.open(&sealed, DATA), Ok(SECRET.to_vec()));
}

#[test]
fn a_wrapped_key_names_its_key_and_carries_a_nonce_and_a_tag_and_not_the_key() {
    let data = DataKey::from_bytes([5; 32]);
    let wrapped = wrap(&kek(), &key_id(), SUBJECT, &data).expect("wrap");
    assert_eq!(wrapped.key_id, key_id().as_str());
    assert_eq!(wrapped.nonce.len(), 12);
    assert_eq!(wrapped.ciphertext.len(), 32 + 16);
    assert!(!wrapped.ciphertext.windows(32).any(|w| w == [5u8; 32]));
}

#[test]
fn two_wraps_of_one_key_differ() {
    let data = DataKey::from_bytes([5; 32]);
    let a = wrap(&kek(), &key_id(), SUBJECT, &data).expect("wrap");
    let b = wrap(&kek(), &key_id(), SUBJECT, &data).expect("wrap");
    assert_ne!(a.nonce, b.nonce);
    assert_ne!(a.ciphertext, b.ciphertext);
}

#[test]
fn each_changed_field_of_a_wrapped_key_fails_to_unwrap() {
    let data = DataKey::from_bytes([5; 32]);
    let good = wrap(&kek(), &key_id(), SUBJECT, &data).expect("wrap");

    let mut w = good.clone();
    let mut c = w.ciphertext.to_vec();
    c[0] ^= 1;
    w.ciphertext = c.into();
    assert_eq!(unwrap(&kek(), &w, SUBJECT).unwrap_err(), WrapError::Failed);

    let mut w = good.clone();
    let mut n = w.nonce.to_vec();
    n[0] ^= 1;
    w.nonce = n.into();
    assert_eq!(unwrap(&kek(), &w, SUBJECT).unwrap_err(), WrapError::Failed);

    let mut w = good.clone();
    w.key_id = "kek/other".to_string();
    assert_eq!(unwrap(&kek(), &w, SUBJECT).unwrap_err(), WrapError::Failed);

    assert_eq!(
        unwrap(&kek(), &good, b"projects/p/sessions/other").unwrap_err(),
        WrapError::Failed
    );
}

#[test]
fn another_key_encryption_key_does_not_unwrap() {
    let data = DataKey::from_bytes([5; 32]);
    let wrapped = wrap(&kek(), &key_id(), SUBJECT, &data).expect("wrap");
    let other = KeyEncryptionKey::from_bytes([4; 32]);
    assert_eq!(
        unwrap(&other, &wrapped, SUBJECT).unwrap_err(),
        WrapError::Failed
    );
}

#[test]
fn a_wrong_size_nonce_is_malformed_not_a_panic() {
    let data = DataKey::from_bytes([5; 32]);
    let mut wrapped = wrap(&kek(), &key_id(), SUBJECT, &data).expect("wrap");
    wrapped.nonce = vec![0; 11].into();
    assert_eq!(
        unwrap(&kek(), &wrapped, SUBJECT).unwrap_err(),
        WrapError::Malformed {
            field: "nonce",
            why: "not 12 bytes"
        }
    );
}

#[test]
fn a_data_key_sealed_value_fails_each_tamper() {
    let data = DataKey::from_bytes([5; 32]);
    let good = data.seal(&key_id(), DATA, SECRET).expect("seal");
    assert!(good.encapsulated_key.is_empty());
    assert_eq!(good.nonce.len(), 12);

    let mut s = good.clone();
    let mut c = s.ciphertext.to_vec();
    c[0] ^= 1;
    s.ciphertext = c.into();
    assert_eq!(data.open(&s, DATA), Err(OpenError::Failed));

    let mut s = good.clone();
    let mut n = s.nonce.to_vec();
    n[0] ^= 1;
    s.nonce = n.into();
    assert_eq!(data.open(&s, DATA), Err(OpenError::Failed));

    let mut s = good.clone();
    s.key_id = "other".to_string();
    assert_eq!(data.open(&s, DATA), Err(OpenError::Failed));

    assert_eq!(data.open(&good, b"another field"), Err(OpenError::Failed));
    assert_eq!(
        DataKey::from_bytes([6; 32]).open(&good, DATA),
        Err(OpenError::Failed)
    );
}

#[test]
fn a_destroyed_data_key_seals_and_opens_nothing() {
    let mut data = DataKey::from_bytes([5; 32]);
    let sealed = data.seal(&key_id(), DATA, SECRET).expect("seal");
    data.destroy();
    assert!(data.is_destroyed());
    assert_eq!(data.open(&sealed, DATA), Err(OpenError::KeyDestroyed));
    assert_eq!(
        data.seal(&key_id(), DATA, SECRET).unwrap_err(),
        SealError::KeyDestroyed
    );
}

/// The shred: nothing but the wrapped record and the key-encryption key
/// recovers a data key; with the record gone (or the key-encryption key
/// replaced), the sealed value is unrecoverable by any key tried.
#[test]
fn shredding_the_wrapped_record_leaves_the_sealed_value_unrecoverable() {
    let data = DataKey::generate().expect("generate");
    let sealed = data.seal(&key_id(), DATA, SECRET).expect("seal");
    let wrapped = wrap(&kek(), &key_id(), SUBJECT, &data).expect("wrap");
    drop(data);
    drop(wrapped);
    let guess = DataKey::from_bytes([0; 32]);
    assert_eq!(guess.open(&sealed, DATA), Err(OpenError::Failed));
}

#[test]
fn the_keys_debug_renders_no_byte() {
    let data = DataKey::from_bytes([0xab; 32]);
    let kek = KeyEncryptionKey::from_bytes([0xcd; 32]);
    for shown in [format!("{data:?}"), format!("{kek:?}")] {
        assert!(!shown.contains("ab") || shown.contains("Key"), "{shown}");
        assert!(
            !shown.contains("171") && !shown.contains("205"),
            "no decimal bytes: {shown}"
        );
        assert!(
            !shown.contains("abab") && !shown.contains("cdcd"),
            "{shown}"
        );
    }
}
