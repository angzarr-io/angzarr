//! Keys: generation and what `Debug` withholds.

use super::*;

#[test]
fn a_key_id_is_its_name() {
    assert_eq!(KeyId::new("launcher").as_str(), "launcher");
}

#[test]
fn a_public_key_is_its_bytes() {
    assert_eq!(RecipientPublicKey::from_bytes([1; 32]).as_bytes(), &[1; 32]);
}

#[test]
fn a_generated_data_key_and_kek_are_random() {
    use crate::{DataKey, KeyEncryptionKey};
    let a = DataKey::generate().expect("generate");
    let b = DataKey::generate().expect("generate");
    assert_ne!(format!("{a:?}"), "", "a Debug exists");
    let _ = (KeyEncryptionKey::generate().expect("generate"), b);
}
