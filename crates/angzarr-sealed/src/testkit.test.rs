//! Shared fixtures of the tests.

use crate::{KeyId, RecipientPrivateKey, RecipientPublicKey};

/// Hex text to bytes.
pub(crate) fn unhex(text: &str) -> Vec<u8> {
    assert!(text.len() % 2 == 0, "odd hex: {text}");
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect()
}

/// Hex text of exactly N bytes.
pub(crate) fn unhex_n<const N: usize>(text: &str) -> [u8; N] {
    unhex(text).try_into().expect("the vector's size")
}

/// A recipient key pair from fixed bytes (never a secret of anything).
pub(crate) fn recipient() -> (RecipientPrivateKey, RecipientPublicKey) {
    let private = RecipientPrivateKey::from_bytes([7; 32]);
    let public = private.public_key();
    (private, public)
}

pub(crate) fn key_id() -> KeyId {
    KeyId::new("launcher/2026-11")
}

pub(crate) const DATA: &[u8] = b"RunEnqueued.env/ANTHROPIC_API_KEY@projects/p/sessions/s";
pub(crate) const SECRET: &[u8] = b"sk-ant-not-a-real-key-0123456789";
