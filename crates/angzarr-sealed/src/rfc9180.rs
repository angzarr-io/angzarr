//! The steps of RFC 9180 (HPKE) this crate's one suite is made of, public so
//! that the RFC's Appendix A.2 test vectors can hold each, byte for byte.
//! The suite: DHKEM(X25519, HKDF-SHA256) (kem_id 32), HKDF-SHA256 (kdf_id 1),
//! ChaCha20-Poly1305 (aead_id 3), base mode (mode 0).
//!
//! HKDF's extract is `ring::hmac` over the salt (ring's `hkdf::Prk` does not
//! give its bytes back, and the vectors name the PRK) and its expand is
//! `ring::hkdf::Prk::new_less_safe(..).expand(..)`, whose `info` is a list of
//! slices, which is `LabeledExpand`'s concatenation without a copy.

use std::fmt;

use crate::{OpenError, RecipientPrivateKey, RecipientPublicKey, SealError};

/// `suite_id` of the KEM's labels (RFC 9180 section 4.1): `"KEM" || I2OSP(32, 2)`.
pub const KEM_SUITE_ID: [u8; 5] = *b"KEM\x00\x20";

/// `suite_id` of the key schedule's labels (RFC 9180 section 5.1):
/// `"HPKE" || I2OSP(kem_id, 2) || I2OSP(kdf_id, 2) || I2OSP(aead_id, 2)`.
pub const HPKE_SUITE_ID: [u8; 10] = *b"HPKE\x00\x20\x00\x01\x00\x03";

/// `LabeledExtract(salt, label, ikm)` (RFC 9180 section 4): HKDF-Extract of
/// `"HPKE-v1" || suite_id || label || ikm`. Returns the 32-byte PRK.
pub fn labeled_extract(_suite_id: &[u8], _salt: &[u8], _label: &[u8], _ikm: &[u8]) -> [u8; 32] {
    todo!("HMAC-SHA256(salt, labeled ikm)")
}

/// `LabeledExpand(prk, label, info, L)` (RFC 9180 section 4): HKDF-Expand of
/// `I2OSP(L, 2) || "HPKE-v1" || suite_id || label || info`.
pub fn labeled_expand(
    _suite_id: &[u8],
    _prk: &[u8; 32],
    _label: &[u8],
    _info: &[u8],
    _len: usize,
) -> Vec<u8> {
    todo!("HKDF-Expand through ring")
}

/// `DeriveKeyPair(ikm)` for DHKEM(X25519) (RFC 9180 section 7.1.3).
pub fn derive_key_pair(_ikm: &[u8]) -> (RecipientPrivateKey, RecipientPublicKey) {
    todo!("dkp_prk, then sk, then pk")
}

/// `Encap(pkR)` with the given ephemeral private key (RFC 9180 section 4.1):
/// returns `(shared_secret, enc)`, `enc` the ephemeral public key. The
/// all-zero Diffie-Hellman output is refused ([`SealError::WeakSharedSecret`]).
pub fn encap(
    _recipient: &RecipientPublicKey,
    _ephemeral: &RecipientPrivateKey,
) -> Result<([u8; 32], [u8; 32]), SealError> {
    todo!("ExtractAndExpand(DH(skE, pkR), enc || pkR)")
}

/// `Decap(enc, skR)` (RFC 9180 section 4.1): the shared secret. The all-zero
/// Diffie-Hellman output is refused ([`OpenError::Failed`]).
pub fn decap(_enc: &[u8; 32], _recipient: &RecipientPrivateKey) -> Result<[u8; 32], OpenError> {
    todo!("ExtractAndExpand(DH(skR, pkE), enc || pkR)")
}

/// `key_schedule_context` for base mode (RFC 9180 section 5.1): `mode ||
/// psk_id_hash || info_hash`.
pub fn key_schedule_context(_info: &[u8]) -> Vec<u8> {
    todo!("0x00 || LabeledExtract(\"\", \"psk_id_hash\", \"\") || LabeledExtract(\"\", \"info_hash\", info)")
}

/// An HPKE encryption context (RFC 9180 section 5.1), its sequence number
/// counting each seal or open. Its `Debug` renders no key.
pub struct Schedule {
    /// The extracted `secret` (RFC 9180 section 5.1).
    pub secret: [u8; 32],
    /// The AEAD key.
    pub key: [u8; 32],
    /// The base nonce.
    pub base_nonce: [u8; 12],
    /// The exporter secret.
    pub exporter_secret: [u8; 32],
    sequence: u64,
}

/// `KeySchedule(mode_base, shared_secret, info, "", "")` (RFC 9180 section 5.1).
pub fn key_schedule(_shared_secret: &[u8; 32], _info: &[u8]) -> Schedule {
    todo!("secret, key, base_nonce, exporter_secret")
}

impl Schedule {
    /// The nonce of sequence number `seq`: `base_nonce XOR I2OSP(seq, 12)`
    /// (RFC 9180 section 5.2).
    pub fn nonce(&self, _seq: u64) -> [u8; 12] {
        todo!("xor")
    }

    /// `ctx.Seal(aad, pt)` at the next sequence number (RFC 9180 section 5.2).
    pub fn seal(&mut self, _aad: &[u8], _plaintext: &[u8]) -> Vec<u8> {
        todo!("ChaCha20-Poly1305 at nonce(sequence), then sequence += 1")
    }

    /// `ctx.Open(aad, ct)` at the next sequence number.
    pub fn open(&mut self, _aad: &[u8], _ciphertext: &[u8]) -> Result<Vec<u8>, OpenError> {
        todo!("ChaCha20-Poly1305 open")
    }

    /// The sequence number the next seal or open uses.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Sets the sequence number the next seal or open uses: the RFC's vectors
    /// give sequence numbers 0, 1, 2, 4, 255 and 256, not a run.
    pub fn at_sequence(&mut self, _seq: u64) {
        todo!("set it")
    }

    /// `ctx.Export(exporter_context, L)` (RFC 9180 section 5.3).
    pub fn export(&self, _exporter_context: &[u8], _len: usize) -> Vec<u8> {
        todo!("LabeledExpand(exporter_secret, \"sec\", exporter_context, L)")
    }
}

impl fmt::Debug for Schedule {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("name the type, no byte")
    }
}

#[cfg(test)]
#[path = "rfc9180.test.rs"]
mod tests;
