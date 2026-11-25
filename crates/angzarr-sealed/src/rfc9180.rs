//! The steps of RFC 9180 (HPKE) this crate's one suite is made of, crate-private
//! (a caller-chosen ephemeral key, a settable sequence number and a
//! readable key schedule are hazards a public API must not offer) and held
//! by the tests to the RFC's Appendix A.2 test vectors, byte for byte.
//! The suite: DHKEM(X25519, HKDF-SHA256) (kem_id 32), HKDF-SHA256 (kdf_id 1),
//! ChaCha20-Poly1305 (aead_id 3), base mode (mode 0).
//!
//! HKDF's extract is `ring::hmac` over the salt (ring's `hkdf::Prk` does not
//! give its bytes back, and the vectors name the PRK) and its expand is
//! `ring::hkdf::Prk::new_less_safe(..).expand(..)`, whose `info` is a list of
//! slices, which is `LabeledExpand`'s concatenation without a copy.

use std::fmt;

use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::{hkdf, hmac};
use zeroize::{Zeroize, Zeroizing};

use crate::{OpenError, RecipientPrivateKey, RecipientPublicKey, SealError};

/// `suite_id` of the KEM's labels (RFC 9180 section 4.1): `"KEM" || I2OSP(32, 2)`.
pub const KEM_SUITE_ID: [u8; 5] = *b"KEM\x00\x20";

/// `suite_id` of the key schedule's labels (RFC 9180 section 5.1):
/// `"HPKE" || I2OSP(kem_id, 2) || I2OSP(kdf_id, 2) || I2OSP(aead_id, 2)`.
pub const HPKE_SUITE_ID: [u8; 10] = *b"HPKE\x00\x20\x00\x01\x00\x03";

/// The protocol version label every labeled step starts with.
const VERSION: &[u8] = b"HPKE-v1";

/// ChaCha20-Poly1305's tag, nonce and key sizes (RFC 9180 section 7.3).
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;

/// The output length of a `LabeledExpand`, as ring's `KeyType`.
struct Length(usize);

impl hkdf::KeyType for Length {
    fn len(&self) -> usize {
        self.0
    }
}

/// `LabeledExtract(salt, label, ikm)` (RFC 9180 section 4): HKDF-Extract of
/// `"HPKE-v1" || suite_id || label || ikm`. Returns the 32-byte PRK.
pub fn labeled_extract(suite_id: &[u8], salt: &[u8], label: &[u8], ikm: &[u8]) -> [u8; 32] {
    let labeled: Zeroizing<Vec<u8>> = Zeroizing::new([VERSION, suite_id, label, ikm].concat());
    let key = hmac::Key::new(hmac::HMAC_SHA256, salt);
    let tag = hmac::sign(&key, &labeled);
    let mut prk = [0u8; 32];
    prk.copy_from_slice(tag.as_ref());
    prk
}

/// `LabeledExpand(prk, label, info, L)` (RFC 9180 section 4): HKDF-Expand of
/// `I2OSP(L, 2) || "HPKE-v1" || suite_id || label || info`.
///
/// # Panics
/// When `len` is over 65535 (the two-byte length) or over HKDF-SHA256's
/// limit of 8160: this crate asks for 12, 32 and exporter lengths only.
pub fn labeled_expand(
    suite_id: &[u8],
    prk: &[u8; 32],
    label: &[u8],
    info: &[u8],
    len: usize,
) -> Vec<u8> {
    let length_word = u16::try_from(len).expect("L fits two bytes").to_be_bytes();
    let prk = hkdf::Prk::new_less_safe(hkdf::HKDF_SHA256, prk);
    let labeled = [&length_word[..], VERSION, suite_id, label, info];
    let mut out = vec![0u8; len];
    prk.expand(&labeled, Length(len))
        .expect("L within HKDF-SHA256's limit")
        .fill(&mut out)
        .expect("the buffer is L bytes");
    out
}

/// `LabeledExpand` of exactly `N` bytes.
fn expand_array<const N: usize>(
    suite_id: &[u8],
    prk: &[u8; 32],
    label: &[u8],
    info: &[u8],
) -> [u8; N] {
    let mut bytes = Zeroizing::new(labeled_expand(suite_id, prk, label, info, N));
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes);
    bytes.zeroize();
    out
}

/// X25519 (RFC 7748) of a private scalar and a public point.
fn dh(private: &[u8; 32], public: &[u8; 32]) -> Zeroizing<[u8; 32]> {
    Zeroizing::new(x25519_dalek::x25519(*private, *public))
}

/// Whether a Diffie-Hellman output is all zero, by an OR-fold with no
/// early exit (the output is a secret; ring's constant-time compare is
/// deprecated)
/// (RFC 7748 section 6.1, RFC 9180 section 7.1.4).
fn is_all_zero(shared: &[u8; 32]) -> bool {
    shared.iter().fold(0u8, |seen, byte| seen | byte) == 0
}

/// `DeriveKeyPair(ikm)` for DHKEM(X25519) (RFC 9180 section 7.1.3). Only the
/// vectors derive keys this way.
#[cfg(test)]
pub fn derive_key_pair(ikm: &[u8]) -> (RecipientPrivateKey, RecipientPublicKey) {
    let dkp_prk = Zeroizing::new(labeled_extract(&KEM_SUITE_ID, b"", b"dkp_prk", ikm));
    let sk = Zeroizing::new(expand_array::<32>(&KEM_SUITE_ID, &dkp_prk, b"sk", b""));
    let private = RecipientPrivateKey::from_bytes(*sk);
    let public = private.public_key();
    (private, public)
}

/// `ExtractAndExpand(dh, kem_context)` (RFC 9180 section 4.1).
fn extract_and_expand(dh: &[u8; 32], kem_context: &[u8]) -> [u8; 32] {
    let eae_prk = Zeroizing::new(labeled_extract(&KEM_SUITE_ID, b"", b"eae_prk", dh));
    expand_array::<32>(&KEM_SUITE_ID, &eae_prk, b"shared_secret", kem_context)
}

/// `Encap(pkR)` with the given ephemeral private key (RFC 9180 section 4.1):
/// returns `(shared_secret, enc)`, `enc` the ephemeral public key. The
/// all-zero Diffie-Hellman output is refused ([`SealError::WeakSharedSecret`]).
pub fn encap(
    recipient: &RecipientPublicKey,
    ephemeral: &RecipientPrivateKey,
) -> Result<([u8; 32], [u8; 32]), SealError> {
    let shared_dh = dh(ephemeral.expose_secret(), recipient.as_bytes());
    if is_all_zero(&shared_dh) {
        return Err(SealError::WeakSharedSecret);
    }
    let enc = *ephemeral.public_key().as_bytes();
    let kem_context = [&enc[..], &recipient.as_bytes()[..]].concat();
    Ok((extract_and_expand(&shared_dh, &kem_context), enc))
}

/// `Decap(enc, skR)` (RFC 9180 section 4.1): the shared secret. The all-zero
/// Diffie-Hellman output is refused ([`OpenError::Failed`]).
pub fn decap(enc: &[u8; 32], recipient: &RecipientPrivateKey) -> Result<[u8; 32], OpenError> {
    let shared_dh = dh(recipient.expose_secret(), enc);
    if is_all_zero(&shared_dh) {
        return Err(OpenError::Failed);
    }
    let kem_context = [&enc[..], &recipient.public_key().as_bytes()[..]].concat();
    Ok(extract_and_expand(&shared_dh, &kem_context))
}

/// `key_schedule_context` for base mode (RFC 9180 section 5.1): `mode ||
/// psk_id_hash || info_hash`.
pub fn key_schedule_context(info: &[u8]) -> Vec<u8> {
    let psk_id_hash = labeled_extract(&HPKE_SUITE_ID, b"", b"psk_id_hash", b"");
    let info_hash = labeled_extract(&HPKE_SUITE_ID, b"", b"info_hash", info);
    [&[0u8][..], &psk_id_hash, &info_hash].concat()
}

/// An HPKE encryption context (RFC 9180 section 5.1), its sequence number
/// counting each seal or open. Its `Debug` renders no key; its secrets are
/// wiped when it is dropped.
///
/// The fields are private; the tests, a child module, read them against the
/// vectors.
pub struct Schedule {
    // The extracted `secret` (RFC 9180 section 5.1).
    secret: [u8; 32],
    // The AEAD key.
    key: [u8; 32],
    // The base nonce.
    base_nonce: [u8; 12],
    // The exporter secret.
    exporter_secret: [u8; 32],
    sequence: u64,
}

/// `KeySchedule(mode_base, shared_secret, info, "", "")` (RFC 9180 section 5.1).
pub fn key_schedule(shared_secret: &[u8; 32], info: &[u8]) -> Schedule {
    let context = key_schedule_context(info);
    // psk is empty in base mode.
    let secret = labeled_extract(&HPKE_SUITE_ID, shared_secret, b"secret", b"");
    Schedule {
        key: expand_array::<KEY_LEN>(&HPKE_SUITE_ID, &secret, b"key", &context),
        base_nonce: expand_array::<NONCE_LEN>(&HPKE_SUITE_ID, &secret, b"base_nonce", &context),
        exporter_secret: expand_array::<32>(&HPKE_SUITE_ID, &secret, b"exp", &context),
        secret,
        sequence: 0,
    }
}

impl Schedule {
    /// The nonce of sequence number `seq`: `base_nonce XOR I2OSP(seq, 12)`
    /// (RFC 9180 section 5.2).
    pub fn nonce(&self, seq: u64) -> [u8; 12] {
        let mut nonce = self.base_nonce;
        for (byte, counter) in nonce[NONCE_LEN - 8..].iter_mut().zip(seq.to_be_bytes()) {
            *byte ^= counter;
        }
        nonce
    }

    /// The AEAD key and the nonce of the current sequence number.
    fn aead(&self) -> (LessSafeKey, Nonce) {
        let unbound = UnboundKey::new(&aead::CHACHA20_POLY1305, &self.key)
            .expect("a 32-byte ChaCha20-Poly1305 key");
        let nonce = Nonce::assume_unique_for_key(self.nonce(self.sequence));
        (LessSafeKey::new(unbound), nonce)
    }

    /// The next sequence number. RFC 9180 section 5.2 refuses a seal or open
    /// past 2^64 - 1 (`MessageLimitReached`).
    fn advance(&mut self) -> Result<(), ()> {
        self.sequence = self.sequence.checked_add(1).ok_or(())?;
        Ok(())
    }

    /// `ctx.Seal(aad, pt)` at the next sequence number (RFC 9180 section 5.2).
    ///
    /// # Errors
    /// [`SealError::EncryptionFailed`]: the plaintext is beyond the AEAD's
    /// limit, or the sequence numbers are exhausted.
    pub fn seal(&mut self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SealError> {
        let (key, nonce) = self.aead();
        let mut out = plaintext.to_vec();
        key.seal_in_place_append_tag(nonce, Aad::from(aad), &mut out)
            .map_err(|_| SealError::EncryptionFailed)?;
        self.advance().map_err(|()| SealError::EncryptionFailed)?;
        Ok(out)
    }

    /// `ctx.Open(aad, ct)` at the next sequence number.
    pub fn open(&mut self, aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, OpenError> {
        let (key, nonce) = self.aead();
        let mut buffer = ciphertext.to_vec();
        let opened = key
            .open_in_place(nonce, Aad::from(aad), &mut buffer)
            .map(|plain| plain.len());
        match opened {
            Ok(len) => {
                buffer.truncate(len);
                self.advance().map_err(|()| OpenError::Failed)?;
                Ok(buffer)
            }
            Err(_) => Err(OpenError::Failed),
        }
    }

    /// The sequence number the next seal or open uses.
    #[cfg(test)]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Sets the sequence number the next seal or open uses: the RFC's vectors
    /// give sequence numbers 0, 1, 2, 4, 255 and 256, not a run. Test-only: a
    /// settable sequence number is how a nonce is reused.
    #[cfg(test)]
    pub fn at_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }

    /// `ctx.Export(exporter_context, L)` (RFC 9180 section 5.3). The suite
    /// uses no exporter; the vectors hold it.
    #[cfg(test)]
    pub fn export(&self, exporter_context: &[u8], len: usize) -> Vec<u8> {
        labeled_expand(
            &HPKE_SUITE_ID,
            &self.exporter_secret,
            b"sec",
            exporter_context,
            len,
        )
    }
}

impl Drop for Schedule {
    fn drop(&mut self) {
        self.secret.zeroize();
        self.key.zeroize();
        self.base_nonce.zeroize();
        self.exporter_secret.zeroize();
    }
}

impl fmt::Debug for Schedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Schedule")
            .field("sequence", &self.sequence)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "rfc9180.test.rs"]
mod tests;
