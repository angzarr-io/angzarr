//! Seal and open: the round trip, each tamper, the wrong key, the weak key,
//! and the whole path held to the RFC's vectors.
//! Why: a sealed secret is trusted by what it refuses; each field a reader
//! could rewrite must be one whose rewrite fails.

use super::*;
use crate::rfc9180_a2::*;
use crate::testkit::{key_id, recipient, unhex, unhex_n, DATA, SECRET as SECRET_TEXT};
use crate::{Aead, Kdf, Kem};
use crate::{AAD_LABEL, INFO};
use coded_diagnostic::Diagnostic;

fn sealed() -> (RecipientPrivateKey, Sealed) {
    let (private, public) = recipient();
    let sealed = seal(&public, &key_id(), DATA, SECRET_TEXT).expect("seal");
    (private, sealed)
}

#[test]
fn the_suite_is_the_one_the_rfc_names() {
    let a = suite();
    assert_eq!(a.kem(), Kem::DhkemX25519HkdfSha256);
    assert_eq!(a.kdf(), Kdf::HkdfSha256);
    assert_eq!(a.aead(), Aead::Chacha20Poly1305);
}

#[test]
fn a_sealed_names_its_key_and_suite_and_carries_an_ephemeral_key_and_a_tag() {
    let (_, sealed) = sealed();
    assert_eq!(sealed.key_id, key_id().as_str());
    assert_eq!(sealed.algorithm, Some(suite()));
    assert_eq!(sealed.encapsulated_key.len(), 32);
    assert!(sealed.nonce.is_empty(), "HPKE derives its nonce");
    assert_eq!(sealed.ciphertext.len(), SECRET_TEXT.len() + 16);
}

#[test]
fn a_sealed_opens_to_what_was_sealed() {
    let (private, sealed) = sealed();
    assert_eq!(open(&private, &sealed, DATA), Ok(SECRET_TEXT.to_vec()));
}

#[test]
fn an_empty_secret_round_trips() {
    let (private, public) = recipient();
    let sealed = seal(&public, &key_id(), DATA, b"").expect("seal");
    assert_eq!(open(&private, &sealed, DATA), Ok(Vec::new()));
}

#[test]
fn two_seals_of_one_secret_differ_so_nothing_is_learned_by_comparing() {
    let (_, public) = recipient();
    let a = seal(&public, &key_id(), DATA, SECRET_TEXT).expect("seal");
    let b = seal(&public, &key_id(), DATA, SECRET_TEXT).expect("seal");
    assert_ne!(a.encapsulated_key, b.encapsulated_key);
    assert_ne!(a.ciphertext, b.ciphertext);
}

#[test]
fn a_sealed_survives_the_wire() {
    use prost::Message;
    let (private, sealed) = sealed();
    let decoded = Sealed::decode(sealed.encode_to_vec().as_slice()).expect("decode");
    assert_eq!(open(&private, &decoded, DATA), Ok(SECRET_TEXT.to_vec()));
}

#[test]
fn a_changed_ciphertext_does_not_open() {
    let (private, mut sealed) = sealed();
    let mut bytes = sealed.ciphertext.to_vec();
    bytes[0] ^= 1;
    sealed.ciphertext = bytes.into();
    assert_eq!(open(&private, &sealed, DATA), Err(OpenError::Failed));
}

#[test]
fn a_changed_tag_does_not_open() {
    let (private, mut sealed) = sealed();
    let mut bytes = sealed.ciphertext.to_vec();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    sealed.ciphertext = bytes.into();
    assert_eq!(open(&private, &sealed, DATA), Err(OpenError::Failed));
}

#[test]
fn a_changed_encapsulated_key_does_not_open() {
    let (private, mut sealed) = sealed();
    let mut bytes = sealed.encapsulated_key.to_vec();
    bytes[0] ^= 1;
    sealed.encapsulated_key = bytes.into();
    assert_eq!(open(&private, &sealed, DATA), Err(OpenError::Failed));
}

#[test]
fn a_changed_key_id_does_not_open() {
    let (private, mut sealed) = sealed();
    sealed.key_id = "launcher/2026-12".to_string();
    assert_eq!(open(&private, &sealed, DATA), Err(OpenError::Failed));
}

#[test]
fn a_changed_data_does_not_open() {
    let (private, sealed) = sealed();
    assert_eq!(
        open(
            &private,
            &sealed,
            b"RunEnqueued.env/OTHER@projects/p/sessions/s"
        ),
        Err(OpenError::Failed)
    );
    assert_eq!(open(&private, &sealed, b""), Err(OpenError::Failed));
}

#[test]
fn a_stripped_or_rewritten_algorithm_does_not_open() {
    let (private, mut sealed) = sealed();
    sealed.algorithm = None;
    assert_eq!(
        open(&private, &sealed, DATA),
        Err(OpenError::UnsupportedAlgorithm)
    );
    let (private, mut sealed) = self::sealed();
    sealed.algorithm = Some(Algorithm {
        kem: Kem::Unspecified.into(),
        kdf: Kdf::HkdfSha256.into(),
        aead: Aead::Chacha20Poly1305.into(),
    });
    assert_eq!(
        open(&private, &sealed, DATA),
        Err(OpenError::UnsupportedAlgorithm)
    );
}

#[test]
fn a_wrong_size_encapsulated_key_is_malformed_not_a_panic() {
    let (private, mut sealed) = sealed();
    sealed.encapsulated_key = vec![1; 31].into();
    assert_eq!(
        open(&private, &sealed, DATA),
        Err(OpenError::Malformed {
            field: "encapsulated_key",
            why: "not 32 bytes"
        })
    );
}

#[test]
fn a_truncated_ciphertext_does_not_open() {
    let (private, mut sealed) = sealed();
    sealed.ciphertext = sealed.ciphertext.slice(..10);
    assert!(open(&private, &sealed, DATA).is_err());
    sealed.ciphertext = Default::default();
    assert!(open(&private, &sealed, DATA).is_err());
}

#[test]
fn another_recipients_key_does_not_open() {
    let (_, sealed) = sealed();
    let other = RecipientPrivateKey::from_bytes([9; 32]);
    assert_eq!(open(&other, &sealed, DATA), Err(OpenError::Failed));
}

#[test]
fn a_low_order_recipient_key_is_refused_to_seal_to() {
    let zero = RecipientPublicKey::from_bytes([0; 32]);
    assert_eq!(
        seal(&zero, &key_id(), DATA, SECRET_TEXT).unwrap_err(),
        SealError::WeakSharedSecret
    );
}

#[test]
fn a_sealed_with_a_low_order_encapsulated_key_does_not_open() {
    let (private, mut sealed) = sealed();
    sealed.encapsulated_key = vec![0; 32].into();
    assert_eq!(open(&private, &sealed, DATA), Err(OpenError::Failed));
}

#[test]
fn the_associated_data_separates_its_parts() {
    let a = associated_data(&KeyId::new("ab"), &suite(), b"c").expect("ad");
    let b = associated_data(&KeyId::new("a"), &suite(), b"bc").expect("ad");
    assert_ne!(a, b, "length-prefixed, so a boundary cannot move");
    assert_ne!(
        associated_data(&key_id(), &suite(), b"x").expect("ad"),
        associated_data(&key_id(), &Algorithm::default(), b"x").expect("ad")
    );
    assert_eq!(
        a,
        associated_data(&KeyId::new("ab"), &suite(), b"c").expect("ad"),
        "deterministic"
    );
}

/// The whole path, RFC 9180 A.2.1 with the vector's ephemeral key, on the
/// crate's own info and aad: the vector's `enc` and `shared_secret` are
/// reached and the ciphertext opens, so the construction is the RFC's and
/// not merely self-consistent.
#[test]
fn sealing_with_the_vectors_ephemeral_key_gives_the_vectors_encapsulated_key() {
    let ephemeral = RecipientPrivateKey::from_bytes(unhex_n(SKEM));
    let pk_r = RecipientPublicKey::from_bytes(unhex_n(PKRM));
    let sealed =
        seal_with_ephemeral(&ephemeral, &pk_r, &key_id(), DATA, SECRET_TEXT).expect("seal");
    assert_eq!(sealed.encapsulated_key.to_vec(), unhex(ENC));
    let sk_r = RecipientPrivateKey::from_bytes(unhex_n(SKRM));
    assert_eq!(open(&sk_r, &sealed, DATA), Ok(SECRET_TEXT.to_vec()));
}

#[test]
fn the_private_key_debug_renders_no_byte() {
    let key = RecipientPrivateKey::from_bytes(unhex_n(SKRM));
    let shown = format!("{key:?}");
    assert!(!shown.contains(SKRM), "{shown}");
    assert!(!shown.contains("8057991e"), "{shown}");
    assert!(!shown.contains("128, 87"), "no decimal bytes: {shown}");
    assert!(shown.contains("RecipientPrivateKey"), "{shown}");
}

#[test]
fn a_sealed_debug_prints_no_plaintext_and_the_ciphertext_is_not_it() {
    let (_, sealed) = sealed();
    let shown = format!("{sealed:?}");
    assert!(!shown.contains("sk-ant"), "{shown}");
    assert!(!sealed
        .ciphertext
        .windows(SECRET_TEXT.len())
        .any(|w| w == SECRET_TEXT));
}

#[test]
fn a_generated_private_key_matches_its_public_key_and_two_differ() {
    let a = RecipientPrivateKey::generate().expect("generate");
    let b = RecipientPrivateKey::generate().expect("generate");
    assert_ne!(a.expose_secret(), b.expose_secret());
    let sealed = seal(&a.public_key(), &key_id(), DATA, SECRET_TEXT).expect("seal");
    assert_eq!(open(&a, &sealed, DATA), Ok(SECRET_TEXT.to_vec()));
    assert_eq!(open(&b, &sealed, DATA), Err(OpenError::Failed));
}

/// The worked example the owner reviews: the exact bytes of `info` and of
/// the associated data for key id "k1", the suite and data
/// "session:abc", field by field. Why: these bytes are the format; a
/// change to one is a change to every sealed value ever made.
#[test]
fn the_worked_example_bytes_are_the_documented_ones() {
    assert_eq!(INFO, &unhex("616e677a6172722d7365616c6564207631")[..]);
    assert_eq!(INFO, b"angzarr-sealed v1");
    assert_eq!(
        AAD_LABEL,
        &unhex("616e677a6172722d7365616c656420763120616164")[..]
    );
    let want = [
        "616e677a6172722d7365616c656420763120616164",
        "00000002",
        "6b31",
        "000000010000000100000001",
        "0000000b",
        "73657373696f6e3a616263",
    ]
    .concat();
    let got = associated_data(&KeyId::new("k1"), &suite(), b"session:abc").expect("ad");
    assert_eq!(got, unhex(&want));
    assert_eq!(got.len(), 54);
}

/// Why: the length word is 4 bytes, so a longer field cannot be said, and
/// a silent truncation (`len as u32`) would let two different inputs share
/// an aad. The bound is judged in `length_word`, which takes the length so
/// no 4 GiB buffer is built (a slice that long cannot be made without
/// `unsafe`, which the crate forbids).
#[test]
fn a_field_over_u32_max_bytes_is_refused_and_one_at_it_is_not() {
    use crate::seal::length_word;
    assert_eq!(length_word("data", 0), Ok([0, 0, 0, 0]));
    assert_eq!(length_word("data", 11), Ok([0, 0, 0, 11]));
    assert_eq!(
        length_word("data", u32::MAX as usize),
        Ok([255, 255, 255, 255])
    );
    if let Ok(over) = usize::try_from(u64::from(u32::MAX) + 1) {
        assert_eq!(
            length_word("data", over),
            Err(SealError::TooLong { field: "data" })
        );
        assert_eq!(
            length_word("key_id", usize::MAX),
            Err(SealError::TooLong { field: "key_id" })
        );
    }
}

#[test]
fn the_too_long_refusal_names_its_field_and_has_a_message() {
    let err = SealError::TooLong { field: "key_id" };
    assert_eq!(err.args().len(), 1);
    assert!(err.to_string().contains("seal-too-long"), "{err}");
}
