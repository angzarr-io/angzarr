//! The steps of RFC 9180 against Appendix A.2.1's vectors (base mode,
//! DHKEM(X25519, HKDF-SHA256), HKDF-SHA256, ChaCha20Poly1305), byte for
//! byte. Why: the suite is a standard, and a standard's implementation is
//! right only where it reproduces the standard's own numbers; a mistake in a
//! label, a length prefix or a nonce's order is silent otherwise.

use super::*;
use crate::rfc9180_a2::*;
use crate::testkit::{unhex, unhex_n};

fn recipient_private() -> RecipientPrivateKey {
    RecipientPrivateKey::from_bytes(unhex_n(SKRM))
}

fn schedule() -> Schedule {
    key_schedule(&unhex_n(SHARED_SECRET), &unhex(INFO))
}

#[test]
fn the_suite_ids_are_the_rfcs() {
    assert_eq!(KEM_SUITE_ID.to_vec(), unhex("4b454d0020"));
    assert_eq!(HPKE_SUITE_ID.to_vec(), unhex("48504b45002000010003"));
}

#[test]
fn derive_key_pair_reproduces_the_ephemeral_and_recipient_pairs() {
    let (sk, pk) = derive_key_pair(&unhex(IKME));
    assert_eq!(sk.expose_secret().to_vec(), unhex(SKEM));
    assert_eq!(pk.as_bytes().to_vec(), unhex(PKEM));
    let (sk, pk) = derive_key_pair(&unhex(IKMR));
    assert_eq!(sk.expose_secret().to_vec(), unhex(SKRM));
    assert_eq!(pk.as_bytes().to_vec(), unhex(PKRM));
}

#[test]
fn a_private_key_gives_the_public_key_of_the_vectors() {
    let sk = RecipientPrivateKey::from_bytes(unhex_n(SKRM));
    assert_eq!(sk.public_key().as_bytes().to_vec(), unhex(PKRM));
}

#[test]
fn encap_reproduces_enc_and_the_shared_secret() {
    let ephemeral = RecipientPrivateKey::from_bytes(unhex_n(SKEM));
    let pk_r = RecipientPublicKey::from_bytes(unhex_n(PKRM));
    let (shared, enc) = encap(&pk_r, &ephemeral).expect("encap");
    assert_eq!(enc.to_vec(), unhex(ENC));
    assert_eq!(shared.to_vec(), unhex(SHARED_SECRET));
}

#[test]
fn decap_reproduces_the_shared_secret() {
    let shared = decap(&unhex_n(ENC), &recipient_private()).expect("decap");
    assert_eq!(shared.to_vec(), unhex(SHARED_SECRET));
}

#[test]
fn the_key_schedule_context_is_the_vectors() {
    assert_eq!(
        key_schedule_context(&unhex(INFO)),
        unhex(KEY_SCHEDULE_CONTEXT)
    );
}

#[test]
fn the_key_schedule_reproduces_secret_key_nonce_and_exporter_secret() {
    let s = schedule();
    assert_eq!(s.secret.to_vec(), unhex(SECRET));
    assert_eq!(s.key.to_vec(), unhex(KEY));
    assert_eq!(s.base_nonce.to_vec(), unhex(BASE_NONCE));
    assert_eq!(s.exporter_secret.to_vec(), unhex(EXPORTER_SECRET));
}

#[test]
fn the_nonce_of_each_sequence_number_is_the_vectors() {
    let s = schedule();
    for (seq, _, _, nonce, _) in ENCRYPTIONS {
        assert_eq!(s.nonce(*seq).to_vec(), unhex(nonce), "sequence {seq}");
    }
}

#[test]
fn the_encryptions_are_the_vectors() {
    for (seq, pt, aad, _, ct) in ENCRYPTIONS {
        let mut s = schedule();
        s.at_sequence(*seq);
        assert_eq!(s.seal(&unhex(aad), &unhex(pt)), unhex(ct), "sequence {seq}");
    }
}

#[test]
fn the_vectors_ciphertexts_open() {
    for (seq, pt, aad, _, ct) in ENCRYPTIONS {
        let mut s = schedule();
        s.at_sequence(*seq);
        assert_eq!(
            s.open(&unhex(aad), &unhex(ct)),
            Ok(unhex(pt)),
            "sequence {seq}"
        );
    }
}

#[test]
fn the_sequence_advances_by_one_per_seal() {
    let mut s = schedule();
    let (_, pt, aad0, _, ct0) = &ENCRYPTIONS[0];
    let (_, _, aad1, _, ct1) = &ENCRYPTIONS[1];
    assert_eq!(s.seal(&unhex(aad0), &unhex(pt)), unhex(ct0));
    assert_eq!(s.seal(&unhex(aad1), &unhex(pt)), unhex(ct1));
    assert_eq!(s.sequence(), 2);
}

#[test]
fn the_exported_values_are_the_vectors() {
    let s = schedule();
    for (context, value) in EXPORTS {
        assert_eq!(
            s.export(&unhex(context), 32),
            unhex(value),
            "context {context:?}"
        );
    }
}

#[test]
fn labeled_extract_and_expand_of_the_key_schedule_match_the_secret_and_key() {
    // The pieces the schedule is made of, held apart so a failure names the
    // step: `secret` is LabeledExtract(shared_secret, "secret", psk = "").
    let secret = labeled_extract(&HPKE_SUITE_ID, &unhex(SHARED_SECRET), b"secret", b"");
    assert_eq!(secret.to_vec(), unhex(SECRET));
    let key = labeled_expand(
        &HPKE_SUITE_ID,
        &secret,
        b"key",
        &unhex(KEY_SCHEDULE_CONTEXT),
        32,
    );
    assert_eq!(key, unhex(KEY));
}

#[test]
fn an_all_zero_diffie_hellman_output_is_refused_on_both_sides() {
    // The all-zero public key is a low-order point: X25519 gives all zero.
    let zero = RecipientPublicKey::from_bytes([0; 32]);
    let ephemeral = RecipientPrivateKey::from_bytes(unhex_n(SKEM));
    assert_eq!(encap(&zero, &ephemeral), Err(SealError::WeakSharedSecret));
    assert_eq!(
        decap(&[0; 32], &recipient_private()),
        Err(OpenError::Failed)
    );
}

#[test]
fn the_schedule_debug_renders_no_key_byte() {
    let shown = format!("{:?}", schedule());
    for secret in [KEY, SECRET, EXPORTER_SECRET, BASE_NONCE] {
        assert!(!shown.contains(secret), "{shown}");
    }
}

#[test]
fn the_schedule_debug_names_the_type_and_the_sequence() {
    let shown = format!("{:?}", schedule());
    assert!(
        shown.contains("Schedule") && shown.contains("sequence: 0"),
        "{shown}"
    );
}
