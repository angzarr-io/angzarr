use super::*;

/// A credential never prints its secret, or a metadata value: the scheme and
/// the metadata's names only.
#[test]
fn credential_debug_never_prints_the_secret() {
    let credential = Credential::bearer("hunter2-secret").with_metadata("run", "run-4417");
    for shown in [format!("{credential:?}"), format!("{credential:#?}")] {
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(!shown.contains("run-4417"), "{shown}");
        assert!(shown.contains("redacted"), "{shown}");
        assert!(shown.contains("Bearer"), "{shown}");
        assert!(shown.contains("run"), "{shown}");
    }
}

/// The bytes of a secret are not printed under any spelling either.
#[test]
fn a_debug_print_of_a_credential_is_redacted() {
    let credential = Credential::bearer(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    let shown = format!("{credential:?}");
    for leak in [
        "222", "173", "190", "239", "DE", "de, ad", "deadbeef", "[222",
    ] {
        assert!(!shown.contains(leak), "{shown} leaks {leak}");
    }
    assert_eq!(
        shown,
        "Credential { scheme: Bearer, secret: \"<redacted>\", metadata: [] }"
    );
    let certificate = Credential::peer_certificate(b"cert-bytes".to_vec());
    assert_eq!(certificate.scheme(), Scheme::PeerCertificate);
    assert!(!format!("{certificate:?}").contains("cert-bytes"));
    assert!(format!("{certificate:?}").contains("PeerCertificate"));
}

#[test]
fn a_credential_has_a_scheme_a_secret_and_metadata_pairs() {
    let credential = Credential::bearer("tok")
        .with_metadata("run", "r1")
        .with_metadata("host", "h1")
        .with_metadata("run", "r2");
    assert_eq!(credential.scheme(), Scheme::Bearer);
    assert_eq!(credential.expose_secret(), b"tok");
    // A second pair of the same name replaces the first.
    assert_eq!(credential.metadata("run"), Some("r2"));
    assert_eq!(credential.metadata("host"), Some("h1"));
    assert_eq!(credential.metadata("absent"), None);
    let shown = format!("{credential:?}");
    assert!(shown.contains("host") && shown.contains("run"));
}

#[test]
fn constant_time_compare_is_equality_of_the_bytes() {
    assert!(constant_time_eq(b"", b""));
    assert!(constant_time_eq(b"a", b"a"));
    assert!(constant_time_eq(b"secret-token", b"secret-token"));
    // A difference at the first, a middle and the last byte.
    assert!(!constant_time_eq(b"Xecret-token", b"secret-token"));
    assert!(!constant_time_eq(b"secrXt-token", b"secret-token"));
    assert!(!constant_time_eq(b"secret-tokeX", b"secret-token"));
    // Differing lengths, whichever is the prefix, or empty.
    assert!(!constant_time_eq(b"secret", b"secret-token"));
    assert!(!constant_time_eq(b"secret-token", b"secret"));
    assert!(!constant_time_eq(b"", b"s"));
    assert!(!constant_time_eq(b"s", b""));
    // A trailing zero is not nothing, and a zero byte differs from absent.
    assert!(!constant_time_eq(b"a\0", b"a"));
    assert!(!constant_time_eq(b"a", b"a\0"));
    assert!(!constant_time_eq(&[0], &[]));
    // One bit apart.
    assert!(!constant_time_eq(&[0b0000_0001], &[0b0000_0000]));
    assert!(!constant_time_eq(&[0xFF; 64], &[0xFE; 64]));
    assert!(constant_time_eq(&[0xFF; 64], &[0xFF; 64]));
}

#[test]
fn a_credentials_secret_matches_only_the_same_bytes() {
    let credential = Credential::bearer("tok-1");
    assert!(credential.secret_matches(b"tok-1"));
    assert!(!credential.secret_matches(b"tok-2"));
    assert!(!credential.secret_matches(b"tok-"));
    assert!(!credential.secret_matches(b""));
    // The metadata is no part of the secret.
    let with_meta = Credential::bearer("tok-1").with_metadata("tok-2", "tok-2");
    assert!(!with_meta.secret_matches(b"tok-2"));
}
