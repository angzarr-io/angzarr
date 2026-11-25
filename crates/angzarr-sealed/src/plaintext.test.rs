//! The opened secret: wiped on drop (by `Zeroizing`), compared by bytes,
//! never printed.

use super::*;

#[test]
fn a_plaintext_is_its_bytes_and_prints_only_its_length() {
    let p = Plaintext::from(b"hunter2".as_slice());
    assert_eq!(&*p, b"hunter2");
    assert_eq!(p, Plaintext::from(b"hunter2".to_vec()));
    assert_ne!(p, Plaintext::from(b"hunter3".to_vec()));
    let shown = format!("{p:?}");
    assert_eq!(shown, "Plaintext(7 bytes)");
    assert!(!shown.contains("hunter"));
}

#[test]
fn into_vec_gives_the_bytes() {
    assert_eq!(Plaintext::from(vec![1, 2, 3]).into_vec(), vec![1, 2, 3]);
}
