//! Spelling a resolved path as a person writes it, for every flavor on
//! every host.

use std::path::PathBuf;

use ::os::Flavor;

use super::plain;

#[test]
fn test_a_resolved_path_is_spelled_without_windows_verbatim_prefix() {
    for (verbatim, spelled) in [
        (r"\\?\C:\Users\u", r"C:\Users\u"),
        (r"\\?\UNC\srv\share\x", r"\\srv\share\x"),
        (r"C:\already\plain", r"C:\already\plain"),
    ] {
        assert_eq!(
            plain(Flavor::Windows, PathBuf::from(verbatim)),
            PathBuf::from(spelled)
        );
    }
    let odd = PathBuf::from(r"\\?\C:\x");
    assert_eq!(
        plain(Flavor::Unix, odd.clone()),
        odd,
        "unix spells it as it is"
    );
}

#[test]
fn test_a_path_that_is_not_utf8_is_left_as_it_is() {
    let odd = PathBuf::from(::os::not_unicode(r"\\?\C:\x"));
    assert_eq!(plain(Flavor::Windows, odd.clone()), odd);
}
