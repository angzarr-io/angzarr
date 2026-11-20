//! A test directory is fresh: a leftover at its name (a reused process id's
//! debris) is cleared first. Alone in its binary, as it predicts the
//! binary's next directory name.

use angzarr_test_dir::TestDir;

#[test]
fn a_leftover_at_a_test_dirs_name_is_cleared() {
    let prefix = concat!(env!("CARGO_PKG_NAME"), "-fresh");
    let first = TestDir::new(prefix);
    let name = first.file_name().unwrap().to_string_lossy().into_owned();
    let (stem, n) = name.rsplit_once('-').unwrap();
    let next: usize = n.parse::<usize>().unwrap() + 1;
    let leftover = first.with_file_name(format!("{stem}-{next}"));
    std::fs::create_dir_all(&leftover).unwrap();
    std::fs::write(leftover.join("debris"), "x").unwrap();
    let second = TestDir::new(prefix);
    assert_eq!(second.path(), leftover);
    assert!(std::fs::read_dir(&second).unwrap().next().is_none());
}
