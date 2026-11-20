//! The crate's shape: a leaf, shared by angzarr-embedded and angzarr.

/// The adapter is angzarr's as much as angzarr-embedded's: it depends on angzarr-ports
/// and SQL, and on no crate of angzarr-embedded (its kernel above all), so angzarr's
/// runtimes can use it without angzarr-embedded, and angzarr-embedded's kernel never reaches
/// into it.
#[test]
fn a_store_depends_on_nothing_of_angzarr_embedded() {
    let manifest = include_str!("../Cargo.toml");
    let dependencies: Vec<&str> = manifest
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#') && !line.starts_with('['))
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
        .collect();
    let ours: Vec<&&str> = dependencies
        .iter()
        .filter(|name| name.starts_with("angzarr-embedded"))
        .collect();
    assert!(ours.is_empty(), "depends on {ours:?}");
    assert!(dependencies.contains(&"angzarr-ports"));
}
