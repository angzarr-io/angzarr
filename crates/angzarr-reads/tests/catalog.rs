//! The catalog and its errors agree: a message for every code, using
//! exactly its args, and no message without an error.

use std::collections::BTreeSet;

use angzarr_reads::angzarr_diagnostic::catalog;

/// The crate offers its catalogs by locale under the same name, built from
/// `locales/` with no list of its own: en-US alone, the same text as
/// `EN_US`.
#[test]
fn its_locales_are_every_catalog_under_locales_en_us_alone() {
    let (name, table) = angzarr_reads::LOCALES;
    assert_eq!(name, angzarr_reads::EN_US.0);
    assert_eq!(table, &[("en-US", angzarr_reads::EN_US.1)]);
}

#[test]
fn reads_errors_have_catalog_messages() {
    let (name, text) = angzarr_reads::EN_US;
    assert_eq!(name, env!("CARGO_PKG_NAME"));
    let samples = angzarr_reads::diagnostic_samples();
    let sampled: BTreeSet<String> = samples.iter().map(|d| d.code().to_string()).collect();
    let expected: BTreeSet<String> = [
        "angzarr-reads-no-domain",
        "angzarr-reads-bad-root",
        "angzarr-reads-empty-edition",
        "angzarr-reads-bad-correlation",
        "angzarr-reads-no-target",
        "angzarr-reads-many-streams",
        "angzarr-reads-too-large",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert_eq!(sampled, expected, "one sample per error variant");
    assert_eq!(catalog::check(name, text, &samples), []);
}
