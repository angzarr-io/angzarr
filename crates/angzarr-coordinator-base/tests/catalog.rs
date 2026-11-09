//! The catalog and its errors agree: a message for every code, using
//! exactly its args, and no message without an error.

use std::collections::BTreeSet;

use angzarr_coordinator_base::angzarr_diagnostic::catalog;

/// The crate offers its catalogs by locale under the same name, built from
/// `locales/` with no list of its own: en-US alone, the same text as
/// `EN_US`.
#[test]
fn its_locales_are_every_catalog_under_locales_en_us_alone() {
    let (name, table) = angzarr_coordinator_base::LOCALES;
    assert_eq!(name, angzarr_coordinator_base::EN_US.0);
    assert_eq!(table, &[("en-US", angzarr_coordinator_base::EN_US.1)]);
}

#[test]
fn base_errors_have_catalog_messages() {
    let (name, text) = angzarr_coordinator_base::EN_US;
    assert_eq!(name, env!("CARGO_PKG_NAME"));
    let samples = angzarr_coordinator_base::diagnostic_samples();
    let sampled: BTreeSet<String> = samples.iter().map(|d| d.code().to_string()).collect();
    let expected: BTreeSet<String> = [
        "angzarr-coordinator-base-upcast-unavailable",
        "angzarr-coordinator-base-upcast-refused",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert_eq!(sampled, expected, "one sample per error variant");
    assert_eq!(catalog::check(name, text, &samples), []);
}
