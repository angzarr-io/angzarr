//! The catalog and its errors agree: a message for every code, using
//! exactly its args, and no message without an error.

use std::collections::BTreeSet;

use angzarr_coordinator::angzarr_diagnostic::catalog;

/// The crate offers its catalogs by locale under the same name, built from
/// `locales/` with no list of its own: en-US alone, the same text as
/// `EN_US`.
#[test]
fn its_locales_are_every_catalog_under_locales_en_us_alone() {
    let (name, table) = angzarr_coordinator::LOCALES;
    assert_eq!(name, angzarr_coordinator::EN_US.0);
    assert_eq!(table, &[("en-US", angzarr_coordinator::EN_US.1)]);
}

#[test]
fn coordinator_errors_have_catalog_messages() {
    let (name, text) = angzarr_coordinator::EN_US;
    assert_eq!(name, env!("CARGO_PKG_NAME"));
    let samples = angzarr_coordinator::diagnostic_samples();
    let sampled: BTreeSet<String> = samples.iter().map(|d| d.code().to_string()).collect();
    let expected: BTreeSet<String> = [
        "angzarr-coordinator-hidden",
        "angzarr-coordinator-unimplemented",
        "angzarr-coordinator-unauthenticated",
        "angzarr-coordinator-framework-only",
        "angzarr-coordinator-denied-not-exposed",
        "angzarr-coordinator-denied-type-not-allowed",
        "angzarr-coordinator-denied-body-kind-not-allowed",
        "angzarr-coordinator-denied-not-owner",
        "angzarr-coordinator-denied-selection-not-allowed",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert_eq!(sampled, expected, "one sample per error");
    assert_eq!(catalog::check(name, text, &samples), []);
}
