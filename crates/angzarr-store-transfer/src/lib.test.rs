//! The crate's catalog against its errors.

use std::collections::BTreeSet;

use super::*;

/// The crate offers its catalogs by locale under its name: en-US alone.
#[test]
fn its_locales_are_every_catalog_under_locales_en_us_alone() {
    let (name, table) = LOCALES;
    assert_eq!(name, EN_US.0);
    assert_eq!(table, &[("en-US", EN_US.1)]);
}

/// A message for every code, using exactly its args, and no message
/// without an error; the samples are one of every variant, each once.
#[test]
fn the_catalog_has_a_message_for_every_code_with_exactly_its_args() {
    let (name, text) = EN_US;
    assert_eq!(name, env!("CARGO_PKG_NAME"));
    let samples = diagnostic_samples();
    let codes: BTreeSet<&str> = samples.iter().map(|d| d.code()).collect();
    assert_eq!(codes.len(), samples.len(), "one sample per variant");
    assert_eq!(angzarr_diagnostic::catalog::check(name, text, &samples), []);
}
