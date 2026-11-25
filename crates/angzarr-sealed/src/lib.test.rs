//! The crate's catalog: every code has a message using exactly its args.

use super::*;

#[test]
fn the_catalog_has_a_message_for_every_code_with_exactly_its_args() {
    let (name, text) = EN_US;
    assert_eq!(name, env!("CARGO_PKG_NAME"));
    assert_eq!(
        coded_diagnostic::catalog::check(name, text, &diagnostic_samples()),
        []
    );
}

#[test]
fn its_locales_are_en_us_alone() {
    let (name, table) = LOCALES;
    assert_eq!(name, EN_US.0);
    assert_eq!(table, &[("en-US", EN_US.1)]);
}
