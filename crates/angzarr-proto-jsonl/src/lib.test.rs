use super::*;

#[test]
fn its_locales_are_every_catalog_under_locales_en_us_alone() {
    let (name, table) = LOCALES;
    assert_eq!(name, EN_US.0);
    assert_eq!(table, &[("en-US", EN_US.1)]);
}

/// The catalog has a message for every code, using exactly its args, and no
/// message without an error.
#[test]
fn the_catalog_has_a_message_for_every_code_with_exactly_its_args() {
    let (name, text) = EN_US;
    assert_eq!(name, env!("CARGO_PKG_NAME"));
    assert_eq!(
        angzarr_diagnostic::catalog::check(name, text, &diagnostic_samples()),
        []
    );
}
