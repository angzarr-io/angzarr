//! The locale a person asked for, from the POSIX variables as a binary
//! passes them in.

use std::cell::RefCell;
use std::collections::BTreeMap;

use super::{detect, fallback};

/// `detect` over exactly these variables, the rest unset.
fn detected(vars: &[(&str, &str)]) -> String {
    let vars: BTreeMap<&str, &str> = vars.iter().copied().collect();
    detect(|name| vars.get(name).map(ToString::to_string)).to_string()
}

#[test]
fn test_the_fallback_is_en_us() {
    assert_eq!(fallback().to_string(), "en-US");
}

#[test]
fn test_lc_all_then_lc_messages_then_lang_decides() {
    let all = [
        ("LC_ALL", "de_DE.UTF-8"),
        ("LC_MESSAGES", "fr_FR.UTF-8"),
        ("LANG", "pl_PL.UTF-8"),
    ];
    assert_eq!(detected(&all), "de-DE");
    assert_eq!(detected(&all[1..]), "fr-FR");
    assert_eq!(detected(&all[2..]), "pl-PL");
}

#[test]
fn test_an_empty_variable_is_unset() {
    assert_eq!(
        detected(&[("LC_ALL", ""), ("LC_MESSAGES", ""), ("LANG", "pl_PL")]),
        "pl-PL"
    );
}

#[test]
fn test_a_posix_locale_is_its_language_and_territory() {
    assert_eq!(detected(&[("LANG", "de_DE.UTF-8")]), "de-DE");
    assert_eq!(detected(&[("LANG", "de_DE@euro")]), "de-DE");
    assert_eq!(detected(&[("LANG", "sr_RS.UTF-8@latin")]), "sr-RS");
    assert_eq!(detected(&[("LANG", "de")]), "de");
    assert_eq!(detected(&[("LANG", "pt-BR")]), "pt-BR");
}

#[test]
fn test_unset_c_posix_or_unparseable_is_en_us() {
    assert_eq!(detected(&[]), "en-US");
    for value in ["C", "POSIX", "C.UTF-8", "!!", "_", ".UTF-8"] {
        assert_eq!(detected(&[("LANG", value)]), "en-US", "{value}");
    }
}

#[test]
fn test_the_first_set_variable_decides_even_when_it_is_c() {
    assert_eq!(
        detected(&[("LC_ALL", "C"), ("LANG", "de_DE.UTF-8")]),
        "en-US"
    );
}

#[test]
fn test_only_the_three_posix_variables_are_read_in_order() {
    let read = RefCell::new(Vec::new());
    detect(|name| {
        read.borrow_mut().push(name.to_string());
        None
    });
    assert_eq!(read.into_inner(), ["LC_ALL", "LC_MESSAGES", "LANG"]);
}
