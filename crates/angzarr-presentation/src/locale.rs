//! The locale a person asked for, and en-US, the one every catalog falls
//! back to.
//!
//! [`detect`] reads nothing itself: a binary passes the environment in
//! (`locale::detect(|name| std::env::var(name).ok())`), and hands what it
//! answers to [`Catalog::for_locale`](crate::Catalog::for_locale).

use unic_langid::LanguageIdentifier;

/// The variables that name the locale of messages, in POSIX's order of
/// precedence: the first one set (and not empty) decides.
const VARIABLES: [&str; 3] = ["LC_ALL", "LC_MESSAGES", "LANG"];

/// en-US: the locale whose catalogs every crate ships, and every catalog's
/// last resort.
pub fn fallback() -> LanguageIdentifier {
    "en-US".parse().unwrap_or_default()
}

/// The locale `var` names, `var` answering an environment variable's value
/// by its name: the first of `LC_ALL`, `LC_MESSAGES` and `LANG` set and not
/// empty, as a language identifier (`de_DE.UTF-8` is `de-DE`). en-US
/// ([`fallback`]) when none is set, or the one set is `C`, `POSIX` or does
/// not parse.
pub fn detect(var: impl Fn(&str) -> Option<String>) -> LanguageIdentifier {
    VARIABLES
        .iter()
        .filter_map(|name| var(name))
        .find(|value| !value.is_empty())
        .and_then(|value| posix(&value))
        .unwrap_or_else(fallback)
}

/// A POSIX locale name (`language[_territory][.codeset][@modifier]`) as a
/// language identifier, its language and territory; none for `C` and
/// `POSIX`, which name no language, or for what does not parse.
fn posix(value: &str) -> Option<LanguageIdentifier> {
    let name = value.split(['.', '@']).next().unwrap_or_default();
    if name == "C" || name == "POSIX" {
        return None;
    }
    let locale: LanguageIdentifier = name.replace('_', "-").parse().ok()?;
    (!locale.language.is_empty()).then_some(locale)
}

#[cfg(test)]
#[path = "locale.test.rs"]
mod tests;
