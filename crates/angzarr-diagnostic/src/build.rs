//! For a crate's build script: its catalogs, from sources that name each
//! message by its code's suffix, written under the crate's package name, so
//! no catalog spells its crate's name and a renamed crate's follow it.

use std::path::Path;
use std::{env, fs};

use crate::catalog::{prefixed, Catalog};

/// The catalog source's name in each locale's directory: a crate's en-US
/// catalog is `locales/en-US/messages.ftl`.
pub const SOURCE: &str = "messages.ftl";

/// Call from a crate's build script (`build.rs`'s `main`): each
/// `locales/<locale>/messages.ftl` of the crate being built, written to
/// `OUT_DIR` as `<locale>.ftl` with every message and term id, and every
/// reference to one, prefixed with the crate's package name and a dash
/// ([`prefixed`]). The crate exports its en-US catalog as
///
/// ```ignore
/// pub const EN_US: (&str, &str) = (
///     env!("CARGO_PKG_NAME"),
///     include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
/// );
/// ```
///
/// Rebuilt when anything under `locales` changes. Panics, failing the
/// build, on a source that cannot be read or does not parse
/// ([`Catalog::parse`]: the line numbers are the source's).
pub fn catalogs() {
    let var = |name: &str| env::var(name).unwrap_or_else(|e| panic!("{name}: {e}"));
    let locales = Path::new(&var("CARGO_MANIFEST_DIR")).join("locales");
    println!("cargo:rerun-if-changed={}", locales.display());
    if let Err(e) = write(&var("CARGO_PKG_NAME"), &locales, Path::new(&var("OUT_DIR"))) {
        panic!("{e}");
    }
}

/// Each locale's source under `locales`, prefixed with `krate`, written to
/// `out` as `<locale>.ftl`: the locales written, sorted, or what went
/// wrong.
fn write(krate: &str, locales: &Path, out: &Path) -> Result<Vec<String>, String> {
    let at = |path: &Path, e: &dyn std::fmt::Debug| format!("{}: {e:?}", path.display());
    let mut written = Vec::new();
    for dir in fs::read_dir(locales).map_err(|e| at(locales, &e))? {
        let dir = dir.map_err(|e| at(locales, &e))?.path();
        let source = dir.join(SOURCE);
        let text = fs::read_to_string(&source).map_err(|e| at(&source, &e))?;
        let catalog = prefixed(krate, &text);
        Catalog::parse(&catalog).map_err(|problems| at(&source, &problems))?;
        let locale = dir.file_name().unwrap_or_default().to_string_lossy();
        let target = out.join(format!("{locale}.ftl"));
        fs::write(&target, catalog).map_err(|e| at(&target, &e))?;
        written.push(locale.into_owned());
    }
    written.sort();
    Ok(written)
}

#[cfg(test)]
#[path = "build.test.rs"]
mod tests;
