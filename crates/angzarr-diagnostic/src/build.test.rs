use super::*;

use std::path::PathBuf;

/// A fresh directory of this test's own under the system's temporary one.
fn scratch(name: &str) -> PathBuf {
    let dir = env::temp_dir().join(format!(
        "angzarr-diagnostic-build-{}-{name}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Each locale's source is written to `out` as `<locale>.ftl`, under the
/// crate's name, its references too.
#[test]
fn every_locales_source_is_written_under_the_crates_name() {
    let dir = scratch("written");
    let (locales, out) = (dir.join("locales"), dir.join("out"));
    fs::create_dir_all(locales.join("en-US")).unwrap();
    fs::create_dir_all(locales.join("de")).unwrap();
    fs::create_dir_all(&out).unwrap();
    fs::write(
        locales.join("en-US").join(SOURCE),
        "# en\ntoo-new = newer: { other }\nother = { $n }\n",
    )
    .unwrap();
    fs::write(locales.join("de").join(SOURCE), "too-new = neuer\n").unwrap();

    assert_eq!(
        write("my-crate", &locales, &out),
        Ok(vec!["de".to_string(), "en-US".to_string()])
    );
    assert_eq!(
        fs::read_to_string(out.join("en-US.ftl")).unwrap(),
        "# en\nmy-crate-too-new = newer: { my-crate-other }\nmy-crate-other = { $n }\n"
    );
    assert_eq!(
        fs::read_to_string(out.join("de.ftl")).unwrap(),
        "my-crate-too-new = neuer\n"
    );
    fs::remove_dir_all(&dir).unwrap();
}

/// Beside the catalogs, `out` gets their table: a Rust expression, each
/// locale with its catalog's text embedded, sorted, so a crate exports every
/// locale under `locales` with no edit of its own when one is added.
#[test]
fn the_locales_table_embeds_every_locales_catalog_sorted() {
    let dir = scratch("table");
    let (locales, out) = (dir.join("locales"), dir.join("out"));
    for locale in ["en-US", "de-DE"] {
        fs::create_dir_all(locales.join(locale)).unwrap();
        fs::write(locales.join(locale).join(SOURCE), "x = y\n").unwrap();
    }
    fs::create_dir_all(&out).unwrap();

    write("my-crate", &locales, &out).unwrap();
    assert_eq!(
        fs::read_to_string(out.join(TABLE)).unwrap(),
        r#"// Each locale's catalog (angzarr_diagnostic::build), sorted.
&[
    ("de-DE", include_str!(concat!(env!("OUT_DIR"), "/de-DE.ftl"))),
    ("en-US", include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl"))),
]
"#
    );
    fs::remove_dir_all(&dir).unwrap();
}

/// A locale without the source, a source that does not parse, and no
/// `locales` at all each fail the build, naming the source and, for a
/// syntax error, its line.
#[test]
fn a_missing_or_unparsable_source_fails_the_build() {
    let dir = scratch("failing");
    let (locales, out) = (dir.join("locales"), dir.join("out"));
    fs::create_dir_all(&out).unwrap();
    let missing = write("k", &locales, &out).unwrap_err();
    assert!(
        missing.starts_with(&locales.display().to_string()),
        "{missing}"
    );

    fs::create_dir_all(locales.join("en-US")).unwrap();
    let absent = write("k", &locales, &out).unwrap_err();
    let source = locales.join("en-US").join(SOURCE);
    assert!(
        absent.starts_with(&source.display().to_string()),
        "{absent}"
    );

    fs::write(&source, "ok = fine\nnot an entry\n").unwrap();
    assert_eq!(
        write("k", &locales, &out),
        Err(format!("{}: [Syntax {{ line: 2 }}]", source.display()))
    );
    assert!(!out.join("en-US.ftl").exists());
    assert!(!out.join(TABLE).exists());
    fs::remove_dir_all(&dir).unwrap();
}

/// A build script's call reads the crate's name, its directory and
/// `OUT_DIR` from cargo's environment, and fails the build (panics) on a
/// source that does not parse. The one test that sets that environment.
#[test]
fn a_build_scripts_call_writes_the_crates_catalogs_from_cargos_environment() {
    let dir = scratch("environment");
    let out = dir.join("out");
    fs::create_dir_all(dir.join("locales/en-US")).unwrap();
    fs::create_dir_all(&out).unwrap();
    let source = dir.join("locales/en-US").join(SOURCE);
    fs::write(&source, "too-new = newer\n").unwrap();
    env::set_var("CARGO_MANIFEST_DIR", &dir);
    env::set_var("CARGO_PKG_NAME", "my-crate");
    env::set_var("OUT_DIR", &out);

    catalogs();
    assert_eq!(
        fs::read_to_string(out.join("en-US.ftl")).unwrap(),
        "my-crate-too-new = newer\n"
    );
    assert!(out.join(TABLE).exists(), "the locales table is written too");

    fs::write(&source, "not an entry\n").unwrap();
    let failed = std::panic::catch_unwind(catalogs).unwrap_err();
    let message = failed.downcast_ref::<String>().unwrap();
    assert_eq!(
        *message,
        format!("{}: [Syntax {{ line: 1 }}]", source.display())
    );
    fs::remove_dir_all(&dir).unwrap();
}
