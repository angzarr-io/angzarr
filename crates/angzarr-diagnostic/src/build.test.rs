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
    fs::remove_dir_all(&dir).unwrap();
}
