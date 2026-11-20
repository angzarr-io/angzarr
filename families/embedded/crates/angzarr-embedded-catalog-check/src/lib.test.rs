//! Every error code angzarr-embedded's crates can produce has a message in
//! every shipped locale, using exactly the error's args; codes are unique;
//! and no variant goes unsampled.
//!
//! No crate's name is written here: each checked crate is named by its own
//! exported `EN_US.0` (its package name, `CARGO_PKG_NAME`), each crate of
//! the workspace by its manifest, so a renamed crate is checked under its
//! new name with no edit here.
//!
//! The errors come from each crate's samples, one value of every variant.
//! Each crate's catalog sources, `crates/<crate>/locales/<locale>/messages.ftl`,
//! named by the codes' suffixes, are read under the crate's name as its
//! build writes them ([`catalog::prefixed`]) and checked against them by
//! angzarr-diagnostic's [`catalog::check`]: a message for every code with
//! exactly its args, an error for every message, codes under the crate's
//! name, and a display of the code and args. A nested cause of another
//! crate's is that crate's to check (angzarr-ports' store errors, which
//! angzarr-embedded forwards under their own codes, are checked against
//! `angzarr_ports::EN_US` in core). A variant added without a sample is
//! caught by reading the crates' sources: every code a crate writes there
//! (`code!("...")`, under the crate's name) must be the code of some
//! sample, and a crate that writes codes must be checked. Each crate's
//! en-US catalog is also its exported `EN_US`, which must be its source
//! under its name.
//!
//! A condition the spec keys is coded under its key (decision 14.60), its
//! English the spec's catalog of the key's package (decision 14.64:
//! `angzarr_proto::REASONS_EN_US` for core's keys,
//! `angzarr_proto_transport_embedded::REASONS_EN_US` for a child's), never a
//! crate's: each such sample, and each such nested cause, must have its
//! message there, give only args its key declares, and give every arg its
//! message names; and every live key's message renders with its declared
//! args.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use angzarr_diagnostic::build::SOURCE;
use angzarr_diagnostic::catalog::{self, Catalog};
use angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_embedded::angzarr_proto;

/// A checked crate's exported en-US catalog: its package name and its text.
type Exported = (&'static str, &'static str);

/// The crates whose errors are checked: each one's exported catalog, which
/// names it, and one value of every error it defines.
fn checked() -> Vec<(Exported, Vec<Box<dyn Diagnostic>>)> {
    vec![
        (
            angzarr_embedded::EN_US,
            angzarr_embedded::diagnostic_samples(),
        ),
        (
            angzarr_embedded_host::EN_US,
            angzarr_embedded_host::diagnostic_samples(),
        ),
        (angzarr_store_sqlite::EN_US, sqlite_samples()),
    ]
}

/// The checked crates, by name, and their samples.
fn samples() -> Vec<(&'static str, Vec<Box<dyn Diagnostic>>)> {
    checked()
        .into_iter()
        .map(|((name, _), samples)| (name, samples))
        .collect()
}

/// angzarr-store-sqlite's errors: each one it puts in a store error as its
/// cause, and a damage report.
fn sqlite_samples() -> Vec<Box<dyn Diagnostic>> {
    let mut samples: Vec<Box<dyn Diagnostic>> = angzarr_store_sqlite::errors()
        .into_iter()
        .map(|error| -> Box<dyn Diagnostic> { Box::new(error) })
        .collect();
    samples.push(Box::new(angzarr_store_sqlite::damage_sample()));
    samples
}

/// Whether `code` is angzarr-ports' own: a store error a crate forwards
/// under its own code, checked against angzarr-ports' catalog.
fn is_forwarded(code: &str) -> bool {
    code.strip_prefix(angzarr_ports::EN_US.0)
        .is_some_and(|rest| rest.starts_with('-'))
}

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

/// The `name` of `manifest`'s `[package]`.
fn package_name(manifest: &str) -> Option<String> {
    let mut package = false;
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            package = line == "[package]";
        } else if let Some(value) = line
            .strip_prefix("name")
            .map(str::trim_start)
            .and_then(|rest| rest.strip_prefix('='))
            .filter(|_| package)
        {
            return Some(value.trim().trim_matches('"').to_string());
        }
    }
    None
}

/// The workspace's crates (`crates/*`, as the root manifest's members):
/// each one's package name, read from its manifest, and its directory.
fn members() -> BTreeMap<String, PathBuf> {
    let mut out = BTreeMap::new();
    for entry in fs::read_dir(crates_dir()).unwrap() {
        let dir = entry.unwrap().path();
        let Ok(manifest) = fs::read_to_string(dir.join("Cargo.toml")) else {
            continue;
        };
        let name = package_name(&manifest)
            .unwrap_or_else(|| panic!("{}: no [package] name", dir.display()));
        out.insert(name, dir);
    }
    out
}

/// Every crate's catalogs, by crate (its package name) then locale: each
/// `locales/<locale>/messages.ftl` under the crate's name, as its build
/// writes it. Any other file there is a problem.
fn catalogs(problems: &mut Vec<String>) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for (name, dir) in members() {
        let Ok(locales) = fs::read_dir(dir.join("locales")) else {
            continue;
        };
        let mut by_locale = BTreeMap::new();
        for locale in locales {
            let locale = locale.unwrap().path();
            let tag = locale.file_name().unwrap().to_string_lossy().into_owned();
            let file = locale.join(SOURCE);
            for other in fs::read_dir(&locale).unwrap() {
                let other = other.unwrap().path();
                if other != file {
                    problems.push(format!("{}: not {SOURCE}", other.display()));
                }
            }
            match fs::read_to_string(&file) {
                Ok(text) => {
                    by_locale.insert(tag, catalog::prefixed(&name, &text));
                }
                Err(_) => problems.push(format!("{}: missing", file.display())),
            }
        }
        out.insert(name, by_locale);
    }
    out
}

/// Every code `d` produces that is `krate`'s: its own and, recursively, its
/// nested causes' under the crate's name.
fn own_codes(d: &dyn Diagnostic, krate: &str, out: &mut BTreeSet<String>) {
    out.insert(d.code().to_string());
    for (_, arg) in d.args() {
        if let Arg::Nested(inner) = arg {
            if inner.code().starts_with(&format!("{krate}-")) {
                own_codes(inner.as_ref(), krate, out);
            }
        }
    }
}

/// A spec package whose keys angzarr-embedded answers (decision 14.64): its
/// lookup of a key's declared args, and its en-US catalog.
type SpecPackage = (
    fn(&str) -> Option<&'static [&'static str]>,
    (&'static str, &'static str),
);

/// The spec's packages with keys that embedded answers: core's, and the
/// local-process transport's (a child's failures).
const SPEC_PACKAGES: [SpecPackage; 2] = [
    (angzarr_proto::reason_key_args, angzarr_proto::REASONS_EN_US),
    (
        angzarr_proto_transport_embedded::reason_key_args,
        angzarr_proto_transport_embedded::REASONS_EN_US,
    ),
];

/// `code`'s declared args and its package's catalog, when it is a live key
/// of the spec's (decisions 14.60 and 14.64).
fn spec_key(code: &str) -> Option<(&'static [&'static str], (&'static str, &'static str))> {
    SPEC_PACKAGES
        .iter()
        .find_map(|(args, catalog)| args(code).map(|declared| (declared, *catalog)))
}

/// Whether `code` is a live key of the spec's.
fn is_spec_key(code: &str) -> bool {
    spec_key(code).is_some()
}

/// `d` and each of its nested causes, where coded under a spec key, held to
/// the spec: a message in its catalog, only args its key declares (a
/// flattened `cause.<arg>` counting as `cause`), and every variable its
/// message names given.
fn spec_keyed(d: &dyn Diagnostic, problems: &mut Vec<String>) {
    let code = d.code();
    if let Some((declared, (_, text))) = spec_key(code) {
        let catalog = Catalog::parse(text).expect("the spec's catalog parses");
        let given: BTreeSet<&str> = d.args().iter().map(|(name, _)| *name).collect();
        for name in &given {
            let base = name.split_once('.').map_or(*name, |(base, _)| base);
            if !declared.contains(&base) {
                problems.push(format!("{code}: gives {name}, which it does not declare"));
            }
        }
        match catalog.variables(code) {
            None => problems.push(format!("{code}: no message in the spec's catalog")),
            Some(vars) => {
                for var in vars.iter().filter(|v| !given.contains(v.as_str())) {
                    problems.push(format!(
                        "{code}: its message names {var}, which it does not give"
                    ));
                }
            }
        }
    }
    for (_, arg) in d.args() {
        if let Arg::Nested(inner) = arg {
            spec_keyed(inner.as_ref(), problems);
        }
    }
}

/// Every code a crate's library source (not its `*.test.rs` tests) writes,
/// outside comments: each `code!("suffix")` there, under the crate's name
/// `krate`, as the macro expands it.
fn source_codes(krate: &str, dir: &Path) -> BTreeSet<String> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs")
                && !p.to_string_lossy().ends_with(".test.rs")
            {
                out.push(p);
            }
        }
    }
    let mut files = Vec::new();
    walk(&dir.join("src"), &mut files);
    let mut codes = BTreeSet::new();
    for file in files {
        let text = fs::read_to_string(&file).unwrap();
        let code: Vec<&str> = text
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect();
        codes.extend(suffixes(&code.join("\n")).map(|s| format!("{krate}-{s}")));
    }
    codes
}

/// The suffix of each `code!("...")` in `source`, spaces and line breaks
/// inside the call allowed.
fn suffixes(source: &str) -> impl Iterator<Item = &str> {
    source.split("code!(").skip(1).filter_map(|call| {
        let (suffix, _) = call.trim_start().strip_prefix('"')?.split_once('"')?;
        suffix
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            .then_some(suffix)
    })
}

#[test]
fn every_code_has_a_message_with_exactly_its_args_in_every_locale() {
    let mut problems = Vec::new();
    let catalogs = catalogs(&mut problems);
    let locales: BTreeSet<String> = catalogs.values().flat_map(|c| c.keys().cloned()).collect();
    assert!(
        locales.contains("en-US"),
        "no en-US catalogs under {}",
        crates_dir().display()
    );

    let checked: Vec<&str> = samples().into_iter().map(|(krate, _)| krate).collect();
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    for (krate, diagnostics) in samples() {
        if diagnostics.is_empty() {
            problems.push(format!("{krate}: no samples"));
        }
        // A spec key is checked against the spec's catalog, a store error the
        // crate forwards under angzarr-ports' own code against angzarr-ports',
        // every other sample against the crate's own.
        for d in &diagnostics {
            spec_keyed(&**d, &mut problems);
        }
        let (forwarded, own): (Vec<_>, Vec<_>) = diagnostics
            .into_iter()
            .filter(|d| !is_spec_key(d.code()))
            .partition(|d| is_forwarded(d.code()));
        for locale in &locales {
            match catalogs.get(krate).and_then(|c| c.get(locale)) {
                None => problems.push(format!("{krate}: no {locale} catalog")),
                Some(ftl) => problems.extend(
                    catalog::check(krate, ftl, &own)
                        .into_iter()
                        .map(|p| format!("{krate} ({locale}): {p:?}")),
                ),
            }
        }
        // Every forwarded error has its message there; that each of
        // angzarr-ports' messages has an error is checked in core.
        let (ports, ports_ftl) = angzarr_ports::EN_US;
        problems.extend(
            catalog::check(ports, ports_ftl, &forwarded)
                .into_iter()
                .filter(|p| !matches!(p, catalog::Problem::Unsampled { .. }))
                .map(|p| format!("{krate} forwarding {ports}: {p:?}")),
        );
        let mut codes = BTreeSet::new();
        for d in &own {
            own_codes(&**d, krate, &mut codes);
        }
        for code in codes {
            if let Some(previous) = seen.insert(code.clone(), krate) {
                problems.push(format!("{code}: produced by {previous} and by {krate}"));
            }
        }
    }

    // Every crate with a catalog is checked, every checked crate is one of
    // the workspace's, and a crate that writes codes is checked.
    for name in catalogs.keys() {
        if !checked.contains(&name.as_str()) {
            problems.push(format!("{name}: a catalog no samples are checked against"));
        }
    }
    let members = members();
    for krate in &checked {
        if !members.contains_key(*krate) {
            problems.push(format!("{krate}: checked, but no crate of this workspace"));
        }
    }

    // No variant without a sample: every code in a crate's source is seen.
    for (name, dir) in &members {
        let checked = checked.contains(&name.as_str());
        for code in source_codes(name, dir) {
            if !checked {
                problems.push(format!(
                    "{code}: in {name}'s source, whose samples are not checked"
                ));
            } else if !seen.contains_key(&code) {
                problems.push(format!("{code}: in {name}'s source but in no sample"));
            }
        }
    }

    problems.sort();
    problems.dedup();
    assert!(
        problems.is_empty(),
        "{} problem(s):\n{}",
        problems.len(),
        problems.join("\n")
    );
}

/// A dependent renders from the exported catalog, so it must be the
/// crate's en-US source under the crate's own name, as checked here.
#[test]
fn every_checked_crate_exports_its_en_us_catalog() {
    let members = members();
    for ((name, text), _) in checked() {
        let dir = members
            .get(name)
            .unwrap_or_else(|| panic!("{name}: no crate of this workspace"));
        let path = dir.join("locales/en-US").join(SOURCE);
        let file =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert_eq!(
            text,
            catalog::prefixed(name, &file),
            "{name}::EN_US is not {} under {name}",
            path.display()
        );
    }
}

/// A code in a crate's source is each `code!("...")` call's suffix, under
/// the crate's name, wherever the call's literal sits; a commented-out
/// call, and anything that is no such call, is none.
#[test]
fn a_crates_codes_are_its_code_calls_under_its_name() {
    let source = "x => code!(\"one\"),\n\
                  y => angzarr_diagnostic::code!( \"two-2\" ),\n\
                  z => code!(\n    \"three\"\n),\n\
                  // w => code!(\"commented\"),\n\
                  v => code!(SUFFIX), u => code!(\"Not-Kebab\"), t => \"code!(\",\n";
    let found: Vec<&str> = suffixes(source).collect();
    assert_eq!(found, ["one", "two-2", "three", "commented"]);

    let dir =
        std::env::temp_dir().join(format!("{}-{}", env!("CARGO_PKG_NAME"), std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("src/inner")).unwrap();
    fs::write(dir.join("src/lib.rs"), source).unwrap();
    fs::write(dir.join("src/inner/deep.rs"), "code!(\"deep\")").unwrap();
    fs::write(dir.join("src/lib.test.rs"), "code!(\"tested\")").unwrap();
    fs::write(dir.join("src/notes.txt"), "code!(\"text\")").unwrap();
    let codes: Vec<String> = source_codes("my-crate", &dir).into_iter().collect();
    assert_eq!(
        codes,
        [
            "my-crate-deep",
            "my-crate-one",
            "my-crate-three",
            "my-crate-two-2"
        ]
    );
    fs::remove_dir_all(&dir).unwrap();
}

/// A crate's name is its manifest's `[package]` `name`, not a dependency's
/// or a target's.
#[test]
fn a_crates_name_is_its_packages() {
    let manifest = "[lib]\nname = \"lib_name\"\n\n[package]\nversion = \"1\"\n\
                    name   =   \"the-package\"\n\n[dependencies]\nname = \"x\"\n";
    assert_eq!(package_name(manifest).as_deref(), Some("the-package"));
    assert_eq!(package_name("[dependencies]\nname = \"x\"\n"), None);
    let members = members();
    assert_eq!(
        members.get(env!("CARGO_PKG_NAME")),
        Some(&PathBuf::from(env!("CARGO_MANIFEST_DIR")))
    );
    for ((name, _), _) in checked() {
        assert!(members.contains_key(name), "{name}");
    }
}

/// The store errors angzarr-embedded forwards under their own codes are
/// rendered from angzarr-ports' own catalog, re-exported here: no copy.
#[test]
fn angzarr_embedded_reexports_the_angzarr_ports_catalog() {
    assert_eq!(angzarr_embedded::angzarr_ports::EN_US, angzarr_ports::EN_US);
}

/// Every live key of the spec's enums embedded answers, with its declared
/// args: core's `AngzarrReason`, and the local-process transport's
/// `EmbeddedReason`. An enum's numbers may skip those reserved for keys it
/// moved (decision 14.64), so every number up to a bound is tried.
fn spec_keys() -> Vec<(&'static str, &'static [&'static str])> {
    use angzarr_embedded::v1::AngzarrReason;
    use angzarr_proto_transport_embedded::v1::EmbeddedReason;
    let numbers = || 1..=128;
    let core = numbers().filter_map(|n| AngzarrReason::try_from(n).ok());
    let embedded = numbers().filter_map(|n| EmbeddedReason::try_from(n).ok());
    core.map(|r| (r.as_str_name(), r.args()))
        .chain(embedded.map(|r| (r.as_str_name(), r.args())))
        .collect()
}

/// Every core key angzarr-router can refuse with is the kernel's code too,
/// verbatim, with the router's args (decision 14.60): `KernelError::Router`
/// forwards the router's `CodedError` under it, its message the spec's.
#[test]
fn every_core_key_is_forwarded_under_its_own_key() {
    use angzarr_embedded::angzarr_router::error::CodedError;
    use angzarr_embedded::v1::AngzarrReason;
    use angzarr_embedded::KernelError;

    let core = (1..=128).filter_map(|n| AngzarrReason::try_from(n).ok());
    let mut problems = Vec::new();
    for reason in core {
        let key = reason.as_str_name();
        let error = KernelError::Router(CodedError {
            code: key.to_string(),
            extras: [("type_url".to_string(), "/a.B".to_string())].into(),
            grpc: angzarr_embedded::angzarr_router::error::GrpcCode::Internal,
            refusal: None,
        });
        if error.code() != key {
            problems.push(format!("{key} is {}", error.code()));
        }
        let args: Vec<String> = error.args().iter().map(|(n, _)| n.to_string()).collect();
        if args != ["type_url"] {
            problems.push(format!("{key}: args {args:?}"));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// Every core key angzarr-embedded's samples answer under gives, across
/// its answerers, exactly the args the key declares, so each answered key
/// is held to the spec's catalog whole. A key whose answerers differ (the
/// router's duplicate and the registry's, decision 14.44) gives every
/// declared arg across them.
#[test]
fn every_sampled_core_key_gives_its_declared_args() {
    use angzarr_embedded::v1::AngzarrReason;
    let samples = angzarr_embedded::diagnostic_samples();
    let core = (1..=128).filter_map(|n| AngzarrReason::try_from(n).ok());
    let mut problems = Vec::new();
    let mut answered = 0;
    for reason in core {
        let key = reason.as_str_name();
        let answers: Vec<_> = samples.iter().filter(|d| d.code() == key).collect();
        if answers.is_empty() {
            continue;
        }
        answered += 1;
        let args: BTreeSet<&str> = answers
            .iter()
            .flat_map(|d| d.args().into_iter().map(|(n, _)| n))
            .collect();
        let declared: BTreeSet<&str> = reason.args().iter().copied().collect();
        if args != declared {
            problems.push(format!("{key}: args {args:?}, declared {declared:?}"));
        }
    }
    assert!(answered > 0, "no core key sampled");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// The spec's catalog renders every live key, in en-US, with its declared
/// args and no error: the parity the crates' own catalogs have, for the
/// catalog every runtime embeds.
#[test]
fn the_spec_catalog_renders_every_key_with_its_declared_args() {
    let mut problems = Vec::new();
    for (key, declared) in spec_keys() {
        let args: Vec<(&str, Arg)> = declared.iter().map(|a| (*a, Arg::Uint(2))).collect();
        let (text, errors) = render(key, &args, "en-US");
        problems.extend(errors);
        if text.is_empty() {
            problems.push(format!("{key}: renders empty"));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// A spec-keyed error that gives an arg its key does not declare, or omits
/// one its message names, is caught.
#[test]
fn a_spec_keyed_error_is_held_to_its_keys_args() {
    use angzarr_diagnostic::Report;
    let mut problems = Vec::new();
    spec_keyed(
        &Report::new("ANGZARR_REASON_CHAIN_TOO_DEEP").arg("stream", "a/b"),
        &mut problems,
    );
    assert_eq!(
        problems,
        [
            "ANGZARR_REASON_CHAIN_TOO_DEEP: gives stream, which it does not declare",
            "ANGZARR_REASON_CHAIN_TOO_DEEP: its message names limit, which it does not give",
        ]
    );
    let mut nested = Vec::new();
    spec_keyed(
        &Report::new("angzarr-embedded-downstream").arg(
            "error",
            Arg::nested(&Report::new("ANGZARR_REASON_STORE_UNAVAILABLE").arg(
                "cause",
                Arg::nested(&Report::new("x").arg("ext_err_txt", "e")),
            )),
        ),
        &mut nested,
    );
    assert!(nested.is_empty(), "{nested:?}");
    let mut stray = Vec::new();
    spec_keyed(&Report::new("ANGZARR_REASON_UNSPECIFIED"), &mut stray);
    assert!(stray.is_empty(), "UNSPECIFIED is no live key: {stray:?}");
}

/// The kernel's codes were `angzarr-embedded-next-*` and are
/// `angzarr-embedded-*`, the one rule being "strip `next-`"
/// (docs/design/audit.md, *The renames of the kernel's codes*). The rule
/// collides with nothing: no kernel code is left under the old
/// prefix, in its source or any catalog; none lands under another checked
/// crate's name (`angzarr-embedded-host-…`), where it would read as that
/// crate's; and no two codes, the kernel's or another crate's, are the
/// same, so no old code maps onto one that already meant something else.
#[test]
fn the_kernels_codes_lost_next_and_collide_with_nothing() {
    let kernel_name = angzarr_embedded::EN_US.0;
    let old = format!("{kernel_name}-next-");
    let mut problems = Vec::new();
    let catalogs = catalogs(&mut problems);
    let mut all: BTreeMap<String, &str> = BTreeMap::new();
    let mut kernel = BTreeSet::new();
    for (krate, diagnostics) in samples() {
        let mut codes = BTreeSet::new();
        for d in diagnostics
            .iter()
            .filter(|d| !is_forwarded(d.code()) && !is_spec_key(d.code()))
        {
            own_codes(&**d, krate, &mut codes);
        }
        for code in &codes {
            if let Some(previous) = all.insert(code.clone(), krate) {
                problems.push(format!("{code}: {previous}'s and {krate}'s"));
            }
        }
        if krate == kernel_name {
            kernel = codes;
        }
    }
    assert!(!kernel.is_empty(), "no kernel codes sampled");
    for code in &kernel {
        if code.starts_with(&old) {
            problems.push(format!("{code}: still under {old}"));
        }
        if owner(code) != Some(kernel_name) {
            problems.push(format!("{code}: reads as {:?}'s", owner(code)));
        }
    }
    for (krate, by_locale) in &catalogs {
        for (locale, ftl) in by_locale {
            if ftl.contains(&old) {
                problems.push(format!("{krate} ({locale}): names {old}…"));
            }
        }
    }
    for code in source_codes(kernel_name, &members()[kernel_name]) {
        if code.starts_with(&old) {
            problems.push(format!("{code}: in {kernel_name}'s source"));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// The crates outside this workspace whose errors this workspace's carry
/// under their own codes, each with its exported en-US catalog:
/// angzarr-ports (the store errors angzarr-embedded forwards) and
/// angzarr-yaml (the host config's load errors, the cause of
/// angzarr-embedded-host's `ConfigInvalid`).
fn foreign() -> [(&'static str, &'static str); 2] {
    [
        angzarr_ports::EN_US,
        angzarr_embedded_host::angzarr_embedded_host_config::angzarr_yaml::EN_US,
    ]
}

/// The crate a code belongs to: the longest name it starts with, of the
/// checked crates and the [`foreign`] ones.
fn owner(code: &str) -> Option<&'static str> {
    samples()
        .into_iter()
        .map(|(krate, _)| krate)
        .chain(foreign().map(|(krate, _)| krate))
        .filter(|k| code.starts_with(&format!("{k}-")))
        .max_by_key(|k| k.len())
}

/// `code`'s message in `locale`, rendered with `args`: the text, and every
/// error Fluent reported (a variable the message names and `args` lack is
/// one). The catalog is the code's owner's ([`owner`]): a checked crate's
/// source under its name ([`catalogs`]), or a [`foreign`] crate's own
/// `EN_US`.
fn render(code: &str, args: &[(&str, Arg)], locale: &str) -> (String, Vec<String>) {
    use fluent_bundle::{FluentArgs, FluentBundle, FluentResource, FluentValue};
    let spec = spec_key(code).map(|(_, catalog)| catalog);
    let krate = spec
        .map(|(name, _)| name)
        .or_else(|| owner(code))
        .unwrap_or_else(|| panic!("{code}: no crate owns it"));
    let exported = spec
        .into_iter()
        .chain(foreign())
        .find(|(name, _)| *name == krate);
    let (path, text) = match exported {
        Some((name, text)) if locale == "en-US" => {
            (PathBuf::from(format!("{name}::EN_US")), text.to_string())
        }
        Some((name, _)) => {
            return (
                String::new(),
                vec![format!("{code}: {name} ships no {locale} catalog")],
            )
        }
        None => {
            let text = catalogs(&mut Vec::new())
                .get(krate)
                .and_then(|by_locale| by_locale.get(locale))
                .unwrap_or_else(|| panic!("{krate}: no {locale} catalog"))
                .clone();
            (PathBuf::from(format!("{krate} ({locale})")), text)
        }
    };
    let resource = FluentResource::try_new(text).unwrap_or_else(|(r, _)| r);
    let mut bundle = FluentBundle::new(vec![locale.parse().expect("a locale tag")]);
    bundle.set_use_isolating(false);
    bundle
        .add_resource(resource)
        .unwrap_or_else(|e| panic!("{}: {e:?}", path.display()));
    let mut fluent = FluentArgs::new();
    for (name, arg) in args {
        let value = match arg {
            Arg::Int(n) => FluentValue::from(*n),
            Arg::Uint(n) => FluentValue::from(*n),
            Arg::Str(s) => FluentValue::from(s.clone()),
            Arg::Nested(cause) => FluentValue::from(cause.code().to_string()),
        };
        fluent.set(*name, value);
    }
    let Some(pattern) = bundle.get_message(code).and_then(|m| m.value()) else {
        return (
            String::new(),
            vec![format!("{code}: no message in {locale}")],
        );
    };
    let mut errors = Vec::new();
    let rendered = bundle.format_pattern(pattern, Some(&fluent), &mut errors);
    (
        rendered.into_owned(),
        errors.iter().map(|e| format!("{code}: {e:?}")).collect(),
    )
}

/// Every sample's message, in every shipped locale, renders with the
/// sample's own args and no error: no message names a variable its error
/// does not give, whichever args that error has.
#[test]
fn every_samples_message_renders_with_its_args() {
    let mut problems = Vec::new();
    let locales: BTreeSet<String> = catalogs(&mut problems)
        .values()
        .flat_map(|c| c.keys().cloned())
        .collect();
    fn each(d: &dyn Diagnostic, locales: &BTreeSet<String>, problems: &mut Vec<String>) {
        for locale in locales {
            let (_, errors) = render(d.code(), &d.args(), locale);
            problems.extend(errors.into_iter().map(|e| format!("{e} ({locale})")));
        }
        for (_, arg) in d.args() {
            if let Arg::Nested(inner) = arg {
                each(inner.as_ref(), locales, problems);
            }
        }
    }
    for (_, diagnostics) in samples() {
        for d in &diagnostics {
            each(&**d, &locales, &mut problems);
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// SQLite's failure renders whether or not the driver gave SQLite's result
/// code: with it, as the database's failure naming the code; without it
/// (an I/O error, a pool timing out), under a key of its own that names
/// none, so no `$errno` goes missing.
#[test]
fn sqlites_failure_renders_with_and_without_its_errno() {
    use angzarr_store_sqlite::SqlError;
    let detail = "database is locked".to_string();
    let rendered = |error: SqlError| render(error.code(), &Diagnostic::args(&error), "en-US");

    let with = rendered(SqlError::Unavailable {
        errno: Some(5),
        detail: detail.clone(),
    });
    assert_eq!(
        with,
        (
            "the database failed (SQLite code 5): database is locked".to_string(),
            vec![]
        )
    );

    let without = rendered(SqlError::Unavailable {
        errno: None,
        detail,
    });
    assert_eq!(
        without,
        (
            "the database driver failed: database is locked".to_string(),
            vec![]
        )
    );
}
