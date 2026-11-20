//! The file side of `angzarr scaffold app`: the host config and the driver
//! written once and never overwritten, build.rs and main.rs rewritten every
//! run from the protos and the host config on disk, a refused config
//! writing nothing; and `app`, the one call the generated build.rs makes.

use std::path::Path;

use angzarr_build::{
    AppSeed, BuildError, DomainCrate, RetrySeed, Scaffolded, app_into, app_scaffold_into,
    app_without_domain_into,
};
use angzarr_codegen::{AppError, CodegenError};

use angzarr_test_dir::TestDir;

const BUYIN: &[&str] = &[
    "buyin/v1/wallet.proto",
    "buyin/v1/buy_in.proto",
    "buyin/v1/policies.proto",
];
const SHOP: &[&str] = &["shop/v1/shop.proto"];
const APP_FILES: &[&str] = &[
    "Cargo.toml",
    "angzarr.yaml",
    "build.rs",
    "src/driver.rs",
    "src/main.rs",
];

fn fixtures() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../angzarr-codegen/tests/fixtures")
        .display()
        .to_string()
}

/// A fresh, empty directory.
fn empty_dir() -> TestDir {
    TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-app-test"))
}

fn seed() -> AppSeed {
    AppSeed {
        name: "buyin".to_string(),
        includes: vec!["proto".to_string()],
        languages: Vec::new(),
        expose: Vec::new(),
        retry: RetrySeed::default(),
        in_memory: true,
        angzarr_build: None,
        domain: None,
    }
}

fn scaffold(root: &Path, protos: &[&str], seed: &AppSeed) -> Vec<Scaffolded> {
    app_scaffold_into(root, protos, &[&fixtures()], seed).unwrap()
}

/// Each file's action, by its path under `root`, sorted by path.
fn actions(root: &Path, done: &[Scaffolded]) -> Vec<(String, &'static str)> {
    let mut out: Vec<(String, &'static str)> = done
        .iter()
        .map(|d| {
            let (path, action) = match d {
                Scaffolded::Created(p) => (p, "created"),
                Scaffolded::Completed { path, .. } => (path, "completed"),
                Scaffolded::Regenerated(p) => (p, "regenerated"),
                Scaffolded::Kept { path, .. } => (path, "kept"),
                Scaffolded::Obsolete(p) => (p, "obsolete"),
                Scaffolded::Deleted(p) => (p, "deleted"),
            };
            let rel = path.strip_prefix(root).unwrap_or(path);
            (rel.display().to_string(), action)
        })
        .collect();
    out.sort();
    out
}

fn all(action: &'static str) -> Vec<(String, &'static str)> {
    APP_FILES.iter().map(|p| (p.to_string(), action)).collect()
}

fn read(root: &Path, path: &str) -> String {
    std::fs::read_to_string(root.join(path)).unwrap_or_else(|e| panic!("{path}: {e}"))
}

#[test]
fn scaffolding_an_app_writes_its_config_build_script_main_and_driver() {
    let root = empty_dir();
    assert_eq!(
        actions(&root, &scaffold(&root, SHOP, &seed())),
        all("created")
    );
    for path in APP_FILES {
        assert!(root.join(path).exists(), "{path}");
    }
}

#[test]
fn rescaffolding_an_app_unchanged_keeps_every_file_byte_for_byte() {
    let root = empty_dir();
    scaffold(&root, SHOP, &seed());
    let before: Vec<String> = APP_FILES.iter().map(|p| read(&root, p)).collect();
    assert_eq!(actions(&root, &scaffold(&root, SHOP, &seed())), all("kept"));
    let after: Vec<String> = APP_FILES.iter().map(|p| read(&root, p)).collect();
    assert_eq!(before, after);
}

#[test]
fn rescaffolding_never_overwrites_the_users_files_and_rewrites_the_clis_build_script() {
    let root = empty_dir();
    scaffold(&root, SHOP, &seed());
    let config = format!(
        "{}\nretry:\n  max_retries: 9\n",
        read(&root, "angzarr.yaml")
    );
    let driver = "// mine\npub async fn drive(_: &angzarr_embedded_host::App) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }\n";
    let main = format!("{}\npub mod mine;\n", read(&root, "src/main.rs"));
    std::fs::write(root.join("angzarr.yaml"), &config).unwrap();
    std::fs::write(root.join("src/driver.rs"), driver).unwrap();
    std::fs::write(root.join("src/main.rs"), &main).unwrap();
    let build = read(&root, "build.rs");
    std::fs::write(root.join("build.rs"), format!("{build}// edited\n")).unwrap();

    // A different seed changes nothing of a config that exists.
    let other = AppSeed {
        in_memory: false,
        retry: RetrySeed {
            max_retries: Some(1),
            max_race_runs: Some(1),
        },
        ..seed()
    };
    assert_eq!(
        actions(&root, &scaffold(&root, SHOP, &other)),
        [
            ("Cargo.toml".to_string(), "kept"),
            ("angzarr.yaml".to_string(), "kept"),
            ("build.rs".to_string(), "regenerated"),
            ("src/driver.rs".to_string(), "kept"),
            ("src/main.rs".to_string(), "kept"),
        ]
    );
    assert_eq!(read(&root, "angzarr.yaml"), config);
    assert_eq!(read(&root, "src/driver.rs"), driver);
    assert_eq!(read(&root, "src/main.rs"), main);
    assert_eq!(read(&root, "build.rs"), build);
}

/// The build script is the CLI's: one without the generated marker (written
/// by hand in its place) is refused, never overwritten (item S, H1).
#[test]
fn a_build_script_without_the_marker_is_refused() {
    let root = empty_dir();
    scaffold(&root, SHOP, &seed());
    std::fs::write(root.join("build.rs"), "// mine\nfn main() {}\n").unwrap();
    let refused = app_scaffold_into(&root, SHOP, &[&fixtures()], &seed());
    assert!(
        matches!(&refused, Err(BuildError::GeneratedUnmarked { path }) if path.ends_with("build.rs")),
        "{refused:?}"
    );
    assert_eq!(read(&root, "build.rs"), "// mine\nfn main() {}\n");
}

/// The application's main is the user's, written once with a module per
/// in-process component's implementation (item S Q7): an edit to the
/// config is followed by the build's generated layer, never by a rewrite
/// of main.
#[test]
fn main_is_written_once_with_the_in_process_implementations() {
    let root = empty_dir();
    scaffold(&root, BUYIN, &seed());
    let main = read(&root, "src/main.rs");
    for module in [
        "pub mod buy_in_state_impl;",
        "pub mod wallet_state_impl;",
        "pub mod driver;",
    ] {
        assert!(main.contains(module), "{module}:\n{main}");
    }
    assert!(!main.contains("pub mod buy_in_state;"), "{main}");
    assert!(
        main.contains("include!(concat!(env!(\"OUT_DIR\"), \"/angzarr-app.rs\"));"),
        "{main}"
    );
    let config = read(&root, "angzarr.yaml").replace(
        "  - name: pmg-buyin\n    kind: domain\n    language: rust\n",
        "  - name: pmg-buyin\n    kind: domain\n    language: python\n    \
         entry: [angzarr-buyin]\n",
    );
    assert_ne!(config, read(&root, "angzarr.yaml"));
    std::fs::write(root.join("angzarr.yaml"), &config).unwrap();
    scaffold(&root, BUYIN, &seed());
    assert_eq!(read(&root, "src/main.rs"), main);
}

/// A manifest is the user's: its pins that are not this CLI's are
/// reported, and it is never edited (item S Q8).
#[test]
fn a_manifests_drifted_pin_is_reported_and_the_manifest_kept() {
    let root = empty_dir();
    scaffold(&root, SHOP, &seed());
    let rev = angzarr_build::ANGZARR_EMBEDDED_REV;
    let manifest = read(&root, "Cargo.toml").replace(rev, "0123abcd");
    std::fs::write(root.join("Cargo.toml"), &manifest).unwrap();
    let done = scaffold(&root, SHOP, &seed());
    let drift = done
        .iter()
        .find_map(|d| match d {
            Scaffolded::Kept { path, notes } if path.ends_with("Cargo.toml") => {
                Some(notes.drift.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("{done:?}"));
    assert!(
        drift
            .iter()
            .any(|d| d.dependency == "angzarr-embedded" && d.pinned == "0123abcd" && d.cli == rev),
        "{drift:?}"
    );
    assert_eq!(read(&root, "Cargo.toml"), manifest);
}

#[test]
fn a_config_with_lints_is_refused_and_nothing_is_written() {
    let root = empty_dir();
    let dealer = AppSeed {
        languages: vec![("dealer".to_string(), "python".to_string())],
        ..seed()
    };
    let refused = app_scaffold_into(&root, BUYIN, &[&fixtures()], &dealer);
    assert!(
        matches!(
            &refused,
            Err(BuildError::Codegen(CodegenError::App(AppError::Refused { lints }))) if lints.len() == 1
        ),
        "{refused:?}"
    );
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
}

#[test]
fn app_into_writes_the_whole_layer_the_configs_remote_domains_as_command_and_event_sets() {
    let out = empty_dir();
    let config = out.join("angzarr.yaml");
    std::fs::write(
        &config,
        "schema_version: 1\
         \ncomponent:\
         \n  - name: wallet\
         \n    kind: domain\
         \n    language: rust\
         \n  - name: pmg-buyin\
         \n    kind: domain\
         \n    language: python\
         \n",
    )
    .unwrap();
    app_into(&out, &config, BUYIN, &[&fixtures()]).unwrap();
    for file in [
        "buyin.v1.rs",
        "buyin.v1.domain.rs",
        "angzarr-adapters.rs",
        "angzarr-descriptors.rs",
        "angzarr-register.rs",
        "angzarr-app.rs",
    ] {
        assert!(out.join(file).exists(), "{file}");
    }
    let layer = std::fs::read_to_string(out.join("angzarr-app.rs")).unwrap();
    assert!(
        layer.contains("pub mod domain {\n    include!(concat!(env!(\"OUT_DIR\"), \"/buyin.v1.domain.rs\"));\n}"),
        "{layer}"
    );
    let register = std::fs::read_to_string(out.join("angzarr-register.rs")).unwrap();
    assert!(register.contains("adapters::Wallet"), "{register}");
    // The Python child's process manager, listed by its name, is its
    // domain's command and event sets.
    let adapters = std::fs::read_to_string(out.join("angzarr-adapters.rs")).unwrap();
    assert!(
        adapters.contains("`buyin.v1.BuyInState`, served by a child process"),
        "{adapters}"
    );
    assert!(
        adapters.contains("`buyin.v1.WalletState`, delegating to"),
        "{adapters}"
    );
    assert!(
        !register.contains("adapters::BuyIn>"),
        "the buy-in is the Python child's: {register}"
    );
}

#[test]
fn app_without_domain_into_writes_the_layer_but_the_wire_types_and_domain_layer() {
    let out = empty_dir();
    let config = out.join("angzarr.yaml");
    std::fs::write(
        &config,
        "schema_version: 1\
         \ncomponent:\
         \n  - name: wallet\
         \n    kind: domain\
         \n    language: rust\
         \n  - name: pmg-buyin\
         \n    kind: domain\
         \n    language: python\
         \n",
    )
    .unwrap();
    std::fs::write(
        out.join("Cargo.toml"),
        "[package]\nname = \"x\"\n\n[package.metadata.angzarr.domain]\ncrate = \"buyin_proto\"\nwire = \"buyin_proto::v1\"\n",
    )
    .unwrap();
    app_without_domain_into(&out, &config, BUYIN, &[&fixtures()]).unwrap();
    for file in [
        "angzarr-adapters.rs",
        "angzarr-descriptors.rs",
        "angzarr-register.rs",
        "angzarr-app.rs",
    ] {
        assert!(out.join(file).exists(), "{file}");
    }
    for file in ["buyin.v1.rs", "buyin.v1.domain.rs"] {
        assert!(!out.join(file).exists(), "{file} is the domain crate's");
    }
    // The layer main.rs includes names the manifest's domain crate.
    let layer = std::fs::read_to_string(out.join("angzarr-app.rs")).unwrap();
    for expected in [
        "\npub use buyin_proto::domain;\n",
        "\npub use buyin_proto::v1 as wire;\n",
        "include!(concat!(env!(\"OUT_DIR\"), \"/angzarr-adapters.rs\"));",
    ] {
        assert!(layer.contains(expected), "{expected}:\n{layer}");
    }
}

#[test]
fn rescaffolding_follows_the_domain_crate_the_manifest_on_disk_names() {
    let root = empty_dir();
    scaffold(&root, SHOP, &seed());
    let manifest = read(&root, "Cargo.toml").replace(
        "\n[dependencies]\n",
        "\n[package.metadata.angzarr.domain]\ncrate = \"shop_proto\"\n\n[dependencies]\n",
    );
    std::fs::write(root.join("Cargo.toml"), &manifest).unwrap();
    let done = scaffold(&root, SHOP, &seed());
    assert_eq!(
        actions(&root, &done),
        [
            ("Cargo.toml".to_string(), "kept"),
            ("angzarr.yaml".to_string(), "kept"),
            ("build.rs".to_string(), "regenerated"),
            ("src/driver.rs".to_string(), "kept"),
            ("src/main.rs".to_string(), "kept"),
        ]
    );
    assert!(read(&root, "build.rs").contains("angzarr_build::app_without_domain("));
    assert_eq!(read(&root, "Cargo.toml"), manifest);
    // A seed's domain crate does not rewrite the user's manifest.
    let over = AppSeed {
        domain: Some(DomainCrate {
            path: "other".to_string(),
            wire: None,
        }),
        ..seed()
    };
    scaffold(&root, SHOP, &over);
    assert!(!read(&root, "build.rs").contains("other"));
}

#[test]
fn a_config_that_cannot_be_read_is_an_error_not_a_new_config() {
    let root = empty_dir();
    std::fs::create_dir_all(root.join("angzarr.yaml")).unwrap();
    let refused = app_scaffold_into(&root, SHOP, &[&fixtures()], &seed());
    assert!(matches!(refused, Err(BuildError::Io { .. })), "{refused:?}");
}
