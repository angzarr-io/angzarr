//! An angzarr-embedded application's startup, generated (`angzarr
//! scaffold app`): the host config and the driver the user's, written
//! once; build.rs and main.rs the CLI's, derived from the protos and the
//! host config on disk. Pinned by golden files (`tests/golden/app`), and
//! the host config's lints against the protos.
//!
//! The host config is `angzarr.yaml`, read through
//! angzarr-embedded-host-config, the schema angzarr-embedded-host runs:
//! stamped `schema_version`, `kind` required on every component, every
//! component listed by its name from the protos, unknown keys refused at
//! their key path.

mod common;

use angzarr_codegen::{
    ANGZARR_BUILD_GIT, ANGZARR_BUILD_REV, AppError, AppLint, AppSeed, CodegenError, DomainCrate,
    Exposure, Owner, RetrySeed, ScaffoldFile, Warning, app, app_layer, child_components, lint_app,
    remote_domains,
};
use common::*;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/app");

/// The blackjack example: the table aggregate, the player aggregate, and
/// the policies between them (and one to the wallet, outside the app).
const BLACKJACK: &[&str] = &[
    "io/angzarr/examples/v1/table.proto",
    "io/angzarr/examples/v1/player.proto",
    "io/angzarr/examples/v1/policies.proto",
];
const SHOP: &[&str] = &["shop/v1/shop.proto"];

fn seed(name: &str) -> AppSeed {
    AppSeed {
        name: name.to_string(),
        includes: vec!["proto".to_string()],
        languages: Vec::new(),
        expose: Vec::new(),
        retry: RetrySeed::default(),
        in_memory: false,
        angzarr_build: None,
        domain: None,
    }
}

/// What blackjack's e2e and examples-rust's `remote_player` hand-write:
/// the table and its policies in process, the player a Python child, the
/// store in memory.
fn blackjack() -> AppSeed {
    AppSeed {
        languages: vec![("player".to_string(), "python".to_string())],
        in_memory: true,
        ..seed("blackjack")
    }
}

fn file<'a>(files: &'a [ScaffoldFile], path: &str) -> &'a ScaffoldFile {
    files
        .iter()
        .find(|f| f.path == path)
        .unwrap_or_else(|| panic!("no {path} among {:?}", paths(files)))
}

fn paths(files: &[ScaffoldFile]) -> Vec<&str> {
    let mut paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    paths.sort_unstable();
    paths
}

fn golden(app: &str, path: &str) -> String {
    std::fs::read_to_string(format!("{GOLDEN}/{app}/{path}")).unwrap()
}

/// Every file of `files` is `app`'s golden (rewritten under
/// `ANGZARR_BLESS`), and owned as the split says: the host config, the
/// manifest, main.rs and the driver the user's, build.rs the CLI's (item S
/// Q7).
fn assert_golden(app: &str, files: &[ScaffoldFile]) {
    assert_eq!(
        paths(files),
        [
            "Cargo.toml",
            "angzarr.yaml",
            "build.rs",
            "src/driver.rs",
            "src/main.rs"
        ]
    );
    for f in files {
        let owner = match f.path.as_str() {
            "angzarr.yaml" | "Cargo.toml" | "src/driver.rs" | "src/main.rs" => Owner::User,
            _ => Owner::Generated,
        };
        assert_eq!(f.owner, owner, "{}", f.path);
        if std::env::var_os("ANGZARR_BLESS").is_some() {
            let path = format!("{GOLDEN}/{app}/{}", f.path);
            std::fs::create_dir_all(std::path::Path::new(&path).parent().unwrap()).unwrap();
            std::fs::write(&path, &f.source).unwrap();
            continue;
        }
        assert_eq!(f.source, golden(app, &f.path), "{}", f.path);
    }
}

#[test]
fn a_single_aggregate_rust_app_is_its_golden_config_build_script_main_and_driver() {
    let files = app(&descriptors(SHOP), SHOP, &seed("shop"), None, None).unwrap();
    assert_golden("shop", &files);
}

#[test]
fn the_mixed_blackjack_app_is_its_golden_rust_table_python_player_and_policies() {
    let files = app(&descriptors(BLACKJACK), BLACKJACK, &blackjack(), None, None).unwrap();
    assert_golden("blackjack", &files);
}

/// examples-rust's layout: the domain layer is a library crate's
/// (examples-proto runs `wire` and `markup_domain`), so the app generates
/// none of its own.
fn examples_proto() -> DomainCrate {
    DomainCrate {
        path: "examples_proto".to_string(),
        wire: Some("examples_proto::v1".to_string()),
    }
}

#[test]
fn an_app_over_a_domain_crate_reuses_its_domain_layer_and_generates_none() {
    let seeded = AppSeed {
        domain: Some(examples_proto()),
        ..blackjack()
    };
    let files = app(&descriptors(BLACKJACK), BLACKJACK, &seeded, None, None).unwrap();
    assert_golden("blackjack-domain-crate", &files);
    let main = &file(&files, "src/main.rs").source;
    assert!(
        main.contains("include!(concat!(env!(\"OUT_DIR\"), \"/angzarr-app.rs\"));"),
        "{main}"
    );
    // The generated layer main.rs includes names the domain crate's.
    let layer = app_layer(&[], Some(&examples_proto()));
    assert!(
        layer.contains("\npub use examples_proto::domain;\n"),
        "{layer}"
    );
    assert!(
        layer.contains("\npub use examples_proto::v1 as wire;\n"),
        "{layer}"
    );
    assert!(
        !layer.contains(".domain.rs") && !layer.contains("examples.v1.rs"),
        "no generated wire types or domain layer: {layer}"
    );
    let build = &file(&files, "build.rs").source;
    assert!(
        build.contains("angzarr_build::app_without_domain(\"angzarr.yaml\", PROTOS, INCLUDES)"),
        "{build}"
    );
    let manifest = &file(&files, "Cargo.toml").source;
    assert!(
        manifest.contains(
            "\n[package.metadata.angzarr.domain]\ncrate = \"examples_proto\"\nwire = \"examples_proto::v1\"\n"
        ),
        "{manifest}"
    );
}

#[test]
fn a_domain_crate_with_no_wire_path_has_no_wire_module() {
    let seeded = AppSeed {
        domain: Some(DomainCrate {
            wire: None,
            ..examples_proto()
        }),
        ..blackjack()
    };
    let files = app(&descriptors(BLACKJACK), BLACKJACK, &seeded, None, None).unwrap();
    let layer = app_layer(&[], seeded.domain.as_ref());
    assert!(
        layer.contains("\npub use examples_proto::domain;\n"),
        "{layer}"
    );
    assert!(
        !layer.contains(" as wire;") && !layer.contains("mod wire"),
        "{layer}"
    );
    let manifest = &file(&files, "Cargo.toml").source;
    assert!(
        manifest.contains("\n[package.metadata.angzarr.domain]\ncrate = \"examples_proto\"\n\n"),
        "{manifest}"
    );
}

#[test]
fn the_manifest_on_disk_names_the_domain_crate_not_the_seed() {
    let set = descriptors(BLACKJACK);
    let seeded = AppSeed {
        domain: Some(examples_proto()),
        ..blackjack()
    };
    let over = app(&set, BLACKJACK, &seeded, None, None).unwrap();
    let manifest = file(&over, "Cargo.toml").source.clone();
    // The user's manifest names the crate: the seed (none) is unused.
    let files = app(&set, BLACKJACK, &blackjack(), None, Some(&manifest)).unwrap();
    // The manifest as this CLI would write it from the seed, which an
    // existing one's pins are judged against; the one on disk is kept.
    assert_eq!(
        file(&files, "Cargo.toml").source,
        file(
            &app(&set, BLACKJACK, &blackjack(), None, None).unwrap(),
            "Cargo.toml"
        )
        .source
    );
    assert_eq!(
        file(&files, "build.rs").source,
        file(&over, "build.rs").source
    );
    // And a manifest naming none generates the layer, whatever the seed.
    let plain = app(&set, BLACKJACK, &blackjack(), None, None).unwrap();
    let mine = file(&plain, "Cargo.toml").source.clone();
    let files = app(&set, BLACKJACK, &seeded, None, Some(&mine)).unwrap();
    assert_eq!(
        file(&files, "build.rs").source,
        file(&plain, "build.rs").source
    );
}

#[test]
fn a_domain_crate_that_is_no_rust_path_or_a_manifest_that_does_not_read_is_refused() {
    let set = descriptors(BLACKJACK);
    for (path, wire) in [("examples-proto", None), ("examples_proto", Some("v1 x"))] {
        let seeded = AppSeed {
            domain: Some(DomainCrate {
                path: path.to_string(),
                wire: wire.map(str::to_string),
            }),
            ..blackjack()
        };
        let bad = wire.unwrap_or(path).to_string();
        assert_eq!(
            app(&set, BLACKJACK, &seeded, None, None),
            Err(CodegenError::App(AppError::DomainPath { path: bad }))
        );
    }
    for manifest in [
        "[package\n",
        "[package]\nname = \"x\"\n[package.metadata.angzarr.domain]\nwire = \"x\"\n",
        "[package]\nname = \"x\"\n[package.metadata.angzarr.domain]\ncrate = \"x\"\nwires = \"x\"\n",
    ] {
        assert!(
            matches!(
                app(&set, BLACKJACK, &blackjack(), None, Some(manifest)),
                Err(CodegenError::App(AppError::Manifest { .. }))
            ),
            "{manifest}"
        );
    }
}

/// The config, its comments and blank lines dropped.
fn settings(toml: &str) -> Vec<&str> {
    toml.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect()
}

#[test]
fn the_generated_blackjack_config_is_the_one_its_e2e_hand_writes() {
    let files = app(&descriptors(BLACKJACK), BLACKJACK, &blackjack(), None, None).unwrap();
    // What the e2e hand-wrote, now with every component listed, each with
    // its kind (the owner's config rulings): the table, the player a
    // Python child, the two policies between them in process; the wallet
    // policy's target is not in the application, so it is not listed.
    let hand = "schema_version: 4\
                \ncomponent:\
                \n  - name: table\
                \n    kind: domain\
                \n    language: rust\
                \n  - name: player\
                \n    kind: domain\
                \n    language: python\
                \n    dir: player\
                \n    entry: [angzarr-player]\
                \n  - name: policy-table-player\
                \n    kind: policy\
                \n    language: rust\
                \n  - name: policy-player-table\
                \n    kind: policy\
                \n    language: rust\
                \nstore:\
                \n  in_memory: true\
                \n";
    assert_eq!(
        settings(&file(&files, "angzarr.yaml").source),
        settings(hand)
    );
    // examples-rust's remote_player: the same components, its `dir` an
    // environment variable's and its handshake a slow first `uv` run's. It
    // is a version-1 file, which reads as the current version: its stamp is
    // not a setting the generated one must share.
    let remote_player = "component:\
                         \n  - name: table\
                         \n    kind: domain\
                         \n    language: rust\
                         \n  - name: player\
                         \n    kind: domain\
                         \n    language: python\
                         \n    entry: [angzarr-player]\
                         \n";
    let generated = settings(&file(&files, "angzarr.yaml").source);
    for line in settings(remote_player) {
        assert!(generated.contains(&line), "{line}: {generated:?}");
    }
}

#[test]
fn main_declares_only_the_in_process_components_implementations() {
    let files = app(&descriptors(BLACKJACK), BLACKJACK, &blackjack(), None, None).unwrap();
    let main = &file(&files, "src/main.rs").source;
    for module in [
        "pub mod table_state_impl;",
        "pub mod table_player_policy_impl;",
        "pub mod player_table_policy_impl;",
    ] {
        assert!(main.contains(module), "{module}:\n{main}");
    }
    // The player is the Python child's; the wallet policy's target is
    // outside the application.
    for absent in ["pub mod player_state", "pub mod table_wallet_policy"] {
        assert!(!main.contains(absent), "{absent}:\n{main}");
    }
}

#[test]
fn expose_and_the_retry_settings_seeded_are_emitted_and_the_unseeded_are_not() {
    let seeded = AppSeed {
        expose: vec![("table".to_string(), Exposure::Loopback)],
        retry: RetrySeed {
            max_retries: Some(5),
            max_race_runs: Some(0),
        },
        ..blackjack()
    };
    let files = app(&descriptors(BLACKJACK), BLACKJACK, &seeded, None, None).unwrap();
    let config = &file(&files, "angzarr.yaml").source;
    assert!(
        config.contains(
            "  - name: table\n    kind: domain\n    language: rust\n    expose: loopback\n"
        ),
        "{config}"
    );
    assert!(
        config.contains("\nretry:\n  max_retries: 5\n  max_race_runs: 0\n"),
        "{config}"
    );
    let ip = AppSeed {
        expose: vec![("table".to_string(), Exposure::Ip)],
        ..blackjack()
    };
    let files = app(&descriptors(BLACKJACK), BLACKJACK, &ip, None, None).unwrap();
    let config = &file(&files, "angzarr.yaml").source;
    assert!(config.contains("    expose: ip\n"), "{config}");
    assert!(
        !config.contains("retry:"),
        "no retry key seeded, no table: the host's defaults: {config}"
    );

    let plain = app(&descriptors(BLACKJACK), BLACKJACK, &blackjack(), None, None).unwrap();
    let config = &file(&plain, "angzarr.yaml").source;
    assert!(
        !config.contains("expose") && !config.contains("retry:"),
        "{config}"
    );
}

#[test]
fn an_existing_config_is_the_input_the_generated_files_follow_not_the_seed() {
    // The user moved the table to a Go child of their own and kept the
    // player in Rust; the seed still says the player is Python.
    let mine = "schema_version: 1\
                \ncomponent:\
                \n  - name: table\
                \n    kind: domain\
                \n    language: go\
                \n    launch: [./bin/table]\
                \n  - name: player\
                \n    kind: domain\
                \n    language: rust\
                \n";
    let files = app(
        &descriptors(BLACKJACK),
        BLACKJACK,
        &blackjack(),
        Some(mine),
        None,
    )
    .unwrap();
    let config = file(&files, "angzarr.yaml");
    assert_eq!(config.owner, Owner::User);
    assert_eq!(config.source, mine, "the config on disk is passed through");
    let main = &file(&files, "src/main.rs").source;
    assert!(main.contains("pub mod player_state_impl;"), "{main}");
    assert!(!main.contains("pub mod table_state_impl;"), "{main}");
}

#[test]
fn generating_twice_from_the_same_inputs_is_byte_identical() {
    let set = descriptors(BLACKJACK);
    let first = app(&set, BLACKJACK, &blackjack(), None, None).unwrap();
    let config = file(&first, "angzarr.yaml").source.clone();
    let second = app(&set, BLACKJACK, &blackjack(), Some(&config), None).unwrap();
    assert!(!first.is_empty());
    assert_eq!(first, second);
}

#[test]
fn a_seed_naming_a_component_the_protos_lack_is_refused() {
    let seeded = AppSeed {
        languages: vec![("dealer".to_string(), "python".to_string())],
        ..blackjack()
    };
    assert_eq!(
        app(&descriptors(BLACKJACK), BLACKJACK, &seeded, None, None),
        Err(CodegenError::App(AppError::Refused {
            lints: vec![AppLint::UnknownComponent {
                name: "dealer".to_string(),
                kind: "domain".to_string(),
            }]
        }))
    );
}

#[test]
fn a_seed_exposing_a_process_manager_is_refused() {
    // A process manager's name is pmg- and its domain (decision 14.50).
    let buyin = AppSeed {
        expose: vec![("pmg-buyin".to_string(), Exposure::Loopback)],
        ..seed("buyin")
    };
    assert_eq!(
        app(&descriptors(BUYIN), BUYIN, &buyin, None, None),
        Err(CodegenError::App(AppError::Refused {
            lints: vec![AppLint::NotExposable {
                name: "pmg-buyin".to_string(),
                kind: "process_manager".to_string(),
            }]
        }))
    );
}

#[test]
fn an_existing_config_with_lints_is_refused() {
    let mine = "schema_version: 1\
                \ncomponent:\
                \n  - name: dealer\
                \n    kind: domain\
                \n    language: rust\
                \n";
    let refused = app(
        &descriptors(BLACKJACK),
        BLACKJACK,
        &blackjack(),
        Some(mine),
        None,
    );
    assert!(
        matches!(&refused, Err(CodegenError::App(AppError::Refused { lints })) if !lints.is_empty()),
        "{refused:?}"
    );
}

fn lints(config: &str, files: &[&str]) -> Vec<AppLint> {
    lint_app(config, &descriptors(BLACKJACK), files)
        .unwrap()
        .lints
}

#[test]
fn the_generated_configs_have_no_lints() {
    assert_eq!(
        lint_app(&golden("shop", "angzarr.yaml"), &descriptors(SHOP), SHOP)
            .unwrap()
            .lints,
        []
    );
    assert_eq!(lints(&golden("blackjack", "angzarr.yaml"), BLACKJACK), []);
}

#[test]
fn lint_app_flags_an_unknown_component_a_duplicate_and_an_unlisted_domain() {
    let config = "schema_version: 1\
                  \ncomponent:\
                  \n  - name: table\
                  \n    kind: domain\
                  \n    language: rust\
                  \n  - name: table\
                  \n    kind: domain\
                  \n    language: rust\
                  \n  - name: dealer\
                  \n    kind: domain\
                  \n    language: rust\
                  \n";
    assert_eq!(
        lints(config, BLACKJACK),
        [
            AppLint::DuplicateComponent {
                name: "table".to_string()
            },
            AppLint::UnknownComponent {
                name: "dealer".to_string(),
                kind: "domain".to_string(),
            },
            AppLint::UnlistedDomain {
                domain: "player".to_string()
            },
        ]
    );
}

#[test]
fn lint_app_flags_a_policy_component_the_protos_do_not_declare() {
    let config = "schema_version: 1\
                  \ncomponent:\
                  \n  - name: table\
                  \n    kind: domain\
                  \n    language: rust\
                  \n  - name: player\
                  \n    kind: domain\
                  \n    language: python\
                  \n  - name: dealer-table-policy\
                  \n    kind: policy\
                  \n    language: python\
                  \n";
    assert_eq!(
        lints(config, BLACKJACK),
        [AppLint::UnknownComponent {
            name: "dealer-table-policy".to_string(),
            kind: "policy".to_string(),
        }]
    );
}

#[test]
fn lint_app_flags_a_policy_whose_source_domain_is_not_in_the_app() {
    // The table alone, and the player-to-table policy listed: it reads the
    // player, which nothing of the application owns. A policy of the
    // protos the config does not list is not registered, and no lint.
    let config = "schema_version: 1\
                  \ncomponent:\
                  \n  - name: table\
                  \n    kind: domain\
                  \n    language: rust\
                  \n  - name: policy-player-table\
                  \n    kind: policy\
                  \n    language: rust\
                  \n";
    let files = [BLACKJACK[0], BLACKJACK[2]];
    assert_eq!(
        lints(config, &files),
        [AppLint::PolicySourceAbsent {
            policy: "policy-player-table".to_string(),
            domain: "player".to_string(),
        }]
    );
    let table = "schema_version: 1\
                 \ncomponent:\
                 \n  - name: table\
                 \n    kind: domain\
                 \n    language: rust\
                 \n";
    assert_eq!(lints(table, &files), []);
}

#[test]
fn lint_app_flags_a_remote_policy_whose_source_domain_is_not_in_the_app() {
    // A Python child serving the player-to-table policy, by its name from
    // the protos, beside the table alone.
    let config = "schema_version: 1\
                  \ncomponent:\
                  \n  - name: table\
                  \n    kind: domain\
                  \n    language: rust\
                  \n  - name: policy-player-table\
                  \n    kind: policy\
                  \n    language: python\
                  \n    entry: [-m, player_table.serve]\
                  \n";
    let files = [BLACKJACK[0], BLACKJACK[2]];
    assert_eq!(
        lints(config, &files),
        [AppLint::PolicySourceAbsent {
            policy: "policy-player-table".to_string(),
            domain: "player".to_string(),
        }]
    );
}

#[test]
fn lint_app_flags_a_policy_whose_target_is_not_in_the_app_unless_declared_remote() {
    const WALLET: &str = "io/angzarr/examples/v1/wallet.proto";
    let files = [BLACKJACK[0], BLACKJACK[1], BLACKJACK[2], WALLET];
    let set = descriptors(&files);
    let base = "schema_version: 1\
                \ncomponent:\
                \n  - name: table\
                \n    kind: domain\
                \n    language: rust\
                \n  - name: player\
                \n    kind: domain\
                \n    language: python\
                \n  - name: policy-table-wallet\
                \n    kind: policy\
                \n    language: rust\
                \n";
    assert_eq!(
        lint_app(base, &set, &files).unwrap().lints,
        [
            AppLint::PolicyTargetAbsent {
                policy: "policy-table-wallet".to_string(),
                domain: "wallet".to_string(),
            },
            AppLint::UnlistedDomain {
                domain: "wallet".to_string()
            },
        ]
    );
    // The wallet declared remote, a child of its own: no lint.
    let remote = format!(
        "{base}  - name: wallet\n    kind: domain\n    language: go\n    launch: [./bin/wallet]\n"
    );
    assert_eq!(lint_app(&remote, &set, &files).unwrap().lints, []);
}

#[test]
fn lint_app_flags_exposing_anything_but_an_aggregate() {
    let config = "schema_version: 1\
                  \ncomponent:\
                  \n  - name: table\
                  \n    kind: domain\
                  \n    language: rust\
                  \n    expose: ip\
                  \n  - name: player\
                  \n    kind: domain\
                  \n    language: python\
                  \n  - name: policy-table-player\
                  \n    kind: policy\
                  \n    language: python\
                  \n    expose: loopback\
                  \n  - name: table-view\
                  \n    kind: projector\
                  \n    language: python\
                  \n    expose: ip\
                  \n";
    assert_eq!(
        lints(config, BLACKJACK),
        [
            AppLint::NotExposable {
                name: "policy-table-player".to_string(),
                kind: "policy".to_string(),
            },
            AppLint::NotExposable {
                name: "table-view".to_string(),
                kind: "projector".to_string(),
            },
        ],
        "an exposed aggregate is fine; a projector is not checked against the protos"
    );
    let buyin = "schema_version: 1\
                 \ncomponent:\
                 \n  - name: wallet\
                 \n    kind: domain\
                 \n    language: rust\
                 \n  - name: pmg-buyin\
                 \n    kind: domain\
                 \n    language: rust\
                 \n    expose: loopback\
                 \n";
    assert_eq!(
        lint_app(buyin, &descriptors(BUYIN), BUYIN).unwrap().lints,
        [AppLint::NotExposable {
            name: "pmg-buyin".to_string(),
            kind: "process_manager".to_string(),
        }]
    );
}

#[test]
fn lint_app_flags_a_language_with_no_launch_and_an_empty_launch() {
    let config = "schema_version: 1\
                  \ncomponent:\
                  \n  - name: table\
                  \n    kind: domain\
                  \n    language: rust\
                  \n  - name: player\
                  \n    kind: domain\
                  \n    language: cobol\
                  \n";
    assert_eq!(
        lints(config, BLACKJACK),
        [AppLint::UnknownLanguage {
            name: "player".to_string(),
            language: "cobol".to_string(),
        }]
    );
    let empty = "schema_version: 1\
                 \ncomponent:\
                 \n  - name: table\
                 \n    kind: domain\
                 \n    language: rust\
                 \n  - name: player\
                 \n    kind: domain\
                 \n    language: python\
                 \n    launch: []\
                 \n";
    assert_eq!(
        lints(empty, BLACKJACK),
        [AppLint::EmptyLaunch {
            name: "player".to_string()
        }]
    );
}

#[test]
fn lint_app_refuses_what_is_not_the_hosts_config() {
    // Each refused at its key path, the reader's own error kept as the
    // cause.
    for (config, key_path) in [
        // An unknown key, of a component and of a table.
        (
            "schema_version: 1\
             \ncomponent:\
             \n  - name: player\
             \n    kind: domain\
             \n    language: python\
             \n    lanuch: [x]\
             \n",
            "component[0].lanuch",
        ),
        (
            "schema_version: 1\
             \nretry:\
             \n  max_race_runz: 0\
             \n",
            "retry.max_race_runz",
        ),
        // A value outside a closed set.
        (
            "schema_version: 1\
             \ncomponent:\
             \n  - name: table\
             \n    kind: saga\
             \n    language: rust\
             \n",
            "component[0].kind",
        ),
        (
            "schema_version: 1\
             \ncomponent:\
             \n  - name: table\
             \n    kind: domain\
             \n    language: rust\
             \n    expose: world\
             \n",
            "component[0].expose",
        ),
        (
            "schema_version: 1\
             \npolicy:\
             \n  on_bug: ignore\
             \n",
            "policy.on_bug",
        ),
        // A component with no kind (the owner's ruling: kind is required).
        (
            "schema_version: 1\
             \ncomponent:\
             \n  - name: table\
             \n    language: rust\
             \n",
            "component[0].kind",
        ),
        // A key of the wrong type.
        (
            "schema_version: 1\
             \nstore:\
             \n  in_memory: yes\
             \n",
            "store.in_memory",
        ),
        (
            "schema_version: 1\
             \nsupervision:\
             \n  call_timeout_ms: -1\
             \n",
            "supervision.call_timeout_ms",
        ),
    ] {
        let refused = lint_app(config, &descriptors(BLACKJACK), BLACKJACK);
        assert!(
            matches!(&refused, Err(e @ CodegenError::App(AppError::Config { .. }))
                if e.to_string().contains(&format!("problem.key_path={key_path} "))),
            "{config}: {refused:?}"
        );
    }
    // The old TOML is not the host's config, and neither is a file of a
    // newer version.
    for config in [
        "[[component]]\nname = \"table\"\nkind = \"domain\"\nlanguage = \"rust\"\n",
        "schema_version: 5\n",
    ] {
        assert!(
            matches!(
                lint_app(config, &descriptors(BLACKJACK), BLACKJACK),
                Err(CodegenError::App(AppError::Config { .. }))
            ),
            "{config}"
        );
    }
}

/// A config with no stamp is the oldest generation, version 0, which has
/// version 1's shape: it reads through every step, and lints as the
/// stamped one does; so does the same file stamped any older version.
#[test]
fn an_unstamped_or_older_config_lints_as_the_current_one() {
    let stamped = golden("blackjack", "angzarr.yaml");
    assert!(stamped.contains("\nschema_version: 4\n"), "{stamped}");
    for older in ["1", "2", "3"] {
        let config = stamped.replace(
            "\nschema_version: 4\n",
            &format!("\nschema_version: {older}\n"),
        );
        assert_eq!(lints(&config, BLACKJACK), [], "version {older}");
    }
    let unstamped = stamped.replace("\nschema_version: 4\n", "\n");
    assert_eq!(lints(&unstamped, BLACKJACK), []);
}

#[test]
fn remote_domains_are_the_domains_not_in_rust_or_given_a_launch() {
    let config = "schema_version: 1\
                  \ncomponent:\
                  \n  - name: table\
                  \n    kind: domain\
                  \n    language: rust\
                  \n  - name: player\
                  \n    kind: domain\
                  \n    language: python\
                  \n  - name: dealer\
                  \n    kind: domain\
                  \n    language: rust\
                  \n    launch: [./dealer]\
                  \n  - name: table-view\
                  \n    kind: projector\
                  \n    language: python\
                  \n";
    assert_eq!(
        remote_domains(config).unwrap(),
        ["player".to_string(), "dealer".to_string()]
    );
}

/// What a child process serves besides a domain: every policy, projector
/// or upcaster not in Rust, or given a `launch`, by name.
#[test]
fn child_components_are_the_others_not_in_rust_or_given_a_launch() {
    let config = "schema_version: 1\
                  \ncomponent:\
                  \n  - name: player\
                  \n    kind: domain\
                  \n    language: python\
                  \n  - name: policy-table-player\
                  \n    kind: policy\
                  \n    language: python\
                  \n  - name: policy-player-table\
                  \n    kind: policy\
                  \n    language: rust\
                  \n  - name: table-view\
                  \n    kind: projector\
                  \n    language: rust\
                  \n    launch: [./view]\
                  \n";
    assert_eq!(
        child_components(config).unwrap(),
        ["policy-table-player".to_string(), "table-view".to_string()]
    );
}

#[test]
fn a_projector_is_listed_when_its_sources_are_all_the_applications() {
    const WITH_LEDGER: &[&str] = &[
        "io/angzarr/examples/v1/table.proto",
        "io/angzarr/examples/v1/wallet.proto",
        "io/angzarr/examples/v1/ledger.proto",
        "io/angzarr/examples/v1/player.proto",
    ];
    let set = descriptors(WITH_LEDGER);
    let listed = |name: &str, files: &[&str], seed: &AppSeed| {
        let generated = app(&set, files, seed, None, None).unwrap();
        file(&generated, "angzarr.yaml")
            .source
            .contains(&format!("- name: {name}\n    kind: projector\n"))
    };
    // The ledger is two projectors, each of one domain's events.
    for name in ["projector-wallet-ledger", "projector-table-ledger"] {
        assert!(listed(name, WITH_LEDGER, &seed("ledger")), "{name}");
    }
    // A Python domain that is not one of its sources leaves it listed.
    let player = AppSeed {
        languages: vec![("player".to_string(), "python".to_string())],
        ..seed("ledger")
    };
    assert!(listed("projector-wallet-ledger", WITH_LEDGER, &player));
    // The wallet a Python child: the host records its events, so the
    // projector of it runs in process all the same.
    let python = AppSeed {
        languages: vec![("wallet".to_string(), "python".to_string())],
        ..seed("ledger")
    };
    assert!(listed("projector-wallet-ledger", WITH_LEDGER, &python));
    // A source no component of the application owns (the wallet's protos
    // not generated) leaves its projector unlisted, the other's not.
    let alone = [WITH_LEDGER[0], WITH_LEDGER[2]];
    assert!(!listed("projector-wallet-ledger", &alone, &seed("ledger")));
    assert!(listed("projector-table-ledger", &alone, &seed("ledger")));
    // A projector of one source, in an application of its one domain.
    const LONE: &[&str] = &["lone/v1/lone.proto"];
    let files = app(&descriptors(LONE), LONE, &seed("lone"), None, None).unwrap();
    assert!(
        file(&files, "angzarr.yaml")
            .source
            .contains("- name: projector-slot-view\n    kind: projector\n"),
        "{}",
        file(&files, "angzarr.yaml").source
    );
}

#[test]
fn a_projector_of_two_domains_is_listed_only_when_both_are_the_applications() {
    const DUO: &[&str] = &[
        "duo/v1/left.proto",
        "duo/v1/right.proto",
        "duo/v1/view.proto",
    ];
    let set = descriptors(DUO);
    let listed = |name: &str, files: &[&str]| {
        let generated = app(&set, files, &seed("duo"), None, None).unwrap();
        file(&generated, "angzarr.yaml")
            .source
            .contains(&format!("- name: {name}\n    kind: projector\n"))
    };
    assert!(listed("projector-duo-view", DUO));
    assert!(!listed("projector-duo-view", &[DUO[0], DUO[2]]));
    assert!(!listed("projector-duo-view", &[DUO[1], DUO[2]]));
}

#[test]
fn an_exposure_lands_on_its_component_alone_and_one_naming_none_is_a_lint() {
    let seeded = AppSeed {
        expose: vec![("table".to_string(), Exposure::Ip)],
        ..blackjack()
    };
    let files = app(&descriptors(BLACKJACK), BLACKJACK, &seeded, None, None).unwrap();
    let config = &file(&files, "angzarr.yaml").source;
    assert_eq!(config.matches("expose: ").count(), 1, "{config}");
    let nowhere = AppSeed {
        expose: vec![("nowhere".to_string(), Exposure::Ip)],
        ..blackjack()
    };
    assert_eq!(
        app(&descriptors(BLACKJACK), BLACKJACK, &nowhere, None, None),
        Err(CodegenError::App(AppError::Refused {
            lints: vec![AppLint::UnknownComponent {
                name: "nowhere".to_string(),
                kind: "domain".to_string(),
            }]
        }))
    );
}

#[test]
fn the_manifests_package_is_the_apps_name_as_a_crate_name() {
    for (name, package) in [
        ("shop", "shop"),
        ("  My Shop!!  App ", "my-shop-app"),
        ("-x", "x"),
    ] {
        let files = app(&descriptors(SHOP), SHOP, &seed(name), None, None).unwrap();
        let manifest = &file(&files, "Cargo.toml").source;
        assert!(
            manifest.contains(&format!("\nname = \"{package}\"\n")),
            "{name}: {manifest}"
        );
    }
}

fn language(name: &str, language: &str) -> (String, String) {
    (name.to_string(), language.to_string())
}

/// `config`'s entry for `name`, from its `- name:` to the next entry or
/// mapping.
fn entry<'a>(config: &'a str, name: &str) -> &'a str {
    let at = config
        .find(&format!("  - name: {name}\n"))
        .unwrap_or_else(|| panic!("no {name}:\n{config}"));
    let rest = &config[at..];
    let end = rest[1..]
        .find("\n  - ")
        .map(|i| i + 1)
        .or_else(|| rest.find("\n\n"))
        .or_else(|| rest.find("\nstore:"))
        .unwrap_or(rest.len());
    &rest[..end]
}

/// Every kind takes its language from the seed, by its name in the
/// application: a Python one is a child, its `dir` and entry point the
/// project `angzarr scaffold python` writes for that name.
#[test]
fn a_seed_puts_each_component_it_names_in_its_language() {
    let seeded = AppSeed {
        languages: vec![
            language("player", "python"),
            language("policy-table-player", "python"),
        ],
        ..blackjack()
    };
    let files = app(&descriptors(BLACKJACK), BLACKJACK, &seeded, None, None).unwrap();
    let config = &file(&files, "angzarr.yaml").source;
    assert_eq!(
        entry(config, "policy-table-player"),
        "  - name: policy-table-player\n    kind: policy\n    language: python\n    \
         dir: policy-table-player\n    entry: [angzarr-policy-table-player]"
    );
    assert_eq!(
        entry(config, "policy-player-table"),
        "  - name: policy-player-table\n    kind: policy\n    language: rust"
    );
    // The Python policy is the child's: no module of it in main.rs.
    let main = &file(&files, "src/main.rs").source;
    assert!(!main.contains("pub mod table_player_policy"), "{main}");
    assert!(main.contains("pub mod player_table_policy_impl;"), "{main}");
}

#[test]
fn a_seed_puts_a_projector_in_its_language() {
    const WITH_LEDGER: &[&str] = &[
        "io/angzarr/examples/v1/table.proto",
        "io/angzarr/examples/v1/wallet.proto",
        "io/angzarr/examples/v1/ledger.proto",
    ];
    let seeded = AppSeed {
        languages: vec![language("projector-wallet-ledger", "python")],
        ..seed("ledger")
    };
    let files = app(&descriptors(WITH_LEDGER), WITH_LEDGER, &seeded, None, None).unwrap();
    assert_eq!(
        entry(
            &file(&files, "angzarr.yaml").source,
            "projector-wallet-ledger"
        ),
        "  - name: projector-wallet-ledger\n    kind: projector\n    language: python\n    \
         dir: projector-wallet-ledger\n    entry: [angzarr-projector-wallet-ledger]"
    );
}

/// A domain is named by its name in the application or by its domain; its
/// Python project is its domain's, as `scaffold python --domain` names it.
#[test]
fn a_seed_names_a_domain_by_its_component_name_or_its_domain() {
    const BUYIN: &[&str] = &[
        "buyin/v1/wallet.proto",
        "buyin/v1/buy_in.proto",
        "buyin/v1/policies.proto",
    ];
    for name in ["pmg-buyin", "buyin"] {
        let seeded = AppSeed {
            languages: vec![language(name, "python")],
            ..seed("buyin")
        };
        let files = app(&descriptors(BUYIN), BUYIN, &seeded, None, None).unwrap();
        assert_eq!(
            entry(&file(&files, "angzarr.yaml").source, "pmg-buyin"),
            "  - name: pmg-buyin\n    kind: domain\n    language: python\n    \
             dir: buyin\n    entry: [angzarr-buyin]",
            "{name}"
        );
    }
}

/// A language the host has no launch for is written as given, and the
/// lint refuses it: a `launch` is the user's to add.
#[test]
fn a_seed_in_a_language_with_no_launch_is_refused() {
    let seeded = AppSeed {
        languages: vec![language("policy-table-player", "go")],
        ..blackjack()
    };
    assert_eq!(
        app(&descriptors(BLACKJACK), BLACKJACK, &seeded, None, None),
        Err(CodegenError::App(AppError::Refused {
            lints: vec![AppLint::UnknownLanguage {
                name: "policy-table-player".to_string(),
                language: "go".to_string(),
            }]
        }))
    );
}

/// The protos' declaration warnings come with the lints and fail nothing: a
/// process manager with a starting input (buy-in's) draws no warning that
/// nothing starts it.
#[test]
fn a_process_manager_with_a_starting_input_is_not_warned_of() {
    let buyin = "schema_version: 1\ncomponent:\n  - name: pmg-buyin\n    kind: domain\n    language: rust\n";
    let linted = lint_app(buyin, &descriptors(BUYIN), BUYIN).unwrap();
    assert!(
        linted
            .warnings
            .iter()
            .all(|w| !matches!(w, Warning::NoStartingInput { .. })),
        "{:?}",
        linted.warnings
    );
}

/// The generated layer `src/main.rs` includes: the application's own wire
/// types and domain layer, the domain crate's named in their place, or,
/// with neither, the adapters and `register()` alone (item S Q7).
#[test]
fn the_app_layer_holds_the_generated_modules_the_app_has() {
    let own = app_layer(&["shop.v1".to_string()], None);
    for expected in [
        "pub mod wire {\n    include!(concat!(env!(\"OUT_DIR\"), \"/shop.v1.rs\"));\n}",
        "pub mod domain {\n    include!(concat!(env!(\"OUT_DIR\"), \"/shop.v1.domain.rs\"));\n}",
        "include!(concat!(env!(\"OUT_DIR\"), \"/angzarr-adapters.rs\"));",
        "include!(concat!(env!(\"OUT_DIR\"), \"/angzarr-register.rs\"));",
    ] {
        assert!(own.contains(expected), "{expected}:\n{own}");
    }
    let bare = app_layer(&[], None);
    assert!(!bare.contains("pub mod"), "{bare}");
    assert!(bare.contains("/angzarr-register.rs"), "{bare}");
}

/// A manifest names its domain crate in `[package.metadata.angzarr.domain]`,
/// what `app_without_domain` reads; one naming none has none.
#[test]
fn a_manifests_domain_crate_is_read_from_its_metadata() {
    let manifest = "[package]\nname = \"x\"\n\n[package.metadata.angzarr.domain]\n\
                    crate = \"shop_proto\"\nwire = \"shop_proto::v1\"\n";
    assert_eq!(
        angzarr_codegen::manifest_domain_crate(manifest).unwrap(),
        Some(DomainCrate {
            path: "shop_proto".to_string(),
            wire: Some("shop_proto::v1".to_string()),
        })
    );
    assert_eq!(
        angzarr_codegen::manifest_domain_crate("[package]\nname = \"x\"\n").unwrap(),
        None
    );
}

/// The host config names the components whose scaffold takes snapshots,
/// each by its entry's `scaffold.snapshots`, in the config's order; one
/// setting none is the default, every component taking none; a config that
/// does not load is refused as the config's.
#[test]
fn a_host_configs_scaffold_settings_are_the_scaffolds_configuration() {
    let config = "schema_version: 4\ncomponent:\n\
                  - {name: carts, kind: domain, language: rust, scaffold: {snapshots: true}}\n\
                  - {name: pmg-checkout, kind: domain, language: rust, scaffold: {snapshots: true}}\n\
                  - {name: stock, kind: domain, language: rust}\n\
                  - {name: ledger, kind: domain, language: rust, scaffold: {snapshots: false}}\n";
    assert_eq!(
        angzarr_codegen::host_scaffold(config).unwrap(),
        angzarr_codegen::ScaffoldConfig {
            snapshots: vec!["carts".to_string(), "pmg-checkout".to_string()],
        }
    );
    assert_eq!(
        angzarr_codegen::host_scaffold("schema_version: 4\n").unwrap(),
        angzarr_codegen::ScaffoldConfig::default()
    );
    let unknown = "schema_version: 4\ncomponent:\n\
                   - {name: carts, kind: domain, language: rust, scaffold: {snapshot: true}}\n";
    assert!(
        matches!(
            angzarr_codegen::host_scaffold(unknown),
            Err(angzarr_codegen::CodegenError::App(
                angzarr_codegen::AppError::Config { .. }
            ))
        ),
        "{:?}",
        angzarr_codegen::host_scaffold(unknown)
    );
    assert_eq!(angzarr_codegen::HOST_CONFIG, "angzarr.yaml");
}

/// The manifest's build-dependency names the angzarr-build repository, at
/// the pinned commit: not the CLI's repository, which no longer holds it.
#[test]
fn the_default_manifest_pins_angzarr_build_in_its_own_repository() {
    assert_eq!(
        ANGZARR_BUILD_GIT,
        "https://github.com/angzarr-io/angzarr-build"
    );
    let files = app(&descriptors(SHOP), SHOP, &seed("shop"), None, None).unwrap();
    let manifest = &file(&files, "Cargo.toml").source;
    let expected = format!(
        "angzarr-build = {{ git = \"{ANGZARR_BUILD_GIT}\", rev = \"{ANGZARR_BUILD_REV}\" }}\n"
    );
    assert!(manifest.ends_with(&expected), "{manifest}");
}

/// A pin is a full commit hash, never a branch or an abbreviation.
#[test]
fn the_angzarr_build_pin_is_a_full_commit_hash() {
    assert_eq!(ANGZARR_BUILD_REV.len(), 40, "{ANGZARR_BUILD_REV}");
    assert!(
        ANGZARR_BUILD_REV
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "{ANGZARR_BUILD_REV}"
    );
}

/// The pinned commit is in this repository's history, so a generated
/// application can fetch it. Needs the full history (a shallow clone cannot
/// say), so it runs with the slow tests.
#[test]
#[ignore = "needs the repository's full history; runs in `just test`'s slow pass"]
fn the_angzarr_build_pin_is_an_ancestor_of_this_checkout() {
    let status = std::process::Command::new("git")
        .args(["merge-base", "--is-ancestor", ANGZARR_BUILD_REV, "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("git runs");
    assert!(
        status.success(),
        "{ANGZARR_BUILD_REV} is not an ancestor of HEAD: pin a commit that is pushed to main"
    );
}
