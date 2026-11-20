//! End to end on angzarr-embedded and angzarr-router, at the family's pins:
//! the fixture apps under `tests/local` are workspaces of their own, each
//! generated from protos by its `build.rs` and built by cargo here.
//!
//! - `buyin-app`: the buy-in fixture's domain layer, adapters and
//!   `register()`, its components the scaffold with their bodies written,
//!   type-checked, then run: one command, a policy step each way, and a
//!   refusal answered by a policy.
//! - `kinds-app`: the kinds fixture (a typed refusal of each field kind, a
//!   command refusing several, several emits, a projector of two domains),
//!   built and run, a policy swapped for one written by hand.
//! - `shapes-app`: the shapes fixtures, three packages each referencing
//!   another's types (one of types alone), angzarr's UUID and id newtypes,
//!   Duration, oneofs, nested types and anchors: wire types and
//!   domain layers, whose tests (property tests) convert each domain type to
//!   its wire type, through prost's bytes, and back.
//! - `json-app`: the json fixture's wire types and their generated proto3
//!   JSON, held to golden files, round-tripped (property tests) and
//!   cross-checked with prost-reflect's canonical mapping.
//! - `anchors-app`: the shapes fixtures' aggregates, in a crate whose
//!   components are in two packages, each package's layer at its own
//!   path: the generated adapters, register() and the scaffold of the day
//!   (written as the app's host config configures it) type-checked; the
//!   orders, whose scaffold takes snapshots, snapshot as their anchor's
//!   wire type and resume from it, and the catalog takes none.
//! - `blackjack-app`: the blackjack example's protos (the spec's): the
//!   generated layer and the scaffold of the day, `todo!()` bodies and all,
//!   type-checked. `bare-app` is the same for a domain with no events. Nothing
//!   of either is run, and the scaffold is written there by the test, not
//!   committed.
//!
//! They build the embedded runtime and its dependencies, fetched from the
//! network on a cold cache, so they are `#[ignore]`d from a plain `cargo
//! test` (and from the mutation runs, which would repeat them for every
//! mutant): `just test` runs them after the rest, `just test-slow` alone.
//! The apps share one target directory.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use angzarr_build::{
    Existing, ScaffoldConfig, Scaffolded, host_scaffold, scaffold_into, scaffold_with_into,
};

use angzarr_test_dir::TestDir;

const BUYIN: &[&str] = &[
    "buyin/v1/wallet.proto",
    "buyin/v1/buy_in.proto",
    "buyin/v1/policies.proto",
];
const BUYIN_COMPONENTS: &[&str] = &[
    "buy_in_state_impl.rs",
    "buy_in_wallet_policy_impl.rs",
    "wallet_buy_in_policy_impl.rs",
    "wallet_state_impl.rs",
];
const KINDS: &[&str] = &["kinds/v1/kinds.proto"];
const KINDS_COMPONENTS: &[&str] = &[
    "audit_state_impl.rs",
    "ledger_audit_policy_impl.rs",
    "ledger_state_impl.rs",
    "ledger_view_impl.rs",
];
/// The blackjack example, in the order its `build.rs` names them.
const BLACKJACK: &[&str] = &[
    "io/angzarr/examples/v1/cards.proto",
    "io/angzarr/examples/v1/wallet.proto",
    "io/angzarr/examples/v1/buy_in.proto",
    "io/angzarr/examples/v1/table.proto",
    "io/angzarr/examples/v1/player.proto",
    "io/angzarr/examples/v1/policies.proto",
    "io/angzarr/examples/v1/ledger.proto",
];
/// The implementations its `src/lib.rs` declares: the scaffold of the
/// example writes exactly these.
const BLACKJACK_COMPONENTS: &[&str] = &[
    "buy_in_state_impl.rs",
    "buy_in_table_policy_impl.rs",
    "buy_in_wallet_policy_impl.rs",
    "player_state_impl.rs",
    "player_table_policy_impl.rs",
    "table_buy_in_policy_impl.rs",
    "table_ledger_projection_impl.rs",
    "table_player_policy_impl.rs",
    "table_state_impl.rs",
    "table_wallet_policy_impl.rs",
    "wallet_buy_in_policy_impl.rs",
    "wallet_ledger_projection_impl.rs",
    "wallet_state_impl.rs",
];

fn app(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/local")
        .join(name)
}

fn fixtures() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../angzarr-codegen/tests/fixtures")
        .display()
        .to_string()
}

/// The spec's protos, which hold the blackjack example's.
fn spec() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../angzarr-project/proto")
        .display()
        .to_string()
}

/// `cargo <args>` on `app`'s manifest, in the one target directory the
/// fixture apps share (so the embedded runtime is built once).
fn cargo(app: &Path, args: &[&str]) -> Output {
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/local-apps");
    Command::new(env!("CARGO"))
        .args(args)
        .arg("--manifest-path")
        .arg(app.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target)
        .output()
        .expect("cargo runs")
}

/// `app`'s components are the scaffold of today's protos: completing a
/// copy of them adds nothing. (So the app is the scaffold's shape, not a
/// hand-made one.)
fn assert_complete_scaffold(app: &Path, protos: &[&str], components: &[&str], includes: &[&str]) {
    let src = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-local-test"));
    for file in components {
        std::fs::copy(app.join("src").join(file), src.join(file)).unwrap();
    }
    let mut done = scaffold_into(&src, protos, includes, Existing::Complete).unwrap();
    done.sort_by_key(|d| format!("{d:?}"));
    // Each is kept as it is, lacking nothing: every method is required
    // (decision 14.76) but an aggregate's or a process manager's
    // `snapshot` and `restore`, which its interface defaults, offered.
    let kept: Vec<(PathBuf, Vec<String>, Vec<String>)> = done
        .into_iter()
        .filter_map(|d| match d {
            Scaffolded::Kept { path, notes } => Some((path, notes.lacks, notes.optional)),
            _ => None,
        })
        .collect();
    let snapshots = ["snapshot", "restore"].map(String::from).to_vec();
    let expected: Vec<(PathBuf, Vec<String>, Vec<String>)> = components
        .iter()
        .zip(&kept)
        .map(|(file, (_, _, optional))| {
            let offered = if optional.is_empty() {
                Vec::new()
            } else {
                snapshots.clone()
            };
            (src.join(file), Vec::new(), offered)
        })
        .collect();
    assert_eq!(kept, expected);
}

#[test]
fn test_the_apps_components_are_the_complete_scaffold() {
    assert_complete_scaffold(&app("buyin-app"), BUYIN, BUYIN_COMPONENTS, &[&fixtures()]);
    assert_complete_scaffold(&app("kinds-app"), KINDS, KINDS_COMPONENTS, &[&fixtures()]);
}

/// The generated adapters and register() compile against angzarr-embedded
/// with the app's interfaces and implementations.
#[test]
#[ignore = "builds angzarr-embedded and its dependencies: slow; `just test-slow`"]
fn test_the_app_type_checks_against_angzarr_embedded() {
    let out = cargo(&app("buyin-app"), &["check", "--quiet", "--lib"]);
    assert!(
        out.status.success(),
        "the app type-checks:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The generated register() is what angzarr_embedded_host::run takes: the
/// app, run from its angzarr.yaml, handles one command, a policy step each
/// way, and a refusal its policy answers with a fact.
#[test]
#[ignore = "builds angzarr-embedded, angzarr-embedded-host and their dependencies, fetched from \
            the network on a cold cache: slow; `just test-slow`"]
fn test_the_generated_register_runs_a_command_a_policy_step_and_a_refusal() {
    let out = cargo(&app("buyin-app"), &["run", "--quiet"]);
    assert!(
        out.status.success(),
        "the app builds and runs:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        String::from_utf8(out.stdout).unwrap(),
        [
            // One command.
            "wallet balance=100 held=0",
            // BuyInStarted -> Hold (BuyInWallet); Held -> RecordHeld (WalletBuyIn).
            "wallet balance=100 held=30",
            "buyin held=30 failed=false",
            // Hold refused, recording nothing; answered with HoldLost.
            "wallet balance=100 held=30",
            "buyin held=0 failed=true",
            "",
        ]
        .join("\n")
    );
}

/// `tests/local/kinds-app`: the generated domain layer of a typed refusal
/// of each field kind and a command refusing several, inputs of several
/// emits, each returning a list, and a projector of two domains, with their adapters, built
/// against angzarr-embedded and run: each refusal's code and args, then
/// commands whose events the generated projector and one written by hand
/// (registered through angzarr-embedded's public API beside the generated
/// register()) project and a generated policy reacts to, translating a
/// refusal of a command refusing two types as their choice, and a refusal
/// refused under its code's status; then the policy
/// swapped for one written by hand under the generated one's name
/// (`angzarr_build::component_names`): the same derived command id.
#[test]
#[ignore = "builds angzarr-embedded and its dependencies: slow; `just test-slow`"]
fn test_the_kinds_app_refuses_records_and_projects_as_generated() {
    let out = cargo(&app("kinds-app"), &["run", "--quiet"]);
    assert!(
        out.status.success(),
        "the app builds and runs:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        String::from_utf8(out.stdout).unwrap(),
        [
            "kinds-v1-postrefused-over-limit reason=OVER_LIMIT small=-1 big=2 count=3 total=4 \
             text=t data=ab01 flag=true ratio=0.5 share=1.5 tone=LOUD limit=9",
            "kinds-v1-postrefused-over-limit",
            "kinds-v1-unreasoned reason=LOUD",
            "kinds-v1-listed ",
            "kinds-v1-elsewhere why=AWAY",
            "kinds-v1-mislabeled reason=LATE",
            // Open: Posted and Noted; the view projects Posted, and Noted
            // sets the policy off, whose Audit the view projects too.
            "view: Posted from ledger (1)",
            "view: Audited from audit (2)",
            "tally: 2 events from ledger",
            "generated policy: 49bd00eef2815f39bc2a91179c4619f9",
            // Batch: Noted then Posted, one step each. The audit refuses
            // root 2 with one of Audit's two refusal types, and the
            // policy's callback is handed it as their choice, typed.
            "audit refused: Unreasoned(Unreasoned { reason: Loud })",
            "view: Posted from ledger (3)",
            "tally: 2 events from ledger",
            // Audit, the view's other source.
            "view: Audited from audit (4)",
            // Post refuses OVER_LIMIT, whose refusal_status is OUT_OF_RANGE
            // (decision 14.52): refused under it, not FAILED_PRECONDITION.
            "post refused: OutOfRange",
            // The policy swapped for one written by hand under the
            // generated one's name: the same command id.
            "swapped",
            "view: Posted from ledger (1)",
            "view: Audited from audit (2)",
            "tally: 2 events from ledger",
            "hand-written policy: 49bd00eef2815f39bc2a91179c4619f9",
            "",
        ]
        .join("\n")
    );
}

/// `tests/local/shapes-app`: every domain type of the shapes fixtures
/// round-trips through its wire type, and what the domain cannot hold is
/// refused, naming the field.
#[test]
#[ignore = "builds angzarr-embedded and its dependencies: slow; `just test-slow`"]
fn test_the_shapes_apps_domain_types_round_trip_through_the_wire() {
    let out = cargo(&app("shapes-app"), &["test", "--quiet"]);
    assert!(
        out.status.success(),
        "the shapes app's tests pass:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `tests/local/json-app`: the generated JSON is its golden files, reads
/// back what it writes, and agrees with prost-reflect's canonical mapping
/// both ways.
#[test]
#[ignore = "builds angzarr-proto, prost-reflect and proptest: slow; `just test-slow`"]
fn test_the_json_apps_json_is_golden_round_trips_and_agrees_with_prost_reflect() {
    let out = cargo(&app("json-app"), &["test", "--quiet"]);
    assert!(
        out.status.success(),
        "the json app's tests pass:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `app`'s components, the scaffold of today's protos written fresh into
/// its `src` (never committed: its `.gitignore`), type-check against
/// angzarr-embedded and angzarr-router with the layer its `build.rs`
/// generates. The scaffold's bodies are `todo!()`s, so nothing runs.
fn assert_scaffold_type_checks(
    app: &Path,
    protos: &[&str],
    components: &[&str],
    includes: &[&str],
) {
    write_scaffold(app, protos, components, includes, &[], &[]);
    assert_type_checks(app);
}

/// Each package's Rust path, by its proto package.
type Paths<'a> = &'a [(&'a str, &'a str)];

/// The scaffold of today's protos, written fresh into `app`'s `src` with
/// the packages' wire and domain paths, as `app`'s host config configures it:
/// the modules the app declares.
fn write_scaffold(
    app: &Path,
    protos: &[&str],
    components: &[&str],
    includes: &[&str],
    wire: Paths,
    domain: Paths,
) {
    let src = app.join("src");
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        if path.to_string_lossy().ends_with("_impl.rs") {
            std::fs::remove_file(path).unwrap();
        }
    }
    let host = app.join("angzarr.yaml");
    let config = match host.exists() {
        true => host_scaffold(&host).unwrap(),
        false => ScaffoldConfig::default(),
    };
    let written = scaffold_with_into(
        &src,
        protos,
        includes,
        Existing::Complete,
        wire,
        domain,
        &config,
    )
    .unwrap();
    let mut files: Vec<String> = written
        .iter()
        .filter_map(|d| match d {
            Scaffolded::Created(path) => path.file_name().map(|n| n.to_string_lossy().to_string()),
            _ => None,
        })
        .collect();
    files.sort();
    assert_eq!(
        files, components,
        "the scaffold writes the modules the app declares: {written:?}"
    );
}

/// `app`'s library type-checks.
fn assert_type_checks(app: &Path) {
    let out = cargo(app, &["check", "--quiet", "--lib"]);
    assert!(
        out.status.success(),
        "{} type-checks:\n{}",
        app.display(),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The blackjack example's protos, the spec's own: the generated layer
/// (wire types, domain layer, adapters, `register()`) and the scaffold of
/// the day.
#[test]
#[ignore = "builds angzarr-embedded and its dependencies: slow; `just test-slow`"]
fn test_the_blackjack_examples_generated_layer_and_scaffold_type_check() {
    assert_scaffold_type_checks(
        &app("blackjack-app"),
        BLACKJACK,
        BLACKJACK_COMPONENTS,
        &[&spec()],
    );
}

/// A domain with no events: its decisions return nothing (or a refusal),
/// and the adapter still builds each decision's `Emit` of no events.
#[test]
#[ignore = "builds angzarr-embedded and its dependencies: slow; `just test-slow`"]
fn test_a_domain_with_no_events_generated_layer_and_scaffold_type_check() {
    assert_scaffold_type_checks(
        &app("bare-app"),
        &["bare/v1/bare.proto"],
        &["bare_state_impl.rs"],
        &[&fixtures(), &spec()],
    );
}

/// The shapes fixtures' protos: one package of types alone, and two whose
/// components are anchored on their state.
const SHAPES: &[&str] = &[
    "shapes/common/v1/common.proto",
    "shapes/catalog/v1/catalog.proto",
    "shapes/order/v1/order.proto",
];
/// Each shapes package's wire types and domain layer, at their paths in
/// `anchors-app` (its `build.rs` names the same).
const SHAPES_WIRE: &[(&str, &str)] = &[
    ("shapes.common.v1", "crate::wire::shapes::common::v1"),
    ("shapes.catalog.v1", "crate::wire::shapes::catalog::v1"),
    ("shapes.order.v1", "crate::wire::shapes::order::v1"),
];
const SHAPES_DOMAIN: &[(&str, &str)] = &[
    ("shapes.common.v1", "crate::domain::common"),
    ("shapes.catalog.v1", "crate::domain::catalog"),
    ("shapes.order.v1", "crate::domain::order"),
];

/// `tests/local/anchors-app`: aggregates in a crate whose components are
/// in two packages, each package's domain layer at its own path. The
/// generated adapters and register() over the scaffold of the day, written
/// as the app's host config configures it, type-check; then the orders'
/// aggregate snapshots as its anchor's wire type and resumes from it, the
/// catalog's takes none, and register() lists each as its anchor (the
/// app's tests).
#[test]
#[ignore = "builds angzarr-embedded and its dependencies: slow; `just test-slow`"]
fn test_state_anchors_of_two_packages_type_check_and_snapshot() {
    let app = app("anchors-app");
    write_scaffold(
        &app,
        SHAPES,
        &["catalog_state_impl.rs", "order_state_impl.rs"],
        &[&fixtures()],
        SHAPES_WIRE,
        SHAPES_DOMAIN,
    );
    assert_type_checks(&app);
    let out = cargo(&app, &["test", "--quiet"]);
    assert!(
        out.status.success(),
        "the anchors app's tests pass:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
