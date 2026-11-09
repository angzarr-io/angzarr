//! What this crate may link: the base vocabulary, ports, proto and
//! diagnostic, the reads engine (behind the `reads` feature alone), and no
//! transport and no runtime: the tonic server is the crate above this one.

use std::collections::BTreeSet;
use std::process::Command;

/// `cargo tree`'s package names under this crate, every edge kind (its own
/// dev-dependencies included), at `depth` (all of them when `None`), with the
/// default features, or with `features` alone (`--no-default-features`).
fn tree(depth: Option<u32>, features: Option<&str>) -> BTreeSet<String> {
    let mut command = Command::new(env!("CARGO"));
    command
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "tree",
            "--locked",
            "--offline",
            "-e",
            "all",
            "--prefix",
            "none",
        ])
        .args(["-p", env!("CARGO_PKG_NAME")]);
    if let Some(depth) = depth {
        command.arg("--depth").arg(depth.to_string());
    }
    if let Some(features) = features {
        command
            .arg("--no-default-features")
            .arg("--features")
            .arg(features);
    }
    let output = command.output().expect("cargo tree runs");
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("utf-8")
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_string)
        .collect()
}

const ALLOWED: [&str; 5] = [
    "angzarr-coordinator-base",
    "angzarr-diagnostic",
    "angzarr-proto",
    "angzarr-ports",
    "angzarr-reads",
];

/// The angzarr crates under this one are the five allowed, and every
/// transport and runtime (and everything above this crate) is absent from
/// the closure.
#[test]
fn coordinator_depends_on_no_transport_or_runtime() {
    let every = tree(None, None);
    let angzarr: BTreeSet<_> = every
        .iter()
        .filter(|name| name.starts_with("angzarr-") && *name != env!("CARGO_PKG_NAME"))
        .cloned()
        .collect();
    let allowed: BTreeSet<_> = ALLOWED.iter().map(ToString::to_string).collect();
    assert_eq!(angzarr, allowed, "the angzarr crates in the closure");

    for banned in [
        "tonic",
        "tokio",
        "hyper",
        "tower",
        "h2",
        "mio",
        "async-std",
        "futures",
        "angzarr-router",
        "angzarr-embedded",
        "angzarr-coordinator-grpc",
    ] {
        assert!(
            !every
                .iter()
                .any(|name| name == banned || name.starts_with(&format!("{banned}-"))),
            "{banned} is in the closure"
        );
    }
}

/// Directly, the five angzarr crates and no third-party crate.
#[test]
fn coordinator_links_no_third_party_crate_of_its_own() {
    let direct = tree(Some(1), None);
    let own: BTreeSet<_> = direct
        .into_iter()
        .filter(|name| name != env!("CARGO_PKG_NAME"))
        .collect();
    let allowed: BTreeSet<_> = ALLOWED.iter().map(ToString::to_string).collect();
    assert_eq!(own, allowed);
}

/// Without the `reads` feature the reads engine is not linked at all, and
/// without `commands` nothing above the reads engine is: a read-only build
/// links the engine and the base vocabulary alone.
#[test]
fn the_reads_engine_is_linked_by_the_reads_feature_alone() {
    let commands_only = tree(None, Some("commands"));
    assert!(!commands_only.contains("angzarr-reads"));
    let reads_only = tree(None, Some("reads"));
    assert!(reads_only.contains("angzarr-reads"));
    let neither = tree(None, Some(""));
    assert!(!neither.contains("angzarr-reads"));
}
