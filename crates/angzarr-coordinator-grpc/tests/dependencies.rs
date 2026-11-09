//! What this crate may link of angzarr's: the coordinators' base, the edge
//! and the reads engine (behind the `commands` and `reads` features), the
//! wire types and the distributed transport's stubs, and what those link;
//! the pipeline stays out, and so do the router and the embedded runtime. This crate is the one core crate that links a
//! transport and a runtime, so none of them is banned here.

use std::collections::BTreeSet;
use std::process::Command;

/// `cargo tree`'s package names under this crate, every edge kind (its own
/// dev-dependencies included), at `depth` (all of them when `None`).
fn tree(depth: Option<u32>) -> BTreeSet<String> {
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

const ALLOWED: [&str; 10] = [
    "angzarr-coordinator",
    "angzarr-coordinator-base",
    "angzarr-diagnostic",
    "angzarr-ports",
    "angzarr-proto",
    "angzarr-proto-component",
    "angzarr-proto-handler",
    "angzarr-proto-transport-distributed",
    "angzarr-reads",
    // The tests' scratch directories (a dev-dependency).
    "angzarr-test-dir",
];

/// The angzarr crates in the closure are the allowed ones (the base, the
/// edge, the reads engine, the wire types and the transport's stubs, and the
/// tests' scratch directory), and none of the router, the embedded runtime
/// or the pipeline.
#[test]
fn the_listener_links_only_the_edge_the_reads_and_the_wire() {
    let every = tree(None);
    let angzarr: BTreeSet<_> = every
        .iter()
        .filter(|name| name.starts_with("angzarr-") && *name != env!("CARGO_PKG_NAME"))
        .cloned()
        .collect();
    let allowed: BTreeSet<_> = ALLOWED.iter().map(ToString::to_string).collect();
    assert_eq!(angzarr, allowed, "the angzarr crates in the closure");
}

/// The transport-free crates stay so: the closure of the base, which this
/// crate builds on, has no tonic and no tokio of its own making. (Here they
/// are expected: this crate is the one that links them.)
#[test]
fn the_listener_is_the_crate_that_links_tonic_and_tokio() {
    let every = tree(None);
    for linked in ["tonic", "tokio", "tonic-health", "tokio-stream"] {
        assert!(every.contains(linked), "{linked} is not in the closure");
    }
}

/// Directly: the angzarr crates this one uses and the transport stack, and
/// no other third-party crate.
#[test]
fn the_listener_links_no_third_party_crate_outside_the_tonic_stack() {
    let direct = tree(Some(1));
    let own: BTreeSet<_> = direct
        .into_iter()
        .filter(|name| name != env!("CARGO_PKG_NAME"))
        .collect();
    let allowed: BTreeSet<_> = [
        "angzarr-coordinator",
        "angzarr-coordinator-base",
        "angzarr-diagnostic",
        "angzarr-ports",
        "angzarr-proto",
        "angzarr-proto-transport-distributed",
        "angzarr-reads",
        "angzarr-test-dir",
        "hyper-util",
        "prost",
        "prost-types",
        "tokio",
        "tokio-stream",
        "tonic",
        "tonic-health",
        "tonic-prost",
    ]
    .into_iter()
    .map(ToString::to_string)
    .collect();
    assert_eq!(own, allowed);
}
