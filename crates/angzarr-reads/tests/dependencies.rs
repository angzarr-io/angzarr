//! What this crate may link: base, ports, proto and diagnostic, and nothing
//! above them. A read-only container links this crate alone, so no
//! command-side crate (the pipeline, the edge, the gRPC server, the router,
//! the embedded runtime) and no transport or runtime is in its closure.

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

const ALLOWED: [&str; 4] = [
    "angzarr-coordinator-base",
    "angzarr-diagnostic",
    "angzarr-ports",
    "angzarr-proto",
];

/// The angzarr crates under this one are exactly base, ports, proto and
/// diagnostic; every command-side crate, transport and runtime is absent.
#[test]
fn reads_depends_on_no_command_side_crate() {
    let every = tree(None);
    let angzarr: BTreeSet<_> = every
        .iter()
        .filter(|name| name.starts_with("angzarr-") && *name != env!("CARGO_PKG_NAME"))
        .cloned()
        .collect();
    let allowed: BTreeSet<_> = ALLOWED.iter().map(ToString::to_string).collect();
    assert_eq!(angzarr, allowed, "the angzarr crates in the closure");

    for banned in [
        "angzarr-pipeline",
        "angzarr-coordinator",
        "angzarr-coordinator-grpc",
        "angzarr-router",
        "angzarr-embedded",
        "tonic",
        "tokio",
        "hyper",
        "tower",
        "h2",
        "mio",
        "async-std",
        "futures",
    ] {
        assert!(
            !every.iter().any(|name| name == banned),
            "{banned} is in the closure"
        );
    }
    for prefix in ["tonic-", "tokio-", "hyper-", "tower-"] {
        assert!(
            !every.iter().any(|name| name.starts_with(prefix)),
            "a {prefix}* crate is in the closure"
        );
    }
}

/// Directly: the four angzarr crates and `prost`/`prost-types`, and no other
/// third-party crate.
#[test]
fn reads_links_no_third_party_crate_of_its_own_but_prost() {
    let own: BTreeSet<_> = tree(Some(1))
        .into_iter()
        .filter(|name| name != env!("CARGO_PKG_NAME"))
        .collect();
    let allowed: BTreeSet<_> = ALLOWED
        .iter()
        .chain(&["prost", "prost-types"])
        .map(ToString::to_string)
        .collect();
    assert_eq!(own, allowed);
}
