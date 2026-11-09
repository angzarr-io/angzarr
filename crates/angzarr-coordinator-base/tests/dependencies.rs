//! What this crate may link: diagnostic, proto and ports, and nothing above
//! them (the reads engine, the pipeline and the edge build on this crate);
//! no transport and no runtime.

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

const ALLOWED: [&str; 3] = ["angzarr-diagnostic", "angzarr-proto", "angzarr-ports"];

/// The angzarr crates under this one are the three allowed, and everything
/// above ports (and every transport and runtime) is absent from the
/// closure.
#[test]
fn base_depends_on_nothing_above_ports() {
    let every = tree(None);
    let angzarr: BTreeSet<_> = every
        .iter()
        .filter(|name| name.starts_with("angzarr-") && *name != env!("CARGO_PKG_NAME"))
        .cloned()
        .collect();
    // Ports' own closure: diagnostic and proto (and what they link), no
    // other angzarr crate.
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
    ] {
        assert!(
            !every
                .iter()
                .any(|name| name == banned || name.starts_with(&format!("{banned}-"))),
            "{banned} is in the closure"
        );
    }
}

/// Directly, the three angzarr crates and `prost-types` (for `Any`), and
/// no other third-party crate.
#[test]
fn base_links_no_third_party_crate_of_its_own_but_prost_types() {
    let direct = tree(Some(1));
    let own: BTreeSet<_> = direct
        .into_iter()
        .filter(|name| name != env!("CARGO_PKG_NAME"))
        .collect();
    let allowed: BTreeSet<_> = ALLOWED
        .iter()
        .chain(&["prost-types"])
        .map(ToString::to_string)
        .collect();
    assert_eq!(own, allowed);
}
