//! The features keep a read-only container free of command code: with
//! `commands` off the crate has no `commands` module, no `Commands`, no
//! `Caller`, no `Unserved`, no `NoCommands`; the default build has both
//! halves. Each case builds a probe example against the feature set, in the
//! same target directory as the tests (cargo's own lock serializes them).

use std::process::{Command, Output};

fn build(example: &str, features: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO"));
    command
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "build",
            "--locked",
            "--offline",
            "-p",
            env!("CARGO_PKG_NAME"),
        ])
        .args(["--example", example, "--no-default-features"]);
    if !features.is_empty() {
        command.arg("--features").arg(features.join(","));
    }
    command.output().expect("cargo build runs")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// With `reads` alone the crate builds, and the command half is not there.
#[test]
fn a_reads_only_build_has_no_commands_module() {
    let reads = build("reads_probe", &["reads"]);
    assert!(
        reads.status.success(),
        "reads-only build: {}",
        stderr(&reads)
    );
    let commands = build("commands_probe", &["reads"]);
    assert!(
        !commands.status.success(),
        "the command half is linked into a reads-only build"
    );
    let text = stderr(&commands);
    for name in ["Caller", "NoCommands"] {
        assert!(
            text.contains(name),
            "the build failed, but not on `{name}`: {text}"
        );
    }
}

/// With `commands` alone the crate builds the command half, and the read
/// half's marker is not there.
#[test]
fn a_commands_only_build_has_no_reads_marker() {
    let commands = build("commands_probe", &["commands"]);
    assert!(
        commands.status.success(),
        "commands-only build: {}",
        stderr(&commands)
    );
    let reads = build("reads_probe", &["commands"]);
    assert!(
        !reads.status.success(),
        "the read half is linked into a commands-only build"
    );
    assert!(stderr(&reads).contains("NoReads"), "{}", stderr(&reads));
}

/// The default build is both halves, and the feature table is what the
/// manifest says: each feature stands alone, neither pulls the other, and
/// only `reads` enables a dependency (the reads engine).
#[test]
fn the_default_is_both_halves_and_each_feature_stands_alone() {
    let both = build("commands_probe", &["commands", "reads"]);
    assert!(both.status.success(), "{}", stderr(&both));
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("the manifest reads");
    let features = manifest
        .split("[features]")
        .nth(1)
        .and_then(|rest| rest.split("\n[").next())
        .expect("a [features] table");
    let lines: Vec<&str> = features
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    assert_eq!(
        lines,
        [
            r#"default = ["commands", "reads"]"#,
            "commands = []",
            r#"reads = ["dep:angzarr-reads"]"#
        ]
    );
}
