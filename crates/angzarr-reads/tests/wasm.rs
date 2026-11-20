//! The crate builds for the browser: no thread, no socket, no runtime.

#![cfg(not(target_arch = "wasm32"))]

use std::process::Command;

#[test]
fn reads_builds_for_wasm32() {
    let output = Command::new(env!("CARGO"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "build",
            "--locked",
            "--offline",
            "-p",
            env!("CARGO_PKG_NAME"),
        ])
        .args(["--target", "wasm32-unknown-unknown"])
        .output()
        .expect("cargo build runs");
    assert!(
        output.status.success(),
        "wasm32 build failed (`rustup target add wasm32-unknown-unknown`?): {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
