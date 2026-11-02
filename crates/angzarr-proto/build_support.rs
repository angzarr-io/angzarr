//! What every angzarr-core proto crate's build script shares (`#[path]`):
//! finding the spec's protos, listing a package's files, and the reruns.
//! (A package with reason keys also includes `build_reasons.rs`.)
//! Each crate generates one package of angzarr-project's `proto/`
//! (decision 14.64): angzarr-proto core's `io.angzarr.v1`, and its
//! siblings the component contract and each transport, extern-pathing what
//! they import onto the crate that owns it.

use std::path::{Path, PathBuf};

/// Resolves the proto root: the `ANGZARR_PROJECT_PROTO` override first,
/// then the angzarr-project submodule's `proto/` at the repository root
/// (two levels above every crate under `crates/`).
pub fn proto_root() -> PathBuf {
    println!("cargo:rerun-if-env-changed=ANGZARR_PROJECT_PROTO");
    println!("cargo:rerun-if-env-changed=PROTOC");
    if let Ok(root) = std::env::var("ANGZARR_PROJECT_PROTO") {
        return PathBuf::from(root);
    }
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let submodule = manifest.join("../../angzarr-project/proto");
    if submodule.join("io/angzarr/v1/types.proto").exists() {
        return submodule
            .canonicalize()
            .expect("canonicalize the proto root");
    }
    panic!(
        "angzarr protos not found at {} (is the angzarr-project submodule checked out, at a \
         revision with proto/io/angzarr/v1?); set ANGZARR_PROJECT_PROTO",
        submodule.display()
    );
}

/// Every `.proto` of the package directory `package` (under `root`),
/// recursively, in a stable order; reruns the build when one is added.
pub fn package_files(root: &Path, package: &str) -> Vec<PathBuf> {
    let dir = root.join(package);
    println!("cargo:rerun-if-changed={}", dir.display());
    let mut found = Vec::new();
    protos_under(&dir, &mut found);
    found
}

fn protos_under(dir: &Path, found: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|entry| entry.expect("a directory entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            protos_under(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "proto") {
            found.push(path);
        }
    }
}

/// Reruns the build when any of `files` changes.
pub fn rerun_if_changed<'a>(files: impl IntoIterator<Item = &'a PathBuf>) {
    for f in files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
}
