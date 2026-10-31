//! Generates the domain-neutral `io.angzarr.v1` types from this repo's
//! canonical protos (lifted from angzarr-router's `crates/router/build.rs`).
//!
//! Only `types.proto` (with `sererr/v1/sererr.proto`, which it imports),
//! `upcaster.proto`, `options.proto`, `meta.proto`, `reasons.proto` (the
//! framework's reason-key enums) and the meta-events'
//! `meta/events.proto` (package `io.angzarr.v1.meta`) are compiled. prost generates the
//! option messages (`ConflictUnit`, `ComponentOptions`, ...) but not the
//! `extend` declarations, which it ignores; their values are read from
//! descriptors (see `tests/conflict_units.rs`). prost-build emits messages only: the
//! `UpcasterService` in upcaster.proto and the `EditionAdminService` in
//! meta.proto are skipped because no service generator is configured, so
//! meta.proto contributes its messages only (`ForkStreamRequest`,
//! `ListEditionsRequest`/`Response`, `EditionSummary`,
//! `DeleteEditionRequest`). meta.proto imports
//! `google/api/annotations.proto` for its HTTP bindings: protoc resolves it
//! from this repo's vendored `proto/google/api/`, and nothing generated
//! here references `google.api` (its code is emitted to `$OUT_DIR` but not
//! included by the library).
//!
//! The same `.proto` files, with the vendored `google/api` files meta.proto
//! imports, are also copied to `$OUT_DIR/proto/`, keeping
//! their import paths, so the crate can embed them ([`protos`] in the
//! library): a dependent's own protos import angzarr's (`options.proto` for
//! the `state` and `conflict` options) through
//! `angzarr_proto::protos::write_include_dir`, with no vendored copy and no
//! path into this repository.
//!
//! Requires `protoc` on PATH (or `PROTOC` set): prost-build 0.14 does not
//! bundle it.

use std::path::PathBuf;

/// Resolves the proto root: env override first, then this repo's `proto/`.
fn proto_root() -> PathBuf {
    if let Ok(root) = std::env::var("ANGZARR_PROJECT_PROTO") {
        return PathBuf::from(root);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_proto = manifest.join("../../proto");
    if repo_proto.join("io/angzarr/v1/types.proto").exists() {
        return repo_proto;
    }
    panic!("angzarr protos not found at ../../proto; set ANGZARR_PROJECT_PROTO");
}

fn main() {
    let root = proto_root();
    let files = [
        root.join("io/angzarr/v1/types.proto"),
        root.join("io/angzarr/v1/upcaster.proto"),
        root.join("io/angzarr/v1/options.proto"),
        root.join("io/angzarr/v1/meta.proto"),
        root.join("io/angzarr/v1/reasons.proto"),
        root.join("io/angzarr/v1/meta/events.proto"),
        root.join("sererr/v1/sererr.proto"),
    ];
    // Embedded but not generated: meta.proto's HTTP-binding imports, so a
    // dependent's protoc can import meta.proto from the include directory.
    let imports_only = [
        root.join("google/api/annotations.proto"),
        root.join("google/api/http.proto"),
    ];
    println!("cargo:rerun-if-env-changed=ANGZARR_PROJECT_PROTO");
    println!("cargo:rerun-if-env-changed=PROTOC");
    for f in files.iter().chain(&imports_only) {
        println!("cargo:rerun-if-changed={}", f.display());
    }

    export_protos(&root, files.iter().chain(&imports_only));

    let mut config = prost_build::Config::new();
    config
        .enable_type_names()
        // Every proto `bytes` field is a `bytes::Bytes`: cheap to clone, and
        // `Hash`/`Eq`, so `Uuid` can key a map. Pure Rust, wasm32-safe.
        .bytes(["."]);
    // `Eq` and `Hash` need no configuration: prost-build 0.14 derives them
    // on every message without float fields (directly or nested), which
    // covers the key types (`Uuid`, `Cover`, `EventRef`, `FactOrigin`,
    // `StateDescriptor`, ...). `tests/roundtrip.rs` pins that at compile
    // time, so a float added to one of them fails the build.
    config
        .compile_protos(&files, &[root])
        .expect("prost: compile angzarr domain-neutral protos");
}

/// Copies each compiled proto to `$OUT_DIR/proto/<its path under root>`,
/// for `src/protos.rs` to embed with `include_str!`.
fn export_protos<'a>(root: &std::path::Path, files: impl Iterator<Item = &'a PathBuf>) {
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("proto");
    for file in files {
        let relative = file.strip_prefix(root).expect("a proto under the root");
        let target = out.join(relative);
        std::fs::create_dir_all(target.parent().expect("a parent")).expect("create proto dir");
        std::fs::copy(file, &target).expect("copy proto");
    }
}
