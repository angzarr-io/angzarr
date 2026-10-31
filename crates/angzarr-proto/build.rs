//! Generates every `io.angzarr.v1` message from the canonical protos:
//! angzarr-project's `proto/`, which this repository carries as the
//! `angzarr-project` submodule (the crate moved here from angzarr-project's
//! `rust/`).
//!
//! Every `.proto` under `io/angzarr/v1/` is compiled, found by walking the
//! directory, so a file the spec adds is generated with no edit here: the
//! books and pages of `types.proto`, the options, the reason keys, the
//! edition messages and meta-events (`meta/events.proto`, package
//! `io.angzarr.v1.meta`), and the request and response messages of every
//! service proto (`command_handler.proto`, `component.proto`,
//! `policy.proto`, `projector.proto`, `query.proto`, `stream.proto`,
//! `upcaster.proto`, `meta.proto`, ...), with `sererr/v1/sererr.proto`,
//! which `types.proto` imports. prost generates the option messages
//! (`ConflictUnit`, `ComponentOptions`, ...) but not the `extend`
//! declarations, which it ignores; their values are read from descriptors
//! (see `tests/conflict_units.rs`). The service protos import
//! `google/api/annotations.proto` for their HTTP bindings: protoc resolves
//! it from angzarr-project's vendored `proto/google/api/`, and nothing
//! generated here references `google.api` (its code is emitted to
//! `$OUT_DIR` but not included by the library).
//!
//! The message pass writes the encoded `FileDescriptorSet` of those files,
//! with their imports, to `$OUT_DIR/file_descriptor_set.bin`, which the
//! library exposes as `FILE_DESCRIPTOR_SET` (for a gRPC reflection server).
//!
//! With the `grpc` feature, a second pass runs tonic-prost-build over the
//! same files with every message extern-pathed onto the first pass's
//! (`crate::v1`, `crate::sererr::v1`), so it writes only the services'
//! client and server modules, to `$OUT_DIR/grpc/`, which the library
//! includes in `v1`. No message is generated twice, and the messages are
//! the same with the feature on or off.
//!
//! The same `.proto` files, with the vendored `google/api` files they
//! import, are also copied to `$OUT_DIR/proto/`, keeping their import
//! paths, and listed in `$OUT_DIR/proto_files.rs`, so the crate can embed
//! them ([`protos`] in the library): a dependent's own protos import
//! angzarr's (`options.proto` for the `state` and `conflict` options)
//! through `angzarr_proto::protos::write_include_dir`, with no vendored
//! copy and no path into this repository.
//!
//! The reason keys' args are read from that descriptor set (their
//! `(io.angzarr.v1.reason)` options, which prost does not generate) and
//! written to `$OUT_DIR/reason_args.rs` (`build_reasons.rs`), which the
//! library includes in `v1`. The spec's en-US catalog of the keys,
//! `io/angzarr/v1/reasons.en-US.ftl`, is copied to `$OUT_DIR` for the
//! library to embed as `REASONS_EN_US` (decision 14.60).
//!
//! The proto root is exported to the crate's own tests as
//! `ANGZARR_PROTO_ROOT` (`env!("ANGZARR_PROTO_ROOT")`), so they compile the
//! very protos the library was generated from.
//!
//! Requires `protoc` on PATH (or `PROTOC` set): prost-build 0.14 does not
//! bundle it.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

#[path = "build_reasons.rs"]
mod reasons;

/// The spec's en-US catalog of the reason keys, under the proto root.
const REASONS_CATALOG: &str = "io/angzarr/v1/reasons.en-US.ftl";

/// Resolves the proto root: the `ANGZARR_PROJECT_PROTO` override first,
/// then the angzarr-project submodule's `proto/` at the repository root.
fn proto_root() -> PathBuf {
    if let Ok(root) = std::env::var("ANGZARR_PROJECT_PROTO") {
        return PathBuf::from(root);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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

/// Every `.proto` under `dir`, recursively, in a stable order.
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

fn main() {
    let root = proto_root();
    let mut files = Vec::new();
    protos_under(&root.join("io/angzarr/v1"), &mut files);
    files.push(root.join("sererr/v1/sererr.proto"));
    // Embedded but not generated: the service protos' HTTP-binding
    // imports, so a dependent's protoc can import them from the include
    // directory.
    let imports_only = [
        root.join("google/api/annotations.proto"),
        root.join("google/api/http.proto"),
    ];
    println!("cargo:rerun-if-env-changed=ANGZARR_PROJECT_PROTO");
    // The tests read the root the library was generated from.
    println!("cargo:rustc-env=ANGZARR_PROTO_ROOT={}", root.display());
    println!("cargo:rerun-if-env-changed=PROTOC");
    // A file added to the directory reruns the walk.
    println!(
        "cargo:rerun-if-changed={}",
        root.join("io/angzarr/v1").display()
    );
    let catalog = root.join(REASONS_CATALOG);
    for f in files.iter().chain(&imports_only).chain([&catalog]) {
        println!("cargo:rerun-if-changed={}", f.display());
    }

    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    export_protos(&root, &out, files.iter().chain(&imports_only));

    let mut config = prost_build::Config::new();
    config
        .enable_type_names()
        // For a reflection server: the files and their imports, encoded.
        .file_descriptor_set_path(out.join("file_descriptor_set.bin"))
        // Every proto `bytes` field is a `bytes::Bytes`: cheap to clone, and
        // `Hash`/`Eq`, so `Uuid` can key a map. Pure Rust, wasm32-safe.
        .bytes(["."]);
    // `Eq` and `Hash` need no configuration: prost-build 0.14 derives them
    // on every message without float fields (directly or nested), which
    // covers the key types (`Uuid`, `Cover`, `EventRef`, `FactOrigin`,
    // `StateDescriptor`, ...). `tests/roundtrip.rs` pins that at compile
    // time, so a float added to one of them fails the build.
    config
        .compile_protos(&files, std::slice::from_ref(&root))
        .expect("prost: compile angzarr's io.angzarr.v1 protos");

    let set = std::fs::read(out.join("file_descriptor_set.bin")).expect("read the descriptor set");
    std::fs::write(out.join("reason_args.rs"), reasons::generate(&set))
        .expect("write reason_args.rs");
    std::fs::copy(&catalog, out.join("reasons.en-US.ftl")).unwrap_or_else(|e| {
        panic!(
            "copy {} (an angzarr-project revision before decision 14.60?): {e}",
            catalog.display()
        )
    });

    #[cfg(feature = "grpc")]
    grpc(&root, &out, &files);
}

/// The `grpc` feature's pass: tonic client and server modules for every
/// service, on the message pass's types, to `$OUT_DIR/grpc/`.
#[cfg(feature = "grpc")]
fn grpc(root: &Path, out: &Path, files: &[PathBuf]) {
    let dir = out.join("grpc");
    std::fs::create_dir_all(&dir).expect("create the grpc out dir");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        // No `connect`: the caller picks the transport and hands a client
        // its channel (`XClient::new`), so tonic's transport stays out.
        .build_transport(false)
        .extern_path(".io.angzarr.v1", "crate::v1")
        .extern_path(".sererr.v1", "crate::sererr::v1")
        .out_dir(&dir)
        .compile_protos(files, &[root.to_path_buf()])
        .expect("tonic: compile angzarr's io.angzarr.v1 services");
}

/// Copies each proto to `$OUT_DIR/proto/<its path under root>` and lists
/// them in `$OUT_DIR/proto_files.rs`, for `src/protos.rs` to embed with
/// `include_str!`.
fn export_protos<'a>(root: &Path, out: &Path, files: impl Iterator<Item = &'a PathBuf>) {
    let mut listing = String::from("&[\n");
    for file in files {
        let relative = file.strip_prefix(root).expect("a proto under the root");
        let target = out.join("proto").join(relative);
        std::fs::create_dir_all(target.parent().expect("a parent")).expect("create proto dir");
        std::fs::copy(file, &target).expect("copy proto");
        let path = relative
            .to_str()
            .expect("a UTF-8 proto path")
            .replace('\\', "/");
        writeln!(
            listing,
            "    ProtoFile {{ path: {path:?}, contents: include_str!(concat!(env!(\"OUT_DIR\"), \"/proto/\", {path:?})) }},"
        )
        .expect("write to a String");
    }
    listing.push(']');
    std::fs::write(out.join("proto_files.rs"), listing).expect("write proto_files.rs");
}
