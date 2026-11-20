//! Generates every `io.angzarr.v1` message from the canonical protos:
//! angzarr-project's `proto/`, which this repository carries as the
//! `angzarr-project` submodule (the crate moved here from angzarr-project's
//! `rust/`).
//!
//! Core's package only (decision 14.64): every `.proto` under
//! `io/angzarr/v1/` is compiled, found by walking the directory, so a file
//! the spec adds is generated with no edit here: the books and pages, the
//! meta-events and dead letters of `types.proto`, the options, the reason
//! keys and the edition summary (`meta.proto`). With them the vendored
//! `google/rpc/code.proto` (decision 14.104), which `reasons.proto` imports
//! for a key's status. Core imports nothing of angzarr's other packages and
//! no sererr, and declares no service, so this crate has no tonic and no
//! `google.api` (decision 1.12). prost generates the option messages
//! (`ComponentOptions`, `CommandOptions`, ...) but not the `extend`
//! declarations, which it ignores; their values are read from descriptors
//! (see `tests/options.rs`).
//!
//! The pass writes the encoded `FileDescriptorSet` of those files, with
//! their imports, to `$OUT_DIR/file_descriptor_set.bin`, which the library
//! exposes as `FILE_DESCRIPTOR_SET` (for a gRPC reflection server).
//!
//! The same `.proto` files are also copied to `$OUT_DIR/proto/`, keeping
//! their import paths, and listed in `$OUT_DIR/proto_files.rs`, so the
//! crate can embed them ([`protos`] in the library): a dependent's own
//! protos import angzarr's (`options.proto` for the component options)
//! through `angzarr_proto::protos::write_include_dir`, with no vendored copy
//! and no path into this repository.
//!
//! The reason keys' args and surfaces are read from that descriptor set
//! (their `(io.angzarr.v1.reason)` options, which prost does not generate)
//! and written to `$OUT_DIR/reason_args.rs` (`build_reasons.rs`), which the
//! library includes in `v1`. Core's en-US catalog of its keys,
//! `io/angzarr/v1/reasons.en-US.ftl`, is copied to `$OUT_DIR` for the
//! library to embed as `REASONS_EN_US` (decisions 14.60 and 14.64).
//! `build_descriptor.rs`, `build_reasons.rs` and `build_support.rs` are
//! shared with the sibling proto crates' build scripts.
//!
//! The proto root is exported to the crate's own tests as
//! `ANGZARR_PROTO_ROOT` (`env!("ANGZARR_PROTO_ROOT")`), so they compile the
//! very protos the library was generated from.
//!
//! Requires `protoc` on PATH (or `PROTOC` set): prost-build 0.14 does not
//! bundle it.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

#[path = "build_descriptor.rs"]
mod descriptor;
#[path = "build_reasons.rs"]
mod reasons;
#[path = "build_support.rs"]
mod support;

/// Core's package, under the proto root.
const PACKAGE: &str = "io/angzarr/v1";

/// The vendored `google.rpc.Code`, under the proto root (decision 14.104).
const GOOGLE_RPC_CODE: &str = "google/rpc/code.proto";

fn main() {
    let root = support::proto_root();
    let files = support::package_files(&root, PACKAGE);
    // The tests read the root the library was generated from.
    println!("cargo:rustc-env=ANGZARR_PROTO_ROOT={}", root.display());
    support::rerun_if_changed(&files);

    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));

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
    // google.rpc.Code, the type of a reason key's `grpc_status` (decision
    // 14.104): the spec vendors googleapis' `google/rpc/code.proto`, and it
    // is generated here, beside the core package that imports it.
    let mut compiled = files.clone();
    compiled.push(root.join(GOOGLE_RPC_CODE));
    support::rerun_if_changed(&compiled[files.len()..]);
    // Embedded with the core files, which import it (reasons.proto).
    export_protos(&root, &out, compiled.iter());
    config
        .compile_protos(&compiled, std::slice::from_ref(&root))
        .expect("prost: compile angzarr's io.angzarr.v1 protos");

    let set = std::fs::read(out.join("file_descriptor_set.bin")).expect("read the descriptor set");
    reasons::reason_keys(&root, &out, &set, PACKAGE, &["AngzarrReason"]);
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
