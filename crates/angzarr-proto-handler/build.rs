//! Generates angzarr's handler layer, `io.angzarr.handler.v1` (decision
//! 14.78): every `.proto` under `io/angzarr/handler/v1/` of
//! angzarr-project's `proto/` (the `angzarr-project` submodule), found by
//! walking the directory. One prost pass writes the package's messages, with
//! core's extern-pathed onto angzarr-proto (`::angzarr_proto::v1`), so none
//! is generated twice. The layer depends on core only, and core on nothing
//! of it (docs/design/handler-contract.md §1).
//!
//! The encoded `FileDescriptorSet` of the files, with their imports, goes
//! to `$OUT_DIR/file_descriptor_set.bin` (the library's
//! `FILE_DESCRIPTOR_SET`). The proto root is exported to the tests as
//! `ANGZARR_PROTO_ROOT`.
//!
//! Requires `protoc` on PATH (or `PROTOC` set).

use std::path::PathBuf;

#[path = "../angzarr-proto/build_support.rs"]
mod support;

/// The handler layer's package, under the proto root.
const PACKAGE: &str = "io/angzarr/handler/v1";

fn main() {
    let root = support::proto_root();
    let files = support::package_files(&root, PACKAGE);
    println!("cargo:rustc-env=ANGZARR_PROTO_ROOT={}", root.display());
    support::rerun_if_changed(&files);
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));

    prost_build::Config::new()
        .enable_type_names()
        .file_descriptor_set_path(out.join("file_descriptor_set.bin"))
        .bytes(["."])
        .extern_path(".io.angzarr.v1", "::angzarr_proto::v1")
        .compile_protos(&files, std::slice::from_ref(&root))
        .expect("prost: compile angzarr's io.angzarr.handler.v1 protos");
}
