//! Generates angzarr's component contract, `io.angzarr.component.v1`
//! (decision 14.64): every `.proto` under `io/angzarr/component/v1/` of
//! angzarr-project's `proto/` (the `angzarr-project` submodule), found by
//! walking the directory. One tonic-prost-build pass writes the package's
//! own messages (`DescribeRequest`) and every service's client and server
//! modules, with core's messages extern-pathed onto angzarr-proto
//! (`::angzarr_proto::v1`), so none is generated twice. No `connect` is
//! generated (`build_transport(false)`): the caller picks the transport.
//!
//! The encoded `FileDescriptorSet` of the files, with their imports, goes
//! to `$OUT_DIR/file_descriptor_set.bin` (the library's
//! `FILE_DESCRIPTOR_SET`, for a gRPC reflection server). The proto root is
//! exported to the tests as `ANGZARR_PROTO_ROOT`.
//!
//! Requires `protoc` on PATH (or `PROTOC` set).

use std::path::PathBuf;

#[path = "../angzarr-proto/build_support.rs"]
mod support;

/// The component contract's package, under the proto root.
const PACKAGE: &str = "io/angzarr/component/v1";

fn main() {
    let root = support::proto_root();
    let files = support::package_files(&root, PACKAGE);
    println!("cargo:rustc-env=ANGZARR_PROTO_ROOT={}", root.display());
    support::rerun_if_changed(&files);
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));

    let mut config = prost_build::Config::new();
    config
        .enable_type_names()
        .file_descriptor_set_path(out.join("file_descriptor_set.bin"))
        .bytes(["."]);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .extern_path(".io.angzarr.v1", "::angzarr_proto::v1")
        .compile_with_config(config, &files, std::slice::from_ref(&root))
        .expect("tonic: compile angzarr's io.angzarr.component.v1 services");
}
