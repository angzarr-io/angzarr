//! Generates angzarr's distributed transport,
//! `io.angzarr.transport.distributed.v1` (decision 14.64): every `.proto`
//! under `io/angzarr/transport/distributed/v1/` of angzarr-project's
//! `proto/` (the `angzarr-project` submodule). A prost pass first generates
//! `sererr/v1/sererr.proto`, which its dead-letter envelope imports for the
//! captured stack trace (the one package that imports sererr); then one
//! tonic-prost-build pass writes the package's messages and every service's
//! client and server modules, with core's messages extern-pathed onto
//! angzarr-proto (`::angzarr_proto::v1`) and sererr's onto the first pass's
//! (`crate::sererr::v1`). No `connect` is generated
//! (`build_transport(false)`). The service protos'
//! `google/api/annotations.proto` resolves from angzarr-project's vendored
//! `proto/google/api/`; nothing included references `google.api`.
//!
//! From the encoded `FileDescriptorSet` (also the library's
//! `FILE_DESCRIPTOR_SET`): the package's reason keys' args, to
//! `$OUT_DIR/reason_args.rs`, and its en-US catalog copied to `$OUT_DIR`
//! (angzarr-proto's `build_reasons.rs`).
//!
//! The proto root is exported to the tests as `ANGZARR_PROTO_ROOT`.
//! Requires `protoc` on PATH (or `PROTOC` set).

use std::path::PathBuf;

#[path = "../angzarr-proto/build_descriptor.rs"]
mod descriptor;
#[path = "../angzarr-proto/build_reasons.rs"]
mod reasons;
#[path = "../angzarr-proto/build_support.rs"]
mod support;

/// The distributed transport's package, under the proto root.
const PACKAGE: &str = "io/angzarr/transport/distributed/v1";

fn main() {
    let root = support::proto_root();
    let files = support::package_files(&root, PACKAGE);
    let sererr = root.join("sererr/v1/sererr.proto");
    let cloudevents = root.join("io/cloudevents/v1/cloudevents.proto");
    println!("cargo:rustc-env=ANGZARR_PROTO_ROOT={}", root.display());
    support::rerun_if_changed(files.iter().chain([&sererr, &cloudevents]));
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));

    // sererr's messages first, alone, for the envelope to extern-path onto.
    prost_build::Config::new()
        .enable_type_names()
        .bytes(["."])
        .compile_protos(&[&sererr], &[&root])
        .expect("prost: compile sererr.v1");
    // The vendored official CloudEvents format (decision 14.108), alone
    // too, for CloudEventsResponse to extern-path onto.
    prost_build::Config::new()
        .enable_type_names()
        .bytes(["."])
        .compile_protos(&[&cloudevents], &[&root])
        .expect("prost: compile io.cloudevents.v1");

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
        .extern_path(".sererr.v1", "crate::sererr::v1")
        .extern_path(".io.cloudevents.v1", "crate::cloudevents::v1")
        .compile_with_config(config, &files, std::slice::from_ref(&root))
        .expect("tonic: compile angzarr's io.angzarr.transport.distributed.v1 protos");

    let set = std::fs::read(out.join("file_descriptor_set.bin")).expect("read the descriptor set");
    reasons::reason_keys(&root, &out, &set, PACKAGE, &["DistributedReason"]);
}
