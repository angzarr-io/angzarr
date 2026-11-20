use std::path::PathBuf;

/// Compile the conformance fixture proto (`test.counter`) and emit a
/// FileDescriptorSet covering it and the framework protos it imports — the
/// pool the harness uses to parse `.txtpb` fixtures (textproto, with Any
/// expansion) into typed messages.
///
/// `io.angzarr.v1.*`, `io.angzarr.handler.v1.*` and `google.rpc.*` are
/// extern-pathed to the router crate
/// so we DON'T regenerate them: the harness must build the SAME Rust types
/// `DomainDispatch::dispatch` consumes. extern_path suppresses codegen
/// only — the descriptor set still carries those files for the pool. The
/// framework protos are the ones angzarr-proto embeds (the files its types
/// were generated from, written out by `protos::write_include_dir`), so the
/// pool describes exactly those types; only the fixtures come from this
/// repository's angzarr-project submodule.
fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let top = manifest.join("../..");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let framework_proto = angzarr_proto::protos::write_include_dir(&out_dir.join("angzarr-proto"))
        .expect("write angzarr-proto's protos");
    // The handler-contract tier's fixtures (decision 14.42): the router's
    // complete contract, shared by every router.
    let fixture = top.join("angzarr-project/features/handler-contract/fixture");
    let counter = fixture.join("counter.proto");
    let order = fixture.join("order.proto");
    // The fixture pool parses the handler layer's requests (HandleRequest,
    // decision 14.78) and core's EventPage from the .txtpb fixtures. The
    // message-annotation counter.proto imports neither, so the harness
    // names them explicitly — its descriptor needs are its own, not an
    // artifact of what the fixture proto imports. The handler layer's
    // protos come from this repository's angzarr-project submodule, held to
    // the same spec commit as core's (`just check-core-lockstep`); core's
    // resolve from angzarr-proto's embedded copies, the first include.
    let spec_proto = top.join("angzarr-project/proto");
    let types = framework_proto.join("io/angzarr/v1/types.proto");
    let command_handler = spec_proto.join("io/angzarr/handler/v1/command_handler.proto");

    let fds_path = out_dir.join("conformance_fds.bin");

    println!("cargo:rerun-if-changed={}", fixture.display());

    let mut config = prost_build::Config::new();
    config
        .enable_type_names()
        .extern_path(".io.angzarr.v1", "::angzarr_router::pb")
        .extern_path(".io.angzarr.handler.v1", "::angzarr_router::hpb")
        .extern_path(
            ".google.rpc",
            "::angzarr_router::angzarr_proto::google::rpc",
        )
        .file_descriptor_set_path(&fds_path)
        .compile_protos(
            &[counter, order, types, command_handler],
            &[framework_proto, spec_proto, fixture],
        )
        .expect("prost: compile conformance protos");
}
