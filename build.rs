use std::collections::HashSet;
use std::path::PathBuf;

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Rerun if proto files or migration files change.
    println!(
        "cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/types.proto"
    );
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/command_handler.proto");
    println!(
        "cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/projector.proto"
    );
    println!(
        "cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/saga.proto"
    );
    println!("cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/process_manager.proto");
    println!(
        "cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/query.proto"
    );
    println!(
        "cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/stream.proto"
    );
    println!(
        "cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/upcaster.proto"
    );
    println!(
        "cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/meta.proto"
    );
    println!("cargo:rerun-if-changed=angzarr-project/proto/angzarr_client/proto/angzarr/v1/cloudevents.proto");
    println!("cargo:rerun-if-changed=proto/io/cloudevents/v1/cloudevents.proto");
    println!("cargo:rerun-if-changed=proto/angzarr/status/dlq_admin.proto");
    // Sererr proto schema lives in the `sererr/` submodule; rerun if it
    // changes upstream.
    println!("cargo:rerun-if-changed=sererr/proto/sererr/v1/sererr.proto");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let descriptor_path = out_dir.join("descriptor.bin");

    let mut prost_config = prost_build::Config::new();
    prost_config.enable_type_names();
    // Don't generate `sererr.v1.*` types here — angzarr-project's
    // types.proto imports them, but the actual Rust types come from
    // the `sererr-proto` crate to avoid duplication. extern_path tells
    // prost to reference `::sererr_proto::sererr_v1::*` instead.
    prost_config.extern_path(".sererr.v1", "::sererr_proto::sererr_v1");

    tonic_prost_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        .build_server(true)
        .build_client(true)
        .type_attribute(
            ".angzarr_client.proto.angzarr.v1.BusinessResponse.result",
            "#[allow(clippy::large_enum_variant)]",
        )
        .compile_with_config(
            prost_config,
            &[
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/types.proto",
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/command_handler.proto",
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/projector.proto",
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/saga.proto",
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/process_manager.proto",
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/query.proto",
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/stream.proto",
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/upcaster.proto",
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/meta.proto",
                "angzarr-project/proto/angzarr_client/proto/angzarr/v1/cloudevents.proto",
                "proto/io/cloudevents/v1/cloudevents.proto",
                "proto/angzarr/status/dlq_admin.proto",
            ],
            // Include paths: angzarr's own protos, our local protos,
            // AND sererr's proto root so types.proto can resolve
            // `import "sererr/sererr.proto"`.
            &["angzarr-project/proto", "proto", "sererr/proto"],
        )?;

    // H-33: build a *public* descriptor subset for gRPC reflection.
    //
    // The full descriptor at `descriptor.bin` contains every framework
    // proto (command-handler, saga, PM, projector, query, stream,
    // upcaster, types) and exposes internal messages (Confirmation,
    // Revocation, NoOp, AngzarrDeferredSequence, ...) to anyone who
    // calls `grpcurl list`. For the reflection-exposed surface, we
    // ship only `proto/angzarr/status/dlq_admin.proto` and its
    // transitive imports.
    //
    // The in-process pool keeps loading the full set via
    // `EMBEDDED_DESCRIPTOR` so payload-rendering paths (DLQ admin
    // payload_view, future GraphQL gateway) still decode framework
    // messages.
    emit_public_descriptor_subset(
        &descriptor_path,
        &out_dir.join("descriptor_public.bin"),
        &["angzarr/status/dlq_admin.proto"],
    )?;

    Ok(())
}

/// Read the full `FileDescriptorSet`, filter to the files reachable
/// (via `import`) from `public_roots`, and write the trimmed set to
/// `out_path`.
///
/// File-name matching uses the protobuf file's own `name` field
/// (relative path as seen by `protoc`'s include paths), which matches
/// how prost-reflect's `DescriptorPool` indexes files.
fn emit_public_descriptor_subset(
    full_path: &PathBuf,
    out_path: &PathBuf,
    public_roots: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = std::fs::read(full_path)?;
    let full = prost_types::FileDescriptorSet::decode(&*bytes)?;

    let mut by_name: std::collections::HashMap<&str, &prost_types::FileDescriptorProto> =
        std::collections::HashMap::new();
    for f in &full.file {
        if let Some(name) = f.name.as_deref() {
            by_name.insert(name, f);
        }
    }

    // BFS transitive imports starting from each requested root.
    let mut keep: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = Vec::new();
    for root in public_roots {
        if by_name.contains_key(*root) {
            keep.insert((*root).to_string());
            frontier.push((*root).to_string());
        } else {
            // Fail loudly so a misspelled public root is caught at
            // compile time rather than producing a silently-empty
            // reflection surface.
            return Err(format!(
                "public root {root} not present in descriptor.bin (file list: {:?})",
                by_name.keys().collect::<Vec<_>>()
            )
            .into());
        }
    }

    while let Some(name) = frontier.pop() {
        let Some(file) = by_name.get(name.as_str()) else {
            continue;
        };
        for dep in &file.dependency {
            if keep.insert(dep.clone()) {
                frontier.push(dep.clone());
            }
        }
    }

    let public_set = prost_types::FileDescriptorSet {
        file: full
            .file
            .iter()
            .filter(|f| f.name.as_deref().is_some_and(|n| keep.contains(n)))
            .cloned()
            .collect(),
    };

    let mut out_bytes = Vec::with_capacity(bytes.len() / 4);
    public_set.encode(&mut out_bytes)?;
    std::fs::write(out_path, out_bytes)?;
    Ok(())
}
