//! The io.angzarr.v1 wire types come from angzarr-project's angzarr-proto
//! crate (types.proto, upcaster.proto, options.proto and sererr). That crate
//! compiles no services, so this script generates only the gRPC stubs core
//! serves or calls, and the few messages their files define
//! (CommandHandlerResponse, CommandResponse, DescribeRequest). Every type
//! angzarr-proto provides is extern-pathed to it, so no io.angzarr.v1 type
//! exists twice
//! (backlog TG-1). Same approach as angzarr-router's crates/router/build.rs.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use heck::ToUpperCamelCase;

/// The proto root of the angzarr-project submodule.
const PROTO_ROOT: &str = "angzarr-project/proto";

/// The protos angzarr-proto compiles: their types are its, never ours.
const ANGZARR_PROTO_FILES: [&str; 3] = [
    "io/angzarr/v1/types.proto",
    "io/angzarr/v1/upcaster.proto",
    "io/angzarr/v1/options.proto",
];

/// The service protos whose stubs (and service-file messages) core needs.
const SERVICE_FILES: [&str; 5] = [
    "io/angzarr/v1/command_handler.proto",
    "io/angzarr/v1/component.proto",
    "io/angzarr/v1/projector.proto",
    "io/angzarr/v1/query.proto",
    "io/angzarr/v1/upcaster.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(PROTO_ROOT);
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-env-changed=PROTOC");
    for file in ANGZARR_PROTO_FILES.iter().chain(SERVICE_FILES.iter()) {
        println!("cargo:rerun-if-changed={}", root.join(file).display());
    }

    let services: Vec<PathBuf> = SERVICE_FILES.iter().map(|f| root.join(f)).collect();
    let mut config = prost_build::Config::new();
    let fds = config.load_fds(&services, &[&root])?;

    config.enable_type_names();
    let mut builder = tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .extern_path(".sererr.v1", "::angzarr_proto::sererr::v1");
    for name in angzarr_proto_types(&fds) {
        builder = builder.extern_path(
            format!(".io.angzarr.v1.{name}"),
            format!("::angzarr_proto::v1::{}", name.to_upper_camel_case()),
        );
    }
    builder.compile_fds_with_config(fds, config)?;
    Ok(())
}

/// The top-level message and enum names defined in angzarr-proto's files.
fn angzarr_proto_types(fds: &prost_types::FileDescriptorSet) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for file in &fds.file {
        let is_angzarr_proto = ANGZARR_PROTO_FILES
            .iter()
            .any(|f| Path::new(file.name()) == Path::new(f));
        if is_angzarr_proto {
            names.extend(file.message_type.iter().map(|m| m.name().to_string()));
            names.extend(file.enum_type.iter().map(|e| e.name().to_string()));
        }
    }
    names
}
