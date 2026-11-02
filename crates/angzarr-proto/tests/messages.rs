//! Every `io.angzarr.v1` message is angzarr-proto's: the messages the router
//! and the kernel use in process too, which angzarr-router used to generate
//! itself. Core is messages only (decision 14.64): no service, and no import
//! of another angzarr package, sererr or `google/api`.

use std::path::{Path, PathBuf};

use angzarr_proto::protos::FILES;
use angzarr_proto::v1;
use prost::{Message, Name};

/// Every `.proto` under `dir`, as an import path relative to `root`.
fn protos_under(root: &Path, dir: &Path, found: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            protos_under(root, &path, found);
        } else if path.extension().is_some_and(|e| e == "proto") {
            let relative = path.strip_prefix(root).unwrap();
            found.push(relative.to_str().unwrap().replace('\\', "/"));
        }
    }
}

#[test]
fn every_io_angzarr_v1_proto_is_generated_and_embedded() {
    let root = PathBuf::from(env!("ANGZARR_PROTO_ROOT"));
    let mut spec = Vec::new();
    protos_under(&root, &root.join("io/angzarr/v1"), &mut spec);
    assert!(
        spec.len() >= 9 && spec.contains(&"io/angzarr/v1/types.proto".to_string()),
        "found only {spec:?}"
    );
    let embedded: Vec<_> = FILES.iter().map(|f| f.path).collect();
    for path in &spec {
        assert!(embedded.contains(&path.as_str()), "{path} not generated");
    }
}

#[test]
fn the_in_process_messages_are_angzarr_protos() {
    // Named here, so a message the spec drops or renames fails the build.
    assert_eq!(
        v1::CommandHandlerResponse::type_url(),
        "/io.angzarr.v1.CommandHandlerResponse"
    );
    assert_eq!(
        v1::PolicyResponse::type_url(),
        "/io.angzarr.v1.PolicyResponse"
    );
    assert_eq!(v1::Refusal::type_url(), "/io.angzarr.v1.Refusal");
    assert_eq!(
        v1::OnRefusedResponse::type_url(),
        "/io.angzarr.v1.OnRefusedResponse"
    );
    assert_eq!(v1::Projection::type_url(), "/io.angzarr.v1.Projection");
    assert_eq!(
        v1::UpcastResponse::type_url(),
        "/io.angzarr.v1.UpcastResponse"
    );
}

#[test]
fn the_descriptor_set_holds_every_generated_file() {
    let set = prost_types::FileDescriptorSet::decode(angzarr_proto::FILE_DESCRIPTOR_SET).unwrap();
    let names: Vec<_> = set.file.iter().map(|f| f.name()).collect();
    for file in FILES.iter().filter(|f| f.path.starts_with("io/")) {
        assert!(
            names.contains(&file.path),
            "{} not in the descriptor set",
            file.path
        );
    }
    // With its imports, for a reflection server to resolve them.
    assert!(names.contains(&"google/protobuf/any.proto"));
}

#[test]
fn core_is_messages_only_and_imports_no_other_package() {
    let set = prost_types::FileDescriptorSet::decode(angzarr_proto::FILE_DESCRIPTOR_SET).unwrap();
    let mut core = 0;
    for file in &set.file {
        assert!(
            !file.name().starts_with("sererr/") && !file.name().starts_with("google/api/"),
            "{} is in core's descriptor set",
            file.name()
        );
        if !file.name().starts_with("io/") {
            continue;
        }
        core += 1;
        assert!(
            file.package() == "io.angzarr.v1" || file.package() == "io.angzarr.v1.meta",
            "{} is in package {}",
            file.name(),
            file.package()
        );
        assert!(
            file.service.is_empty(),
            "{} declares a service",
            file.name()
        );
        for import in &file.dependency {
            assert!(
                import.starts_with("io/angzarr/v1/") || import.starts_with("google/protobuf/"),
                "{} imports {import}",
                file.name()
            );
        }
    }
    assert!(core > 5, "only {core} core files");
}
