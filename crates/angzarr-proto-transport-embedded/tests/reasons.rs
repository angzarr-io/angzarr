//! The local-process transport's reason keys (decisions 14.64 and 14.104):
//! `EmbeddedReason`, named by layer, their args and surfaces from the
//! spec's `(reason)` options, their English the spec's catalog beside the
//! file.

use std::collections::BTreeSet;
use std::path::PathBuf;

use angzarr_proto_transport_embedded::v1::{reason_args, EmbeddedReason};
use angzarr_proto_transport_embedded::{reason_key_args, FILE_DESCRIPTOR_SET, REASONS_EN_US};
use prost::Message;

/// Every live key, with its args.
fn every_key() -> Vec<(&'static str, &'static [&'static str])> {
    (1..=64)
        .filter_map(|n| EmbeddedReason::try_from(n).ok())
        .map(|r| (r.as_str_name(), r.args()))
        .collect()
}

#[test]
fn the_keys_are_the_childs() {
    let keys: Vec<_> = every_key().into_iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        [
            "EMBEDDED_REASON_CHILD_NOT_SERVING",
            "EMBEDDED_REASON_CHILD_EXIT_STATUS",
            "EMBEDDED_REASON_CHILD_NOT_EXITED",
            "EMBEDDED_REASON_PROBE_INPUT",
        ]
    );
    assert_eq!(
        EmbeddedReason::from_str_name("EMBEDDED_REASON_CHILD_NOT_EXITED"),
        Some(EmbeddedReason::ChildNotExited)
    );
}

#[test]
fn a_keys_args_are_its_options() {
    // Decision 14.80: not SERVING within the start time limit.
    assert_eq!(
        EmbeddedReason::ChildNotServing.args(),
        &[
            reason_args::COMPONENT,
            reason_args::STATUS,
            reason_args::TIMEOUT_MS
        ]
    );
    assert_eq!(
        EmbeddedReason::ChildExitStatus.args(),
        &["component", "exit_code"]
    );
    assert_eq!(
        EmbeddedReason::ChildNotExited.args(),
        &["component", "ended"]
    );
    assert_eq!(
        EmbeddedReason::ProbeInput.args(),
        &["component", reason_args::KIND]
    );
    assert!(EmbeddedReason::Unspecified.args().is_empty());
}

#[test]
fn a_keys_args_are_found_by_its_name() {
    for (key, args) in every_key() {
        assert_eq!(reason_key_args(key), Some(args), "{key}");
    }
    for other in [
        "EMBEDDED_REASON_UNSPECIFIED",
        // The keys' old names, and core's keys, which are angzarr-proto's.
        "HOST_REASON_CHILD_NOT_SERVING",
        "ANGZARR_REASON_HANDLER_UNAVAILABLE",
        "",
    ] {
        assert_eq!(reason_key_args(other), None, "{other}");
    }
}

#[test]
fn the_catalog_is_the_specs_file_with_a_message_per_key() {
    let path = PathBuf::from(env!("ANGZARR_PROTO_ROOT"))
        .join("io/angzarr/transport/embedded/v1/reasons.en-US.ftl");
    let file = std::fs::read_to_string(&path).expect("read the spec's catalog");
    assert_eq!(
        REASONS_EN_US,
        ("angzarr-reasons-transport-embedded", file.as_str())
    );
    let ids: BTreeSet<&str> = REASONS_EN_US
        .1
        .lines()
        .filter(|l| !l.starts_with('#') && !l.starts_with(' '))
        .filter_map(|l| l.split_once(" = ").map(|(id, _)| id))
        .collect();
    let keys: BTreeSet<&str> = every_key().into_iter().map(|(k, _)| k).collect();
    assert_eq!(ids, keys);
}

#[test]
fn the_package_imports_only_core_and_declares_no_service() {
    let set = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
    let ours: Vec<_> = set
        .file
        .iter()
        .filter(|f| f.package() == "io.angzarr.transport.embedded.v1")
        .collect();
    assert_eq!(ours.len(), 2, "child_protocol.proto and reasons.proto");
    for file in ours {
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
}

// Decision 14.104: a key's surface is its option's, a google.rpc.Code.
#[test]
fn a_keys_surface_is_its_options() {
    use angzarr_proto::google::rpc::Code;
    use angzarr_proto::ReasonSurface;
    let status = |code| Some(ReasonSurface::GrpcStatus(code));
    assert_eq!(
        EmbeddedReason::ChildNotServing.surface(),
        status(Code::Internal)
    );
    assert_eq!(
        EmbeddedReason::ChildNotExited.surface(),
        status(Code::Internal)
    );
    assert_eq!(
        EmbeddedReason::ProbeInput.surface(),
        status(Code::InvalidArgument)
    );
    assert_eq!(EmbeddedReason::Unspecified.surface(), None);
}
