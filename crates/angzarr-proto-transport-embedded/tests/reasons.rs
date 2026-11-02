//! The local-process transport's reason keys (decision 14.64): HostReason's
//! child keys, moved here from core with their strings unchanged, their
//! args from the spec's `(reason)` options, their English the spec's
//! catalog beside the file.

use std::collections::BTreeSet;
use std::path::PathBuf;

use angzarr_proto_transport_embedded::v1::{reason_args, HostReason};
use angzarr_proto_transport_embedded::{reason_key_args, FILE_DESCRIPTOR_SET, REASONS_EN_US};
use prost::Message;

/// Every live key, with its args.
fn every_key() -> Vec<(&'static str, &'static [&'static str])> {
    (1..=64)
        .filter_map(|n| HostReason::try_from(n).ok())
        .map(|r| (r.as_str_name(), r.args()))
        .collect()
}

#[test]
fn the_keys_are_the_childs_and_keep_their_strings() {
    let keys: Vec<_> = every_key().into_iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        [
            "HOST_REASON_HANDSHAKE_MALFORMED",
            "HOST_REASON_HANDSHAKE_TIMEOUT",
            "HOST_REASON_HANDSHAKE_CLOSED",
            "HOST_REASON_CHILD_NOT_SERVING",
            "HOST_REASON_CHILD_EXIT_STATUS",
            "HOST_REASON_CHILD_NOT_EXITED",
            "HOST_REASON_PROBE_INPUT",
        ]
    );
    assert_eq!(
        HostReason::from_str_name("HOST_REASON_HANDSHAKE_TIMEOUT"),
        Some(HostReason::HandshakeTimeout)
    );
}

#[test]
fn a_keys_args_are_its_options() {
    assert_eq!(HostReason::HandshakeMalformed.args(), &[reason_args::LINE]);
    assert_eq!(HostReason::HandshakeTimeout.args(), &["timeout_ms"]);
    assert!(HostReason::HandshakeClosed.args().is_empty());
    assert_eq!(
        HostReason::ChildNotServing.args(),
        &[reason_args::COMPONENT, reason_args::STATUS]
    );
    assert_eq!(
        HostReason::ChildExitStatus.args(),
        &["component", "exit_code"]
    );
    assert_eq!(HostReason::ChildNotExited.args(), &["component", "ended"]);
    assert_eq!(
        HostReason::ProbeInput.args(),
        &["component", reason_args::KIND]
    );
    assert!(HostReason::Unspecified.args().is_empty());
}

#[test]
fn a_keys_args_are_found_by_its_name() {
    for (key, args) in every_key() {
        assert_eq!(reason_key_args(key), Some(args), "{key}");
    }
    for other in [
        "HOST_REASON_UNSPECIFIED",
        // Core's keys are angzarr-proto's.
        "HOST_REASON_PROJECTOR_NO_INPUT_DOMAIN",
        "COORDINATOR_REASON_HANDLER_UNAVAILABLE",
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

// Decision 14.65: a key's status is its option's, in core's ReasonStatus.
#[test]
fn a_keys_status_is_its_options() {
    use angzarr_proto::v1::ReasonStatus;
    assert_eq!(
        HostReason::HandshakeMalformed.status(),
        ReasonStatus::Internal
    );
    assert_eq!(HostReason::ChildNotExited.status(), ReasonStatus::Internal);
    assert_eq!(
        HostReason::ProbeInput.status(),
        ReasonStatus::InvalidArgument
    );
    assert_eq!(HostReason::Unspecified.status(), ReasonStatus::Unspecified);
}
