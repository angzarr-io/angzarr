//! The distributed transport's reason keys (decisions 14.64, 14.104 and
//! 14.106): `DistributedReason`, named by layer, the keys only a
//! distributed coordinator answers.

use std::collections::BTreeSet;
use std::path::PathBuf;

use angzarr_proto::google::rpc::Code;
use angzarr_proto::ReasonSurface;
use angzarr_proto_transport_distributed::v1::{reason_args, DistributedReason};
use angzarr_proto_transport_distributed::{reason_key_args, REASONS_EN_US};

fn every_key() -> Vec<DistributedReason> {
    (1..)
        .map_while(|n| DistributedReason::try_from(n).ok())
        .collect()
}

#[test]
fn the_keys_are_the_distribution_only_ones() {
    let keys: Vec<_> = every_key().into_iter().map(|r| r.as_str_name()).collect();
    assert_eq!(
        keys,
        [
            "DISTRIBUTED_REASON_COMPONENT_NOT_REGISTERED",
            "DISTRIBUTED_REASON_COMMANDS_NOT_SERVED",
            "DISTRIBUTED_REASON_PAYLOAD_UNAVAILABLE",
            // Decision 14.106: the DLQ admin's failures.
            "DISTRIBUTED_REASON_DEAD_LETTER_STORE_UNAVAILABLE",
            "DISTRIBUTED_REASON_DEAD_LETTER_NOT_FOUND",
            "DISTRIBUTED_REASON_DEAD_LETTER_QUERY_INVALID",
        ]
    );
}

#[test]
fn a_keys_args_are_its_options_and_found_by_its_name() {
    assert_eq!(
        DistributedReason::ComponentNotRegistered.args(),
        &[reason_args::DOMAIN]
    );
    assert_eq!(
        DistributedReason::PayloadUnavailable.args(),
        &[reason_args::URI, reason_args::CAUSE]
    );
    assert_eq!(DistributedReason::DeadLetterNotFound.args(), &["id"]);
    assert_eq!(DistributedReason::DeadLetterQueryInvalid.args(), &["field"]);
    for key in every_key() {
        assert_eq!(
            reason_key_args(key.as_str_name()),
            Some(key.args()),
            "{key:?}"
        );
    }
    for other in [
        "DISTRIBUTED_REASON_UNSPECIFIED",
        "COORDINATOR_REASON_PAYLOAD_UNAVAILABLE",
        "ANGZARR_REASON_HANDLER_UNAVAILABLE",
    ] {
        assert_eq!(reason_key_args(other), None, "{other}");
    }
}

#[test]
fn the_catalog_is_the_specs_file_with_a_message_per_key() {
    let path = PathBuf::from(env!("ANGZARR_PROTO_ROOT"))
        .join("io/angzarr/transport/distributed/v1/reasons.en-US.ftl");
    let file = std::fs::read_to_string(&path).expect("read the spec's catalog");
    assert_eq!(
        REASONS_EN_US,
        ("angzarr-reasons-transport-distributed", file.as_str())
    );
    let ids: BTreeSet<&str> = REASONS_EN_US
        .1
        .lines()
        .filter(|l| !l.starts_with('#') && !l.starts_with(' '))
        .filter_map(|l| l.split_once(" = ").map(|(id, _)| id))
        .collect();
    let keys: BTreeSet<&str> = every_key().into_iter().map(|r| r.as_str_name()).collect();
    assert_eq!(ids, keys);
}

// Decision 14.104: a key's surface is its option's, a google.rpc.Code.
#[test]
fn a_keys_surface_is_its_options() {
    let status = |code| Some(ReasonSurface::GrpcStatus(code));
    for (key, expected) in [
        (
            DistributedReason::ComponentNotRegistered,
            status(Code::Unavailable),
        ),
        (
            DistributedReason::CommandsNotServed,
            status(Code::Unimplemented),
        ),
        (
            DistributedReason::PayloadUnavailable,
            status(Code::Unavailable),
        ),
        (
            DistributedReason::DeadLetterStoreUnavailable,
            status(Code::Unavailable),
        ),
        (
            DistributedReason::DeadLetterNotFound,
            status(Code::NotFound),
        ),
        (
            DistributedReason::DeadLetterQueryInvalid,
            status(Code::InvalidArgument),
        ),
        (DistributedReason::Unspecified, None),
    ] {
        assert_eq!(key.surface(), expected, "{key:?}");
    }
}
