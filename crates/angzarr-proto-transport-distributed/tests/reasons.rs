//! The distributed transport's reason keys (decision 14.64): the keys only
//! a distributed coordinator answers, moved here from core's
//! CoordinatorReason with their strings unchanged.

use std::collections::BTreeSet;
use std::path::PathBuf;

use angzarr_proto_transport_distributed::v1::{reason_args, CoordinatorReason};
use angzarr_proto_transport_distributed::{reason_key_args, REASONS_EN_US};

fn every_key() -> Vec<(&'static str, &'static [&'static str])> {
    (1..=64)
        .filter_map(|n| CoordinatorReason::try_from(n).ok())
        .map(|r| (r.as_str_name(), r.args()))
        .collect()
}

#[test]
fn the_keys_are_the_distribution_only_ones_with_their_strings() {
    let keys: Vec<_> = every_key().into_iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        [
            "COORDINATOR_REASON_COMPONENT_NOT_REGISTERED",
            "COORDINATOR_REASON_COMMANDS_NOT_SERVED",
            "COORDINATOR_REASON_PAYLOAD_UNAVAILABLE",
        ]
    );
}

#[test]
fn a_keys_args_are_its_options_and_found_by_its_name() {
    assert_eq!(
        CoordinatorReason::ComponentNotRegistered.args(),
        &[reason_args::DOMAIN]
    );
    assert_eq!(CoordinatorReason::CommandsNotServed.args(), &["domain"]);
    assert_eq!(
        CoordinatorReason::PayloadUnavailable.args(),
        &[reason_args::URI, reason_args::CAUSE]
    );
    for (key, args) in every_key() {
        assert_eq!(reason_key_args(key), Some(args), "{key}");
    }
    for other in [
        "COORDINATOR_REASON_UNSPECIFIED",
        "COORDINATOR_REASON_HANDLER_UNAVAILABLE",
        "HOST_REASON_HANDSHAKE_MALFORMED",
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
    let keys: BTreeSet<&str> = every_key().into_iter().map(|(k, _)| k).collect();
    assert_eq!(ids, keys);
}

// Decision 14.65: a key's status is its option's, in core's ReasonStatus.
#[test]
fn a_keys_status_is_its_options() {
    use angzarr_proto::v1::ReasonStatus;
    assert_eq!(
        CoordinatorReason::ComponentNotRegistered.status(),
        ReasonStatus::Unavailable
    );
    assert_eq!(
        CoordinatorReason::CommandsNotServed.status(),
        ReasonStatus::Unimplemented
    );
    assert_eq!(
        CoordinatorReason::PayloadUnavailable.status(),
        ReasonStatus::Unavailable
    );
}
