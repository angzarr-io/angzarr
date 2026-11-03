//! Core's reason keys are one enum, `AngzarrReason` (decision 14.104):
//! keys are named by layer, never by role, so the router's, the component
//! host's and the coordinator's keys share the `ANGZARR_REASON_` prefix
//! (the transports' are `EmbeddedReason` and `DistributedReason`, beside
//! their packages). A key travels as its value's full name. The rewrite
//! holds no reservations (decision 14.100): the live keys are numbered
//! densely from 1, and a removed key is simply absent.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

use angzarr_proto::v1::AngzarrReason;
use prost::Message;
use prost_types::FileDescriptorSet;

/// Every live key, in number order: the router's first, then the
/// component host's, then the coordinator's (reasons.proto).
const LIVE: &[&str] = &[
    "ANGZARR_REASON_COMMAND_REFUSED",
    "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR",
    "ANGZARR_REASON_HANDLER_PANICKED",
    "ANGZARR_REASON_FOLD_FAILED",
    "ANGZARR_REASON_FOLD_FAILED_EMITTED",
    "ANGZARR_REASON_PERSISTED_EVENT_CORRUPT",
    "ANGZARR_REASON_ANY_DECODE_FAILED",
    "ANGZARR_REASON_UNKNOWN_COMMAND_TYPE",
    "ANGZARR_REASON_NO_FACT_HANDLER",
    "ANGZARR_REASON_NO_REFUSAL_HANDLER",
    "ANGZARR_REASON_NO_COMPONENT_OF_KIND",
    "ANGZARR_REASON_NO_HANDLERS_REGISTERED",
    "ANGZARR_REASON_DUPLICATE_COMPONENT",
    "ANGZARR_REASON_UPCASTER_WITH_COMPONENT",
    "ANGZARR_REASON_NO_COMPONENTS_REGISTERED",
    "ANGZARR_REASON_UPCASTER_UNKNOWN_DOMAIN",
    "ANGZARR_REASON_UPCASTER_OTHER_DOMAIN",
    "ANGZARR_REASON_REMOTE_SOURCE",
    "ANGZARR_REASON_PM_NO_INSTANCE",
    "ANGZARR_REASON_POLICY_COMMAND_NO_ROOT",
    "ANGZARR_REASON_MISSING_RULES_VERSION",
    "ANGZARR_REASON_MISSING_STATE_TYPE",
    "ANGZARR_REASON_POLICY_NO_OUTPUT_DOMAIN",
    "ANGZARR_REASON_RETRY_CAP_EXHAUSTED",
    "ANGZARR_REASON_EDITION_REMOVAL_FAILED",
    "ANGZARR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN",
    "ANGZARR_REASON_HANDLER_UNAVAILABLE",
    "ANGZARR_REASON_CHAIN_TOO_DEEP",
    "ANGZARR_REASON_STORE_UNAVAILABLE",
    "ANGZARR_REASON_UPCASTER_UNAVAILABLE",
    "ANGZARR_REASON_RULES_VERSION_MISMATCH",
    "ANGZARR_REASON_EDITION_NOT_FORKED",
    "ANGZARR_REASON_POLICY_TARGETS_ITS_TRIGGER",
    "ANGZARR_REASON_NO_INPUT_DOMAIN",
    "ANGZARR_REASON_DESCRIPTOR_NO_KIND",
    "ANGZARR_REASON_FACT_DEAD_LETTERED",
    "ANGZARR_REASON_DESCRIPTOR_NO_DOMAIN",
    "ANGZARR_REASON_STATE_TYPE_MISMATCH",
    "ANGZARR_REASON_SYNC_PROJECTION_FAILED",
    "ANGZARR_REASON_SYNC_NO_PROJECTORS",
    "ANGZARR_REASON_SYNC_PROJECTOR_NOT_SUBSCRIBED",
];

fn live() -> Vec<&'static str> {
    (1..)
        .map_while(|n| AngzarrReason::try_from(n).ok())
        .map(|r| r.as_str_name())
        .collect()
}

#[test]
fn a_key_travels_as_its_full_name() {
    assert_eq!(
        AngzarrReason::DuplicateComponent.as_str_name(),
        "ANGZARR_REASON_DUPLICATE_COMPONENT"
    );
    assert_eq!(
        AngzarrReason::from_str_name("ANGZARR_REASON_UNKNOWN_COMMAND_TYPE"),
        Some(AngzarrReason::UnknownCommandType)
    );
    // The keys of the old role-named enums are gone (decision 14.104).
    assert_eq!(
        AngzarrReason::from_str_name("ROUTER_REASON_COMMAND_REFUSED"),
        None
    );
}

#[test]
fn the_live_keys_are_these_numbered_densely_from_one() {
    assert_eq!(live(), LIVE);
    // Dense: nothing past the last.
    let next = i32::try_from(LIVE.len()).unwrap() + 1;
    assert!(AngzarrReason::try_from(next).is_err());
    for key in LIVE {
        let rest = key.strip_prefix("ANGZARR_REASON_");
        assert!(rest.is_some_and(|r| !r.is_empty()), "{key}");
        assert!(
            key.chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'),
            "{key} is not SCREAMING_SNAKE"
        );
    }
    let distinct: BTreeSet<_> = LIVE.iter().collect();
    assert_eq!(distinct.len(), LIVE.len());
}

/// reasons.proto compiled by protoc into a FileDescriptorSet.
fn descriptor() -> FileDescriptorSet {
    // The proto root build.rs generated the library from (exported by it).
    let root = Path::new(env!("ANGZARR_PROTO_ROOT"));
    let out = Path::new(env!("CARGO_TARGET_TMPDIR")).join("reasons.binpb");
    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".into());
    let status = Command::new(protoc)
        .arg(format!("-I{}", root.display()))
        .arg(format!("--descriptor_set_out={}", out.display()))
        .arg("io/angzarr/v1/reasons.proto")
        .status()
        .expect("run protoc");
    assert!(status.success(), "protoc failed on reasons.proto");
    FileDescriptorSet::decode(&*std::fs::read(out).expect("read descriptor set"))
        .expect("a descriptor set")
}

#[test]
fn reasons_proto_declares_one_key_enum_and_reserves_nothing() {
    let set = descriptor();
    let file = &set.file[0];
    let enums: Vec<&str> = file.enum_type.iter().map(|e| e.name()).collect();
    // No status enum of its own: a key's status is google.rpc.Code
    // (decision 14.104).
    assert_eq!(enums, ["AngzarrReason"]);
    let e = &file.enum_type[0];
    assert!(e.reserved_name.is_empty() && e.reserved_range.is_empty());
}
