//! The framework's reason keys are the enums of `reasons.proto` (decision
//! 14.44): a key travels as its value's full name, its enum's prefix
//! included, and every enum reserves every retired key under its prefix,
//! so none is answered or reused again.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

use angzarr_proto::v1::{CoordinatorReason, HostReason, RouterReason};
use prost::Message;
use prost_types::FileDescriptorSet;

/// The keys decisions 14.37, 14.39 and 14.42 retired, as they were
/// answered: bare, before the prefix.
const RETIRED: &[&str] = &[
    // 14.37
    "MALFORMED_REFUSAL_FACT",
    "DUPLICATE_REGISTRATION",
    "NO_HANDLER_REGISTERED",
    // 14.39
    "DUPLICATE_UPCASTER_CHAIN",
    "DUPLICATE_COMMAND_HANDLER",
    // 14.42
    "MISSING_COMMAND_BOOK",
    "MISSING_COMMAND_PAGE",
    "MISSING_COMMAND_PAYLOAD",
    "MISSING_EVENT_BOOK_COVER",
    "EMPTY_POLICY_SOURCE",
    "POLICY_SOURCE_OUTSIDE_INPUT_DOMAIN",
    "REFUSAL_FACT_MISSING",
    "REFUSAL_WITHOUT_COMMAND",
    "REFUSAL_WITHOUT_EVENT",
    "REFUSAL_WITHOUT_REFUSED_COMMAND",
    "REFUSED_COMMAND_WITHOUT_ID",
    "REFUSAL_WITHOUT_TRIGGER_STREAM",
    "HOST_STATUS_UNDECODABLE",
    "NULL_ABI_POINTER",
];

/// Every live key of an enum, by name: its values from 1 up, until a
/// number has none (the numbers are dense).
fn names<E>(name: fn(E) -> &'static str) -> Vec<&'static str>
where
    E: TryFrom<i32>,
{
    (1..).map_while(|n| E::try_from(n).ok()).map(name).collect()
}

fn router() -> Vec<&'static str> {
    names::<RouterReason>(|r| r.as_str_name())
}
fn host() -> Vec<&'static str> {
    names::<HostReason>(|r| r.as_str_name())
}
fn coordinator() -> Vec<&'static str> {
    names::<CoordinatorReason>(|r| r.as_str_name())
}

#[test]
fn a_key_travels_as_its_full_name() {
    assert_eq!(
        RouterReason::DuplicateComponent.as_str_name(),
        "ROUTER_REASON_DUPLICATE_COMPONENT"
    );
    assert_eq!(
        HostReason::NoComponentsRegistered.as_str_name(),
        "HOST_REASON_NO_COMPONENTS_REGISTERED"
    );
    assert_eq!(
        CoordinatorReason::ConflictOverlap.as_str_name(),
        "COORDINATOR_REASON_CONFLICT_OVERLAP"
    );
    assert_eq!(
        RouterReason::from_str_name("ROUTER_REASON_UNKNOWN_COMMAND_TYPE"),
        Some(RouterReason::UnknownCommandType)
    );
}

#[test]
fn every_key_carries_its_enums_prefix() {
    for (prefix, keys) in [
        ("ROUTER_REASON_", router()),
        ("HOST_REASON_", host()),
        ("COORDINATOR_REASON_", coordinator()),
    ] {
        assert!(!keys.is_empty(), "{prefix}: no keys");
        for key in keys {
            let rest = key.strip_prefix(prefix);
            assert!(rest.is_some_and(|r| !r.is_empty()), "{key}");
            assert!(
                key.chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'),
                "{key} is not SCREAMING_SNAKE"
            );
        }
    }
}

#[test]
fn the_owners_keys_are_these() {
    assert_eq!(
        host(),
        [
            "HOST_REASON_UPCASTER_WITH_COMPONENT",
            "HOST_REASON_NO_COMPONENTS_REGISTERED"
        ]
    );
    assert!(router().contains(&"ROUTER_REASON_DUPLICATE_COMPONENT"));
    assert!(coordinator().contains(&"COORDINATOR_REASON_PM_NO_INSTANCE"));
    // Decision 14.45.
    assert!(coordinator().contains(&"COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE"));
    // Decision 14.46.
    assert!(coordinator().contains(&"COORDINATOR_REASON_RETRY_CAP_EXHAUSTED"));
    // Decision 14.47.
    assert!(coordinator().contains(&"COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD"));
    // Decision 14.48, amended.
    assert!(coordinator().contains(&"COORDINATOR_REASON_POLICY_COMMAND_WITH_HANDLING"));
    assert!(coordinator().contains(&"COORDINATOR_REASON_EDITION_REMOVAL_FAILED"));
    assert!(coordinator().contains(&"COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY"));
    assert!(coordinator().contains(&"COORDINATOR_REASON_POLICY_COMMAND_IS_FACT"));
    assert!(coordinator().contains(&"COORDINATOR_REASON_POLICY_COMMAND_FACT_ORIGIN"));
    let all: Vec<_> = [router(), host(), coordinator()].concat();
    let distinct: BTreeSet<_> = all.iter().collect();
    assert_eq!(all.len(), distinct.len(), "a key in two enums");
}

/// reasons.proto compiled by protoc into a FileDescriptorSet.
fn descriptor() -> FileDescriptorSet {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
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
fn every_enum_reserves_every_retired_key() {
    let set = descriptor();
    let file = &set.file[0];
    let enums: BTreeSet<&str> = file.enum_type.iter().map(|e| e.name()).collect();
    assert_eq!(
        enums,
        ["CoordinatorReason", "HostReason", "RouterReason"].into()
    );
    for e in &file.enum_type {
        let prefix = match e.name() {
            "RouterReason" => "ROUTER_REASON_",
            "HostReason" => "HOST_REASON_",
            _ => "COORDINATOR_REASON_",
        };
        let retired: BTreeSet<String> = RETIRED.iter().map(|k| format!("{prefix}{k}")).collect();
        let reserved: BTreeSet<String> = e.reserved_name.iter().cloned().collect();
        assert_eq!(reserved, retired, "{}", e.name());
        assert!(
            e.reserved_range.is_empty(),
            "{}: numbers are bookkeeping",
            e.name()
        );
        for v in &e.value {
            let bare = v.name().strip_prefix(prefix).unwrap_or(v.name());
            assert!(!RETIRED.contains(&bare), "{} is retired", v.name());
        }
    }
}
