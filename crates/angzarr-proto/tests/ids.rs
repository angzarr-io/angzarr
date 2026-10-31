//! The wire `UUID` boundary helpers and the UUIDv5 derivations
//! (types.proto, `uuid` and `derived_ids` regions). Expected derived values
//! were computed independently with Python's `uuid.uuid5`, and match the
//! examples documented in the proto.

use angzarr_proto::ids::{
    compute_root, fact_command_id, fact_correlation, policy_command_id, UuidError,
    COMMAND_NAMESPACE, FACT_CORRELATION_NAMESPACE, ROOT_NAMESPACE,
};
use angzarr_proto::uuid::Uuid as RustUuid;
use angzarr_proto::v1::{Cover, EventRef, Uuid};

const TEXT: &str = "01920c8e-7b3a-7cc2-9f4e-5a1b2c3d4e5f";
const BYTES: [u8; 16] = [
    0x01, 0x92, 0x0c, 0x8e, 0x7b, 0x3a, 0x7c, 0xc2, 0x9f, 0x4e, 0x5a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f,
];

#[test]
fn the_namespaces_are_the_documented_values() {
    assert_eq!(
        ROOT_NAMESPACE.to_string(),
        "d4741b1a-52cd-43cf-add1-3dbb9615510b"
    );
    assert_eq!(
        FACT_CORRELATION_NAMESPACE.to_string(),
        "e9ef0ae2-6fc6-4c14-b713-ef03971dd867"
    );
    assert_eq!(
        COMMAND_NAMESPACE.to_string(),
        "e2c9f744-9fe1-4b4b-bfee-ae514dde293b"
    );
    assert_ne!(ROOT_NAMESPACE, FACT_CORRELATION_NAMESPACE);
    assert_ne!(ROOT_NAMESPACE, COMMAND_NAMESPACE);
    assert_ne!(FACT_CORRELATION_NAMESPACE, COMMAND_NAMESPACE);
}

#[test]
fn text_round_trips_through_canonical_lowercase() {
    let id: Uuid = TEXT.parse().unwrap();
    assert_eq!(id.value[..], BYTES);
    assert_eq!(id.to_string(), TEXT);

    // Input accepts either case; output is always lowercase.
    let upper: Uuid = TEXT.to_uppercase().parse().unwrap();
    assert_eq!(upper, id);
    assert_eq!(upper.to_string(), TEXT);
}

#[test]
fn only_the_hyphenated_form_parses() {
    for bad in [
        "",
        "01920c8e7b3a7cc29f4e5a1b2c3d4e5f",              // simple
        "{01920c8e-7b3a-7cc2-9f4e-5a1b2c3d4e5f}",        // braced
        "urn:uuid:01920c8e-7b3a-7cc2-9f4e-5a1b2c3d4e5f", // URN
        "01920c8e-7b3a-7cc2-9f4e-5a1b2c3d4e5g",          // not hex
        "01920c8e7-b3a-7cc2-9f4e-5a1b2c3d4e5f",          // misplaced hyphen
        "invalid-uuid:00",                               // the malformed display
    ] {
        assert_eq!(bad.parse::<Uuid>(), Err(UuidError::Text), "{bad:?}");
    }
}

#[test]
fn bytes_and_uuid_crate_conversions_are_lossless() {
    let id = Uuid::from(BYTES);
    assert_eq!(id, Uuid::from_bytes(BYTES));
    assert_eq!(<[u8; 16]>::try_from(&id), Ok(BYTES));
    assert!(id.is_well_formed());

    let rust = RustUuid::try_from(&id).unwrap();
    assert_eq!(rust.as_bytes(), &BYTES);
    assert_eq!(Uuid::from(rust), id);
}

#[test]
fn a_malformed_wire_value_is_reported_and_never_canonical() {
    for len in [0usize, 15, 17] {
        let id = Uuid {
            value: vec![0xab; len].into(),
        };
        assert!(!id.is_well_formed());
        assert_eq!(id.to_bytes(), Err(UuidError::Length(len)));
        assert_eq!(RustUuid::try_from(&id), Err(UuidError::Length(len)));
        let shown = id.to_string();
        assert_eq!(shown, format!("invalid-uuid:{}", "ab".repeat(len)));
        assert!(shown.parse::<Uuid>().is_err());
    }
}

#[test]
fn compute_root_is_uuidv5_of_domain_colon_key() {
    let root = compute_root("player", "alice@example.com").unwrap();
    assert_eq!(root.to_string(), "cfe936c0-2912-5ca9-96e6-f37c0b441759");
    // Version 5, RFC 9562 variant.
    assert_eq!(root.value[6] >> 4, 5);
    assert_eq!(root.value[8] >> 6, 0b10);

    // The key may hold ':'; the domain may not.
    assert_eq!(
        compute_root("orders", "a:b").unwrap().to_string(),
        "83100d05-781b-57aa-810f-d03c4de94c2a"
    );
    assert_eq!(
        compute_root("orders:a", "b"),
        Err(UuidError::DomainContainsColon)
    );

    // Deterministic, and the domain separates keys.
    assert_eq!(compute_root("player", "x"), compute_root("player", "x"));
    assert_ne!(compute_root("player", "x"), compute_root("table", "x"));
}

#[test]
fn fact_correlation_is_uuidv5_of_external_id_in_its_own_namespace() {
    let corr = fact_correlation("pi_1234");
    assert_eq!(corr.to_string(), "917f26ae-ea2e-58af-86de-77013a69617e");
    assert_eq!(corr.value[6] >> 4, 5);

    // The same name never yields a root: the namespaces differ.
    let name_also_a_root = fact_correlation("player:alice@example.com");
    assert_eq!(
        name_also_a_root.to_string(),
        "2ec2813e-ec7f-5921-8931-a86b44493ffc"
    );
    assert_ne!(
        Some(name_also_a_root),
        compute_root("player", "alice@example.com").ok()
    );
}

#[test]
fn a_fact_command_id_is_its_default_correlation() {
    let id = fact_command_id("pi_1234");
    assert_eq!(id.to_string(), "917f26ae-ea2e-58af-86de-77013a69617e");
    assert_eq!(id, fact_correlation("pi_1234"));
    assert_ne!(id, fact_command_id("pi_1235"));
}

/// The worked example's trigger: list stream on main, sequence 7.
fn trigger(edition: Option<&str>) -> EventRef {
    EventRef {
        stream: Some(Cover {
            domain: "list".into(),
            root: Some(TEXT.parse().unwrap()),
            edition: edition.map(Into::into),
        }),
        sequence: 7,
    }
}

#[test]
fn policy_command_id_matches_the_documented_worked_examples() {
    // Expected values computed independently with Python's uuid.uuid5 over
    // the netstring names (types.proto, derived_ids region).
    let id = policy_command_id(&trigger(None), "reminders", 0).unwrap();
    assert_eq!(id.to_string(), "45764676-e6e4-56fe-8c80-d782423577ee");
    assert_eq!(id.value[6] >> 4, 5);
    assert_eq!(id.value[8] >> 6, 0b10);

    // It is UUIDv5 of exactly the documented 70-byte name.
    let name = "4:list,0:,36:01920c8e-7b3a-7cc2-9f4e-5a1b2c3d4e5f,1:7,9:reminders,1:0,";
    assert_eq!(name.len(), 70);
    assert_eq!(
        id,
        Uuid::from(RustUuid::new_v5(&COMMAND_NAMESPACE, name.as_bytes()))
    );

    for (edition, component, index, expected) in [
        (None, "reminders", 1, "e45bdfdf-d722-56c8-a4bd-35340c9c3c06"),
        (
            Some("what-if"),
            "reminders",
            0,
            "e2321e9e-d55a-5f48-bbe1-b09081c2e445",
        ),
        // Netstrings: ':' and ',' in a component need no escaping.
        (None, "a,b:c", 0, "27ccb6bf-e37e-54bd-b0f0-9eac56ff5f93"),
    ] {
        assert_eq!(
            policy_command_id(&trigger(edition), component, index)
                .unwrap()
                .to_string(),
            expected,
            "{edition:?} {component} {index}"
        );
    }

    // Widest values: u64::MAX sequence, u32::MAX index.
    let mut wide = trigger(None);
    wide.sequence = u64::MAX;
    assert_eq!(
        policy_command_id(&wide, "reminders", u32::MAX)
            .unwrap()
            .to_string(),
        "2803d9f5-d632-58a2-9c28-539bff683295"
    );
}

#[test]
fn policy_command_id_separates_every_input() {
    let base = policy_command_id(&trigger(None), "reminders", 0).unwrap();
    let mut later = trigger(None);
    later.sequence = 8;
    let mut other_domain = trigger(None);
    other_domain.stream.as_mut().unwrap().domain = "lists".into();
    for other in [
        policy_command_id(&later, "reminders", 0),
        policy_command_id(&other_domain, "reminders", 0),
        policy_command_id(&trigger(Some("x")), "reminders", 0),
        policy_command_id(&trigger(None), "reminder", 0),
        policy_command_id(&trigger(None), "reminders", 1),
    ] {
        assert_ne!(other.unwrap(), base);
    }
    // Deterministic, and never the fact id or root of the same text.
    assert_eq!(
        policy_command_id(&trigger(None), "reminders", 0).unwrap(),
        base
    );
    assert_ne!(base, fact_command_id("reminders"));
}

#[test]
fn policy_command_id_refuses_malformed_triggers() {
    assert_eq!(
        policy_command_id(&EventRef::default(), "reminders", 0),
        Err(UuidError::MissingTrigger)
    );
    let mut rootless = trigger(None);
    rootless.stream.as_mut().unwrap().root = None;
    assert_eq!(
        policy_command_id(&rootless, "reminders", 0),
        Err(UuidError::MissingTrigger)
    );
    let mut short = trigger(None);
    short.stream.as_mut().unwrap().root = Some(Uuid {
        value: vec![1; 15].into(),
    });
    assert_eq!(
        policy_command_id(&short, "reminders", 0),
        Err(UuidError::Length(15))
    );
    assert_eq!(
        policy_command_id(&trigger(Some("")), "reminders", 0),
        Err(UuidError::EmptyEdition)
    );
    assert_eq!(
        policy_command_id(&trigger(None), "", 0),
        Err(UuidError::EmptyComponent)
    );
}
