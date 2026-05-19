//! Unit tests for Bigtable storage implementations.
//!
//! These tests focus on pure functions (row key construction, data parsing)
//! that don't require a real Bigtable instance.

use uuid::Uuid;

use super::*;

// ============================================================================
// Row Key Tests
// ============================================================================

mod row_key_tests {
    use super::*;

    #[test]
    fn test_event_row_key_format() {
        let root = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let key = BigtableEventStore::row_key("orders", "main", root, 42);

        // Format: {domain}#{edition}#{root}#{sequence:010}
        assert_eq!(
            key,
            b"orders#main#12345678-1234-1234-1234-123456789abc#0000000042"
        );
    }

    #[test]
    fn test_event_row_key_sequence_padding() {
        let root = Uuid::nil();
        let key = BigtableEventStore::row_key("test", "v1", root, 0);
        assert!(key.ends_with(b"#0000000000"));

        let key = BigtableEventStore::row_key("test", "v1", root, 999999999);
        assert!(key.ends_with(b"#0999999999"));

        let key = BigtableEventStore::row_key("test", "v1", root, u32::MAX);
        assert!(key.ends_with(b"#4294967295"));
    }

    #[test]
    fn test_event_row_key_prefix() {
        let root = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let prefix = BigtableEventStore::row_key_prefix("inventory", "staging", root);

        assert_eq!(
            prefix,
            b"inventory#staging#aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee#"
        );
    }

    #[test]
    fn test_parse_event_row_key_valid() {
        let key = b"orders#main#12345678-1234-1234-1234-123456789abc#0000000042";
        let parsed = BigtableEventStore::parse_row_key(key);

        assert!(parsed.is_some());
        let (domain, edition, root, seq) = parsed.unwrap();
        assert_eq!(domain, "orders");
        assert_eq!(edition, "main");
        assert_eq!(
            root,
            Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap()
        );
        assert_eq!(seq, 42);
    }

    #[test]
    fn test_parse_event_row_key_invalid_format() {
        // Too few parts
        assert!(BigtableEventStore::parse_row_key(b"orders#main#root").is_none());

        // Invalid UUID
        assert!(BigtableEventStore::parse_row_key(b"orders#main#not-a-uuid#0000000001").is_none());

        // Invalid sequence
        assert!(BigtableEventStore::parse_row_key(
            b"orders#main#12345678-1234-1234-1234-123456789abc#notanumber"
        )
        .is_none());
    }

    /// H-26: row keys must round-trip components that contain the `#`
    /// separator. Pre-fix `row_key` interpolated raw `#` and `parse_row_key`
    /// used `splitn(4, '#')` so any `#` inside `domain`, `edition`, or
    /// `root` mis-parsed (the trailing component absorbed the rest of the
    /// string).
    ///
    /// We deliberately exercise three shapes:
    ///
    /// - domain with `#`
    /// - edition with `#`
    /// - both
    ///
    /// All three must round-trip; the sequence still parses as `u32`.
    #[test]
    fn test_event_row_key_round_trip_with_hash_in_components() {
        let root = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();

        for (domain, edition) in [
            ("orders#alpha", "main"),
            ("orders", "v2#preview"),
            ("orders#alpha", "v2#preview"),
            ("orders#", "#main"),
        ] {
            let key = BigtableEventStore::row_key(domain, edition, root, 42);
            let parsed = BigtableEventStore::parse_row_key(&key).unwrap_or_else(|| {
                panic!(
                    "round-trip failed for domain={:?} edition={:?}: parse_row_key returned None",
                    domain, edition
                )
            });
            assert_eq!(parsed.0, domain, "domain must round-trip");
            assert_eq!(parsed.1, edition, "edition must round-trip");
            assert_eq!(parsed.2, root, "root must round-trip");
            assert_eq!(parsed.3, 42, "sequence must round-trip");
        }
    }

    /// H-26 companion: cascade-index row key shape must also round-trip
    /// hash characters in any component.
    #[test]
    fn test_cascade_index_row_key_round_trip_with_hash_in_components() {
        let root = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();

        for (cascade_id, domain, edition) in [
            ("cascade#alpha", "orders", "main"),
            ("cascade", "orders#beta", "main"),
            ("cascade", "orders", "v2#gamma"),
            ("cas#cade", "or#ders", "v2#preview"),
        ] {
            let key =
                BigtableEventStore::cascade_index_row_key(cascade_id, domain, edition, root, 7);
            let parsed = BigtableEventStore::parse_cascade_index_key(&key).unwrap_or_else(|| {
                panic!(
                    "round-trip failed for cascade={:?} domain={:?} edition={:?}",
                    cascade_id, domain, edition
                )
            });
            assert_eq!(parsed.0, cascade_id);
            assert_eq!(parsed.1, domain);
            assert_eq!(parsed.2, edition);
            assert_eq!(parsed.3, root);
            assert_eq!(parsed.4, 7);
        }
    }

    #[test]
    fn test_snapshot_row_key_format() {
        let root = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let key = BigtableSnapshotStore::row_key("orders", "main", root, 100);

        assert_eq!(
            String::from_utf8(key).unwrap(),
            "orders#main#12345678-1234-1234-1234-123456789abc#0000000100"
        );
    }

    #[test]
    fn test_position_row_key_format() {
        let root = [0xDE, 0xAD, 0xBE, 0xEF];
        let key = BigtablePositionStore::row_key("projector-orders", "orders", "main", &root);

        assert_eq!(
            String::from_utf8(key).unwrap(),
            "projector-orders#orders#main#deadbeef"
        );
    }

    #[test]
    fn test_position_row_key_empty_root() {
        let root: [u8; 0] = [];
        let key = BigtablePositionStore::row_key("handler", "domain", "edition", &root);

        assert_eq!(String::from_utf8(key).unwrap(), "handler#domain#edition#");
    }
}

// ============================================================================
// Sequence Extraction Tests
// ============================================================================

mod sequence_tests {
    use super::*;
    use crate::proto::{page_header::SequenceType, EventPage, PageHeader};

    fn event_with_seq(seq: u32) -> EventPage {
        EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(seq)),
            }),
            payload: None,
            created_at: None,
            no_commit: false,
            cascade_id: None,
        }
    }

    #[test]
    fn test_get_sequence() {
        assert_eq!(BigtableEventStore::get_sequence(&event_with_seq(42)), 42);
    }

    #[test]
    fn test_get_sequence_zero() {
        assert_eq!(BigtableEventStore::get_sequence(&event_with_seq(0)), 0);
    }
}

// ============================================================================
// Timestamp Parsing Tests
// ============================================================================

mod timestamp_tests {
    use super::*;

    #[test]
    fn test_parse_iso8601_timestamp_valid() {
        let ts = BigtableEventStore::parse_timestamp("2024-01-15T10:30:00Z");
        assert!(ts.is_some());
        let (secs, nanos) = ts.unwrap();
        assert!(secs > 0);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn test_parse_iso8601_timestamp_with_nanos() {
        let ts = BigtableEventStore::parse_timestamp("2024-01-15T10:30:00.123456789Z");
        assert!(ts.is_some());
        let (_, nanos) = ts.unwrap();
        assert!(nanos > 0);
    }

    #[test]
    fn test_parse_iso8601_timestamp_invalid() {
        assert!(BigtableEventStore::parse_timestamp("not-a-timestamp").is_none());
        assert!(BigtableEventStore::parse_timestamp("2024-13-45").is_none());
    }

    #[test]
    fn test_format_timestamp() {
        let formatted = BigtableEventStore::format_timestamp(1705315800, 0);
        assert!(formatted.contains("2024-01-15"));
    }
}

// ============================================================================
// Mutation Building Tests
// ============================================================================

mod mutation_tests {
    use super::*;
    use crate::proto::EventPage;

    #[test]
    fn test_build_set_cell_mutation() {
        let value = b"test_value";
        let mutation = BigtableEventStore::build_set_cell("cf", b"col", value);

        // Verify the mutation is properly constructed
        assert!(mutation.mutation.is_some());
    }

    #[test]
    fn test_build_event_mutations() {
        use crate::proto::{event_page, page_header::SequenceType, PageHeader};
        let event = EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(0)),
            }),
            payload: Some(event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event".to_string(),
                value: vec![1, 2, 3],
            })),
            created_at: Some(prost_types::Timestamp {
                seconds: 1705315800,
                nanos: 0,
            }),
            no_commit: false,
            cascade_id: None,
        };

        let mutations = BigtableEventStore::build_event_mutations(&event, "corr-123");

        // Should have: data, created_at, correlation_id
        assert!(mutations.len() >= 2);
    }
}
