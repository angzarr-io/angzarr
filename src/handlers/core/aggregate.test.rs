//! Tests for aggregate command handler utilities.
//!
//! The aggregate handler supports async command delivery via bus transport.
//! Commands are wrapped in EventBook format for delivery, then extracted
//! and executed by the receiving aggregate. These tests verify the
//! wrap/extract round-trip preserves command data.
//!
//! Why this matters: If wrapping or extraction fails, async command
//! delivery breaks silently (commands are skipped as "not command events").

use super::*;
use crate::proto::{
    command_page, event_page, page_header, CommandBook, CommandPage, Cover, EventBook, EventPage,
    MergeStrategy, PageHeader, Uuid as ProtoUuid,
};
use crate::proto_ext::CommandPageExt;
use prost::Message;
use prost_types::Any;
use uuid::Uuid;

// ============================================================================
// Test Helpers
// ============================================================================

fn make_proto_uuid(u: Uuid) -> ProtoUuid {
    ProtoUuid {
        value: u.as_bytes().to_vec(),
    }
}

fn make_cover(domain: &str, root: Uuid, correlation_id: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(make_proto_uuid(root)),
        correlation_id: correlation_id.to_string(),
        edition: None,
        ext: None,
    }
}

fn make_command_book(domain: &str, root: Uuid, command_type: &str, data: Vec<u8>) -> CommandBook {
    CommandBook {
        cover: Some(make_cover(domain, root, "test-correlation")),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(5)),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: command_type.to_string(),
                value: data,
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    }
}

// ============================================================================
// wrap_command_for_bus Tests
// ============================================================================

/// Wrapped command preserves the original cover.
///
/// The cover contains domain, root, correlation_id - all essential for
/// routing the command to the correct aggregate instance.
#[test]
fn test_wrap_command_preserves_cover() {
    let root = Uuid::new_v4();
    let command = make_command_book("player", root, "test.CreatePlayer", vec![1, 2, 3]);

    let wrapped = wrap_command_for_bus(&command);

    let cover = wrapped.cover.as_ref().expect("Cover should be preserved");
    assert_eq!(cover.domain, "player");
    assert_eq!(cover.correlation_id, "test-correlation");
    assert_eq!(
        cover.root.as_ref().map(|r| Uuid::from_slice(&r.value).ok()),
        Some(Some(root))
    );
}

/// Wrapped command has exactly one page containing the serialized command.
#[test]
fn test_wrap_command_creates_single_page() {
    let command = make_command_book("player", Uuid::new_v4(), "test.Command", vec![]);

    let wrapped = wrap_command_for_bus(&command);

    assert_eq!(wrapped.pages.len(), 1, "Should have exactly one page");
}

/// Wrapped command page has correct type_url for CommandBook.
///
/// The type_url must end with "angzarr.CommandBook" for extraction to recognize it.
#[test]
fn test_wrap_command_has_correct_type_url() {
    let command = make_command_book("player", Uuid::new_v4(), "test.Command", vec![]);

    let wrapped = wrap_command_for_bus(&command);

    let page = &wrapped.pages[0];
    if let Some(event_page::Payload::Event(any)) = &page.payload {
        assert!(
            any.type_url.ends_with("angzarr.CommandBook"),
            "Type URL should end with angzarr.CommandBook, got: {}",
            any.type_url
        );
        assert_eq!(
            any.type_url, "type.googleapis.com/angzarr.CommandBook",
            "Full type URL should match expected format"
        );
    } else {
        panic!("Expected Event payload");
    }
}

/// Wrapped command can be deserialized back to original.
#[test]
fn test_wrap_command_payload_is_valid_protobuf() {
    let command = make_command_book("player", Uuid::new_v4(), "test.Command", vec![4, 5, 6]);

    let wrapped = wrap_command_for_bus(&command);

    let page = &wrapped.pages[0];
    if let Some(event_page::Payload::Event(any)) = &page.payload {
        let decoded =
            CommandBook::decode(any.value.as_slice()).expect("Should decode back to CommandBook");
        assert_eq!(decoded.pages.len(), 1);
        assert_eq!(decoded.pages[0].sequence_num(), 5);
    } else {
        panic!("Expected Event payload");
    }
}

// ============================================================================
// extract_command_from_event_book Tests
// ============================================================================

/// Extract succeeds for properly wrapped command.
///
/// Round-trip test: wrap then extract should return equivalent command.
#[test]
fn test_extract_command_roundtrip() {
    let root = Uuid::new_v4();
    let original = make_command_book("player", root, "test.CreatePlayer", vec![1, 2, 3]);

    let wrapped = wrap_command_for_bus(&original);
    let extracted = extract_command_from_event_book(&wrapped);

    let extracted = extracted.expect("Should extract command");
    assert_eq!(
        extracted.cover.as_ref().map(|c| c.domain.as_str()),
        Some("player")
    );
    assert_eq!(extracted.pages.len(), 1);
    assert_eq!(extracted.pages[0].sequence_num(), 5);
}

/// Extract returns None for empty EventBook.
#[test]
fn test_extract_command_empty_book_returns_none() {
    let empty = EventBook::default();

    let result = extract_command_from_event_book(&empty);

    assert!(result.is_none(), "Empty book should return None");
}

/// Extract returns None for regular event (not a wrapped command).
///
/// Normal events have different type_urls (like "test.PlayerCreated").
/// Only wrapped commands have type_url ending in "angzarr.CommandBook".
#[test]
fn test_extract_command_regular_event_returns_none() {
    let regular_event = EventBook {
        cover: Some(make_cover("player", Uuid::new_v4(), "corr-123")),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(1)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.googleapis.com/test.PlayerCreated".to_string(),
                value: vec![1, 2, 3],
            })),
            ..Default::default()
        }],
        ..Default::default()
    };

    let result = extract_command_from_event_book(&regular_event);

    assert!(result.is_none(), "Regular event should return None");
}

/// Extract returns None for page with no payload.
#[test]
fn test_extract_command_no_payload_returns_none() {
    let no_payload = EventBook {
        cover: Some(make_cover("player", Uuid::new_v4(), "corr-123")),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(1)),
            }),
            created_at: None,
            payload: None,
            ..Default::default()
        }],
        ..Default::default()
    };

    let result = extract_command_from_event_book(&no_payload);

    assert!(result.is_none(), "No payload should return None");
}

/// Extract returns None for invalid protobuf data.
///
/// If the payload has correct type_url but corrupted data, decode fails gracefully.
#[test]
fn test_extract_command_invalid_protobuf_returns_none() {
    let invalid = EventBook {
        cover: Some(make_cover("player", Uuid::new_v4(), "corr-123")),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(1)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.googleapis.com/angzarr.CommandBook".to_string(),
                value: vec![0xFF, 0xFF, 0xFF], // Invalid protobuf
            })),
            ..Default::default()
        }],
        ..Default::default()
    };

    let result = extract_command_from_event_book(&invalid);

    assert!(result.is_none(), "Invalid protobuf should return None");
}

/// Extract preserves command data through round-trip.
///
/// The command payload bytes should be identical after wrap/extract.
#[test]
fn test_extract_command_preserves_payload_data() {
    let payload_data = vec![10, 20, 30, 40, 50];
    let original = make_command_book(
        "player",
        Uuid::new_v4(),
        "test.Command",
        payload_data.clone(),
    );

    let wrapped = wrap_command_for_bus(&original);
    let extracted = extract_command_from_event_book(&wrapped).expect("Should extract");

    if let Some(command_page::Payload::Command(any)) = &extracted.pages[0].payload {
        assert_eq!(any.value, payload_data, "Payload data should be preserved");
    } else {
        panic!("Expected Command payload");
    }
}

// ============================================================================
// EventHandler::handle Error-Propagation Tests (H-34)
// ============================================================================
//
// Prior behavior: when `execute_command_with_retry` returned `Err`, the
// EventHandler impl logged it via `error!` and returned `Ok(())`. That
// silently acked an async command failure — the bus saw "success" and the
// message disappeared. Saga and Process Manager handlers each carry a
// `propagate_errors` toggle for exactly this reason; the aggregate
// handler needs the same toggle. The default mirrors `ProcessManagerEventHandler`
// (propagate = true / loud-fail): an async command that fails after retry
// is a real consistency event and ought to nack so the bus can route it
// to the DLQ or redeliver.
//
// These tests pin the toggle's behavior on both sides:
//   - default = true → handler returns Err on inner failure
//   - opt-out via `with_error_propagation(false)` → handler returns Ok
//     (the old silent-log behavior, preserved as an explicit opt-in)

/// A factory backed by always-failing context/logic that surfaces a
/// non-retryable `Status` from the very first pipeline stage. We use
/// `Status::invalid_argument` from `parse_command_cover` — the command
/// is shaped as a wrapped CommandBook with NO cover, so the pipeline
/// fails immediately on parse without needing storage or business logic
/// stubs.
struct FailingFactory;

impl crate::orchestration::aggregate::AggregateContextFactory for FailingFactory {
    fn create(&self) -> Arc<dyn crate::orchestration::aggregate::AggregateContext> {
        Arc::new(UnreachableContext)
    }
    fn domain(&self) -> &str {
        "h34-test-domain"
    }
    fn client_logic(&self) -> Arc<dyn crate::orchestration::aggregate::ClientLogic> {
        Arc::new(UnreachableLogic)
    }
}

/// Context that panics if any method is invoked — proves the failure
/// originates in pipeline pre-checks, not in storage.
struct UnreachableContext;

#[async_trait::async_trait]
impl crate::orchestration::aggregate::AggregateContext for UnreachableContext {
    async fn load_prior_events_with_divergence(
        &self,
        _: &str,
        _: &str,
        _: uuid::Uuid,
        _: &crate::orchestration::aggregate::TemporalQuery,
        _: Option<u32>,
    ) -> Result<EventBook, Status> {
        unreachable!("pipeline must fail at parse before reaching the context")
    }
    async fn persist_events(
        &self,
        _: &EventBook,
        _: &EventBook,
        _: &str,
        _: &str,
        _: uuid::Uuid,
        _: &str,
        _: Option<&str>,
        _: Option<&crate::storage::SourceInfo>,
    ) -> Result<crate::orchestration::aggregate::PersistOutcome, Status> {
        unreachable!()
    }
    async fn post_persist(&self, _: &EventBook) -> Result<Vec<crate::proto::Projection>, Status> {
        unreachable!()
    }
}

struct UnreachableLogic;

#[async_trait::async_trait]
impl crate::orchestration::aggregate::ClientLogic for UnreachableLogic {
    async fn invoke(
        &self,
        _: crate::proto::ContextualCommand,
    ) -> Result<crate::proto::BusinessResponse, Status> {
        unreachable!()
    }
    async fn invoke_fact(
        &self,
        _: crate::orchestration::aggregate::FactContext,
    ) -> Result<EventBook, Status> {
        unreachable!()
    }
}

/// Build a wrapped CommandBook whose inner CommandBook has no cover —
/// the pipeline fails at `parse_command_cover` with `Status::invalid_argument`,
/// which is non-retryable, so `execute_command_with_retry` returns the
/// status verbatim.
fn make_failing_wrapped_command() -> EventBook {
    let inner = CommandBook {
        cover: None, // forces parse_command_cover to fail
        pages: vec![],
    };
    wrap_command_for_bus(&inner)
}

/// H-34: with the default error-propagation setting, a failed async
/// command propagates `Err` out of `EventHandler::handle`. The bus
/// dispatch layer translates that into nack/redeliver/DLQ — which is the
/// only outcome that's safe when an async command silently failed.
#[tokio::test]
async fn aggregate_handler_default_propagates_inner_failure() {
    use crate::bus::EventHandler;

    let handler = AggregateCommandHandler::new(Arc::new(FailingFactory));
    let book = Arc::new(make_failing_wrapped_command());

    let result = handler.handle(book).await;

    assert!(
        result.is_err(),
        "Default behavior must propagate inner-pipeline failures \
         (H-34: previously swallowed). Got Ok(())."
    );
}

/// H-34 opt-out: explicit `with_error_propagation(false)` restores the
/// legacy "log and ack" behavior for callers who knowingly want it.
#[tokio::test]
async fn aggregate_handler_with_propagation_disabled_swallows() {
    use crate::bus::EventHandler;

    let handler =
        AggregateCommandHandler::new(Arc::new(FailingFactory)).with_error_propagation(false);
    let book = Arc::new(make_failing_wrapped_command());

    let result = handler.handle(book).await;

    assert!(
        result.is_ok(),
        "Opt-out preserves silent-log behavior for callers that need it."
    );
}

// ============================================================================
// SyncProjectorEntry Tests
// ============================================================================

/// SyncProjectorEntry fields are accessible.
#[test]
fn test_sync_projector_entry_construction() {
    use crate::orchestration::projector::{ProjectionMode, ProjectorHandler};
    use crate::proto::Projection;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct DummyProjector;

    #[async_trait]
    impl ProjectorHandler for DummyProjector {
        async fn handle(
            &self,
            _events: &EventBook,
            _mode: ProjectionMode,
        ) -> Result<Projection, Status> {
            Ok(Projection::default())
        }
    }

    let entry = SyncProjectorEntry {
        name: "test-projector".to_string(),
        handler: Arc::new(DummyProjector),
    };

    assert_eq!(entry.name, "test-projector");
}
