//! Unit tests for the aggregate pipeline's structural behaviour: id
//! assignment, provenance stamping, the append, publishing, output
//! validation and parked facts. The coordinator-contract scenarios
//! (conflict resolution, rules versions, snapshot selection) are bound in
//! `tests/coordinator_contract.rs`.

use tonic::Code;
use uuid::Uuid;

use super::*;
use crate::proto::{
    event_meta::Causation, CommandHandlerResponse, ConflictResolution, DeliveryOutcome, EventBook,
    EventMeta, EventPage,
};
use crate::storage::{EventStore, StreamRef};
use crate::test_utils::*;

const DOMAIN: &str = "order";
const VERSION: &str = "1.4.2+build.7";

fn root() -> Uuid {
    Uuid::from_u128(0x0192_0c8e_7b3a_7cc2_9f4e_5a1b_2c3d_4e5f)
}

fn registered() -> Harness {
    let h = Harness::new(DOMAIN, Answer::events(&["/orders.v1.ItemAdded"], VERSION));
    h.registry
        .register(
            DOMAIN,
            "local",
            aggregate_descriptor("order", "/orders.v1.OrderState", VERSION, false),
        )
        .unwrap();
    h
}

async fn stored(h: &Harness) -> Vec<EventPage> {
    h.events
        .read(StreamRef::main(DOMAIN, root()), 0, None)
        .await
        .unwrap()
}

/// Without a registration the coordinator cannot vet a response's version,
/// so it serves nothing (UNAVAILABLE, retryable) rather than guessing.
#[tokio::test]
async fn test_unregistered_coordinator_answers_unavailable() {
    let h = Harness::new(DOMAIN, Answer::events(&["/orders.v1.ItemAdded"], VERSION));
    let err = h
        .pipeline()
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap_err();
    assert_eq!(err.to_status().code(), Code::Unavailable);
    assert!(h.business.received().is_empty());
}

/// A client command without ids gets a minted v7 command id, and its
/// correlation is that command id (types.proto, command_identity).
#[tokio::test]
async fn test_minted_command_id_is_v7_and_is_the_correlation() {
    let h = registered();
    let decided = h
        .pipeline()
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    assert_eq!(decided.delivery, DeliveryOutcome::Applied);
    let meta = decided.events.pages[0].meta.clone().unwrap();
    let Some(Causation::CommandId(id)) = meta.causation else {
        panic!("causation must be the command id");
    };
    let id = Uuid::try_from(&id).unwrap();
    assert_eq!(id.get_version_num(), 7);
    assert_eq!(Uuid::try_from(&meta.correlation_id.unwrap()).unwrap(), id);
}

/// Supplied ids are kept verbatim, whatever their UUID version.
#[tokio::test]
async fn test_supplied_ids_are_kept() {
    let h = registered();
    let mut book = command(DOMAIN, root(), "/orders.v1.AddItem");
    let page = book.page.as_mut().unwrap();
    page.command_id = Some(Uuid::from_u128(11).into());
    page.correlation_id = Some(Uuid::from_u128(12).into());
    let decided = h.pipeline().execute(book, Delivery::Client).await.unwrap();
    let meta = decided.events.pages[0].meta.clone().unwrap();
    assert_eq!(
        meta.causation,
        Some(Causation::CommandId(Uuid::from_u128(11).into()))
    );
    assert_eq!(meta.correlation_id, Some(Uuid::from_u128(12).into()));
}

/// Every event of one write is sequenced from the head, shares one meta,
/// is appended, and is published with the stream's cover.
#[tokio::test]
async fn test_events_are_sequenced_stamped_appended_and_published() {
    let h = registered();
    h.business.then(Answer::events(
        &["/orders.v1.ItemAdded", "/orders.v1.ShippingQuoted"],
        VERSION,
    ));
    let pipeline = h.pipeline();
    pipeline
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    pipeline
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    let pages = stored(&h).await;
    assert_eq!(
        pages.iter().map(|p| p.sequence).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert_eq!(pages[0].meta, pages[1].meta);
    assert_ne!(pages[1].meta, pages[2].meta);
    let published = h.bus.published().await;
    assert_eq!(published.len(), 2);
    assert_eq!(published[1].next_sequence, 3);
    assert_eq!(published[1].cover, Some(cover(DOMAIN, root())));
}

/// The delivered command has its Handling resolved: an absent expected
/// sequence is stamped with the head.
#[tokio::test]
async fn test_absent_expected_sequence_is_stamped_with_the_head() {
    let h = registered();
    let pipeline = h.pipeline();
    for _ in 0..2 {
        pipeline
            .execute(
                command(DOMAIN, root(), "/orders.v1.AddItem"),
                Delivery::Client,
            )
            .await
            .unwrap();
    }
    let delivered = h.business.received();
    let stamped = |i: usize| {
        delivered[i]
            .command
            .as_ref()
            .unwrap()
            .handling
            .as_ref()
            .unwrap()
            .expected_sequence
    };
    assert_eq!(stamped(0), Some(0));
    assert_eq!(stamped(1), Some(1));
    assert_eq!(delivered[1].events.as_ref().unwrap().next_sequence, 1);
}

/// recorded_at never goes backwards within a stream, even when the clock
/// steps back (EventMeta.recorded_at).
#[tokio::test]
async fn test_recorded_at_is_monotonic_across_a_clock_step_back() {
    let h = registered();
    let pipeline = h.pipeline();
    h.clock.set(2_000);
    pipeline
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    h.clock.set(1_000);
    pipeline
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    let pages = stored(&h).await;
    let at = |p: &EventPage| p.meta.as_ref().unwrap().recorded_at.unwrap().seconds;
    assert_eq!(at(&pages[0]), 2_000);
    assert_eq!(at(&pages[1]), 2_000);
}

fn raw(pages: Vec<EventPage>, cover: Option<crate::proto::Cover>) -> Answer {
    Answer::Raw(CommandHandlerResponse {
        events: Some(EventBook {
            cover,
            pages,
            ..Default::default()
        }),
        rules_version: VERSION.to_string(),
    })
}

fn payload_page() -> EventPage {
    EventPage {
        payload: Some(any("/orders.v1.ItemAdded", b"x")),
        ..Default::default()
    }
}

/// Business output that sets meta, a sequence or a foreign cover is a
/// handler bug: INTERNAL, nothing persisted (BO-1).
#[tokio::test]
async fn test_contract_violating_output_is_internal_and_persists_nothing() {
    let violations = [
        raw(
            vec![EventPage {
                meta: Some(EventMeta::default()),
                ..payload_page()
            }],
            None,
        ),
        raw(
            vec![EventPage {
                sequence: 4,
                ..payload_page()
            }],
            None,
        ),
        raw(vec![payload_page()], Some(cover("inventory", root()))),
        raw(vec![EventPage::default()], None),
    ];
    for violation in violations {
        let h = registered();
        h.business.then(violation);
        let err = h
            .pipeline()
            .execute(
                command(DOMAIN, root(), "/orders.v1.AddItem"),
                Delivery::Client,
            )
            .await
            .unwrap_err();
        assert_eq!(err.to_status().code(), Code::Internal);
        assert!(stored(&h).await.is_empty());
        assert!(h.bus.published().await.is_empty());
    }
}

/// An empty cover, or the command's own, is accepted on output.
#[tokio::test]
async fn test_output_with_the_commands_cover_is_accepted() {
    let h = registered();
    h.business
        .then(raw(vec![payload_page()], Some(cover(DOMAIN, root()))));
    h.pipeline()
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    assert_eq!(stored(&h).await.len(), 1);
}

/// A business rejection is returned unchanged and writes nothing.
#[tokio::test]
async fn test_business_rejection_is_returned_unchanged() {
    let h = registered();
    h.business
        .then(Answer::Error(tonic::Status::failed_precondition(
            "order is closed",
        )));
    let err = h
        .pipeline()
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap_err();
    let status = err.to_status();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(status.message(), "order is closed");
    assert!(stored(&h).await.is_empty());
    assert!(h.dead_letters.received().is_empty());
}

/// A fact's command id and correlation derive from its external id; its
/// events carry the fact origin.
#[tokio::test]
async fn test_fact_ids_derive_from_external_id() {
    let h = registered();
    let decided = h
        .pipeline()
        .execute(
            fact(DOMAIN, root(), "/orders.v1.PaymentReceived", "pi_1234"),
            Delivery::Client,
        )
        .await
        .unwrap();
    let meta = decided.events.pages[0].meta.clone().unwrap();
    let derived = angzarr_proto::ids::fact_command_id("pi_1234");
    assert_eq!(meta.causation, Some(Causation::CommandId(derived.clone())));
    assert_eq!(meta.correlation_id, Some(derived));
    assert_eq!(meta.fact.unwrap().external_id, "pi_1234");
}

/// A failing fact handler is not a rejection: the fact is parked (OK,
/// PARKED, no events), dead-lettered and alerted (types.proto, Fact).
#[tokio::test]
async fn test_failing_fact_ingest_is_parked() {
    let h = registered();
    h.business
        .then(Answer::Error(tonic::Status::internal("ingest panicked")));
    let decided = h
        .pipeline()
        .execute(
            fact(DOMAIN, root(), "/orders.v1.PaymentReceived", "pi_1"),
            Delivery::Client,
        )
        .await
        .unwrap();
    assert_eq!(decided.delivery, DeliveryOutcome::Parked);
    assert!(decided.events.pages.is_empty());
    assert!(stored(&h).await.is_empty());
    assert_eq!(h.dead_letters.received().len(), 1);
    assert_eq!(h.alerts.raised().len(), 1);
}

/// A fact's delivered Handling is the stamped head; it never carries a
/// conflict resolution.
#[tokio::test]
async fn test_fact_handling_is_stamped() {
    let h = registered();
    h.pipeline()
        .execute(
            fact(DOMAIN, root(), "/orders.v1.PaymentReceived", "pi_9"),
            Delivery::Client,
        )
        .await
        .unwrap();
    let handling = h.business.received()[0]
        .command
        .clone()
        .unwrap()
        .handling
        .unwrap();
    assert_eq!(handling.expected_sequence, Some(0));
    assert_eq!(handling.conflict_resolution, 0);
}

/// A malformed client command is refused before business code runs.
#[tokio::test]
async fn test_malformed_command_never_reaches_business_code() {
    let h = registered();
    let mut book = command(DOMAIN, root(), "/orders.v1.AddItem");
    book.page.as_mut().unwrap().command_id = Some(crate::proto::Uuid {
        value: vec![1, 2, 3].into(),
    });
    let err = h
        .pipeline()
        .execute(book, Delivery::Client)
        .await
        .unwrap_err();
    assert_eq!(err.to_status().code(), Code::InvalidArgument);
    assert!(h.business.received().is_empty());
}

/// Persisted events whose publish fails stay persisted (the command
/// succeeds) and are dead-lettered for replay.
#[tokio::test]
async fn test_publish_failure_dead_letters_the_events() {
    let h = registered();
    h.bus.fail_publishes();
    let decided = h
        .pipeline()
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    assert_eq!(decided.delivery, DeliveryOutcome::Applied);
    assert_eq!(stored(&h).await.len(), 1);
    assert_eq!(h.dead_letters.received().len(), 1);
}

/// A snapshot returned with the answer is stored at the write's last event.
#[tokio::test]
async fn test_snapshot_is_stored_at_the_last_event() {
    let h = registered();
    h.business.then(Answer::Events {
        types: vec!["/orders.v1.ItemAdded".into(), "/orders.v1.ItemAdded".into()],
        snapshot: Some(any("/orders.v1.OrderState", b"state")),
        rules_version: VERSION.into(),
    });
    h.pipeline()
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    let snapshot =
        crate::storage::SnapshotStore::get(h.snapshots.as_ref(), StreamRef::main(DOMAIN, root()))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(snapshot.sequence, 1);
}

/// A conflict resolution named on Handling reaches business code as sent
/// (it is resolved in place, never dropped).
#[tokio::test]
async fn test_named_resolution_is_delivered() {
    let h = registered();
    h.pipeline()
        .execute(
            with_handling(
                command(DOMAIN, root(), "/orders.v1.AddItem"),
                None,
                ConflictResolution::Strict,
            ),
            Delivery::Client,
        )
        .await
        .unwrap();
    let handling = h.business.received()[0]
        .command
        .clone()
        .unwrap()
        .handling
        .unwrap();
    assert_eq!(
        handling.conflict_resolution,
        ConflictResolution::Strict as i32
    );
}
