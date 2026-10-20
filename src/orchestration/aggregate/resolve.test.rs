//! Unit tests for Handling resolution, the registration-dependent step-1
//! checks, snapshot bounds and the remaining pipeline edges.
//!
//! Why this matters: the coordinator-contract harness pins these behaviours
//! end to end, but only unit tests run under mutation testing; each test
//! here names the contract rule it holds the pipeline to.

use std::sync::Arc;

use tonic::Code;
use uuid::Uuid;

use super::*;
use crate::proto::{ConflictResolution, DeliveryOutcome, EventRef, Snapshot};
use crate::storage::{EventStore, SnapshotStore, SnapshotsEnableConfig, StreamRef};
use crate::test_utils::*;

const DOMAIN: &str = "order";
const VERSION: &str = "1.4.2+build.7";
const STATE: &str = "/orders.v1.OrderState";

fn root() -> Uuid {
    Uuid::from_u128(7)
}

fn stream() -> StreamRef<'static> {
    StreamRef::main(DOMAIN, root())
}

fn harness(commutative: bool) -> Harness {
    let h = Harness::new(DOMAIN, Answer::events(&["/orders.v1.ItemAdded"], VERSION));
    h.registry
        .register(aggregate_descriptor("order", STATE, VERSION, commutative))
        .unwrap();
    h
}

/// Append `n` events (sequences 0..n) through the pipeline itself.
async fn seed(h: &Harness, n: usize) {
    for _ in 0..n {
        h.pipeline()
            .execute(
                command(DOMAIN, root(), "/orders.v1.AddItem"),
                Delivery::Client,
            )
            .await
            .unwrap();
    }
}

async fn put_snapshot(h: &Harness, sequence: u64) {
    h.snapshots
        .put(
            stream(),
            &Snapshot {
                sequence,
                state: Some(any(STATE, b"s")),
                rules_version: VERSION.into(),
            },
        )
        .await
        .unwrap();
}

fn targeting(seq: u64, r: ConflictResolution) -> crate::proto::CommandBook {
    with_handling(command(DOMAIN, root(), "/orders.v1.AddItem"), Some(seq), r)
}

fn last_delivered(h: &Harness) -> crate::proto::ContextualCommand {
    h.business.received().pop().unwrap()
}

fn resolved(c: &crate::proto::ContextualCommand) -> (Option<u64>, i32) {
    let handling = c.command.as_ref().unwrap().handling.unwrap();
    (handling.expected_sequence, handling.conflict_resolution)
}

#[test]
fn test_accessors() {
    let h = harness(false);
    let p = h.pipeline();
    assert_eq!(p.domain(), DOMAIN);
    assert!(Arc::ptr_eq(p.registry(), &h.registry));
}

/// Unnamed: COMMUTATIVE where the registered state lists it, else STRICT;
/// COMMUTATIVE (like AGGREGATE_HANDLES) gets a nonce, STRICT none.
#[tokio::test]
async fn test_default_resolution_comes_from_the_registered_state() {
    for (commutative, expected) in [
        (true, ConflictResolution::Commutative),
        (false, ConflictResolution::Strict),
    ] {
        let h = harness(commutative);
        seed(&h, 1).await;
        let delivered = last_delivered(&h);
        assert_eq!(resolved(&delivered), (Some(0), expected as i32));
        assert_eq!(delivered.conflict_nonce.is_empty(), !commutative);
    }
}

/// STRICT: stale (behind) and ahead are both ABORTED before business code;
/// at the head it is delivered.
#[tokio::test]
async fn test_strict_checks_the_head() {
    let h = harness(false);
    seed(&h, 2).await;
    let before = h.business.received().len();
    for seq in [1, 3] {
        let err = h
            .pipeline()
            .execute(targeting(seq, ConflictResolution::Strict), Delivery::Client)
            .await
            .unwrap_err();
        assert_eq!(err.to_status().code(), Code::Aborted);
    }
    assert_eq!(h.business.received().len(), before);
    h.pipeline()
        .execute(targeting(2, ConflictResolution::Strict), Delivery::Client)
        .await
        .unwrap();
}

/// AGGREGATE_HANDLES: a stale command is delivered with every window page
/// and a nonce; one ahead of the stream is ABORTED.
#[tokio::test]
async fn test_aggregate_handles_delivers_the_window() {
    let h = harness(false);
    seed(&h, 3).await;
    h.pipeline()
        .execute(
            targeting(1, ConflictResolution::AggregateHandles),
            Delivery::Client,
        )
        .await
        .unwrap();
    let delivered = last_delivered(&h);
    assert!(!delivered.conflict_nonce.is_empty());
    let pages: Vec<u64> = delivered
        .events
        .unwrap()
        .pages
        .iter()
        .map(|p| p.sequence)
        .collect();
    assert_eq!(pages, [0, 1, 2]);
    let err = h
        .pipeline()
        .execute(
            targeting(9, ConflictResolution::AggregateHandles),
            Delivery::Client,
        )
        .await
        .unwrap_err();
    assert_eq!(err.to_status().code(), Code::Aborted);
}

/// The window bound: a snapshot at or past expected_sequence is not used
/// (the window arrives as pages), one below it is; none at sequence 0.
#[tokio::test]
async fn test_snapshot_bound_under_aggregate_handles() {
    for (snapshot_at, expected, offered) in [(2, 2, false), (1, 2, true), (0, 0, false)] {
        let h = harness(false);
        seed(&h, 3).await;
        put_snapshot(&h, snapshot_at).await;
        h.pipeline()
            .execute(
                targeting(expected, ConflictResolution::AggregateHandles),
                Delivery::Client,
            )
            .await
            .unwrap();
        let events = last_delivered(&h).events.unwrap();
        assert_eq!(
            events.snapshot.is_some(),
            offered,
            "snapshot at {snapshot_at}, E {expected}"
        );
        let first = events.pages.first().map(|p| p.sequence);
        assert_eq!(first, Some(if offered { snapshot_at + 1 } else { 0 }));
    }
}

/// A stored snapshot that claims a sequence at or past the head is never
/// offered (it cannot fold events the stream does not have).
#[tokio::test]
async fn test_snapshot_beyond_the_head_is_not_offered() {
    let h = harness(false);
    seed(&h, 2).await;
    put_snapshot(&h, 2).await;
    seed(&h, 1).await;
    assert!(last_delivered(&h).events.unwrap().snapshot.is_none());
}

/// Snapshots are not read when reads are disabled, and a returned state is
/// not stored when writes are disabled.
#[tokio::test]
async fn test_snapshot_switches() {
    let h = harness(false);
    let pipeline = h.pipeline().with_snapshots(SnapshotsEnableConfig {
        read: false,
        write: false,
    });
    h.business.then(Answer::Events {
        types: vec!["/orders.v1.ItemAdded".into()],
        snapshot: Some(any(STATE, b"s")),
        rules_version: VERSION.into(),
    });
    pipeline
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    assert!(h.snapshots.get(stream()).await.unwrap().is_none());
    put_snapshot(&h, 0).await;
    pipeline
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();
    assert!(last_delivered(&h).events.unwrap().snapshot.is_none());
}

/// A named resolution the registered state lacks is malformed; a supported
/// one passes; a reaction command carrying Handling is a reactor bug,
/// dead-lettered and alerted; a malformed queued client command is not.
#[tokio::test]
async fn test_registration_dependent_checks() {
    let h = harness(false);
    let err = h
        .pipeline()
        .execute(
            targeting(0, ConflictResolution::Commutative),
            Delivery::Client,
        )
        .await
        .unwrap_err();
    assert_eq!(err.to_status().code(), Code::InvalidArgument);
    assert!(h.business.received().is_empty());

    let err = h
        .pipeline()
        .execute(
            with_handling(
                command(DOMAIN, root(), "/orders.v1.AddItem"),
                None,
                ConflictResolution::AggregateHandles,
            ),
            Delivery::Async,
        )
        .await;
    assert!(err.is_ok(), "a supported named resolution passes");
    assert!(h.dead_letters.received().is_empty());

    let err = h
        .pipeline()
        .execute(
            targeting(0, ConflictResolution::Commutative),
            Delivery::Async,
        )
        .await
        .unwrap_err();
    assert_eq!(err.to_status().code(), Code::InvalidArgument);
    assert!(
        h.dead_letters.received().is_empty(),
        "a client is not a reactor"
    );

    let mut reaction = command(DOMAIN, root(), "/orders.v1.AddItem");
    let page = reaction.page.as_mut().unwrap();
    page.causation_id = Some(EventRef {
        stream: Some(cover("cart", Uuid::from_u128(3))),
        sequence: 1,
    });
    page.command_id = Some(Uuid::from_u128(4).into());
    let reaction = with_handling(reaction, Some(1), ConflictResolution::Unspecified);
    let err = h
        .pipeline()
        .execute(reaction, Delivery::Reaction)
        .await
        .unwrap_err();
    assert_eq!(err.to_status().code(), Code::InvalidArgument);
    assert_eq!(h.dead_letters.received().len(), 1);
    assert_eq!(h.alerts.raised().len(), 1);
}

/// The command business code receives carries its assigned ids, and the
/// delivered history is the stored pages.
#[tokio::test]
async fn test_delivered_command_carries_assigned_ids_and_history() {
    let h = harness(false);
    seed(&h, 2).await;
    let delivered = last_delivered(&h);
    let page = delivered.command.unwrap().page.unwrap();
    assert!(page.command_id.is_some() && page.correlation_id.is_some());
    let pages = delivered.events.unwrap().pages;
    let stored = h.events.read(stream(), 0, None).await.unwrap();
    assert_eq!(pages, stored[..1].to_vec());
}

/// A duplicate's book reports the stream position after its events.
#[tokio::test]
async fn test_duplicate_next_sequence() {
    let h = harness(false);
    let mut book = command(DOMAIN, root(), "/orders.v1.AddItem");
    book.page.as_mut().unwrap().command_id = Some(Uuid::from_u128(5).into());
    let pipeline = h.pipeline();
    pipeline
        .execute(book.clone(), Delivery::Client)
        .await
        .unwrap();
    let dup = pipeline.execute(book, Delivery::Client).await.unwrap();
    assert_eq!(dup.delivery, DeliveryOutcome::Duplicate);
    assert_eq!(dup.events.next_sequence, 1);
}

/// recorded_at follows the clock forward.
#[tokio::test]
async fn test_recorded_at_moves_forward() {
    let h = harness(false);
    h.clock.set(1_000);
    seed(&h, 1).await;
    h.clock.set(2_000);
    seed(&h, 1).await;
    let pages = h.events.read(stream(), 0, None).await.unwrap();
    assert_eq!(
        pages[1].meta.as_ref().unwrap().recorded_at.unwrap().seconds,
        2_000
    );
}

/// A parked fact's answer names the stream it was addressed to.
#[tokio::test]
async fn test_parked_fact_keeps_its_cover() {
    let h = harness(false);
    h.business
        .then(Answer::Error(tonic::Status::internal("ingest bug")));
    let decided = h
        .pipeline()
        .execute(
            fact(DOMAIN, root(), "/orders.v1.Paid", "pi_1"),
            Delivery::Client,
        )
        .await
        .unwrap();
    assert_eq!(decided.events.cover, Some(cover(DOMAIN, root())));
}

/// A business process that is unreachable is an infrastructure failure
/// (retryable, never dead-lettered, never parked), for commands and facts.
#[tokio::test]
async fn test_unreachable_business_is_unavailable() {
    for book in [
        command(DOMAIN, root(), "/orders.v1.AddItem"),
        fact(DOMAIN, root(), "/orders.v1.Paid", "pi_2"),
    ] {
        let h = harness(false);
        h.business
            .then(Answer::Error(tonic::Status::unavailable("down")));
        let err = h
            .pipeline()
            .execute(book, Delivery::Async)
            .await
            .unwrap_err();
        assert_eq!(err.to_status().code(), Code::Unavailable);
        assert!(h.dead_letters.received().is_empty());
    }
}

/// Handling with an expected sequence but no named resolution is valid: the
/// resolution is filled from the registered state.
#[tokio::test]
async fn test_unspecified_resolution_with_expected_sequence_is_valid() {
    let h = harness(false);
    h.pipeline()
        .execute(
            targeting(0, ConflictResolution::Unspecified),
            Delivery::Client,
        )
        .await
        .unwrap();
    assert_eq!(
        resolved(&last_delivered(&h)),
        (Some(0), ConflictResolution::Strict as i32)
    );
}

/// The coordinator upcasts on every read and publish path of its stream:
/// the history delivered to business code, the events published to the
/// bus, and the stored events a duplicate is answered with. Consumers see
/// only current shapes.
#[tokio::test]
async fn test_upcast_on_load_publish_and_duplicate() {
    let h = harness(false);
    let upcaster = RenamingUpcaster::new("/orders.v1.ItemAdded", "/orders.v2.ItemAdded");
    let pipeline = h.pipeline().with_upcaster(upcaster.clone());
    let mut book = command(DOMAIN, root(), "/orders.v1.AddItem");
    book.page.as_mut().unwrap().command_id = Some(Uuid::from_u128(77).into());
    pipeline
        .execute(book.clone(), Delivery::Client)
        .await
        .unwrap();
    pipeline
        .execute(
            command(DOMAIN, root(), "/orders.v1.AddItem"),
            Delivery::Client,
        )
        .await
        .unwrap();

    let delivered = last_delivered(&h).events.unwrap();
    assert_eq!(
        delivered.pages[0].payload.as_ref().unwrap().type_url,
        "/orders.v2.ItemAdded"
    );
    let published = h.bus.published().await;
    assert!(published
        .iter()
        .flat_map(|b| &b.pages)
        .all(|p| p.payload.as_ref().unwrap().type_url == "/orders.v2.ItemAdded"));
    let dup = pipeline.execute(book, Delivery::Client).await.unwrap();
    assert_eq!(dup.delivery, DeliveryOutcome::Duplicate);
    assert_eq!(
        dup.events.pages[0].payload.as_ref().unwrap().type_url,
        "/orders.v2.ItemAdded"
    );
    // Stored events keep the shape they were recorded in.
    let stored = h.events.read(stream(), 0, None).await.unwrap();
    assert_eq!(
        stored[0].payload.as_ref().unwrap().type_url,
        "/orders.v1.ItemAdded"
    );
}

fn policy(yaml: &str) -> Arc<crate::conflict_resolution::ConflictPolicy> {
    Arc::new(
        crate::conflict_resolution::ConflictPolicy::from_config(
            &serde_yaml::from_str(yaml).unwrap(),
        )
        .unwrap(),
    )
}

/// Configuration for the command type outranks the component's and the
/// built-in default; the registered state supports COMMUTATIVE, yet the
/// configured STRICT applies, so a stale command is ABORTED.
#[tokio::test]
async fn test_configured_command_type_resolution_applies() {
    let h = harness(true);
    seed(&h, 2).await;
    let pipeline = h.pipeline().with_conflict_policy(policy(
        "default: AGGREGATE_HANDLES\ncommands:\n  - type: orders.v1.AddItem\n    resolution: STRICT\n",
    ));
    let err = pipeline
        .execute(
            targeting(0, ConflictResolution::Unspecified),
            Delivery::Client,
        )
        .await
        .unwrap_err();
    assert_eq!(err.to_status().code(), Code::Aborted);
    pipeline
        .execute(
            with_handling(
                command(DOMAIN, root(), "/orders.v1.Close"),
                Some(0),
                ConflictResolution::Unspecified,
            ),
            Delivery::Client,
        )
        .await
        .unwrap();
    assert_eq!(
        resolved(&last_delivered(&h)).1,
        ConflictResolution::AggregateHandles as i32,
        "the component's configured resolution applies to other types"
    );
}

/// Where configuration forbids the override, a Handling that names a
/// resolution is malformed (INVALID_ARGUMENT) and never reaches business
/// code; leaving it unnamed is fine.
#[tokio::test]
async fn test_forbidden_override_is_refused() {
    let h = harness(false);
    let pipeline = h
        .pipeline()
        .with_conflict_policy(policy("allow_override: false\n"));
    let err = pipeline
        .execute(targeting(0, ConflictResolution::Strict), Delivery::Client)
        .await
        .unwrap_err();
    assert_eq!(err.to_status().code(), Code::InvalidArgument);
    assert!(h.business.received().is_empty());
    pipeline
        .execute(
            targeting(0, ConflictResolution::Unspecified),
            Delivery::Client,
        )
        .await
        .unwrap();
}
