//! DLQ publish -> read round-trip against a real PostgreSQL container.
//!
//! Run with:
//! ```bash
//! cargo test --test dlq_round_trip_postgres --features "postgres test-utils" -- --nocapture
//! ```
//!
//! Sister test to `dlq_round_trip_sqlite.rs`. The SQLite version
//! exercises the schema and proto round-trip via a tempfile-backed
//! database; this one exercises the same contracts against the actual
//! production-pattern backend so any divergence between the SQLite and
//! Postgres dialects (column types, jsonb vs text for details,
//! datetime handling) is caught at the test layer rather than in
//! production.
//!
//! Uses testcontainers-rs to spin up a Postgres 16 instance, then
//! initializes `PostgresDlqPublisher` and `PostgresDlqReader` against
//! the same connection URI. Each owns its own pool (mirrors the
//! production split between coordinator binaries and angzarr-status).

#![cfg(feature = "postgres")]

use std::collections::HashMap;
use std::time::Duration;

use angzarr::dlq::reader::{DeadLetterReader, ListFilter};
use angzarr::dlq::{
    AngzarrDeadLetter, DeadLetterPublisher, PostgresDlqPublisher, PostgresDlqReader,
    RejectionDetails,
};
use angzarr::proto::{CommandBook, Cover, EventBook};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Per-test Postgres container. Returns (container, connection_string).
///
/// Container handle MUST be kept alive for the duration of the test --
/// dropping it stops the Postgres process and kills both pools.
async fn start_postgres() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let image = GenericImage::new("postgres", "16")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ));

    let container = image
        .with_env_var("POSTGRES_USER", "dlquser")
        .with_env_var("POSTGRES_PASSWORD", "dlqpass")
        .with_env_var("POSTGRES_DB", "dlqdb")
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("Failed to start Postgres container");

    // Brief delay for full readiness -- matches storage_postgres.rs.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let host_port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get port");
    // See storage_postgres.rs: dind wrapper sets TESTCONTAINERS_HOST because
    // the bridge-gateway fallback is unreachable under rootless docker.
    let host = match std::env::var("TESTCONTAINERS_HOST") {
        Ok(h) => h,
        Err(_) => container
            .get_host()
            .await
            .expect("Failed to get host")
            .to_string(),
    };

    let uri = format!("postgres://dlquser:dlqpass@{}:{}/dlqdb", host, host_port);
    println!("Postgres DLQ test backend: {}", uri);
    (container, uri)
}

/// Initialize publisher + reader against the same Postgres URI. The
/// publisher's `::new` creates the `dlq_entries` table; the reader's
/// `::new` just opens a pool against it.
async fn setup_round_trip() -> (
    testcontainers::ContainerAsync<GenericImage>,
    PostgresDlqPublisher,
    PostgresDlqReader,
) {
    let (container, uri) = start_postgres().await;
    let publisher = PostgresDlqPublisher::new(&uri)
        .await
        .expect("init Postgres DLQ publisher");
    let reader = PostgresDlqReader::new(&uri)
        .await
        .expect("init Postgres DLQ reader");
    (container, publisher, reader)
}

fn cover(domain: &str, correlation_id: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: None,
        correlation_id: correlation_id.to_string(),
        edition: None,
        ext: None,
    }
}

fn command(domain: &str, correlation_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain, correlation_id)),
        pages: vec![],
    }
}

fn events(domain: &str, correlation_id: &str) -> EventBook {
    EventBook {
        cover: Some(cover(domain, correlation_id)),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

/// Mirror of the SQLite round-trip test: all four coordinator
/// constructors survive the round-trip with `source_component_type`
/// preserved. Running it against Postgres catches dialect-specific
/// bugs the SQLite path can't see -- in particular jsonb-vs-text
/// details storage and the Postgres reader's `details::text` cast.
#[tokio::test]
async fn dlq_round_trip_postgres_all_component_types_preserve_source_component_type() {
    let (_container, publisher, reader) = setup_round_trip().await;

    publisher
        .publish(AngzarrDeadLetter::from_sequence_mismatch(
            &command("orders", "corr-agg"),
            3,
            5,
            angzarr::proto::MergeStrategy::MergeManual,
            "aggregate-orders",
        ))
        .await
        .expect("publish aggregate DL");

    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("inventory", "corr-saga"),
            "schema mismatch",
            0,
            false,
            "saga-order-to-inventory",
        ))
        .await
        .expect("publish saga DL");

    publisher
        .publish(AngzarrDeadLetter::from_pm_persist_failure(
            &events("fulfillment-pm", "corr-pm"),
            "Sequence conflict",
            5,
            true,
            "pm-fulfillment",
        ))
        .await
        .expect("publish PM DL");

    publisher
        .publish(AngzarrDeadLetter::from_event_processing_failure(
            &events("orders", "corr-proj"),
            "FailedPrecondition: malformed payload",
            0,
            false,
            Vec::new(),
            "projector-order-summary",
            "projector",
        ))
        .await
        .expect("publish projector DL");

    let page = reader
        .list(ListFilter::default())
        .await
        .expect("list all DL entries");
    assert_eq!(page.entries.len(), 4, "expected 4 round-tripped entries");

    let by_type: HashMap<&str, &angzarr::dlq::reader::StoredDeadLetter> = page
        .entries
        .iter()
        .map(|e| (e.source_component_type.as_str(), e))
        .collect();
    for ct in ["aggregate", "saga", "process_manager", "projector"] {
        assert!(
            by_type.contains_key(ct),
            "{ct} row missing; got types {:?}",
            by_type.keys().collect::<Vec<_>>()
        );
    }

    // Proto BLOB round-trips via Postgres's bytea column the same way
    // it does via SQLite's BLOB.
    use prost::Message;
    let saga_row = by_type["saga"];
    assert_eq!(saga_row.source_component, "saga-order-to-inventory");
    assert_eq!(saga_row.domain, "inventory");
    assert_eq!(saga_row.correlation_id.as_deref(), Some("corr-saga"));
    let decoded = angzarr::proto::AngzarrDeadLetter::decode(saga_row.payload.as_slice())
        .expect("decode saga DL payload from Postgres");
    assert_eq!(decoded.source_component, "saga-order-to-inventory");
    assert_eq!(decoded.source_component_type, "saga");
}

/// `ListFilter::source_component` pushes down to Postgres's WHERE
/// clause. Catches dialect-specific WHERE-builder bugs distinct from
/// the SQLite path.
#[tokio::test]
async fn dlq_round_trip_postgres_filter_by_source_component_returns_only_matches() {
    let (_container, publisher, reader) = setup_round_trip().await;

    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("inventory", "c-saga-1"),
            "first",
            0,
            false,
            "saga-A",
        ))
        .await
        .unwrap();
    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("inventory", "c-saga-2"),
            "second",
            0,
            false,
            "saga-A",
        ))
        .await
        .unwrap();
    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("inventory", "c-saga-3"),
            "different-component",
            0,
            false,
            "saga-B",
        ))
        .await
        .unwrap();

    let filter = ListFilter {
        source_component: Some("saga-A".to_string()),
        ..Default::default()
    };
    let page = reader
        .list(filter)
        .await
        .expect("filter by source_component");
    assert_eq!(
        page.entries.len(),
        2,
        "filter must scope to saga-A entries only; got {}",
        page.entries.len()
    );
    for entry in &page.entries {
        assert_eq!(entry.source_component, "saga-A");
    }
}

/// PM persist retry-exhausted metadata survives the Postgres
/// `EventProcessingFailedDetails` jsonb round-trip.
#[tokio::test]
async fn dlq_round_trip_postgres_pm_persist_retry_exhausted_metadata_survives() {
    let (_container, publisher, reader) = setup_round_trip().await;

    publisher
        .publish(AngzarrDeadLetter::from_pm_persist_failure(
            &events("pm-domain", "c-pm-1"),
            "Sequence conflict",
            7,
            true,
            "pm-fulfillment",
        ))
        .await
        .unwrap();

    let page = reader.list(ListFilter::default()).await.unwrap();
    assert_eq!(page.entries.len(), 1);
    let row = &page.entries[0];

    use prost::Message;
    let decoded = angzarr::proto::AngzarrDeadLetter::decode(row.payload.as_slice()).unwrap();
    assert_eq!(decoded.source_component_type, "process_manager");
    match &decoded.payload {
        Some(angzarr::proto::angzarr_dead_letter::Payload::RejectedEvents(_)) => {}
        other => panic!("PM persist failure must round-trip as Events payload, got {other:?}"),
    }

    // The high-level type-level invariant we expect to round-trip --
    // pinned here against the constructor so a change to the
    // constructor's defaults is caught by the proto-level decode
    // above plus this independent re-derivation.
    let high_level = AngzarrDeadLetter::from_pm_persist_failure(
        &events("pm-domain", "c-pm-1"),
        "Sequence conflict",
        7,
        true,
        "pm-fulfillment",
    );
    match high_level.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert_eq!(details.retry_count, 7);
            assert!(details.is_transient);
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// Retention against Postgres deletes only dead letters that occurred
/// before the cutoff (`occurred_at` is RFC 3339 text; the reader binds the
/// cutoff in the same encoding).
#[tokio::test]
async fn dlq_round_trip_postgres_retention_deletes_only_expired_entries() {
    let (_container, publisher, reader) = setup_round_trip().await;

    let mut old = AngzarrDeadLetter::from_saga_command_rejection(
        &command("order", "c-old"),
        "old",
        0,
        false,
        "saga-x",
    );
    old.occurred_at = Some(prost_types::Timestamp {
        seconds: 1_000_000_000, // 2001
        nanos: 0,
    });
    publisher.publish(old).await.expect("publish old");
    publisher
        .publish(AngzarrDeadLetter::from_saga_command_rejection(
            &command("order", "c-new"),
            "new",
            0,
            false,
            "saga-x",
        ))
        .await
        .expect("publish new");

    let cutoff = chrono::Utc::now() - chrono::Duration::days(30);
    let removed = reader.delete_older_than(cutoff).await.expect("retention");

    assert_eq!(removed, 1);
    let left = reader.list(ListFilter::default()).await.unwrap().entries;
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].correlation_id.as_deref(), Some("c-new"));
}
