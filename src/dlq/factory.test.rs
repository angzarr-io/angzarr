//! Tests for DLQ publisher factory.
//!
//! The factory uses self-registration (inventory crate) to discover available
//! DLQ backends at compile time. This enables modular backend support without
//! explicit registration.
//!
//! Why this matters: New DLQ backends can be added without modifying the
//! factory code. Feature flags control which backends are compiled in.
//!
//! Key behaviors verified:
//! - Empty config returns noop publisher
//! - Unknown backend types are rejected
//! - Single target returns publisher directly (no chaining overhead)

use super::*;

/// Empty config returns noop publisher.
///
/// No DLQ configured means dead letters are silently discarded.
/// is_configured() returns false to signal this to callers.
#[tokio::test]
async fn test_init_dlq_publisher_empty_config() {
    let config = DlqConfig::default();
    let publisher = init_dlq_publisher(&config).await.unwrap();
    assert!(!publisher.is_configured());
}

// ---------------------------------------------------------------------------
// R2-15 hard-fail boot contract
// ---------------------------------------------------------------------------
//
// When dlq.targets is configured but a target cannot be constructed (unknown
// type, unreachable broker, missing credentials, etc.), `init_dlq_publisher`
// must return Err so the calling bin can fail boot loudly. Falling back to a
// noop or chained-with-noop publisher here would silently drop dead letters
// for the duration of the bin's lifetime -- exactly the failure mode R2-15
// is trying to eliminate.
//
// The contract is enforced by the `?` operator on `create_single_publisher`
// in the factory; these tests pin it so a future refactor that swaps `?`
// for `unwrap_or_else(|_| noop)` (or similar) gets caught immediately.

/// Unknown backend type propagates as `DlqError::UnknownType` up to the
/// caller -- not silently swallowed into a noop publisher. Triggered when
/// an operator typos the dlq_type (e.g., "ampq" for "amqp") or when a
/// build is missing a feature flag that registers the named backend.
#[tokio::test]
async fn init_dlq_publisher_unknown_backend_returns_err() {
    use super::super::config::DlqTargetConfig;

    let config = DlqConfig {
        targets: vec![DlqTargetConfig {
            dlq_type: "no-such-backend".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let result = init_dlq_publisher(&config).await;
    assert!(
        result.is_err(),
        "init_dlq_publisher must hard-fail on an unknown backend type; \
         falling back to noop would silently drop dead letters"
    );
}

/// Unreachable AMQP broker -- the lapin client returns an error from
/// `AmqpDeadLetterPublisher::new`, which the backend's inventory closure
/// surfaces as `Some(Err(...))`, which `create_single_publisher` returns,
/// which `init_dlq_publisher` propagates via `?`. Gated on the `amqp`
/// feature because the AMQP backend's inventory registration is gated
/// the same way; without the feature, the test would hit the
/// UnknownType arm (already covered above) instead of the actual
/// connection-refused path. Uses 127.0.0.1:9999 (no DNS, connection
/// refused in ~ms) rather than a remote unreachable host to keep the
/// test fast even on offline machines.
#[cfg(feature = "amqp")]
#[tokio::test]
async fn init_dlq_publisher_unreachable_amqp_returns_err() {
    use super::super::config::{AmqpDlqConfig, DlqTargetConfig};

    let config = DlqConfig {
        targets: vec![DlqTargetConfig {
            dlq_type: "amqp".to_string(),
            amqp: Some(AmqpDlqConfig {
                url: "amqp://127.0.0.1:9999/".to_string(),
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    let result = init_dlq_publisher(&config).await;
    assert!(
        result.is_err(),
        "init_dlq_publisher must hard-fail when the configured AMQP broker \
         is unreachable; falling back to noop would silently drop dead \
         letters until an operator notices the missing audit trail"
    );
}

// ---------------------------------------------------------------------------
// R2-15 step 8: `init_dlq_reader` contract
// ---------------------------------------------------------------------------
//
// Mirrors the publisher-side hard-fail contract for the read path that the
// `angzarr-status` binary uses to back its DLQ admin UI. Three pinned cases:
//
// 1. `dlq.audit = None` -> noop reader, no error. The bin SHOULD log a
//    WARN, but `init_dlq_reader` itself stays quiet.
// 2. `dlq.audit.storage_type = "sqlite"` with in-memory URI -> real
//    SqliteDlqReader. We use ":memory:" so the test stays hermetic and
//    fast; production operators point this at a file or shared DB.
// 3. `dlq.audit.storage_type = "no-such-db"` -> Err(UnknownType). The bin
//    fails to boot, which is the intended R2-15 behavior.

/// `dlq.audit` unset returns a noop reader without error.
///
/// The noop reader is the safe default for operators who configure
/// publishers but no audit DB. The bin should still log a WARN
/// separately so the admin UI's "always empty" behavior is explained.
/// We verify the noop reader's identity via `is_configured() == false`
/// and `source_id() == "noop"` rather than calling `list()` (which
/// returns `NotConfigured` by design so the status handler can render
/// a degraded panel).
#[tokio::test]
async fn init_dlq_reader_audit_unset_returns_noop_without_error() {
    let reader = init_dlq_reader(None).await.unwrap();
    assert!(
        !reader.is_configured(),
        "audit-unset must return the noop reader (is_configured() == false)"
    );
    assert_eq!(
        reader.source_id(),
        "noop",
        "audit-unset must return the noop reader (source_id() == \"noop\")"
    );
}

/// `dlq.audit.storage_type = "sqlite"` with in-memory URI returns a
/// live SqliteDlqReader. The pool connects synchronously during `new`,
/// so a successful return means the storage layer is reachable.
#[tokio::test]
async fn init_dlq_reader_sqlite_in_memory_returns_live_reader() {
    use super::super::config::DatabaseDlqConfig;
    use crate::storage::config::SqliteConfig;

    let audit = DatabaseDlqConfig {
        storage_type: "sqlite".to_string(),
        sqlite: SqliteConfig { path: None }, // -> sqlite::memory:
        ..Default::default()
    };
    let reader = init_dlq_reader(Some(&audit)).await;
    assert!(
        reader.is_ok(),
        "in-memory SQLite must succeed without external services; got: {:?}",
        reader.err().map(|e| e.to_string())
    );
}

/// Unknown `storage_type` propagates as `DlqError::UnknownType` so the
/// status binary fails to boot rather than silently serving a noop.
/// Mirrors the publisher-side `unknown_backend_returns_err` contract.
#[tokio::test]
async fn init_dlq_reader_unknown_storage_type_returns_err() {
    use super::super::config::DatabaseDlqConfig;

    let audit = DatabaseDlqConfig {
        storage_type: "no-such-db".to_string(),
        ..Default::default()
    };
    let result = init_dlq_reader(Some(&audit)).await;
    assert!(
        result.is_err(),
        "init_dlq_reader must hard-fail on an unknown storage_type; \
         falling back to noop would silently mask configuration errors"
    );
}

// ============================================================================
// init_replay_audit_writer
// ============================================================================

/// Without `dlq.audit` replays are not recorded (no-op writer).
#[tokio::test]
async fn replay_audit_writer_without_audit_is_noop() {
    let writer = init_replay_audit_writer(None).await.unwrap();
    assert!(!writer.is_configured());
}

/// With `dlq.audit` the writer is real and its table exists (migrations
/// applied at construction), so the two-phase fence works: a second
/// pending row with the same idempotency key is a conflict.
#[tokio::test]
async fn replay_audit_writer_sqlite_fences_duplicate_replays() {
    use crate::dlq::{ReplayAuditRecord, ReplayMode, ReplayOutcome};
    use crate::storage::config::SqliteConfig;

    // A file, not `:memory:` — every pooled connection to `:memory:` is a
    // separate database.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("audit.db");
    std::fs::File::create(&path).unwrap();
    let audit = DatabaseDlqConfig {
        storage_type: "sqlite".to_string(),
        sqlite: SqliteConfig {
            path: Some(path.display().to_string()),
        },
        ..Default::default()
    };
    let writer = init_replay_audit_writer(Some(&audit)).await.unwrap();
    assert!(writer.is_configured());

    let record = ReplayAuditRecord {
        dlq_id: 7,
        replayed_at: chrono::Utc::now(),
        replay_mode: ReplayMode::AsIs,
        new_correlation_id: "n".to_string(),
        original_correlation_id: None,
        outcome: ReplayOutcome::Pending,
        result_message: None,
        idempotency_key: "replay-7-k".to_string(),
    };
    writer
        .begin_pending(&record)
        .await
        .expect("first replay recorded");
    let dup = writer.begin_pending(&record).await;
    assert!(matches!(dup, Err(DlqError::Conflict(_))), "{dup:?}");
}

/// An unknown audit backend is a boot error, not a silent no-op.
#[tokio::test]
async fn replay_audit_writer_unknown_type_fails() {
    let audit = DatabaseDlqConfig {
        storage_type: "carrier-pigeon".to_string(),
        ..Default::default()
    };
    assert!(init_replay_audit_writer(Some(&audit)).await.is_err());
}
