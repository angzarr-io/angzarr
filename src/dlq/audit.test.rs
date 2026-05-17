//! Tests for the replay-audit trait + record shape.
//!
//! WHY: audit rows are the durable record operators query when asking
//! "what happened to this dead letter?" The trait contract — `record`
//! never silently drops, `is_configured` correctly signals whether a
//! backend is wired, `source_id` is stable — is what the handler keys
//! off when deciding to log-only vs persist.

use chrono::Utc;

use super::*;

fn sample_record() -> ReplayAuditRecord {
    ReplayAuditRecord {
        dlq_id: 42,
        replayed_at: Utc::now(),
        replay_mode: ReplayMode::FreshSequence,
        new_correlation_id: "new-corr-1".to_string(),
        original_correlation_id: Some("old-corr".to_string()),
        outcome: ReplayOutcome::Success,
        result_message: None,
    }
}

#[test]
fn outcome_string_repr_is_stable() {
    // Stored as TEXT in the audit table; operators / SQL queries
    // reach for these values directly. Pinning the strings catches
    // a silent rename mid-refactor.
    assert_eq!(ReplayOutcome::Success.as_str(), "success");
    assert_eq!(ReplayOutcome::Failure.as_str(), "failure");
}

#[tokio::test]
async fn noop_writer_records_without_error() {
    // Tolerance: noop is success at the writer layer. The handler's
    // tracing event is the live-observability fallback.
    let w = NoopReplayAuditWriter;
    w.record(sample_record()).await.unwrap();
}

#[tokio::test]
async fn noop_writer_is_not_configured() {
    let w = NoopReplayAuditWriter;
    assert!(!w.is_configured());
}

#[tokio::test]
async fn noop_writer_source_id_is_noop() {
    let w = NoopReplayAuditWriter;
    assert_eq!(w.source_id(), "noop");
}

#[test]
fn trait_default_is_configured_returns_true() {
    // Same pattern as the other trait defaults: a real backend that
    // doesn't override `is_configured` is assumed live. Catches the
    // mutation that flips the default to false (silently breaks
    // every real writer).
    struct DefaultsWriter;
    #[async_trait::async_trait]
    impl ReplayAuditWriter for DefaultsWriter {
        async fn record(&self, _r: ReplayAuditRecord) -> Result<(), DlqError> {
            Ok(())
        }
    }
    let w = DefaultsWriter;
    assert!(w.is_configured());
    assert_eq!(w.source_id(), "unknown");
}
