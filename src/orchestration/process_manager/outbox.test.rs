//! Tests for the PM command outbox (C04 at-least-once redelivery).
//!
//! The outbox is the mechanism that turns a silently-dropped, transiently
//! failed post-persist command into a redeliverable entry. These tests pin:
//! - the dedup key is stable and derived from `angzarr_deferred` provenance,
//!   so a handler re-run collapses onto one entry;
//! - the in-memory store's enqueue/pending/record/remove semantics, including
//!   the idempotent-on-duplicate contract;
//! - the drain state machine: success removes, transient-within-budget stays
//!   pending, budget exhaustion moves to the DLQ, permanent rejection moves to
//!   the DLQ immediately.

use super::*;

use crate::dlq::{DlqError, RejectionDetails};
use crate::proto::command_page::Payload as CmdPayload;
use crate::proto::page_header::SequenceType;
use crate::proto::{
    AngzarrDeferredSequence, CommandBook, CommandPage, CommandResponse, Cover, MergeStrategy,
    PageHeader, Uuid as ProtoUuid,
};

/// Build a command stamped with an `angzarr_deferred` provenance tuple, the
/// shape `execute_pm_commands` produces for a PM command.
fn deferred_command(
    domain: &str,
    correlation: &str,
    source_component: &str,
    command_index: u32,
    source_seq: u32,
) -> CommandBook {
    let source = Cover {
        domain: "pm".to_string(),
        root: Some(ProtoUuid {
            value: vec![7u8; 16],
        }),
        correlation_id: correlation.to_string(),
        edition: None,
        ext: None,
    };
    let header = PageHeader {
        sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
            source: Some(source),
            source_seq,
            source_component: source_component.to_string(),
            command_index,
        })),
        sync_mode: None,
    };
    let page = CommandPage {
        header: Some(header),
        merge_strategy: MergeStrategy::MergeCommutative as i32,
        payload: Some(CmdPayload::Command(prost_types::Any {
            type_url: "test.PmCommand".to_string(),
            value: vec![],
        })),
    };
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: None,
            correlation_id: correlation.to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![page],
    }
}

/// Command with an explicit destination sequence (no `angzarr_deferred`) —
/// exercises the dedup-key fallback.
fn explicit_seq_command(domain: &str, correlation: &str, seq: u32) -> CommandBook {
    let header = PageHeader {
        sequence_type: Some(SequenceType::Sequence(seq)),
        sync_mode: None,
    };
    let page = CommandPage {
        header: Some(header),
        merge_strategy: MergeStrategy::MergeCommutative as i32,
        payload: Some(CmdPayload::Command(prost_types::Any {
            type_url: "test.PmCommand".to_string(),
            value: vec![],
        })),
    };
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: None,
            correlation_id: correlation.to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![page],
    }
}

// -- Executors -------------------------------------------------------------

struct AlwaysSuccess;
#[async_trait]
impl CommandExecutor for AlwaysSuccess {
    async fn execute(&self, _cmd: CommandBook, _mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
}

struct AlwaysRetryable;
#[async_trait]
impl CommandExecutor for AlwaysRetryable {
    async fn execute(&self, _cmd: CommandBook, _mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Retryable {
            reason: "broker down".to_string(),
            current_state: None,
        }
    }
}

struct AlwaysRejected;
#[async_trait]
impl CommandExecutor for AlwaysRejected {
    async fn execute(&self, _cmd: CommandBook, _mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Rejected {
            code: tonic::Code::InvalidArgument,
            message: "schema mismatch".to_string(),
        }
    }
}

// -- DLQ sink --------------------------------------------------------------

struct CapturingDlq {
    captured: tokio::sync::Mutex<Vec<AngzarrDeadLetter>>,
}
impl CapturingDlq {
    fn new() -> Self {
        Self {
            captured: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}
#[async_trait]
impl DeadLetterPublisher for CapturingDlq {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        self.captured.lock().await.push(dead_letter);
        Ok(())
    }
}

// -- dedup key -------------------------------------------------------------

/// The dedup key of an `angzarr_deferred`-stamped command includes the source
/// component and command index, so two sibling commands of one invocation get
/// distinct keys while a re-run of the same command collapses.
#[tokio::test]
async fn dedup_key_uses_deferred_provenance_and_disambiguates_siblings() {
    let cmd0 = deferred_command("fulfillment", "corr-1", "pm-x", 0, 5);
    let cmd1 = deferred_command("fulfillment", "corr-1", "pm-x", 1, 5);

    let k0 = dedup_key_for(&cmd0);
    let k1 = dedup_key_for(&cmd1);

    assert_ne!(k0, k1, "sibling commands (different index) must not collide");
    assert_eq!(
        k0,
        dedup_key_for(&deferred_command("fulfillment", "corr-1", "pm-x", 0, 5)),
        "the same command must produce the same key (idempotent re-enqueue)"
    );
    assert!(
        k0.ends_with("#pm-x#0"),
        "key must carry source_component and command_index, got {k0}"
    );
}

/// A command with an explicit destination sequence (no provenance) falls back
/// to a domain/correlation/sequence key rather than panicking.
#[tokio::test]
async fn dedup_key_falls_back_for_explicit_sequence_commands() {
    let cmd = explicit_seq_command("table", "corr-9", 3);
    assert_eq!(dedup_key_for(&cmd), "table:corr-9:3");
}

// -- in-memory store -------------------------------------------------------

/// Enqueue then read back; a duplicate enqueue is a no-op that preserves the
/// existing entry's attempt count.
#[tokio::test]
async fn in_memory_enqueue_is_idempotent_on_dedup_key() {
    let outbox = InMemoryCommandOutbox::new();
    let cmd = deferred_command("fulfillment", "corr-1", "pm-x", 0, 5);
    let entry = OutboxEntry::for_redelivery(&cmd, "broker down");
    let key = entry.dedup_key.clone();

    outbox.enqueue(entry).await.unwrap();
    // Simulate one failed drain attempt so the stored entry has progress.
    outbox.record_attempt(&key, "still down").await.unwrap();

    // A duplicate enqueue (e.g. handler re-run) must NOT reset that progress.
    outbox
        .enqueue(OutboxEntry::for_redelivery(&cmd, "broker down"))
        .await
        .unwrap();

    let pending = outbox.pending().await.unwrap();
    assert_eq!(pending.len(), 1, "duplicate enqueue must not add a second entry");
    assert_eq!(pending[0].attempts, 1, "duplicate enqueue must not reset attempts");
    assert_eq!(pending[0].last_error, "still down");
}

/// `record_attempt` on an absent (already-settled) key is a silent no-op.
#[tokio::test]
async fn in_memory_record_attempt_absent_key_is_noop() {
    let outbox = InMemoryCommandOutbox::new();
    outbox.record_attempt("nope", "err").await.unwrap();
    assert!(outbox.pending().await.unwrap().is_empty());
}

/// `remove` deletes the entry.
#[tokio::test]
async fn in_memory_remove_deletes_entry() {
    let outbox = InMemoryCommandOutbox::new();
    let cmd = deferred_command("fulfillment", "corr-1", "pm-x", 0, 5);
    let entry = OutboxEntry::for_redelivery(&cmd, "broker down");
    let key = entry.dedup_key.clone();
    outbox.enqueue(entry).await.unwrap();
    outbox.remove(&key).await.unwrap();
    assert!(outbox.pending().await.unwrap().is_empty());
}

// -- drain state machine ---------------------------------------------------

/// Successful redelivery removes the entry and counts it delivered.
#[tokio::test]
async fn drain_success_removes_entry() {
    let outbox = InMemoryCommandOutbox::new();
    let cmd = deferred_command("fulfillment", "corr-1", "pm-x", 0, 5);
    outbox
        .enqueue(OutboxEntry::for_redelivery(&cmd, "broker down"))
        .await
        .unwrap();

    let stats = drain_once(&outbox, &AlwaysSuccess, None, "pm-x", 5, SyncMode::Simple)
        .await
        .unwrap();

    assert_eq!(stats, DrainStats { delivered: 1, ..Default::default() });
    assert!(
        outbox.pending().await.unwrap().is_empty(),
        "a delivered command must be removed from the outbox"
    );
}

/// A transient failure within budget keeps the entry pending and increments its
/// attempt count — the essence of at-least-once redelivery.
#[tokio::test]
async fn drain_transient_within_budget_keeps_pending_and_counts_attempt() {
    let outbox = InMemoryCommandOutbox::new();
    let cmd = deferred_command("fulfillment", "corr-1", "pm-x", 0, 5);
    outbox
        .enqueue(OutboxEntry::for_redelivery(&cmd, "broker down"))
        .await
        .unwrap();
    let dlq: Arc<dyn DeadLetterPublisher> = Arc::new(CapturingDlq::new());

    let stats = drain_once(&outbox, &AlwaysRetryable, Some(&dlq), "pm-x", 3, SyncMode::Simple)
        .await
        .unwrap();

    assert_eq!(stats, DrainStats { retried: 1, ..Default::default() });
    let pending = outbox.pending().await.unwrap();
    assert_eq!(pending.len(), 1, "a within-budget transient failure stays pending");
    assert_eq!(pending[0].attempts, 1, "the failed attempt is recorded");
    assert_eq!(pending[0].last_error, "broker down");
}

/// When the redelivery budget is spent, the entry moves to the DLQ (marked
/// transient) and is removed from the outbox.
#[tokio::test]
async fn drain_exhausted_budget_moves_to_dlq_transient() {
    let outbox = InMemoryCommandOutbox::new();
    let cmd = deferred_command("fulfillment", "corr-1", "pm-x", 0, 5);
    // Pre-load the entry as already having 2 attempts; with max_attempts=3 the
    // next failure (attempts_after=3) exhausts the budget.
    let mut entry = OutboxEntry::for_redelivery(&cmd, "broker down");
    entry.attempts = 2;
    outbox.enqueue(entry).await.unwrap();

    let capturing = Arc::new(CapturingDlq::new());
    let dlq: Arc<dyn DeadLetterPublisher> = capturing.clone();

    let stats = drain_once(&outbox, &AlwaysRetryable, Some(&dlq), "pm-x", 3, SyncMode::Simple)
        .await
        .unwrap();

    assert_eq!(stats, DrainStats { dead_lettered: 1, ..Default::default() });
    assert!(
        outbox.pending().await.unwrap().is_empty(),
        "an exhausted entry must be removed after DLQ"
    );
    let captured = capturing.captured.lock().await;
    assert_eq!(captured.len(), 1, "exhaustion publishes exactly one DLQ entry");
    match &captured[0].rejection_details {
        Some(RejectionDetails::EventProcessingFailed(d)) => {
            assert!(d.is_transient, "budget exhaustion is a transient failure");
            assert_eq!(d.retry_count, 3, "reports total redelivery attempts");
            assert!(d.error.contains("broker down"));
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
    assert_eq!(captured[0].source_component, "pm-x");
    assert_eq!(captured[0].source_component_type, "process_manager");
}

/// A permanent rejection on redelivery goes straight to the DLQ (not
/// transient) — retrying a rejected command would just re-hit the rejection.
#[tokio::test]
async fn drain_permanent_rejection_moves_to_dlq_immediately() {
    let outbox = InMemoryCommandOutbox::new();
    let cmd = deferred_command("fulfillment", "corr-1", "pm-x", 0, 5);
    outbox
        .enqueue(OutboxEntry::for_redelivery(&cmd, "broker down"))
        .await
        .unwrap();

    let capturing = Arc::new(CapturingDlq::new());
    let dlq: Arc<dyn DeadLetterPublisher> = capturing.clone();

    let stats = drain_once(&outbox, &AlwaysRejected, Some(&dlq), "pm-x", 5, SyncMode::Simple)
        .await
        .unwrap();

    assert_eq!(stats, DrainStats { dead_lettered: 1, ..Default::default() });
    assert!(outbox.pending().await.unwrap().is_empty());
    let captured = capturing.captured.lock().await;
    assert_eq!(captured.len(), 1);
    match &captured[0].rejection_details {
        Some(RejectionDetails::EventProcessingFailed(d)) => {
            assert!(!d.is_transient, "a permanent rejection is not transient");
            assert!(d.error.contains("schema mismatch"));
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// Exhaustion with no DLQ wired still removes the entry (no publisher to sink
/// to) and does not panic.
#[tokio::test]
async fn drain_exhausted_without_dlq_still_removes() {
    let outbox = InMemoryCommandOutbox::new();
    let cmd = deferred_command("fulfillment", "corr-1", "pm-x", 0, 5);
    outbox
        .enqueue(OutboxEntry::for_redelivery(&cmd, "broker down"))
        .await
        .unwrap();

    let stats = drain_once(&outbox, &AlwaysRetryable, None, "pm-x", 1, SyncMode::Simple)
        .await
        .unwrap();

    assert_eq!(stats, DrainStats { dead_lettered: 1, ..Default::default() });
    assert!(outbox.pending().await.unwrap().is_empty());
}

/// Empty outbox drains to an all-zero tally.
#[tokio::test]
async fn drain_empty_outbox_is_noop() {
    let outbox = InMemoryCommandOutbox::new();
    let stats = drain_once(&outbox, &AlwaysSuccess, None, "pm-x", 5, SyncMode::Simple)
        .await
        .unwrap();
    assert_eq!(stats, DrainStats::default());
}
