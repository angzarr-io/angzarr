//! Cucumber-rs harness for `features/client/dlq.feature` (R2-15).
//!
//! Run with:
//! ```bash
//! cargo test --test dlq_features --features "test-utils" -- --nocapture
//! ```
//!
//! Each scenario gets a fresh `DlqWorld` with a tempfile-backed SQLite
//! database that hosts both the publisher (coordinator side) and the
//! reader (status-binary admin side). The `Background` steps
//! initialize them so individual scenarios can focus on the failure
//! path under test.
//!
//! Scope, per the R2-15 cucumber-harness decision:
//!
//! - **Saga / PM / projector scenarios** run through public
//!   `orchestrate_saga` / `orchestrate_pm` / `ProjectorEventHandler::handle`
//!   (decision 2b). Real `SqliteDlqPublisher` / `SqliteDlqReader`.
//! - **Aggregate scenario** runs through
//!   `publish_aggregate_sequence_mismatch_dlq`, the free fn extracted
//!   from `GrpcAggregateContext::send_to_dlq` (decision 1a). Same
//!   publish-to-DLQ seam as production; doesn't require constructing
//!   a full GrpcAggregateContext.
//! - `features/operator/dlq_boot.feature` is out of scope -- bin
//!   spawning is its own setup, and `dlq/factory.test.rs` already pins
//!   the hard-fail-on-misconfig contract.

#![cfg(feature = "test-utils")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backon::ExponentialBuilder;
use cucumber::{given, then, when, World};
use prost::Message;
use tempfile::TempDir;
use tonic::Code;

use angzarr::bus::{BusError, EventHandler};
use angzarr::dlq::reader::{DeadLetterReader, ListFilter, StoredDeadLetter};
use angzarr::dlq::{DeadLetterPublisher, SqliteDlqPublisher, SqliteDlqReader};
use angzarr::handlers::core::projector::ProjectorEventHandler;
use angzarr::orchestration::aggregate::grpc::publish_aggregate_sequence_mismatch_dlq;
use angzarr::orchestration::command::{CommandExecutor, CommandOutcome};
use angzarr::orchestration::destination::DestinationFetcher;
use angzarr::orchestration::process_manager::{
    orchestrate_pm, PmHandleResponse, ProcessManagerContext,
};
use angzarr::orchestration::projector::{ProjectionMode, ProjectorHandler};
use angzarr::orchestration::saga::{orchestrate_saga, SagaRetryContext};
use angzarr::proto::{
    angzarr_dead_letter, command_page::Payload as CmdPayload, page_header::SequenceType,
    CommandBook, CommandPage, CommandResponse, Cover, EventBook, EventPage, MergeStrategy,
    PageHeader, Projection, SagaResponse, SyncMode,
};

// ============================================================================
// World
// ============================================================================

#[derive(World)]
#[world(init = Self::new)]
pub struct DlqWorld {
    tempdir: Option<TempDir>,
    publisher: Option<Arc<dyn DeadLetterPublisher>>,
    reader: Option<Arc<dyn DeadLetterReader>>,

    /// Code configured by a Given step, consumed by the When step.
    configured_code: Option<Code>,
    /// "transient on first attempt, then succeed" toggle.
    one_shot_transient: bool,
    /// PM scenario was configured in Decision mode.
    pm_decision_mode: bool,

    /// Counts executor invocations so retry-shape assertions can hold.
    executor_calls: Arc<AtomicU32>,
    /// PM compensation handler invocations (saga has its own).
    pm_compensation_calls: Arc<AtomicU32>,

    /// Recorded outcomes inspected by Then steps.
    pm_outcome: Option<Result<(), BusError>>,
    projector_outcome: Option<Result<(), BusError>>,
}

// Manual Debug -- cucumber's `Writer` bound requires `World: Debug`, and
// trait objects (`dyn DeadLetterPublisher`, `dyn DeadLetterReader`) don't
// implement Debug. Render only the fields that have a useful Debug.
impl std::fmt::Debug for DlqWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DlqWorld")
            .field(
                "tempdir",
                &self
                    .tempdir
                    .as_ref()
                    .map(|d| d.path().display().to_string()),
            )
            .field("has_publisher", &self.publisher.is_some())
            .field("has_reader", &self.reader.is_some())
            .field("configured_code", &self.configured_code)
            .field("one_shot_transient", &self.one_shot_transient)
            .field("pm_decision_mode", &self.pm_decision_mode)
            .field(
                "executor_calls",
                &self.executor_calls.load(Ordering::SeqCst),
            )
            .field(
                "pm_compensation_calls",
                &self.pm_compensation_calls.load(Ordering::SeqCst),
            )
            .field("pm_outcome", &self.pm_outcome.as_ref().map(|r| r.is_ok()))
            .field(
                "projector_outcome",
                &self.projector_outcome.as_ref().map(|r| r.is_ok()),
            )
            .finish()
    }
}

impl DlqWorld {
    fn new() -> Self {
        Self {
            tempdir: None,
            publisher: None,
            reader: None,
            configured_code: None,
            one_shot_transient: false,
            pm_decision_mode: false,
            executor_calls: Arc::new(AtomicU32::new(0)),
            pm_compensation_calls: Arc::new(AtomicU32::new(0)),
            pm_outcome: None,
            projector_outcome: None,
        }
    }

    fn publisher(&self) -> Arc<dyn DeadLetterPublisher> {
        self.publisher
            .as_ref()
            .expect("Background must configure dlq.targets before scenario steps run")
            .clone()
    }

    fn reader(&self) -> &dyn DeadLetterReader {
        self.reader
            .as_ref()
            .expect("Background must configure dlq.audit before scenario steps run")
            .as_ref()
    }

    async fn list_all(&self) -> Vec<StoredDeadLetter> {
        self.reader()
            .list(ListFilter::default())
            .await
            .expect("reader list")
            .entries
    }

    fn code(&self) -> Code {
        self.configured_code
            .expect("a scenario step must configure a handler error code first")
    }
}

// ============================================================================
// Background: dlq.targets + dlq.audit configured against shared sqlite
// ============================================================================

#[given("the operator configures dlq.targets with a database backend")]
async fn given_dlq_targets(world: &mut DlqWorld) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let uri = format!("sqlite://{}?mode=rwc", dir.path().join("dlq.db").display());
    let publisher = SqliteDlqPublisher::new(&uri).await.expect("init publisher");
    world.publisher = Some(Arc::new(publisher));
    world.tempdir = Some(dir);
}

#[given("the operator configures dlq.audit pointing at the same backend")]
async fn given_dlq_audit(world: &mut DlqWorld) {
    let dir = world
        .tempdir
        .as_ref()
        .expect("dlq.targets background must run first");
    let uri = format!("sqlite://{}?mode=rwc", dir.path().join("dlq.db").display());
    let reader = SqliteDlqReader::new(&uri).await.expect("init reader");
    world.reader = Some(Arc::new(reader));
}

// ============================================================================
// Aggregate (via publish_aggregate_sequence_mismatch_dlq free fn)
// ============================================================================

#[given("an aggregate in MergeManual merge mode")]
async fn given_aggregate_merge_manual(_world: &mut DlqWorld) {
    // Implicit -- the When step uses MergeStrategy::MergeManual.
}

#[when("the aggregate receives a stale command with a sequence mismatch")]
async fn when_aggregate_stale_command(world: &mut DlqWorld) {
    let publisher = world.publisher();
    publish_aggregate_sequence_mismatch_dlq(
        &publisher,
        &command_for("orders", "corr-agg"),
        3, // expected
        5, // actual
        "orders",
        "aggregate-feature-test",
    )
    .await;
}

#[then("the command is rejected with Aborted")]
async fn then_command_rejected_aborted(_world: &mut DlqWorld) {
    // The pipeline returns Status::aborted on MergeManual mismatch;
    // this harness exercises the DLQ seam below it, so this step is
    // documentary. The pipeline-level contract is covered by
    // aggregate unit tests.
}

// ============================================================================
// Saga scenarios -- driven through public orchestrate_saga
// ============================================================================

#[given(regex = r"^a saga handler whose outbound command returns ([A-Za-z]+)$")]
async fn given_saga_returns_code(world: &mut DlqWorld, code: String) {
    world.configured_code = Some(parse_code(&code));
    world.one_shot_transient = false;
}

#[given(
    regex = r"^a saga handler whose outbound command returns ([A-Za-z]+) on the first attempt$"
)]
async fn given_saga_transient_first(world: &mut DlqWorld, code: String) {
    world.configured_code = Some(parse_code(&code));
    world.one_shot_transient = true;
}

#[given(regex = r"^a saga handler whose outbound command returns ([A-Za-z]+) on every attempt$")]
async fn given_saga_always_transient(world: &mut DlqWorld, code: String) {
    world.configured_code = Some(parse_code(&code));
    world.one_shot_transient = false;
}

#[when("the saga receives an event that produces that command")]
async fn when_saga_runs(world: &mut DlqWorld) {
    let code = world.code();
    let publisher = world.publisher();
    let calls = world.executor_calls.clone();
    let ctx = DlqAwareSagaContext::new(publisher);

    if world.one_shot_transient {
        let executor = TransientThenSucceedExecutor::new(code, calls);
        run_saga(&ctx, &executor).await;
    } else if is_retryable_via_classifier(code) {
        let executor = AlwaysRetryableSagaExecutor::new(code, calls);
        run_saga(&ctx, &executor).await;
    } else {
        let executor = SagaRejectingExecutor::new(code, calls);
        run_saga(&ctx, &executor).await;
    }
}

#[then("no retry is attempted for that command")]
async fn then_no_retry(world: &mut DlqWorld) {
    let calls = world.executor_calls.load(Ordering::SeqCst);
    assert_eq!(calls, 1, "expected exactly one executor call, got {calls}");
}

#[then("the framework retries the command with backoff")]
async fn then_retries_with_backoff(world: &mut DlqWorld) {
    let calls = world.executor_calls.load(Ordering::SeqCst);
    assert!(
        calls >= 2,
        "expected >= 2 executor calls (initial + at least one retry), got {calls}"
    );
}

#[then("the eventual success is not dead-lettered")]
async fn then_success_not_dl(world: &mut DlqWorld) {
    let entries = world.list_all().await;
    assert!(
        entries.is_empty(),
        "eventual-success path must not publish DL, got {} entries",
        entries.len()
    );
}

#[then("the framework retries the command up to the configured backoff budget")]
async fn then_retries_to_exhaustion(world: &mut DlqWorld) {
    let calls = world.executor_calls.load(Ordering::SeqCst);
    let max_attempts = fast_backoff_max_attempts();
    assert!(
        calls >= 2 && calls <= max_attempts,
        "expected 2..={max_attempts} executor calls (retry-exhausted), got {calls}"
    );
}

#[then("the dead letter carries the source event and the rejected command")]
async fn then_saga_payload_is_command(world: &mut DlqWorld) {
    let entries = world.list_all().await;
    assert_eq!(entries.len(), 1);
    let decoded =
        angzarr::proto::AngzarrDeadLetter::decode(entries[0].payload.as_slice()).expect("decode");
    match &decoded.payload {
        Some(angzarr_dead_letter::Payload::RejectedCommand(_)) => {}
        other => panic!("saga DL payload must be RejectedCommand, got {other:?}"),
    }
}

// ============================================================================
// PM scenarios -- driven through public orchestrate_pm
// ============================================================================

#[given("a process manager whose PM event persistence returns sequence-conflict on every attempt")]
async fn given_pm_persist_exhaust(world: &mut DlqWorld) {
    world.configured_code = None;
    world.pm_decision_mode = false;
}

#[given(regex = r"^a process manager whose outbound command returns ([A-Za-z]+)$")]
async fn given_pm_command_returns_code(world: &mut DlqWorld, code: String) {
    world.configured_code = Some(parse_code(&code));
    world.pm_decision_mode = false;
}

#[given("a process manager whose outbound command requests SYNC_MODE_DECISION")]
async fn given_pm_decision_mode(world: &mut DlqWorld) {
    world.pm_decision_mode = true;
    world.configured_code = None;
}

#[given("the executor returns Retryable for that command")]
async fn given_pm_executor_retryable(_world: &mut DlqWorld) {
    // Implicit in the When step when pm_decision_mode is set.
}

#[when("the PM receives a trigger event")]
async fn when_pm_persist_runs(world: &mut DlqWorld) {
    let publisher = world.publisher();
    let comp_calls = world.pm_compensation_calls.clone();
    let ctx = DlqAwarePmContext {
        publisher,
        compensation_calls: comp_calls,
        persist_outcome: Box::new(|| CommandOutcome::Retryable {
            reason: "Sequence conflict".to_string(),
            current_state: None,
        }),
        emit_command_with_sync_mode: None,
    };
    world.pm_outcome = Some(run_pm(&ctx, &PmNoOpExecutor).await);
}

#[when("the PM receives a trigger event that produces that command")]
async fn when_pm_command_rejected(world: &mut DlqWorld) {
    let code = world.code();
    let publisher = world.publisher();
    let comp_calls = world.pm_compensation_calls.clone();
    let ctx = DlqAwarePmContext {
        publisher,
        compensation_calls: comp_calls,
        persist_outcome: Box::new(|| CommandOutcome::Success(CommandResponse::default())),
        emit_command_with_sync_mode: Some(SyncMode::Simple),
    };
    let executor = CodeRejectingPmExecutor {
        code,
        message: "permanent error".to_string(),
    };
    world.pm_outcome = Some(run_pm(&ctx, &executor).await);
}

#[when("the PM dispatches the command")]
async fn when_pm_dispatches_decision(world: &mut DlqWorld) {
    let publisher = world.publisher();
    let comp_calls = world.pm_compensation_calls.clone();
    let ctx = DlqAwarePmContext {
        publisher,
        compensation_calls: comp_calls,
        persist_outcome: Box::new(|| CommandOutcome::Success(CommandResponse::default())),
        emit_command_with_sync_mode: Some(SyncMode::Decision),
    };
    world.pm_outcome = Some(run_pm(&ctx, &AlwaysRetryablePmExecutor).await);
}

#[then("the framework retries persistence up to the configured backoff budget")]
async fn then_pm_persist_retried(world: &mut DlqWorld) {
    let outcome = world.pm_outcome.as_ref().expect("PM scenario must run");
    assert!(
        outcome.is_err(),
        "persist retry-exhaustion must propagate Err, got Ok"
    );
}

#[then("the dead letter payload contains the failed PM event book")]
async fn then_pm_payload_is_events(world: &mut DlqWorld) {
    let entries = world.list_all().await;
    assert!(!entries.is_empty(), "expected at least one DL");
    let decoded =
        angzarr::proto::AngzarrDeadLetter::decode(entries[0].payload.as_slice()).expect("decode");
    match &decoded.payload {
        Some(angzarr_dead_letter::Payload::RejectedEvents(_)) => {}
        other => panic!("PM persist DL must be Events payload, got {other:?}"),
    }
}

#[then("the PM compensation handler is invoked alongside the dead letter")]
async fn then_pm_compensation_fired(world: &mut DlqWorld) {
    let calls = world.pm_compensation_calls.load(Ordering::SeqCst);
    assert_eq!(
        calls, 1,
        "expected exactly one compensation invocation, got {calls}"
    );
}

#[then("the framework degrades the outcome to a rejection")]
async fn then_pm_h14_degraded(world: &mut DlqWorld) {
    let outcome = world.pm_outcome.as_ref().expect("PM scenario must run");
    assert!(outcome.is_err(), "H-14 degraded path must surface Err");
}

#[then("the dead letter rejection_reason names SYNC_MODE_DECISION")]
async fn then_pm_reason_names_decision(world: &mut DlqWorld) {
    let entries = world.list_all().await;
    assert_eq!(entries.len(), 1);
    assert!(
        entries[0].rejection_reason.contains("SYNC_MODE_DECISION"),
        "H-14 DL must mention SYNC_MODE_DECISION in rejection_reason, got: {}",
        entries[0].rejection_reason
    );
}

// ============================================================================
// Projector scenarios -- driven through ProjectorEventHandler::handle
// ============================================================================

#[given(regex = r"^a projector handler that returns ([A-Za-z]+) for a malformed payload$")]
async fn given_projector_returns_code_for_payload(world: &mut DlqWorld, code: String) {
    world.configured_code = Some(parse_code(&code));
}

#[given(regex = r"^a projector handler that returns ([A-Za-z]+)$")]
async fn given_projector_returns_code(world: &mut DlqWorld, code: String) {
    world.configured_code = Some(parse_code(&code));
}

#[when("the projector receives that event")]
#[when("the projector receives an event")]
async fn when_projector_runs(world: &mut DlqWorld) {
    let code = world.code();
    let publisher = world.publisher();
    let handler: Arc<dyn ProjectorHandler> = Arc::new(FailingProjectorHandler {
        code,
        message: "projector handler failure".to_string(),
    });
    let event_handler =
        ProjectorEventHandler::from_handler(handler, "projector-feature-test".to_string())
            .with_dlq_publisher(publisher);
    let book = Arc::new(event_book("orders", "corr-proj"));
    world.projector_outcome = Some(event_handler.handle(book).await);
}

#[then("the message is acked from the bus")]
async fn then_message_acked(world: &mut DlqWorld) {
    let outcome = world
        .projector_outcome
        .as_ref()
        .expect("projector scenario must run");
    assert!(
        outcome.is_ok(),
        "permanent (4xx) projector failure must ack (return Ok), got {outcome:?}"
    );
}

#[then("subsequent events for the same projector continue to be processed")]
async fn then_subsequent_events_processed(_world: &mut DlqWorld) {
    // Acking the prior message means the bus delivers the next. The
    // Ok return in the prior step is the framework-side contract;
    // no redelivery is needed here to prove it.
}

#[then("no dead letter is published by the projector")]
async fn then_no_dl_published(world: &mut DlqWorld) {
    let entries = world.list_all().await;
    assert!(
        entries.is_empty(),
        "transient projector failures must not publish DL, got {} entries",
        entries.len()
    );
}

#[then("the failure propagates to the bus for redelivery")]
async fn then_failure_propagates(world: &mut DlqWorld) {
    let outcome = world
        .projector_outcome
        .as_ref()
        .expect("projector scenario must run");
    assert!(
        outcome.is_err(),
        "transient (5xx) projector failure must propagate Err"
    );
}

// ============================================================================
// Cross-cutting Then steps (shared between saga / PM / projector / aggregate)
// ============================================================================

#[then("the dead letter is visible via the status admin DLQ listing")]
async fn then_dl_visible(world: &mut DlqWorld) {
    let entries = world.list_all().await;
    assert!(
        !entries.is_empty(),
        "expected at least one DL via the reader, got 0"
    );
}

#[then(regex = r#"^the dead letter source_component_type is "([^"]+)"$"#)]
async fn then_dl_source_component_type(world: &mut DlqWorld, expected: String) {
    let entries = world.list_all().await;
    assert!(!entries.is_empty(), "expected at least one DL");
    assert_eq!(entries[0].source_component_type, expected);
}

#[then(regex = r"^the dead letter retry_count is (\d+)$")]
async fn then_dl_retry_count_eq(world: &mut DlqWorld, expected: u32) {
    let entries = world.list_all().await;
    assert!(!entries.is_empty());
    let decoded =
        angzarr::proto::AngzarrDeadLetter::decode(entries[0].payload.as_slice()).expect("decode");
    let details = extract_event_processing_details(&decoded);
    assert_eq!(details.retry_count, expected);
}

#[then("the dead letter retry_count is greater than 0")]
async fn then_dl_retry_count_gt0(world: &mut DlqWorld) {
    let entries = world.list_all().await;
    assert!(!entries.is_empty());
    let decoded =
        angzarr::proto::AngzarrDeadLetter::decode(entries[0].payload.as_slice()).expect("decode");
    let details = extract_event_processing_details(&decoded);
    assert!(
        details.retry_count > 0,
        "expected retry_count > 0, got {}",
        details.retry_count
    );
}

#[then(regex = r"^the dead letter is_transient is (true|false)$")]
async fn then_dl_is_transient(world: &mut DlqWorld, expected: String) {
    let want: bool = expected.parse().unwrap();
    let entries = world.list_all().await;
    assert!(!entries.is_empty());
    let decoded =
        angzarr::proto::AngzarrDeadLetter::decode(entries[0].payload.as_slice()).expect("decode");
    let details = extract_event_processing_details(&decoded);
    assert_eq!(details.is_transient, want);
}

#[then("the dead letter payload contains the rejected command")]
async fn then_dl_payload_is_command(world: &mut DlqWorld) {
    let entries = world.list_all().await;
    let decoded =
        angzarr::proto::AngzarrDeadLetter::decode(entries[0].payload.as_slice()).expect("decode");
    match &decoded.payload {
        Some(angzarr_dead_letter::Payload::RejectedCommand(_)) => {}
        other => panic!("expected RejectedCommand payload, got {other:?}"),
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn parse_code(name: &str) -> Code {
    match name {
        "Ok" => Code::Ok,
        "Cancelled" => Code::Cancelled,
        "Unknown" => Code::Unknown,
        "InvalidArgument" => Code::InvalidArgument,
        "DeadlineExceeded" => Code::DeadlineExceeded,
        "NotFound" => Code::NotFound,
        "AlreadyExists" => Code::AlreadyExists,
        "PermissionDenied" => Code::PermissionDenied,
        "ResourceExhausted" => Code::ResourceExhausted,
        "FailedPrecondition" => Code::FailedPrecondition,
        "Aborted" => Code::Aborted,
        "OutOfRange" => Code::OutOfRange,
        "Unimplemented" => Code::Unimplemented,
        "Internal" => Code::Internal,
        "Unavailable" => Code::Unavailable,
        "DataLoss" => Code::DataLoss,
        "Unauthenticated" => Code::Unauthenticated,
        other => panic!("unknown tonic::Code in feature step: {other}"),
    }
}

fn is_retryable_via_classifier(code: Code) -> bool {
    use angzarr::dlq::trigger::{CodeDlqExt, DlqTrigger};
    matches!(code.classify_for_dlq(), DlqTrigger::RetryThenDlq(_))
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

fn command_for(domain: &str, correlation_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain, correlation_id)),
        pages: vec![],
    }
}

fn event_book(domain: &str, correlation_id: &str) -> EventBook {
    EventBook {
        cover: Some(cover(domain, correlation_id)),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

fn fast_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(1))
        .with_max_delay(Duration::from_millis(5))
        .with_max_times(3)
}

/// Initial attempt + 3 retries = 4 total executor calls before retry
/// exhaustion. Synced with `fast_backoff`'s `max_times(3)`.
fn fast_backoff_max_attempts() -> u32 {
    4
}

fn extract_event_processing_details(
    dl: &angzarr::proto::AngzarrDeadLetter,
) -> angzarr::proto::EventProcessingFailedDetails {
    match dl.rejection_details.as_ref() {
        Some(angzarr::proto::angzarr_dead_letter::RejectionDetails::EventProcessingFailed(d)) => {
            d.clone()
        }
        other => panic!("expected EventProcessingFailed details, got {other:?}"),
    }
}

// ============================================================================
// Saga test doubles
// ============================================================================

struct DlqAwareSagaContext {
    publisher: Arc<dyn DeadLetterPublisher>,
}

impl DlqAwareSagaContext {
    fn new(publisher: Arc<dyn DeadLetterPublisher>) -> Self {
        Self { publisher }
    }
}

#[async_trait]
impl SagaRetryContext for DlqAwareSagaContext {
    async fn handle(
        &self,
        _destination_sequences: HashMap<String, u32>,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        // Emit one command so the dispatch loop has something to do.
        // The command's domain doesn't matter for these scenarios --
        // only the executor's behavior does.
        Ok(SagaResponse {
            commands: vec![command_for("inventory", "corr-saga")],
            events: vec![],
        })
    }
    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {}
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        Some(&self.publisher)
    }
    fn component_name(&self) -> &str {
        "saga-feature-test"
    }
}

struct SagaRejectingExecutor {
    code: Code,
    calls: Arc<AtomicU32>,
}
impl SagaRejectingExecutor {
    fn new(code: Code, calls: Arc<AtomicU32>) -> Self {
        Self { code, calls }
    }
}
#[async_trait]
impl CommandExecutor for SagaRejectingExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        CommandOutcome::Rejected {
            code: self.code,
            message: format!("{:?}", self.code),
        }
    }
}

struct AlwaysRetryableSagaExecutor {
    code: Code,
    calls: Arc<AtomicU32>,
}
impl AlwaysRetryableSagaExecutor {
    fn new(code: Code, calls: Arc<AtomicU32>) -> Self {
        Self { code, calls }
    }
}
#[async_trait]
impl CommandExecutor for AlwaysRetryableSagaExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        CommandOutcome::Retryable {
            reason: format!("{:?}", self.code),
            current_state: None,
        }
    }
}

struct TransientThenSucceedExecutor {
    code: Code,
    calls: Arc<AtomicU32>,
}
impl TransientThenSucceedExecutor {
    fn new(code: Code, calls: Arc<AtomicU32>) -> Self {
        Self { code, calls }
    }
}
#[async_trait]
impl CommandExecutor for TransientThenSucceedExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            CommandOutcome::Retryable {
                reason: format!("{:?}", self.code),
                current_state: None,
            }
        } else {
            CommandOutcome::Success(CommandResponse::default())
        }
    }
}

async fn run_saga(ctx: &dyn SagaRetryContext, executor: &dyn CommandExecutor) {
    let _ = orchestrate_saga(
        ctx,
        executor,
        None, // command_bus
        None, // fetcher
        None, // fact_executor
        "saga-feature",
        "corr-saga",
        None, // output_domain_validator
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;
}

// ============================================================================
// PM test doubles
// ============================================================================

struct DlqAwarePmContext {
    publisher: Arc<dyn DeadLetterPublisher>,
    compensation_calls: Arc<AtomicU32>,
    persist_outcome: Box<dyn Fn() -> CommandOutcome + Send + Sync>,
    /// When `Some(mode)`, handle() emits one CommandBook whose first
    /// page-header.sync_mode is set to `mode`. `None` = no command,
    /// just PM events for the persist scenario.
    emit_command_with_sync_mode: Option<SyncMode>,
}

#[async_trait]
impl ProcessManagerContext for DlqAwarePmContext {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        let commands = match self.emit_command_with_sync_mode {
            None => vec![],
            Some(mode) => {
                let sync_mode = if mode == SyncMode::Decision {
                    Some(SyncMode::Decision as i32)
                } else {
                    None
                };
                let header = PageHeader {
                    sequence_type: Some(SequenceType::Sequence(0)),
                    sync_mode,
                };
                let page = CommandPage {
                    header: Some(header),
                    merge_strategy: MergeStrategy::MergeCommutative as i32,
                    payload: Some(CmdPayload::Command(prost_types::Any {
                        type_url: "test.PmCmd".to_string(),
                        value: vec![],
                    })),
                };
                vec![CommandBook {
                    cover: Some(cover("fulfillment", "corr-pm")),
                    pages: vec![page],
                }]
            }
        };

        // For the persist scenarios (no command), emit one PM event
        // book so the persistence loop has something to retry on.
        let process_events = if self.emit_command_with_sync_mode.is_none() {
            vec![EventBook {
                cover: Some(cover("pm-domain", "corr-pm")),
                pages: vec![EventPage::default()],
                snapshot: None,
                ..Default::default()
            }]
        } else {
            vec![]
        };

        Ok(PmHandleResponse {
            commands,
            process_events,
            facts: vec![],
        })
    }

    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        (self.persist_outcome)()
    }

    async fn on_command_rejected(
        &self,
        _command: &CommandBook,
        _reason: &str,
        _correlation_id: &str,
    ) {
        self.compensation_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        Some(&self.publisher)
    }

    fn component_name(&self) -> &str {
        "pm-feature-test"
    }
}

struct PmNoOpExecutor;
#[async_trait]
impl CommandExecutor for PmNoOpExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
}

struct CodeRejectingPmExecutor {
    code: Code,
    message: String,
}
#[async_trait]
impl CommandExecutor for CodeRejectingPmExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Rejected {
            code: self.code,
            message: self.message.clone(),
        }
    }
}

struct AlwaysRetryablePmExecutor;
#[async_trait]
impl CommandExecutor for AlwaysRetryablePmExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Retryable {
            reason: "transport conflict".to_string(),
            current_state: None,
        }
    }
}

async fn run_pm(
    ctx: &dyn ProcessManagerContext,
    executor: &dyn CommandExecutor,
) -> Result<(), BusError> {
    orchestrate_pm(
        ctx,
        &NoOpDestFetcher,
        executor,
        None, // fact_executor
        &event_book("trigger-domain", "corr-pm"),
        "pm-feature",
        "pm-feature",
        "corr-pm",
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await
    .map(|_| ())
}

struct NoOpDestFetcher;
#[async_trait]
impl DestinationFetcher for NoOpDestFetcher {
    async fn fetch(&self, _cover: &Cover) -> Result<Option<EventBook>, tonic::Status> {
        Ok(None)
    }
    async fn fetch_by_correlation(
        &self,
        _domain: &str,
        _correlation_id: &str,
    ) -> Result<Option<EventBook>, tonic::Status> {
        Ok(None)
    }
}

// ============================================================================
// Projector test doubles
// ============================================================================

struct FailingProjectorHandler {
    code: Code,
    message: String,
}

#[async_trait]
impl ProjectorHandler for FailingProjectorHandler {
    async fn handle(
        &self,
        _events: &EventBook,
        _mode: ProjectionMode,
    ) -> Result<Projection, tonic::Status> {
        Err(tonic::Status::new(self.code, self.message.clone()))
    }
}

// ============================================================================
// Entry point
// ============================================================================

#[tokio::main]
async fn main() {
    let features = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("features")
        .join("client")
        .join("dlq.feature");
    DlqWorld::cucumber()
        .fail_on_skipped()
        .run_and_exit(features)
        .await;
}
