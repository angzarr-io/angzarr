//! Process Manager coordinator service.
//!
//! Orchestrates PM execution for distributed mode. Receives trigger events
//! from CASCADE callers and delivers commands to target aggregates via
//! the `orchestrate_pm` function.
//!
//! The coordinator handles:
//! - PM state fetching: Loads existing workflow state by correlation_id
//! - Destination fetching: Retrieves additional aggregate state per PM request
//! - Command delivery: Executes commands with angzarr_deferred for compensation
//! - Sync mode propagation: Passes sync_mode through for recursive CASCADE
//! - Fact injection: Injects PM-produced facts into target aggregates

use std::sync::Arc;

use backon::ExponentialBuilder;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::orchestration::command::CommandExecutor;
use crate::orchestration::destination::DestinationFetcher;
use crate::orchestration::process_manager::{orchestrate_pm, PMContextFactory};
use crate::orchestration::FactExecutor;
use crate::proto::CascadeErrorMode;
use crate::proto::{
    process_manager_coordinator_service_server::ProcessManagerCoordinatorService,
    ProcessManagerCoordinatorRequest, ProcessManagerHandleResponse, SpeculatePmRequest, SyncMode,
};
use crate::proto_ext::{CascadeErrorModeExt, CoverExt, SyncModeExt};
use crate::services::gap_fill::{GapFiller, NoOpPositionStore, RemoteEventSource};

/// Process Manager coordinator service.
///
/// Orchestrates PM execution by receiving events from CASCADE callers
/// and delivering commands to target aggregates.
pub struct PmCoord {
    /// Factory for creating per-invocation PM contexts.
    factory: Arc<dyn PMContextFactory>,
    /// Fetcher for PM state and destination aggregates.
    fetcher: Arc<dyn DestinationFetcher>,
    /// Executor for delivering commands to target aggregates.
    executor: Arc<dyn CommandExecutor>,
    /// Fact executor for injecting PM-produced facts.
    fact_executor: Option<Arc<dyn FactExecutor>>,
    /// Retry backoff configuration.
    backoff: ExponentialBuilder,
    /// Gap filler for incomplete EventBooks.
    gap_filler: Option<Arc<GapFiller<NoOpPositionStore, RemoteEventSource>>>,
}

impl PmCoord {
    /// Create a new PM coordinator with minimal dependencies.
    pub fn new(
        factory: Arc<dyn PMContextFactory>,
        fetcher: Arc<dyn DestinationFetcher>,
        executor: Arc<dyn CommandExecutor>,
    ) -> Self {
        Self {
            factory,
            fetcher,
            executor,
            fact_executor: None,
            backoff: ExponentialBuilder::default(),
            gap_filler: None,
        }
    }

    /// Add fact executor for fact injection.
    pub fn with_fact_executor(mut self, executor: Arc<dyn FactExecutor>) -> Self {
        self.fact_executor = Some(executor);
        self
    }

    /// Add retry backoff configuration.
    pub fn with_backoff(mut self, backoff: ExponentialBuilder) -> Self {
        self.backoff = backoff;
        self
    }

    /// Add gap filler for incomplete EventBooks.
    pub fn with_gap_filler(
        mut self,
        gap_filler: Arc<GapFiller<NoOpPositionStore, RemoteEventSource>>,
    ) -> Self {
        self.gap_filler = Some(gap_filler);
        self
    }

    /// Build a fully-configured PM coordinator.
    pub async fn connect(
        factory: Arc<dyn PMContextFactory>,
        fetcher: Arc<dyn DestinationFetcher>,
        executor: Arc<dyn CommandExecutor>,
        event_query_address: &str,
    ) -> Result<Self, String> {
        let event_source = RemoteEventSource::connect(event_query_address)
            .await
            .map_err(|e| format!("Failed to connect to EventQuery service: {}", e))?;

        let gap_filler = Arc::new(GapFiller::new(NoOpPositionStore, event_source));

        Ok(Self::new(factory, fetcher, executor).with_gap_filler(gap_filler))
    }
}

#[tonic::async_trait]
impl ProcessManagerCoordinatorService for PmCoord {
    /// Handle trigger events: orchestrate PM and deliver commands.
    ///
    /// This is the main entry point for CASCADE mode. The coordinator:
    /// 1. Validates the request has trigger events
    /// 2. Gap-fills the EventBook if incomplete
    /// 3. Extracts correlation_id (required for PM)
    /// 4. Calls orchestrate_pm to handle/deliver commands
    /// 5. Returns the PM response
    async fn handle(
        &self,
        request: Request<ProcessManagerCoordinatorRequest>,
    ) -> Result<Response<ProcessManagerHandleResponse>, Status> {
        let req = request.into_inner();
        let trigger = req.trigger.ok_or_else(|| {
            Status::invalid_argument("ProcessManagerCoordinatorRequest requires trigger events")
        })?;
        // Unknown ints resolve to the zero-value defaults (Async / FailFast).
        let sync_mode = SyncMode::or_default_async(req.sync_mode);
        let cascade_error_mode = CascadeErrorMode::or_default_fail_fast(req.cascade_error_mode);

        let correlation_id = trigger.correlation_id();
        if correlation_id.is_empty() {
            return Err(Status::invalid_argument(
                "ProcessManagerCoordinatorRequest requires correlation_id in trigger events",
            ));
        }
        let correlation_id = correlation_id.to_string();

        let pm_name = self.factory.name();
        let pm_domain = self.factory.pm_domain();

        debug!(
            pm = %pm_name,
            correlation_id = %correlation_id,
            sync_mode = ?sync_mode,
            "Handling PM trigger"
        );

        // Gap-fill EventBook if incomplete
        let trigger = super::fill_gaps_if_needed(self.gap_filler.as_ref(), trigger).await?;

        // Create context and orchestrate
        let ctx = self.factory.create();

        let report = orchestrate_pm(
            ctx.as_ref(),
            self.fetcher.as_ref(),
            self.executor.as_ref(),
            self.fact_executor.as_deref(),
            &trigger,
            pm_name,
            pm_domain,
            &correlation_id,
            sync_mode,
            self.backoff,
            Some(cascade_error_mode),
        )
        .await
        .map_err(|e| super::orchestration_status("PM", e))?;

        // Commands were delivered during orchestration; the response carries
        // CONTINUE-mode reaction errors and, for a COMPENSATE caller, the
        // reaction commands executed (so it can compensate them if another
        // reaction fails), in its metadata.
        let mut response = Response::new(ProcessManagerHandleResponse::default());
        crate::orchestration::shared::attach_reaction_errors(&mut response, report.reaction_errors);
        if cascade_error_mode == CascadeErrorMode::CascadeErrorCompensate {
            crate::orchestration::shared::attach_executed_reactions(
                &mut response,
                &report.executed,
            );
        }
        Ok(response)
    }

    /// Speculative execution - returns commands without side effects.
    ///
    /// Used for previewing what commands a PM would produce without
    /// actually delivering them. Useful for dry-run and testing.
    async fn handle_speculative(
        &self,
        request: Request<SpeculatePmRequest>,
    ) -> Result<Response<ProcessManagerHandleResponse>, Status> {
        let speculate_req = request.into_inner();
        let req = speculate_req
            .request
            .ok_or_else(|| Status::invalid_argument("SpeculatePmRequest requires request"))?;
        let trigger = req.trigger.ok_or_else(|| {
            Status::invalid_argument("ProcessManagerHandleRequest requires trigger")
        })?;

        let pm_name = self.factory.name();

        debug!(
            pm = %pm_name,
            "Handling PM speculatively"
        );

        // Gap-fill EventBook if incomplete
        let trigger = super::fill_gaps_if_needed(self.gap_filler.as_ref(), trigger).await?;

        // Create context and call handle directly (no command delivery)
        let ctx = self.factory.create();

        let response = ctx
            .handle(&trigger, req.process_state.as_ref())
            .await
            .map_err(|e| Status::internal(format!("PM handle failed: {}", e)))?;

        Ok(Response::new(ProcessManagerHandleResponse {
            process_events: response.process_events,
            commands: response.commands,
            facts: response.facts,
            notification: None,
        }))
    }
}

#[cfg(test)]
#[path = "pm_coord.test.rs"]
mod tests;
