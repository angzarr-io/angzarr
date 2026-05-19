//! Saga coordinator service.
//!
//! Orchestrates saga execution for distributed mode. Receives source events
//! from CASCADE callers and delivers commands to target aggregates via
//! the `orchestrate_saga` function.
//!
//! The coordinator handles:
//! - Gap filling: Ensures complete EventBooks before processing
//! - Command delivery: Executes commands with retry on sequence conflict
//! - Sync mode propagation: Passes sync_mode through for recursive CASCADE
//! - Fact injection: Injects saga-produced facts into target aggregates

use std::sync::Arc;

use backon::ExponentialBuilder;
use tonic::{Request, Response, Status};
use tracing::{debug, error};

use crate::bus::CommandBus;
use crate::orchestration::command::CommandExecutor;
use crate::orchestration::destination::DestinationFetcher;
use crate::orchestration::saga::{orchestrate_saga, OutputDomainValidator, SagaContextFactory};
use crate::orchestration::FactExecutor;
use crate::proto::{
    saga_coordinator_service_server::SagaCoordinatorService, SagaHandleRequest, SagaResponse,
    SpeculateSagaRequest, SyncMode,
};
use crate::proto_ext::CoverExt;
use crate::services::gap_fill::{GapFiller, NoOpPositionStore, RemoteEventSource};

/// Saga coordinator service.
///
/// Orchestrates saga execution by receiving events from CASCADE callers
/// and delivering commands to target aggregates.
pub struct SagaCoord {
    /// Factory for creating per-invocation saga contexts.
    factory: Arc<dyn SagaContextFactory>,
    /// Executor for delivering commands to target aggregates.
    executor: Arc<dyn CommandExecutor>,
    /// Command bus for ASYNC mode (optional).
    command_bus: Option<Arc<dyn CommandBus>>,
    /// Destination fetcher (unused in new model, kept for interface).
    #[allow(dead_code)]
    fetcher: Option<Arc<dyn DestinationFetcher>>,
    /// Fact executor for injecting saga-produced facts.
    fact_executor: Option<Arc<dyn FactExecutor>>,
    /// Output domain validator for routing validation.
    output_validator: Option<Arc<OutputDomainValidator>>,
    /// Retry backoff configuration.
    backoff: ExponentialBuilder,
    /// Gap filler for incomplete EventBooks.
    gap_filler: Option<Arc<GapFiller<NoOpPositionStore, RemoteEventSource>>>,
}

impl SagaCoord {
    /// Create a new saga coordinator with minimal dependencies.
    pub fn new(factory: Arc<dyn SagaContextFactory>, executor: Arc<dyn CommandExecutor>) -> Self {
        Self {
            factory,
            executor,
            command_bus: None,
            fetcher: None,
            fact_executor: None,
            output_validator: None,
            backoff: ExponentialBuilder::default(),
            gap_filler: None,
        }
    }

    /// Add command bus for ASYNC mode support.
    pub fn with_command_bus(mut self, bus: Arc<dyn CommandBus>) -> Self {
        self.command_bus = Some(bus);
        self
    }

    /// Add fact executor for fact injection.
    pub fn with_fact_executor(mut self, executor: Arc<dyn FactExecutor>) -> Self {
        self.fact_executor = Some(executor);
        self
    }

    /// Add output domain validator.
    pub fn with_output_validator(mut self, validator: Arc<OutputDomainValidator>) -> Self {
        self.output_validator = Some(validator);
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

    /// Build a fully-configured saga coordinator.
    pub async fn connect(
        factory: Arc<dyn SagaContextFactory>,
        executor: Arc<dyn CommandExecutor>,
        event_query_address: &str,
    ) -> Result<Self, String> {
        let event_source = RemoteEventSource::connect(event_query_address)
            .await
            .map_err(|e| format!("Failed to connect to EventQuery service: {}", e))?;

        let gap_filler = Arc::new(GapFiller::new(NoOpPositionStore, event_source));

        Ok(Self::new(factory, executor).with_gap_filler(gap_filler))
    }
}

#[tonic::async_trait]
impl SagaCoordinatorService for SagaCoord {
    /// Execute saga: translate source events → commands, deliver to targets.
    ///
    /// This is the main entry point for CASCADE mode. The coordinator:
    /// 1. Validates the request has source events
    /// 2. Gap-fills the EventBook if incomplete
    /// 3. Creates a saga context via the factory
    /// 4. Calls orchestrate_saga to handle/deliver commands
    /// 5. Returns the saga response
    async fn execute(
        &self,
        request: Request<SagaHandleRequest>,
    ) -> Result<Response<SagaResponse>, Status> {
        let req = request.into_inner();
        let source = req
            .source
            .ok_or_else(|| Status::invalid_argument("SagaHandleRequest requires source events"))?;
        let sync_mode = SyncMode::try_from(req.sync_mode).unwrap_or(SyncMode::Async);

        let correlation_id = source.correlation_id().to_string();
        let saga_name = self.factory.name();

        debug!(
            saga = %saga_name,
            correlation_id = %correlation_id,
            sync_mode = ?sync_mode,
            "Executing saga"
        );

        // Gap-fill EventBook if incomplete
        let source = super::fill_gaps_if_needed(self.gap_filler.as_ref(), source).await?;

        // Create context and orchestrate
        let ctx = self.factory.create(Arc::new(source));

        orchestrate_saga(
            ctx.as_ref(),
            self.executor.as_ref(),
            self.command_bus.as_deref(),
            None, // fetcher unused in new model
            self.fact_executor.as_deref(),
            saga_name,
            &correlation_id,
            self.output_validator.as_deref(),
            sync_mode,
            self.backoff,
        )
        .await
        .map_err(|e| Status::internal(format!("Saga orchestration failed: {}", e)))?;

        // The saga response is built by the context during handle()
        // For now, return empty response - commands were delivered during orchestration
        Ok(Response::new(SagaResponse {
            commands: vec![],
            events: vec![],
        }))
    }

    /// Speculative execution - returns commands without side effects.
    ///
    /// Used for previewing what commands a saga would produce without
    /// actually delivering them. Useful for dry-run and testing.
    async fn execute_speculative(
        &self,
        request: Request<SpeculateSagaRequest>,
    ) -> Result<Response<SagaResponse>, Status> {
        let speculate_req = request.into_inner();
        let req = speculate_req
            .request
            .ok_or_else(|| Status::invalid_argument("SpeculateSagaRequest requires request"))?;
        let source = req
            .source
            .ok_or_else(|| Status::invalid_argument("SagaHandleRequest requires source events"))?;

        let saga_name = self.factory.name();

        debug!(
            saga = %saga_name,
            "Executing saga speculatively"
        );

        // Gap-fill EventBook if incomplete
        let source = super::fill_gaps_if_needed(self.gap_filler.as_ref(), source).await?;

        // Carry the caller's inherited sync_mode through to the saga handler
        // so the gRPC saga context stamps it on the wire (H-17). Speculative
        // execution still respects the requested mode even though it produces
        // no side effects — handlers can inspect the inherited mode.
        let sync_mode = SyncMode::try_from(req.sync_mode).unwrap_or(SyncMode::Async);

        // Create context and call handle() directly (no command delivery)
        // For speculative execution, pass empty sequences since we're not actually delivering commands
        let ctx = self.factory.create(Arc::new(source));

        let response = ctx
            .handle(std::collections::HashMap::new(), sync_mode)
            .await
            .map_err(|e| {
                error!(error = %e, "Saga handler failed");
                Status::internal(format!("Saga handler failed: {}", e))
            })?;

        Ok(Response::new(response))
    }
}

#[cfg(test)]
#[path = "saga_coord.test.rs"]
mod tests;
