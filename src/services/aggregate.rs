//! Aggregate service (AggregateCoordinator).

use std::sync::Arc;

use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::bus::EventBus;
use crate::config::ResourceLimits;
use crate::discovery::ServiceDiscovery;
use crate::orchestration::aggregate::grpc::GrpcAggregateContext;
use crate::orchestration::aggregate::{
    execute_command_pipeline, execute_command_with_retry, execute_fact_pipeline,
    parse_command_cover, AggregateContext, ClientLogic, GrpcBusinessLogic, PipelineMode,
    TemporalQuery,
};
use crate::proto::{
    business_response,
    command_handler_coordinator_service_server::CommandHandlerCoordinatorService,
    command_handler_service_client::CommandHandlerServiceClient, BusinessResponse, CommandRequest,
    CommandResponse, ContextualCommand, EventRequest, FactInjectionResponse,
    SpeculateCommandHandlerRequest,
};
use crate::proto_ext::CoverExt;
use crate::repository::SnapshotRepository;
use crate::services::upcaster::Upcaster;
use crate::storage::EventStore;
use crate::utils::retry::saga_backoff;
use crate::validation::validate_command_book;

/// Aggregate service.
///
/// Receives commands, loads prior state, calls client logic,
/// persists new events, and notifies projectors.
///
/// Uses the shared aggregate pipeline for both async and sync operations.
pub struct AggregateService {
    event_store: Arc<dyn EventStore>,
    snapshot_repo: Arc<SnapshotRepository>,
    business: Arc<dyn ClientLogic>,
    event_bus: Arc<dyn EventBus>,
    /// Service discovery for projectors (sync operations).
    discovery: Arc<dyn ServiceDiscovery>,
    /// Upcaster for event version transformation.
    upcaster: Option<Arc<Upcaster>>,
    /// Resource limits for validation.
    limits: ResourceLimits,
}

impl AggregateService {
    /// Create a new aggregate service.
    ///
    /// Snapshot policy (read_enabled / write_enabled) lives on the
    /// passed-in `SnapshotRepository`. Callers building one with
    /// defaults: `Arc::new(SnapshotRepository::new(store))`. Callers
    /// wanting explicit flags:
    /// `Arc::new(SnapshotRepository::with_flags(store, read, write))`.
    pub fn new(
        event_store: Arc<dyn EventStore>,
        snapshot_repo: Arc<SnapshotRepository>,
        business_client: CommandHandlerServiceClient<Channel>,
        event_bus: Arc<dyn EventBus>,
        discovery: Arc<dyn ServiceDiscovery>,
    ) -> Self {
        Self {
            event_store,
            snapshot_repo,
            business: Arc::new(GrpcBusinessLogic::new(business_client)),
            event_bus,
            discovery,
            upcaster: None,
            limits: ResourceLimits::default(),
        }
    }

    /// Set the upcaster for event version transformation.
    pub fn with_upcaster(mut self, upcaster: Arc<Upcaster>) -> Self {
        self.upcaster = Some(upcaster);
        self
    }

    /// Set resource limits for validation.
    pub fn with_limits(mut self, limits: ResourceLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Create a new aggregate service with injected business logic.
    ///
    /// Test-only constructor: accepts `Arc<dyn ClientLogic>` directly
    /// instead of a gRPC client.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_business_logic(
        event_store: Arc<dyn EventStore>,
        snapshot_repo: Arc<SnapshotRepository>,
        business: Arc<dyn ClientLogic>,
        event_bus: Arc<dyn EventBus>,
        discovery: Arc<dyn ServiceDiscovery>,
    ) -> Self {
        Self {
            event_store,
            snapshot_repo,
            business,
            event_bus,
            discovery,
            upcaster: None,
            limits: ResourceLimits::default(),
        }
    }

    /// Create an async context (no sync projector calls).
    fn create_async_context(&self) -> GrpcAggregateContext {
        let mut ctx = GrpcAggregateContext::new(
            self.event_store.clone(),
            self.snapshot_repo.clone(),
            self.discovery.clone(),
            self.event_bus.clone(),
        );
        if let Some(ref upcaster) = self.upcaster {
            ctx = ctx.with_upcaster(upcaster.clone());
        }
        ctx
    }

    /// Create a sync context (calls sync projectors).
    fn create_sync_context(&self, sync_mode: crate::proto::SyncMode) -> GrpcAggregateContext {
        let mut ctx = GrpcAggregateContext::new(
            self.event_store.clone(),
            self.snapshot_repo.clone(),
            self.discovery.clone(),
            self.event_bus.clone(),
        )
        .with_sync_mode(sync_mode);
        if let Some(ref upcaster) = self.upcaster {
            ctx = ctx.with_upcaster(upcaster.clone());
        }
        ctx
    }

    /// Create context for the given sync mode integer value.
    ///
    /// Parses the proto sync mode and creates async context for Async mode,
    /// sync context otherwise. This consolidates the repeated pattern of
    /// extracting sync mode and conditionally creating the right context type.
    fn create_context_for_sync_mode(&self, sync_mode_int: i32) -> GrpcAggregateContext {
        let sync_mode = crate::proto::SyncMode::try_from(sync_mode_int)
            .unwrap_or(crate::proto::SyncMode::Async);
        if sync_mode == crate::proto::SyncMode::Async {
            self.create_async_context()
        } else {
            self.create_sync_context(sync_mode)
        }
    }
}

#[tonic::async_trait]
impl CommandHandlerCoordinatorService for AggregateService {
    /// Handle command with optional sync mode (default: async fire-and-forget).
    #[tracing::instrument(name = "aggregate.handle_command", skip_all)]
    async fn handle_command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let sync_request = request.into_inner();
        let command_book = sync_request.command.ok_or_else(|| {
            Status::invalid_argument(super::errmsg::COMMAND_REQUEST_MISSING_COMMAND)
        })?;

        // Validate command book before processing
        validate_command_book(&command_book, &self.limits)?;

        let mut ctx = self.create_context_for_sync_mode(sync_request.sync_mode);
        if let Some(ref cascade_id) = sync_request.cascade_id {
            ctx = ctx.with_cascade_id(cascade_id);
        }

        let result =
            execute_command_with_retry(&ctx, &*self.business, command_book, saga_backoff()).await;

        Ok(Response::new(result?))
    }

    /// Speculative: execute command against temporal state without persisting.
    #[tracing::instrument(name = "aggregate.handle_sync_speculative", skip_all)]
    async fn handle_sync_speculative(
        &self,
        request: Request<SpeculateCommandHandlerRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let speculate_req = request.into_inner();
        let command_book = speculate_req.command.ok_or_else(|| {
            Status::invalid_argument(super::errmsg::SPECULATE_AGG_MISSING_COMMAND)
        })?;

        // Validate command book before processing
        validate_command_book(&command_book, &self.limits)?;

        let (as_of_sequence, as_of_timestamp) = match speculate_req.point_in_time {
            Some(temporal) => match temporal.point_in_time {
                Some(crate::proto::temporal_query::PointInTime::AsOfSequence(seq)) => {
                    (Some(seq), None)
                }
                Some(crate::proto::temporal_query::PointInTime::AsOfTime(ts)) => {
                    let ts_str = format!("{}.{}", ts.seconds, ts.nanos);
                    (None, Some(ts_str))
                }
                None => (None, None),
            },
            None => (None, None),
        };

        let ctx = self.create_async_context();

        let response = execute_command_pipeline(
            &ctx,
            &*self.business,
            command_book,
            PipelineMode::Speculative {
                as_of_sequence,
                as_of_timestamp,
            },
        )
        .await?;

        Ok(Response::new(response))
    }

    /// Handle compensation flow - returns BusinessResponse for saga compensation handling.
    ///
    /// Unlike normal HandleCommand, this returns the raw BusinessResponse so the caller
    /// can inspect revocation flags and decide how to handle (quarantine, notify, etc.).
    /// If business logic returns events, they are persisted before returning.
    #[tracing::instrument(name = "aggregate.handle_compensation", skip_all)]
    async fn handle_compensation(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<BusinessResponse>, Status> {
        let sync_request = request.into_inner();
        let command_book = sync_request.command.ok_or_else(|| {
            Status::invalid_argument(super::errmsg::COMMAND_REQUEST_MISSING_COMMAND)
        })?;
        let (domain, root_uuid) = parse_command_cover(&command_book)?;
        let edition = command_book.edition().unwrap_or_default().to_string();
        let correlation_id =
            crate::orchestration::correlation::extract_correlation_id(&command_book)?;

        let ctx = self.create_context_for_sync_mode(sync_request.sync_mode);

        // Load prior events
        let prior_events = ctx
            .load_prior_events(&domain, &edition, root_uuid, &TemporalQuery::Current)
            .await?;

        // Transform events (upcasting)
        let prior_events = ctx.transform_events(&domain, prior_events).await?;

        // Invoke business logic
        let contextual_command = ContextualCommand {
            events: Some(prior_events.clone()),
            command: Some(command_book),
        };

        let response = self.business.invoke(contextual_command).await?;

        // If business returned events, persist them
        if let Some(business_response::Result::Events(ref events)) = response.result {
            if !events.pages.is_empty() {
                ctx.persist_events(
                    &prior_events,
                    events,
                    &domain,
                    &edition,
                    root_uuid,
                    &correlation_id,
                    None,
                    None, // speculative path doesn't carry source provenance
                )
                .await?;

                // Post-persist: publish to bus
                ctx.post_persist(events).await?;
            }
        }

        Ok(Response::new(response))
    }

    /// Handle event (fact) injection - external realities that cannot be rejected.
    ///
    /// Facts are events that already happened externally and cannot be rejected by business logic.
    /// They are persisted unconditionally with coordinator-assigned sequence numbers.
    ///
    /// `route_to_handler`: When true (default), invokes the aggregate's handle_fact method
    /// for validation/error checking before persistence. The aggregate cannot reject facts,
    /// but can validate data integrity and log warnings. When false, facts are persisted
    /// directly without aggregate involvement.
    ///
    /// Idempotent: subsequent requests with same external_id return original events.
    #[tracing::instrument(name = "aggregate.handle_event", skip_all)]
    async fn handle_event(
        &self,
        request: Request<EventRequest>,
    ) -> Result<Response<FactInjectionResponse>, Status> {
        let sync_event_book = request.into_inner();
        let fact_events = sync_event_book
            .events
            .ok_or_else(|| Status::invalid_argument(super::errmsg::EVENT_REQUEST_MISSING_EVENTS))?;

        let ctx = self.create_context_for_sync_mode(sync_event_book.sync_mode);

        // Use aggregate handler if route_to_handler is true (default behavior)
        let business: Option<&dyn ClientLogic> = if sync_event_book.route_to_handler {
            Some(&*self.business)
        } else {
            None
        };

        let fact_response = execute_fact_pipeline(&ctx, business, fact_events).await?;

        Ok(Response::new(FactInjectionResponse {
            events: Some(fact_response.events),
            already_processed: fact_response.already_processed,
            projections: fact_response.projections,
        }))
    }
}

#[cfg(test)]
#[path = "aggregate.test.rs"]
mod tests;
