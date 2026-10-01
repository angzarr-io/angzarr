//! Aggregate service (AggregateCoordinator).

use std::sync::Arc;

use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::bus::EventBus;
use crate::config::ResourceLimits;
use crate::discovery::ServiceDiscovery;
use crate::dlq::{DeadLetterPublisher, NoopDeadLetterPublisher};
use crate::orchestration::aggregate::grpc::GrpcAggregateContext;
use crate::orchestration::aggregate::{
    execute_command_pipeline, execute_command_with_retry, execute_compensation_pipeline,
    execute_fact_pipeline, ClientLogic, GrpcBusinessLogic, PipelineMode,
};
use crate::orchestration::channels::ChannelCache;
use crate::proto::{
    command_handler_coordinator_service_server::CommandHandlerCoordinatorService,
    command_handler_service_client::CommandHandlerServiceClient, BusinessResponse,
    CascadeErrorMode, CommandRequest, CommandResponse, EventRequest, FactInjectionResponse,
    SpeculateCommandHandlerRequest,
};
use crate::proto_ext::{CascadeErrorModeExt, CoverExt, SyncModeExt};
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
    /// DLQ publisher threaded down to every constructed
    /// [`GrpcAggregateContext`]. Defaults to a noop so callers that don't
    /// care about DLQ (in-process tests, embedded mode without operator
    /// config) stay zero-touch. The bin overrides it at startup via
    /// `with_dlq_publisher(init_dlq_publisher(&config.dlq).await?)`
    /// (R2-15). Hard-fail boot on init error happens at the call site,
    /// not here.
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
    /// The domain this coordinator owns. When set, commands and facts for any
    /// other domain are refused instead of being written into this store.
    domain: Option<String>,
    /// Channels to saga/PM coordinators for CASCADE fan-out, shared by every
    /// command this service handles.
    channels: Arc<ChannelCache>,
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
            dlq_publisher: Arc::new(NoopDeadLetterPublisher),
            domain: None,
            channels: Arc::new(ChannelCache::new()),
        }
    }

    /// Restrict this coordinator to one aggregate domain.
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    /// Refuse a book addressed to a domain this coordinator does not own.
    fn check_domain(&self, book_domain: &str) -> Result<(), Status> {
        match &self.domain {
            Some(own) if own != book_domain => Err(Status::invalid_argument(format!(
                "{}{book_domain} (this coordinator serves {own})",
                super::errmsg::DOMAIN_MISMATCH
            ))),
            _ => Ok(()),
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

    /// Set the DLQ publisher (R2-15). The bin calls this at startup with
    /// the result of `init_dlq_publisher(&config.dlq).await?` so dead
    /// letters from `MergeManual` sequence mismatches reach the
    /// operator-configured backend instead of the default noop.
    pub fn with_dlq_publisher(mut self, publisher: Arc<dyn DeadLetterPublisher>) -> Self {
        self.dlq_publisher = publisher;
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
            dlq_publisher: Arc::new(NoopDeadLetterPublisher),
            domain: None,
            channels: Arc::new(ChannelCache::new()),
        }
    }

    /// Create an async context (no sync projector calls).
    fn create_async_context(&self) -> GrpcAggregateContext {
        let mut ctx = GrpcAggregateContext::new(
            self.event_store.clone(),
            self.snapshot_repo.clone(),
            self.discovery.clone(),
            self.event_bus.clone(),
        )
        .with_dlq_publisher(self.dlq_publisher.clone())
        .with_channel_cache(self.channels.clone());
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
        .with_sync_mode(sync_mode)
        .with_dlq_publisher(self.dlq_publisher.clone())
        .with_channel_cache(self.channels.clone());
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
    /// Unknown ints resolve to Async — see [`crate::proto_ext::SyncModeExt`].
    fn create_context_for_sync_mode(&self, sync_mode_int: i32) -> GrpcAggregateContext {
        let sync_mode = crate::proto::SyncMode::or_default_async(sync_mode_int);
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

        validate_command_book(&command_book, &self.limits)?;
        self.check_domain(command_book.domain())?;

        let ctx = self
            .create_context_for_sync_mode(sync_request.sync_mode)
            .with_cascade_error_mode(CascadeErrorMode::or_default_fail_fast(
                sync_request.cascade_error_mode,
            ));

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

        validate_command_book(&command_book, &self.limits)?;
        self.check_domain(command_book.domain())?;

        let (as_of_sequence, as_of_timestamp) = match speculate_req.point_in_time {
            Some(temporal) => match temporal.point_in_time {
                Some(crate::proto::temporal_query::PointInTime::AsOfSequence(seq)) => {
                    (Some(seq), None)
                }
                Some(crate::proto::temporal_query::PointInTime::AsOfTime(ts)) => (None, Some(ts)),
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
        validate_command_book(&command_book, &self.limits)?;
        self.check_domain(command_book.domain())?;

        let ctx = self.create_context_for_sync_mode(sync_request.sync_mode);
        let response = execute_compensation_pipeline(&ctx, &*self.business, command_book).await?;
        Ok(Response::new(response))
    }

    /// Handle event (fact) injection - external realities that cannot be rejected.
    ///
    /// Facts are events that already happened externally and cannot be rejected by business logic.
    /// They are persisted unconditionally with coordinator-assigned sequence numbers.
    ///
    /// `skip_handler`: When false/unset (the proto3 zero value — the safe default),
    /// the fact is routed through the aggregate's handle_fact method for
    /// validation/error checking before persistence. The aggregate cannot reject
    /// facts, but can validate data integrity and log warnings. When true, facts
    /// are persisted directly without aggregate involvement (projector-originated
    /// writes). This replaces the removed routing bool (EventRequest field 3,
    /// now reserved — see types.proto), whose proto3 zero value silently
    /// bypassed the handler when the field was omitted.
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
        self.check_domain(fact_events.domain())?;

        let ctx = self.create_context_for_sync_mode(sync_event_book.sync_mode);

        // Route through the aggregate's handle_fact unless the caller opted
        // out. skip_handler's proto3 zero value (false/unset) means "route" —
        // the safe default: omission can no longer bypass fact validation the
        // way the removed routing bool's zero value used to.
        let business: Option<&dyn ClientLogic> = if sync_event_book.skip_handler {
            None
        } else {
            Some(&*self.business)
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
