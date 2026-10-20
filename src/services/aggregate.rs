//! The command entry point (`CommandHandlerCoordinatorService`,
//! command_handler.proto): one RPC per command mode.
//!
//! - `Handle` (DECISION): run the pipeline, answer with the decision.
//! - `HandleSync` (SIMPLE): as Handle, then wait for the domain's
//!   synchronous projectors; a projector failure never undoes the decision
//!   and is reported in `Outcome.projector_failures`.
//! - `HandleAsync` (ASYNC): validate, then enqueue durably. No durable
//!   command queue exists in this port yet, so a valid command is refused
//!   UNIMPLEMENTED after validation (never silently dropped).

use std::sync::Arc;

use async_trait::async_trait;
use tonic::{Request, Response, Status};
use tracing::warn;

use crate::config::ResourceLimits;
use crate::discovery::ServiceDiscovery;
use crate::orchestration::aggregate::validate::validate;
use crate::orchestration::aggregate::{AggregatePipeline, Decided, Delivery};
use crate::proto::{
    command_handler_coordinator_service_server::CommandHandlerCoordinatorService, CommandBook,
    CommandResponse, DeliveryOutcome, EventBook, Outcome, Projection, ProjectorFailure,
};
use crate::proto_ext::{correlated_request, CoverExt};

/// Refusal messages.
pub mod errmsg {
    pub const PAYLOAD_TOO_LARGE: &str = "command payload exceeds the configured maximum of ";
    pub const ASYNC_UNAVAILABLE: &str =
        "HandleAsync needs a durable command queue, which this coordinator does not have yet; \
         use Handle or HandleSync";
}

/// The domain's synchronous projectors, called by HandleSync.
#[async_trait]
pub trait SyncProjectors: Send + Sync {
    /// Project `events`; every projector either succeeds or is reported
    /// failed, by name.
    async fn project(&self, events: &EventBook) -> (Vec<Projection>, Vec<ProjectorFailure>);
}

/// [`SyncProjectors`] found through service discovery: every projector
/// discovered for the events' domain.
pub struct DiscoveredSyncProjectors {
    discovery: Arc<dyn ServiceDiscovery>,
}

impl DiscoveredSyncProjectors {
    pub fn new(discovery: Arc<dyn ServiceDiscovery>) -> Self {
        Self { discovery }
    }
}

#[async_trait]
impl SyncProjectors for DiscoveredSyncProjectors {
    async fn project(&self, events: &EventBook) -> (Vec<Projection>, Vec<ProjectorFailure>) {
        let mut projections = Vec::new();
        let mut failures = Vec::new();
        let projectors = match self.discovery.get_all_projectors().await {
            Ok(projectors) => projectors,
            Err(e) => {
                warn!(error = %e, "projector discovery failed");
                return (projections, failures);
            }
        };
        let domain = events.domain();
        let correlation = events
            .pages
            .first()
            .and_then(|p| p.meta.as_ref())
            .and_then(|m| m.correlation_id.as_ref())
            .map(|c| c.to_string())
            .unwrap_or_default();
        for (service, mut client) in projectors {
            if service
                .domain
                .as_deref()
                .is_some_and(|d| d != domain && d != "*")
            {
                continue;
            }
            match client
                .handle_sync(correlated_request(events.clone(), &correlation))
                .await
            {
                Ok(projection) => projections.push(projection.into_inner()),
                Err(status) => failures.push(ProjectorFailure {
                    projector: service.name.clone(),
                    code: status.code() as i32,
                    message: status.message().to_string(),
                }),
            }
        }
        (projections, failures)
    }
}

/// `CommandHandlerCoordinatorService` over one domain's pipeline.
pub struct AggregateService {
    pipeline: Arc<AggregatePipeline>,
    sync_projectors: Arc<dyn SyncProjectors>,
    limits: ResourceLimits,
}

impl AggregateService {
    pub fn new(pipeline: Arc<AggregatePipeline>, sync_projectors: Arc<dyn SyncProjectors>) -> Self {
        Self {
            pipeline,
            sync_projectors,
            limits: ResourceLimits::default(),
        }
    }

    /// Resource limits checked before the pipeline.
    pub fn with_limits(mut self, limits: ResourceLimits) -> Self {
        self.limits = limits;
        self
    }

    fn check_limits(&self, book: &CommandBook) -> Result<(), Status> {
        use crate::proto::command_page::Body;
        let size = match book.page.as_ref().and_then(|p| p.body.as_ref()) {
            Some(Body::Command(c)) => c.payload.as_ref().map(|p| p.value.len()),
            Some(Body::Fact(f)) => f.payload.as_ref().map(|p| p.value.len()),
            None => None,
        };
        match size {
            Some(len) if len > self.limits.max_payload_bytes => {
                Err(Status::invalid_argument(format!(
                    "{}{} bytes",
                    errmsg::PAYLOAD_TOO_LARGE,
                    self.limits.max_payload_bytes
                )))
            }
            _ => Ok(()),
        }
    }

    async fn decide(&self, book: CommandBook) -> Result<Decided, Status> {
        self.check_limits(&book)?;
        self.pipeline
            .execute(book, Delivery::Client)
            .await
            .map_err(|e| e.to_status())
    }
}

fn response(
    decided: Decided,
    projections: Vec<Projection>,
    failures: Vec<ProjectorFailure>,
) -> CommandResponse {
    CommandResponse {
        events: Some(decided.events),
        outcome: Some(Outcome {
            delivery: decided.delivery as i32,
            projections,
            projector_failures: failures,
        }),
    }
}

#[tonic::async_trait]
impl CommandHandlerCoordinatorService for AggregateService {
    #[tracing::instrument(name = "aggregate.handle", skip_all)]
    async fn handle(
        &self,
        request: Request<CommandBook>,
    ) -> Result<Response<CommandResponse>, Status> {
        let decided = self.decide(request.into_inner()).await?;
        Ok(Response::new(response(decided, Vec::new(), Vec::new())))
    }

    #[tracing::instrument(name = "aggregate.handle_sync", skip_all)]
    async fn handle_sync(
        &self,
        request: Request<CommandBook>,
    ) -> Result<Response<CommandResponse>, Status> {
        let decided = self.decide(request.into_inner()).await?;
        let (projections, failures) =
            if decided.delivery == DeliveryOutcome::Applied && !decided.events.pages.is_empty() {
                self.sync_projectors.project(&decided.events).await
            } else {
                (Vec::new(), Vec::new())
            };
        Ok(Response::new(response(decided, projections, failures)))
    }

    #[tracing::instrument(name = "aggregate.handle_async", skip_all)]
    async fn handle_async(&self, request: Request<CommandBook>) -> Result<Response<()>, Status> {
        let book = request.into_inner();
        self.check_limits(&book)?;
        validate(&book, self.pipeline.domain(), Delivery::Async)?;
        Err(Status::unimplemented(errmsg::ASYNC_UNAVAILABLE))
    }
}
