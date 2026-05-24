//! Process Manager event handler.
//!
//! Receives events from the event bus and orchestrates process manager execution
//! using the shared orchestration module.
//!
//! Works with any `PMContextFactory` implementation — gRPC (distributed)
//! or local (in-process) — enabling deploy-anywhere process manager code.

use std::sync::Arc;

use backon::ExponentialBuilder;
use futures::future::BoxFuture;
use tokio::sync::Mutex;
use tracing::{debug, error, Instrument};

use crate::bus::{BusError, EventBus, EventHandler};
use crate::descriptor::Target;
use crate::orchestration::command::CommandExecutor;
use crate::orchestration::destination::DestinationFetcher;
use crate::orchestration::process_manager::grpc::GrpcPMContextFactory;
use crate::orchestration::process_manager::{orchestrate_pm, PMContextFactory};
use crate::orchestration::FactExecutor;
use crate::proto::process_manager_service_client::ProcessManagerServiceClient;
use crate::proto::{EventBook, SyncMode};
use crate::proto_ext::CoverExt;
use crate::storage::EventStore;
use crate::utils::retry::saga_backoff;

/// Event handler that orchestrates process manager execution via a context factory.
///
/// Uses `PMContextFactory` to create per-invocation contexts, enabling
/// the same handler code for both distributed (gRPC) and in-process (local) modes.
pub struct ProcessManagerEventHandler {
    context_factory: Arc<dyn PMContextFactory>,
    destination_fetcher: Arc<dyn DestinationFetcher>,
    command_executor: Arc<dyn CommandExecutor>,
    fact_executor: Option<Arc<dyn FactExecutor>>,
    /// Target filter — only handle events matching these targets.
    /// Empty means handle all events (distributed mode uses bus-level filtering).
    targets: Vec<Target>,
    backoff: ExponentialBuilder,
    /// When true (default), orchestration errors are propagated to the caller.
    /// When false, errors are logged but handler returns Ok(()).
    ///
    /// Default: true (maintains backward compatibility).
    /// Use `with_error_propagation(false)` to swallow errors.
    propagate_errors: bool,
}

impl ProcessManagerEventHandler {
    /// Create from a context factory with fetcher and executor.
    ///
    /// # When to Use
    ///
    /// Use this when you already have a `PMContextFactory` implementation
    /// (typically a custom-wired distributed factory). For standard
    /// distributed mode with the gRPC client, use `new()` instead.
    ///
    /// # Error Propagation
    ///
    /// Unlike `SagaEventHandler`, PM defaults to `propagate_errors = true`.
    /// This reflects the difference in typical error handling: PM state is
    /// persisted independently and errors often indicate data consistency
    /// issues that should be investigated rather than swallowed.
    pub fn from_factory(
        context_factory: Arc<dyn PMContextFactory>,
        destination_fetcher: Arc<dyn DestinationFetcher>,
        command_executor: Arc<dyn CommandExecutor>,
    ) -> Self {
        Self {
            context_factory,
            destination_fetcher,
            command_executor,
            fact_executor: None,
            targets: Vec::new(),
            backoff: saga_backoff(),
            propagate_errors: true,
        }
    }

    /// Set fact executor for fact injection.
    pub fn with_fact_executor(mut self, fact_executor: Option<Arc<dyn FactExecutor>>) -> Self {
        self.fact_executor = fact_executor;
        self
    }

    /// Set target filter for handler-level event filtering.
    ///
    /// # Target Filtering
    ///
    /// In **distributed mode**, filtering typically happens at the bus level
    /// (AMQP routing keys, Kafka topics, etc.). Leave targets empty.
    ///
    /// In **in-process mode**, all events flow through a shared in-process bus.
    /// Use targets to filter which events this PM should process.
    ///
    /// Empty targets = process all events (distributed mode default).
    pub fn with_targets(mut self, targets: Vec<Target>) -> Self {
        self.targets = targets;
        self
    }

    /// Create with custom backoff configuration.
    pub fn with_backoff(mut self, backoff: ExponentialBuilder) -> Self {
        self.backoff = backoff;
        self
    }

    /// Configure error propagation behavior.
    ///
    /// When enabled (default), orchestration errors are returned to the caller,
    /// which may trigger message redelivery depending on the bus implementation.
    /// When disabled, errors are logged but the handler returns Ok(()).
    pub fn with_error_propagation(mut self, propagate: bool) -> Self {
        self.propagate_errors = propagate;
        self
    }

    /// Create a new process manager event handler using gRPC client.
    ///
    /// PM state events are persisted directly to the event store and published
    /// to the event bus, bypassing the command pipeline.
    pub fn new(
        client: ProcessManagerServiceClient<tonic::transport::Channel>,
        process_domain: String,
        destination_fetcher: Arc<dyn DestinationFetcher>,
        command_executor: Arc<dyn CommandExecutor>,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        let factory = Arc::new(GrpcPMContextFactory::new(
            Arc::new(Mutex::new(client)),
            event_store,
            event_bus,
            process_domain.clone(),
            process_domain,
        ));
        Self {
            context_factory: factory,
            destination_fetcher,
            command_executor,
            fact_executor: None,
            targets: Vec::new(),
            backoff: saga_backoff(),
            propagate_errors: true,
        }
    }
}

impl EventHandler for ProcessManagerEventHandler {
    fn handle(&self, book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        // Check subscription filter (handler-level filtering for in-process mode)
        if !self.targets.is_empty() && !crate::bus::any_target_matches(&book, &self.targets) {
            return Box::pin(async { Ok(()) });
        }

        let correlation_id = book.correlation_id().to_string();
        let pm_name = self.context_factory.name().to_string();
        let pm_domain = self.context_factory.pm_domain().to_string();
        let span = tracing::info_span!("pm.handle", %pm_name, %correlation_id, %pm_domain);

        let factory = self.context_factory.clone();
        let destination_fetcher = self.destination_fetcher.clone();
        let command_executor = self.command_executor.clone();
        let fact_executor = self.fact_executor.clone();
        let backoff = self.backoff;
        let propagate_errors = self.propagate_errors;

        Box::pin(
            async move {
                let book_owned = (*book).clone();

                tracing::info!(
                    pages = book_owned.pages.len(),
                    has_snapshot = book_owned.snapshot.is_some(),
                    domain = %book_owned.domain(),
                    "PM handler received book from bus"
                );

                if correlation_id.is_empty() {
                    debug!("Event has no correlation_id, skipping process manager");
                    return Ok(());
                }

                let ctx = factory.create();
                let fact_executor_ref: Option<&dyn FactExecutor> = fact_executor.as_deref();

                // Events received from bus are always async mode (UNSPECIFIED).
                // CASCADE mode doesn't publish to bus, so PMs called via bus
                // execute their commands with standard async behavior.
                if let Err(e) = orchestrate_pm(
                    ctx.as_ref(),
                    destination_fetcher.as_ref(),
                    command_executor.as_ref(),
                    fact_executor_ref,
                    &book_owned,
                    &pm_name,
                    &pm_domain,
                    &correlation_id,
                    SyncMode::Async,
                    backoff,
                )
                .await
                {
                    error!(
                        error = %e,
                        "Process manager orchestration failed"
                    );
                    if propagate_errors {
                        return Err(e);
                    }
                }

                Ok(())
            }
            .instrument(span),
        )
    }
}

#[cfg(test)]
#[path = "process_manager.test.rs"]
mod tests;
