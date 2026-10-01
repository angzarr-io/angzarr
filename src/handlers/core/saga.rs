//! Saga event handler.
//!
//! Receives events from the event bus and orchestrates saga execution
//! using the shared orchestration module.
//!
//! Works with any `SagaContextFactory` implementation — gRPC (distributed)
//! or local (in-process) — enabling deploy-anywhere saga code.
//!
//! Supports:
//! - Retry with backoff on transient delivery failures
//! - Output domain validation
//! - Compensation flow for rejected commands (via gRPC factory)

use std::sync::Arc;

use backon::ExponentialBuilder;
use futures::future::BoxFuture;
use tracing::{error, Instrument};

use crate::bus::{BusError, CommandBus, EventHandler};
use crate::orchestration::command::CommandExecutor;
use crate::orchestration::saga::{orchestrate_saga, OutputDomainValidator, SagaContextFactory};
use crate::orchestration::FactExecutor;
use crate::proto::{EventBook, SyncMode};
use crate::proto_ext::CoverExt;
use crate::utils::retry::saga_backoff;

/// Event handler that orchestrates saga execution via a context factory.
///
/// Uses `SagaContextFactory` to create per-invocation contexts, enabling
/// the same handler code for both distributed (gRPC) and in-process (local) modes.
/// Command execution is passed directly to orchestration functions, matching
/// the PM handler pattern.
pub struct SagaEventHandler {
    context_factory: Arc<dyn SagaContextFactory>,
    command_executor: Arc<dyn CommandExecutor>,
    command_bus: Option<Arc<dyn CommandBus>>,
    fact_executor: Option<Arc<dyn FactExecutor>>,
    output_domain_validator: Option<Arc<OutputDomainValidator>>,
    backoff: ExponentialBuilder,
    /// When true, orchestration errors are propagated to the caller.
    /// When false (default), errors are logged but handler returns Ok(()).
    ///
    /// Default: false (maintains backward compatibility).
    /// Use `with_error_propagation(true)` to enable strict error handling.
    propagate_errors: bool,
}

impl SagaEventHandler {
    /// Create a basic saga handler with minimal dependencies.
    ///
    /// Use this for simple sagas that:
    /// - Only emit commands (no fact injection)
    /// - Don't need output domain validation
    /// - Use default retry backoff
    ///
    /// For advanced features, use `from_factory_with_validator`.
    pub fn from_factory(
        context_factory: Arc<dyn SagaContextFactory>,
        command_executor: Arc<dyn CommandExecutor>,
    ) -> Self {
        Self {
            context_factory,
            command_executor,
            command_bus: None,
            fact_executor: None,
            output_domain_validator: None,
            backoff: saga_backoff(),
            // D-3 (review decision): sagas are at-least-once by DEFAULT.
            // The old `false` default acked transient orchestration
            // failures (saga service blip, retry exhaustion, H-15 fact
            // errors) and silently lost the cross-domain translation —
            // the one job a saga has. PM and aggregate handlers already
            // default to propagation; set `with_error_propagation(false)`
            // explicitly to opt back into ack-on-failure.
            propagate_errors: true,
        }
    }

    /// Create a fully-featured saga handler with all optional dependencies.
    ///
    /// # Parameters
    ///
    /// - `command_bus`: For async command delivery (alternative to sync executor)
    /// - `fact_executor`: For injecting facts (external events) into aggregates
    /// - `output_domain_validator`: For validating saga output commands target allowed domains
    /// - `backoff`: Custom retry configuration for transient failures
    ///
    /// # When to Use Each Dependency
    ///
    /// | Dependency | Use Case |
    /// |------------|----------|
    /// | `command_bus` | Saga outputs async commands (fire-and-forget) |
    /// | `fact_executor` | Saga injects external facts (e.g., webhook events) |
    /// | `output_domain_validator` | Enforce saga can only command specific domains |
    pub fn from_factory_with_validator(
        context_factory: Arc<dyn SagaContextFactory>,
        command_executor: Arc<dyn CommandExecutor>,
        command_bus: Option<Arc<dyn CommandBus>>,
        fact_executor: Option<Arc<dyn FactExecutor>>,
        output_domain_validator: Option<Arc<OutputDomainValidator>>,
        backoff: ExponentialBuilder,
    ) -> Self {
        Self {
            context_factory,
            command_executor,
            command_bus,
            fact_executor,
            output_domain_validator,
            backoff,
            // D-3 (review decision): sagas are at-least-once by DEFAULT.
            // The old `false` default acked transient orchestration
            // failures (saga service blip, retry exhaustion, H-15 fact
            // errors) and silently lost the cross-domain translation —
            // the one job a saga has. PM and aggregate handlers already
            // default to propagation; set `with_error_propagation(false)`
            // explicitly to opt back into ack-on-failure.
            propagate_errors: true,
        }
    }

    /// Configure error propagation behavior.
    ///
    /// When enabled (the DEFAULT since D-3), orchestration errors are
    /// returned to the caller, which triggers message redelivery on the
    /// bus — sagas are at-least-once. When disabled, errors are logged
    /// but the handler returns Ok(()) and the source event is acked
    /// (at-most-once; the translation is lost on any transient failure).
    pub fn with_error_propagation(mut self, propagate: bool) -> Self {
        self.propagate_errors = propagate;
        self
    }
}

impl EventHandler for SagaEventHandler {
    fn handle(&self, book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        let correlation_id = book.correlation_id().to_string();
        let saga_name = self.context_factory.name().to_string();
        let span = tracing::info_span!("saga.handle", %saga_name, %correlation_id);

        let factory = self.context_factory.clone();
        let executor = self.command_executor.clone();
        let command_bus = self.command_bus.clone();
        let fact_executor = self.fact_executor.clone();
        let validator = self.output_domain_validator.clone();
        let backoff = self.backoff;
        let propagate_errors = self.propagate_errors;

        Box::pin(
            async move {
                let ctx = factory.create(book);

                let validator_ref: Option<&OutputDomainValidator> = validator.as_deref();
                let command_bus_ref: Option<&dyn CommandBus> = command_bus.as_deref();
                let fact_executor_ref: Option<&dyn FactExecutor> = fact_executor.as_deref();

                // Events received from bus are always async mode.
                // CASCADE mode doesn't publish to bus, so sagas called via bus
                // execute their commands with standard async behavior.
                if let Err(e) = orchestrate_saga(
                    ctx.as_ref(),
                    executor.as_ref(),
                    command_bus_ref,
                    fact_executor_ref,
                    &saga_name,
                    &correlation_id,
                    validator_ref,
                    SyncMode::Async,
                    backoff,
                    None,
                )
                .await
                {
                    error!(
                        error = %e,
                        "Saga orchestration failed"
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
#[path = "saga.test.rs"]
mod tests;
