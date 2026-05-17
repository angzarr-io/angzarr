//! Aggregate command handler.
//!
//! Receives commands via gRPC or command bus and orchestrates aggregate execution
//! using the command pipeline and context factory.
//!
//! Works with any `AggregateContextFactory` implementation — gRPC (distributed)
//! or local (in-process) — enabling deploy-anywhere aggregate code.
//!
//! Supports:
//! - Synchronous gRPC command handling (client waits for response)
//! - Asynchronous command bus handling (response via published events)
//! - Retry with backoff on sequence conflicts
//! - Per-domain sync projectors

use std::sync::Arc;

use backon::ExponentialBuilder;
use futures::future::BoxFuture;
use tonic::Status;
use tracing::{error, info, Instrument};

use prost::Message;

use crate::bus::{BusError, EventHandler};
use crate::orchestration::aggregate::{execute_command_with_retry, AggregateContextFactory};
use crate::orchestration::projector::{ProjectionMode, ProjectorHandler};
use crate::proto::{CommandBook, CommandResponse, EventBook};
use crate::proto_ext::CoverExt;
use crate::utils::retry::saga_backoff;

/// Type URL suffix for wrapped CommandBook in bus transport.
const COMMAND_BOOK_TYPE_SUFFIX: &str = "angzarr.CommandBook";

/// Sync projector entry for per-domain projector handling.
pub struct SyncProjectorEntry {
    /// Projector name for logging.
    pub name: String,
    /// Handler to call synchronously during command response.
    pub handler: Arc<dyn ProjectorHandler>,
}

/// Command handler that orchestrates aggregate execution via a context factory.
///
/// Uses `AggregateContextFactory` to create per-invocation contexts, enabling
/// the same handler code for both distributed (gRPC) and in-process (local) modes.
///
/// One handler per aggregate domain (like SagaEventHandler is one per saga).
pub struct AggregateCommandHandler {
    context_factory: Arc<dyn AggregateContextFactory>,
    backoff: ExponentialBuilder,
    sync_projectors: Vec<SyncProjectorEntry>,
}

impl AggregateCommandHandler {
    /// Create a new handler from a context factory.
    pub fn new(context_factory: Arc<dyn AggregateContextFactory>) -> Self {
        Self {
            context_factory,
            backoff: saga_backoff(),
            sync_projectors: Vec::new(),
        }
    }

    /// Create with custom backoff configuration.
    pub fn with_backoff(mut self, backoff: ExponentialBuilder) -> Self {
        self.backoff = backoff;
        self
    }

    /// Add sync projectors to be called after command execution.
    pub fn with_sync_projectors(mut self, projectors: Vec<SyncProjectorEntry>) -> Self {
        self.sync_projectors = projectors;
        self
    }

    /// The domain this handler serves.
    pub fn domain(&self) -> &str {
        self.context_factory.domain()
    }

    /// Execute a command synchronously (gRPC path).
    ///
    /// Creates a fresh context, executes the command pipeline with retry,
    /// and returns the response with any sync projector results.
    #[tracing::instrument(
        name = "aggregate.handler.execute",
        skip_all,
        fields(domain = %self.context_factory.domain())
    )]
    pub async fn execute(&self, command: CommandBook) -> Result<CommandResponse, Status> {
        let ctx = self.context_factory.create();
        let business = self.context_factory.client_logic();
        let domain = self.context_factory.domain();

        let correlation_id = command
            .cover
            .as_ref()
            .map(|c| c.correlation_id.as_str())
            .unwrap_or("");

        info!(
            %domain,
            %correlation_id,
            "Executing command"
        );

        let mut response =
            execute_command_with_retry(ctx.as_ref(), business.as_ref(), command, self.backoff)
                .await?;

        // Call sync projectors if we have events
        if let Some(ref events) = response.events {
            if !events.pages.is_empty() {
                for entry in &self.sync_projectors {
                    match entry.handler.handle(events, ProjectionMode::Execute).await {
                        Ok(projection) => {
                            response.projections.push(projection);
                        }
                        Err(e) => {
                            error!(
                                projector = %entry.name,
                                error = %e,
                                "Sync projector failed"
                            );
                        }
                    }
                }
            }
        }

        Ok(response)
    }
}

/// EventHandler implementation for async command delivery via bus.
///
/// Commands can be wrapped in EventBook format and delivered via the command bus.
/// This enables decoupled, replay-safe command delivery across network boundaries.
impl EventHandler for AggregateCommandHandler {
    fn handle(&self, book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        let correlation_id = book.correlation_id().to_string();
        let domain = self.context_factory.domain().to_string();
        let span = tracing::info_span!("aggregate.handler.bus", %domain, %correlation_id);

        let factory = self.context_factory.clone();
        let backoff = self.backoff;

        Box::pin(
            async move {
                // Extract command from EventBook
                // Commands on bus are wrapped as single-page EventBooks with command payload
                let command = match extract_command_from_event_book(&book) {
                    Some(cmd) => cmd,
                    None => {
                        // Not a command event - might be a notification, skip
                        return Ok(());
                    }
                };

                let ctx = factory.create();
                let business = factory.client_logic();

                if let Err(e) =
                    execute_command_with_retry(ctx.as_ref(), business.as_ref(), command, backoff)
                        .await
                {
                    error!(
                        error = %e,
                        "Async command execution failed"
                    );
                }

                // Response events are published by the pipeline's post_persist
                Ok(())
            }
            .instrument(span),
        )
    }
}

/// Wrap a CommandBook as an EventBook for bus transport.
///
/// Commands can be delivered asynchronously via the event bus by wrapping
/// them in EventBook format. The receiving aggregate's EventHandler extracts
/// and executes the command.
///
/// # Format
/// The CommandBook is serialized as `google.protobuf.Any` with type_url
/// `type.googleapis.com/angzarr.CommandBook`.
pub fn wrap_command_for_bus(command: &CommandBook) -> EventBook {
    use crate::proto::{event_page::Payload, page_header::SequenceType, EventPage, PageHeader};
    use prost_types::Any;

    let any = Any {
        type_url: format!("type.googleapis.com/{}", COMMAND_BOOK_TYPE_SUFFIX),
        value: command.encode_to_vec(),
    };

    EventBook {
        cover: command.cover.clone(),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(0)),
            }),
            created_at: None,
            payload: Some(Payload::Event(any)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Extract a CommandBook from an EventBook if it contains a wrapped command.
///
/// Commands delivered via bus are wrapped in EventBook format for transport.
/// The EventBook contains a single EventPage with the CommandBook serialized
/// as a `google.protobuf.Any` with type_url ending in `angzarr.CommandBook`.
///
/// # Format
/// ```text
/// EventBook {
///   cover: { domain: "player", ... },
///   pages: [{
///     event: Any {
///       type_url: "type.googleapis.com/angzarr.CommandBook",
///       value: <serialized CommandBook>
///     }
///   }]
/// }
/// ```
fn extract_command_from_event_book(book: &EventBook) -> Option<CommandBook> {
    // Commands are wrapped as single-page EventBooks
    let page = book.pages.first()?;

    // Extract the Any payload
    let event = match &page.payload {
        Some(crate::proto::event_page::Payload::Event(any)) => any,
        _ => return None,
    };

    // Check if this is a wrapped CommandBook
    if !event.type_url.ends_with(COMMAND_BOOK_TYPE_SUFFIX) {
        return None;
    }

    // Deserialize the CommandBook
    CommandBook::decode(event.value.as_slice()).ok()
}

#[cfg(test)]
#[path = "aggregate.test.rs"]
mod tests;
