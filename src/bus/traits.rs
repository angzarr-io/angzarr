//! Bus trait definitions.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::descriptor::Target;
use crate::proto::{CommandBook, EventBook};
use crate::proto_ext::CoverExt;

use super::error::{BusError, Result};

/// Handler for processing events from the bus.
pub trait EventHandler: Send + Sync {
    /// Process an event book.
    fn handle(&self, book: Arc<EventBook>)
        -> BoxFuture<'static, std::result::Result<(), BusError>>;
}

/// Handler for processing commands from the command bus.
///
/// Used by aggregate coordinators to receive commands in async mode.
/// Commands are published by sagas when `SyncMode::Async` is used.
pub trait CommandHandler: Send + Sync {
    /// Process a command book.
    ///
    /// The handler should execute the command and return. Results
    /// (success or rejection) flow back via the event bus as
    /// `RejectionNotification` for failures.
    fn handle(
        &self,
        command: Arc<CommandBook>,
    ) -> BoxFuture<'static, std::result::Result<(), BusError>>;
}

/// Command bus for async command delivery.
///
/// Sagas publish commands to this bus when `SyncMode::Async` is used.
/// Aggregate coordinators subscribe to receive commands for their domain.
/// This enables true fire-and-forget command execution with rejections
/// flowing back via `RejectionNotification` on the event bus.
#[async_trait]
pub trait CommandBus: Send + Sync {
    /// Publish a command to be executed asynchronously.
    ///
    /// Returns immediately after queuing the command.
    /// The command will be routed to the appropriate aggregate
    /// coordinator based on its domain.
    async fn publish(&self, command: Arc<CommandBook>) -> Result<()>;

    /// Subscribe to commands for a specific domain.
    ///
    /// The handler will be called for each command targeting this domain.
    async fn subscribe(&self, domain: &str, handler: Box<dyn CommandHandler>) -> Result<()>;
}

/// Result of publishing events to the bus.
///
/// # C14: no consumer-ack / projection data (yet)
///
/// This used to carry a `projections: Vec<Projection>` field, but every
/// transport (`amqp`, `kafka`, `pubsub`, `sns_sqs`, `mock`) always
/// constructed it via `PublishResult::default()` — nothing ever populated
/// it, and the one caller that read it (`response_builder::build_command_response`,
/// itself unreachable from any production call site — the aggregate
/// pipeline in `orchestration::aggregate::grpc` discards `EventBus::publish`'s
/// return value and computes its own `Vec<Projection>` directly via
/// synchronous projector RPCs) always saw an empty vec. The field was
/// removed rather than left to imply semantics no transport delivers.
///
/// Currently a zero-field placeholder: kept as a distinct return type (not
/// `()`) so a future transport that can genuinely report consumer
/// acknowledgment or synchronous projector output has somewhere to put it
/// without changing the `EventBus::publish` signature.
#[derive(Debug, Default)]
pub struct PublishResult {}

/// Interface for event delivery to projectors/sagas.
///
/// Implementations handle both publishing and subscriber creation through a
/// single interface. The runtime creates subscribers via `create_subscriber`
/// — no transport-specific code needed.
#[async_trait]
pub trait EventBus: Send + Sync {
    /// Publish events to consumers.
    ///
    /// The EventBook is wrapped in Arc to enforce immutability during distribution.
    /// All consumers receive a zero-copy reference to the same immutable data.
    ///
    /// Every current transport (`amqp`, `kafka`, `pubsub`, `sns_sqs`) queues
    /// the book and returns once the broker accepts it — none blocks until
    /// downstream consumers acknowledge. Synchronous projector output is a
    /// separate mechanism (`orchestration::aggregate::grpc::call_sync_projectors`),
    /// not something this call returns. See `PublishResult`'s docs.
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult>;

    /// Subscribe to events (for projector/saga implementations).
    ///
    /// The handler will be called for each event book received.
    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()>;

    /// Start consuming events (for bus implementations that require explicit start).
    ///
    /// Readiness contract (T10): when this returns `Ok`, the consumer is
    /// ESTABLISHED — every event published afterwards is eligible for
    /// delivery to the registered handlers. Implementations must not
    /// return before the receiving entity exists on the broker (queue
    /// declared + bound, subscription created) or, for log-retaining
    /// transports, before the subscription is registered such that
    /// retention covers the gap (Kafka: `auto.offset.reset=earliest`).
    /// Callers may publish immediately after this resolves without a
    /// grace period; delivery latency remains transport-dependent.
    ///
    /// If the broker is unreachable, implementations should wait through
    /// their reconnect/backoff path until the first successful attach
    /// rather than resolve early.
    ///
    /// Default implementation is a no-op for backwards compatibility.
    async fn start_consuming(&self) -> Result<()> {
        Ok(())
    }

    /// Create a subscriber bus that shares this bus's underlying transport.
    ///
    /// Events published on this bus will be delivered to the returned subscriber.
    /// Each implementation creates a transport-appropriate subscriber:
    /// - IPC: creates a named pipe subscriber
    /// - AMQP: creates a queue bound to the exchange
    /// - Kafka: creates a consumer group subscription
    ///
    /// # Arguments
    /// * `name` — subscriber identity (queue name, consumer group, pipe name)
    /// * `domain_filter` — restrict delivery to this domain (`None` = all domains)
    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>>;

    /// Maximum message size in bytes for this bus.
    ///
    /// Returns the hard limit imposed by the underlying transport.
    /// Implementations should return well-known constants for their transport:
    /// - SNS/SQS: 256 KB
    /// - Pub/Sub: 10 MB
    /// - Kafka: broker-configurable, typically 1 MB default
    /// - AMQP: broker-configurable, typically 128 MB default
    /// - IPC: None (memory-bound only)
    ///
    /// Returns `None` if the bus has no practical limit.
    fn max_message_size(&self) -> Option<usize> {
        None // Default: no limit
    }
}

// ============================================================================
// Subscription Matching
// ============================================================================

/// Check if an EventBook matches a target filter.
///
/// A target matches if:
/// - The domain matches the target's domain
/// - AND either:
///   - The target has no types (matches all events from domain)
///   - OR at least one event in the book has a `type_url` that
///     matches a target type per [`Target::matches_type`]
///     (token-boundary, not substring suffix)
///
/// # Example
/// ```ignore
/// let target = Target {
///     domain: "order".to_string(),
///     types: vec!["OrderCreated".to_string(), "OrderShipped".to_string()],
/// };
/// if target_matches(&book, &target) {
///     // Process the event
/// }
/// ```
pub fn target_matches(book: &EventBook, target: &Target) -> bool {
    let routing_key = book.routing_key();

    // Routing key must match target domain (edition-prefixed)
    if target.domain != routing_key {
        return false;
    }

    // If no types specified, match all events from this domain
    if target.types.is_empty() {
        return true;
    }

    // Check if any event matches any target type
    book.pages.iter().any(|page| {
        if let Some(crate::proto::event_page::Payload::Event(event)) = &page.payload {
            target.matches_type(&event.type_url)
        } else {
            false
        }
    })
}

/// Check if an EventBook matches any of the given targets.
///
/// Returns true if at least one target matches the event book.
pub fn any_target_matches(book: &EventBook, targets: &[Target]) -> bool {
    targets.iter().any(|t| target_matches(book, t))
}

/// Check if a domain matches any of the given domain patterns.
///
/// Supports hierarchical matching:
/// - Empty patterns list matches all domains
/// - Exact match: "orders" matches "orders"
/// - Hierarchical prefix: "game.*" matches "game.player", "game.table", etc.
/// - Wildcard: "*" matches any domain
///
/// Used for subscribe-side filtering when the message bus doesn't support
/// hierarchical topic matching natively (e.g., Pub/Sub, SNS/SQS).
pub fn domain_matches_any(domain: &str, patterns: &[String]) -> bool {
    // Empty patterns means match all
    if patterns.is_empty() {
        return true;
    }

    patterns.iter().any(|pattern| {
        if pattern == "*" {
            true
        } else if let Some(prefix) = pattern.strip_suffix(".*") {
            // Hierarchical match: "game.*" matches "game.player" but not "game" or "gameplay"
            // Domain must be exactly "{prefix}.{something}", not just start with prefix.
            domain.starts_with(prefix)
                && domain.len() > prefix.len()
                && domain.as_bytes().get(prefix.len()) == Some(&b'.')
        } else {
            domain == pattern
        }
    })
}
