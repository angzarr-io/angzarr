//! Streaming event handler for angzarr-stream service.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::bus::{BusError, EventHandler};
use crate::proto::event_stream_service_server::EventStreamService;
use crate::proto::{EventBook, EventStreamFilter};

type SubscriberSender = mpsc::Sender<Result<EventBook, Status>>;

/// Subscriber registration.
pub(crate) struct Subscriber {
    pub(crate) sender: SubscriberSender,
}

// ============================================================================
// Helper Functions for Subscriber Management
// ============================================================================

/// Extract correlation ID from event book if present and non-empty.
fn extract_correlation_id(book: &EventBook) -> Option<&str> {
    book.cover
        .as_ref()
        .filter(|c| !c.correlation_id.is_empty())
        .map(|c| c.correlation_id.as_str())
}

/// Send event to all subscribers, returning (sent_count, indices_to_remove).
///
/// # Slow-consumer policy (H-39)
///
/// `try_send` on a bounded channel can fail for two reasons: `Closed`
/// (subscriber gone) or `Full` (subscriber present but not draining
/// fast enough). Both are terminal here — the subscriber is removed
/// and the caller is expected to reconnect via `subscribe()`. We do
/// NOT retry, sleep, or escalate to `send` with a timeout: this
/// function runs while holding the `subscriptions` `RwLock` write
/// guard, and awaiting on a slow consumer would block delivery for
/// every other correlation_id on the service.
///
/// Stream consumers are externally observable (gRPC streams), so a
/// reconnect after a drop is the cleanest backpressure signal: the
/// next `subscribe()` call gets a fresh, empty channel and the
/// consumer starts over from "now". This matches how k8s pod
/// restarts already force consumers to reconnect, so consumers
/// already need that recovery path.
fn send_to_subscribers(
    subscribers: &[Subscriber],
    book: &EventBook,
    correlation_id: &str,
) -> (usize, Vec<usize>) {
    let mut to_remove = Vec::new();
    let mut sent_count = 0;

    for (idx, sub) in subscribers.iter().enumerate() {
        if sub.sender.is_closed() {
            to_remove.push(idx);
            continue;
        }

        match sub.sender.try_send(Ok(book.clone())) {
            Ok(()) => sent_count += 1,
            Err(e) => {
                // Both `Full` and `Closed` are terminal — remove the
                // subscriber rather than dropping the event silently.
                warn!(
                    correlation_id = %correlation_id,
                    error = %e,
                    "Subscriber send failed; removing subscriber (H-39 slow-consumer policy)"
                );
                to_remove.push(idx);
            }
        }
    }

    (sent_count, to_remove)
}

/// Remove stale subscribers by index (in reverse order to preserve indices).
fn remove_stale_subscribers(
    subscribers: &mut Vec<Subscriber>,
    to_remove: Vec<usize>,
    correlation_id: &str,
) {
    if to_remove.is_empty() {
        return;
    }

    debug!(
        correlation_id = %correlation_id,
        removed = to_remove.len(),
        "Removing disconnected subscribers during event delivery"
    );

    for idx in to_remove.into_iter().rev() {
        subscribers.remove(idx);
    }
}

/// Streaming event service.
///
/// Receives events from AMQP and forwards to subscribers filtered by correlation ID.
/// Events without matching subscribers are dropped.
pub struct StreamService {
    /// Map of correlation_id -> list of subscribers
    subscriptions: Arc<RwLock<HashMap<String, Vec<Subscriber>>>>,
}

impl StreamService {
    /// Create a new stream service.
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get a reference to subscriptions for the event handler.
    pub(crate) fn subscriptions(&self) -> Arc<RwLock<HashMap<String, Vec<Subscriber>>>> {
        Arc::clone(&self.subscriptions)
    }

    /// Handle an event book - forward to matching subscribers.
    ///
    /// Used by the Projector gRPC service to receive events from the projector sidecar.
    pub async fn handle(&self, book: &EventBook) {
        let correlation_id = match extract_correlation_id(book) {
            Some(id) => id,
            None => {
                debug!("Dropping event without correlation_id");
                return;
            }
        };

        let mut subs = self.subscriptions.write().await;
        let Some(subscribers) = subs.get_mut(correlation_id) else {
            return;
        };

        let (sent_count, to_remove) = send_to_subscribers(subscribers, book, correlation_id);
        remove_stale_subscribers(subscribers, to_remove, correlation_id);

        if subscribers.is_empty() {
            subs.remove(correlation_id);
            debug!(
                correlation_id = %correlation_id,
                "No subscribers remaining, removed correlation entry"
            );
        } else {
            debug!(
                correlation_id = %correlation_id,
                sent = sent_count,
                remaining = subscribers.len(),
                "Event delivered to subscribers"
            );
        }
    }
}

impl Default for StreamService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl EventStreamService for StreamService {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<EventBook, Status>> + Send + 'static>>;

    async fn subscribe(
        &self,
        request: Request<EventStreamFilter>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let filter = request.into_inner();

        if filter.correlation_id.is_empty() {
            return Err(Status::invalid_argument(
                "correlation_id is required for event stream subscription",
            ));
        }

        let correlation_id = filter.correlation_id.clone();
        debug!(correlation_id = %correlation_id, "New subscription registered");

        // Create channel for this subscriber
        let (tx, rx) = mpsc::channel(32);

        // Clone tx for cleanup task before moving into subscriber
        let cleanup_tx = tx.clone();

        // Register subscriber
        {
            let mut subs = self.subscriptions.write().await;
            subs.entry(correlation_id.clone())
                .or_default()
                .push(Subscriber { sender: tx });
        }

        // Spawn cleanup task that removes subscriber when channel closes
        // This triggers when the gateway drops its subscription (client disconnect or completion)
        let subscriptions = Arc::clone(&self.subscriptions);
        let cleanup_correlation_id = correlation_id.clone();
        tokio::spawn(async move {
            cleanup_tx.closed().await;
            info!(
                correlation_id = %cleanup_correlation_id,
                "Subscriber disconnected, cleaning up subscription"
            );
            let mut subs = subscriptions.write().await;
            if let Some(subscribers) = subs.get_mut(&cleanup_correlation_id) {
                let before_count = subscribers.len();
                subscribers.retain(|s| !s.sender.is_closed());
                let removed_count = before_count - subscribers.len();
                if subscribers.is_empty() {
                    subs.remove(&cleanup_correlation_id);
                    debug!(
                        correlation_id = %cleanup_correlation_id,
                        "Removed last subscriber, correlation entry cleaned up"
                    );
                } else {
                    debug!(
                        correlation_id = %cleanup_correlation_id,
                        removed = removed_count,
                        remaining = subscribers.len(),
                        "Removed closed subscribers"
                    );
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Event handler that forwards to stream subscribers.
pub struct StreamEventHandler {
    subscriptions: Arc<RwLock<HashMap<String, Vec<Subscriber>>>>,
}

impl StreamEventHandler {
    /// Create handler from stream service.
    pub fn new(service: &StreamService) -> Self {
        Self {
            subscriptions: service.subscriptions(),
        }
    }
}

impl EventHandler for StreamEventHandler {
    fn handle(
        &self,
        book: Arc<EventBook>,
    ) -> futures::future::BoxFuture<'static, Result<(), BusError>> {
        let subscriptions = Arc::clone(&self.subscriptions);
        let book = Arc::clone(&book);

        Box::pin(async move {
            // Skip events without correlation ID
            let correlation_id = match book.cover.as_ref() {
                Some(c) if !c.correlation_id.is_empty() => &c.correlation_id,
                _ => {
                    debug!("Dropping event without correlation_id");
                    return Ok(());
                }
            };

            // Look up subscribers
            let mut subs = subscriptions.write().await;
            if let Some(subscribers) = subs.get_mut(correlation_id) {
                // Send to all subscribers, remove closed ones
                let mut to_remove = Vec::new();
                let mut sent_count = 0;
                for (idx, sub) in subscribers.iter().enumerate() {
                    if sub.sender.is_closed() {
                        to_remove.push(idx);
                        continue;
                    }

                    if let Err(e) = sub.sender.try_send(Ok((*book).clone())) {
                        // H-39 slow-consumer policy: any `try_send` failure
                        // (Full or Closed) removes the subscriber. See
                        // `send_to_subscribers` above for the rationale.
                        warn!(
                            correlation_id = %correlation_id,
                            error = %e,
                            "Subscriber send failed; removing subscriber (H-39 slow-consumer policy)"
                        );
                        to_remove.push(idx);
                    } else {
                        sent_count += 1;
                    }
                }

                // Remove closed subscribers (reverse order to preserve indices)
                if !to_remove.is_empty() {
                    debug!(
                        correlation_id = %correlation_id,
                        removed = to_remove.len(),
                        "Removing disconnected subscribers during event delivery"
                    );
                    for idx in to_remove.into_iter().rev() {
                        subscribers.remove(idx);
                    }
                }

                // Remove correlation_id entry if no subscribers left
                if subscribers.is_empty() {
                    subs.remove(correlation_id);
                    debug!(
                        correlation_id = %correlation_id,
                        "No subscribers remaining, removed correlation entry"
                    );
                } else {
                    debug!(
                        correlation_id = %correlation_id,
                        sent = sent_count,
                        remaining = subscribers.len(),
                        "Event delivered to subscribers"
                    );
                }
            }
            // Events without subscribers are silently dropped (expected behavior)

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
