//! Consumer-side handler decorators: subscription filtering and capped
//! redelivery with dead-lettering.
//!
//! Both wrap an [`EventHandler`] and are transport-agnostic, so every bus
//! backend gets the same semantics:
//!
//! - [`TargetFilterHandler`] drops (acknowledges) events that match none of
//!   the component's subscription targets before they reach the client.
//! - [`DeadLetteringHandler`] counts failed deliveries of each event, waits
//!   an exponential backoff between them, and after the configured budget
//!   publishes the event to the DLQ and acknowledges it, so one poison
//!   event cannot block its key, partition or message group forever.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use prost::Message;
use tracing::{error, warn};

use super::config::DeliveryConfig;
use super::error::BusError;
use super::traits::{any_target_matches, EventHandler};
use crate::descriptor::Target;
use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher};
use crate::proto::EventBook;

/// Upper bound on events whose failure count is tracked at once. Reaching
/// it clears the table, which only ever grants extra attempts.
const MAX_TRACKED_EVENTS: usize = 10_000;

/// Passes through only events matching at least one subscription target.
///
/// Non-matching events are acknowledged without calling the inner handler.
/// An empty target list passes everything.
pub struct TargetFilterHandler {
    inner: Box<dyn EventHandler>,
    targets: Vec<Target>,
}

impl TargetFilterHandler {
    /// Wrap `inner` so it only sees events matching `targets`.
    pub fn new(inner: Box<dyn EventHandler>, targets: Vec<Target>) -> Self {
        Self { inner, targets }
    }
}

impl EventHandler for TargetFilterHandler {
    fn handle(&self, book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        if self.targets.is_empty() || any_target_matches(&book, &self.targets) {
            self.inner.handle(book)
        } else {
            Box::pin(async { Ok(()) })
        }
    }
}

/// Caps redelivery of events whose handler keeps failing.
///
/// Failure counts are kept per event (keyed by a fingerprint of the encoded
/// book) for the life of the process; a restart starts the count again, so
/// the budget is "at least `max_attempts`". When the DLQ publisher is not
/// configured the event is never dropped — it keeps failing at the maximum
/// backoff so the transport keeps it.
pub struct DeadLetteringHandler {
    inner: Arc<dyn EventHandler>,
    policy: DeliveryConfig,
    dlq: Arc<dyn DeadLetterPublisher>,
    component: String,
    component_type: String,
    failures: Arc<Mutex<HashMap<u64, u32>>>,
}

impl DeadLetteringHandler {
    /// Wrap `inner`. `component` / `component_type` identify the sidecar in
    /// dead letters (e.g. `"fulfillment"`, `"saga"`).
    pub fn new(
        inner: Box<dyn EventHandler>,
        policy: DeliveryConfig,
        dlq: Arc<dyn DeadLetterPublisher>,
        component: impl Into<String>,
        component_type: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::from(inner),
            policy,
            dlq,
            component: component.into(),
            component_type: component_type.into(),
            failures: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn fingerprint(book: &EventBook) -> u64 {
        let mut hasher = DefaultHasher::new();
        book.encode_to_vec().hash(&mut hasher);
        hasher.finish()
    }

    /// Record one more failure of `key`; returns the new failure count.
    fn record_failure(failures: &Mutex<HashMap<u64, u32>>, key: u64) -> u32 {
        let mut map = failures.lock().unwrap_or_else(|p| p.into_inner());
        if map.len() >= MAX_TRACKED_EVENTS && !map.contains_key(&key) {
            map.clear();
        }
        let count = map.entry(key).or_insert(0);
        *count = count.saturating_add(1);
        *count
    }

    fn forget(failures: &Mutex<HashMap<u64, u32>>, key: u64) {
        failures
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&key);
    }
}

impl EventHandler for DeadLetteringHandler {
    fn handle(&self, book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        let inner = Arc::clone(&self.inner);
        let policy = self.policy.clone();
        let dlq = Arc::clone(&self.dlq);
        let component = self.component.clone();
        let component_type = self.component_type.clone();
        let failures = Arc::clone(&self.failures);

        Box::pin(async move {
            let key = Self::fingerprint(&book);
            let err = match inner.handle(Arc::clone(&book)).await {
                Ok(()) => {
                    Self::forget(&failures, key);
                    return Ok(());
                }
                Err(e) => e,
            };

            let attempts = Self::record_failure(&failures, key);
            if !policy.is_exhausted(attempts) {
                tokio::time::sleep(policy.backoff(attempts)).await;
                return Err(err);
            }

            if !dlq.is_configured() {
                error!(
                    component = %component,
                    attempts,
                    error = %err,
                    "event exhausted its delivery budget but no DLQ target is \
                     configured; it stays on the bus and blocks its key — set \
                     dlq.targets"
                );
                tokio::time::sleep(policy.backoff(attempts)).await;
                return Err(err);
            }

            let dead_letter = AngzarrDeadLetter::from_event_processing_failure(
                &book,
                &err.to_string(),
                attempts,
                true,
                Vec::new(),
                &component,
                &component_type,
            );
            match dlq.publish(dead_letter).await {
                Ok(()) => {
                    Self::forget(&failures, key);
                    warn!(
                        component = %component,
                        attempts,
                        error = %err,
                        "event dead-lettered after exhausting its delivery budget"
                    );
                    Ok(())
                }
                Err(dlq_err) => {
                    error!(
                        component = %component,
                        attempts,
                        error = %err,
                        dlq_error = %dlq_err,
                        "dead-letter publish failed; event stays on the bus"
                    );
                    tokio::time::sleep(policy.backoff(attempts)).await;
                    Err(err)
                }
            }
        })
    }
}

#[cfg(test)]
#[path = "delivery.test.rs"]
mod tests;
