//! Transparent event bus wrapper that offloads large payloads to external storage.
//!
//! Implements the claim check pattern: when an event payload exceeds the bus's
//! size limit, the payload is stored externally and replaced with a reference.
//! On the receiving side, references are resolved back to full payloads.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use angzarr::bus::{OffloadingEventBus, OffloadingConfig};
//! use angzarr::payload_store::FilesystemPayloadStore;
//!
//! let store = FilesystemPayloadStore::new("/var/angzarr/payloads").await?;
//! let config = OffloadingConfig::new(store)
//!     .with_threshold(256 * 1024);  // 256 KB threshold
//!
//! let bus = OffloadingEventBus::wrap(inner_bus, config);
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use prost::Message;
use tracing::{debug, error};

use super::{BusError, EventBus, EventHandler, PublishResult, Result};
use crate::payload_store::{PayloadStore, PayloadStoreError};
use crate::proto::{EventBook, EventPage};

/// Configuration for payload offloading.
pub struct OffloadingConfig<S: PayloadStore> {
    /// The payload store to use for offloading.
    pub store: Arc<S>,
    /// Minimum payload size to trigger offloading.
    /// Default: use bus's max_message_size() if available.
    pub threshold: Option<usize>,
}

impl<S: PayloadStore> OffloadingConfig<S> {
    /// Create new offloading config with the given payload store.
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            threshold: None,
        }
    }

    /// Set explicit threshold for offloading (overrides bus limit).
    pub fn with_threshold(mut self, bytes: usize) -> Self {
        self.threshold = Some(bytes);
        self
    }
}

/// Transparent event bus wrapper that offloads large payloads.
///
/// Wraps any EventBus and automatically:
/// - On publish: offloads large event payloads to external storage
/// - On subscribe: resolves payload references back to full events
pub struct OffloadingEventBus<S: PayloadStore> {
    inner: Arc<dyn EventBus>,
    store: Arc<S>,
    threshold: Option<usize>,
}

impl<S: PayloadStore + 'static> OffloadingEventBus<S> {
    /// Wrap an event bus with payload offloading.
    pub fn wrap(inner: Arc<dyn EventBus>, config: OffloadingConfig<S>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            store: config.store,
            threshold: config.threshold,
        })
    }

    /// Get effective threshold for this bus.
    fn effective_threshold(&self) -> Option<usize> {
        self.threshold.or_else(|| self.inner.max_message_size())
    }

    /// Process an event book for publishing, offloading large payloads.
    ///
    /// Accepts and returns `Arc<EventBook>` to avoid cloning in the common case
    /// where no offloading is needed (passthrough).
    async fn process_for_publish(&self, book: Arc<EventBook>) -> Result<Arc<EventBook>> {
        let threshold = match self.effective_threshold() {
            Some(t) => t,
            None => return Ok(book), // No limit, pass through (zero-copy)
        };

        // Check total serialized size first
        let total_size = book.encoded_len();
        if total_size <= threshold {
            return Ok(book); // Small enough, pass through (zero-copy)
        }

        // Need to offload - process each page
        use crate::proto::event_page::Payload;

        let mut new_pages = Vec::with_capacity(book.pages.len());

        for page in &book.pages {
            let page_size = page.encoded_len();

            // Only offload pages that are large
            if page_size > threshold / 2 {
                // Offload if page is >50% of threshold
                if let Some(Payload::Event(ref event)) = page.payload {
                    let payload_bytes = event.encode_to_vec();

                    // H-02: `store.put` failure must surface as `Err`. The
                    // previous behaviour (warn! + silently inline the
                    // oversized payload) either produced an opaque
                    // inner-bus rejection later on the call path or — if
                    // the inner bus accepted it — silently violated the
                    // size-bounded contract the offloading wrapper
                    // promises. The bus refuses to lie; the caller is in
                    // the right position to decide retry / DLQ / circuit
                    // break. Matches the explicit-Err idiom landed for
                    // C-12 in `sns_sqs/bus.rs`.
                    let reference = self.store.put(&payload_bytes).await.map_err(|e| {
                        error!(
                            error = %e,
                            page_size = page_size,
                            threshold = threshold,
                            "Payload store rejected put; refusing silent inline fallback"
                        );
                        BusError::Publish(format!(
                            "Payload offload failed: {} (inline fallback refused — \
                             the offloading wrapper will not silently exceed the \
                             inner bus's size limit)",
                            e
                        ))
                    })?;

                    debug!(
                        original_size = payload_bytes.len(),
                        uri = %reference.uri,
                        "Offloaded large event payload"
                    );

                    new_pages.push(EventPage {
                        header: page.header.clone(),
                        created_at: page.created_at,
                        payload: Some(Payload::External(reference)),
                        no_commit: page.no_commit,
                        cascade_id: page.cascade_id.clone(),
                    });
                    continue;
                }
            }

            // Keep original page (small or non-event payload)
            new_pages.push(page.clone());
        }

        Ok(Arc::new(EventBook {
            cover: book.cover.clone(),
            pages: new_pages,
            snapshot: book.snapshot.clone(),
            next_sequence: book.next_sequence,
        }))
    }

    /// Resolve external payload references in an event book.
    pub async fn resolve_payloads(&self, book: &EventBook) -> Result<EventBook> {
        resolve_payloads_with_store(self.store.as_ref(), book).await
    }
}

#[async_trait]
impl<S: PayloadStore + 'static> EventBus for OffloadingEventBus<S> {
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        // Process for offloading (zero-copy passthrough when no offloading needed)
        let processed = self.process_for_publish(book).await?;
        self.inner.publish(processed).await
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        // Wrap handler to resolve external payloads before delivery.
        // This makes offloading transparent to consumers - they receive
        // fully resolved EventBooks regardless of whether payloads were offloaded.
        let resolving_handler = Box::new(ResolvingHandler {
            inner: Arc::from(handler),
            store: Arc::clone(&self.store),
        });
        self.inner.subscribe(resolving_handler).await
    }

    async fn start_consuming(&self) -> Result<()> {
        self.inner.start_consuming().await
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        // Create inner subscriber and wrap it
        let inner_sub = self.inner.create_subscriber(name, domain_filter).await?;
        Ok(Arc::new(OffloadingEventBus {
            inner: inner_sub,
            store: Arc::clone(&self.store),
            threshold: self.threshold,
        }))
    }

    fn max_message_size(&self) -> Option<usize> {
        self.inner.max_message_size()
    }
}

impl From<PayloadStoreError> for BusError {
    fn from(e: PayloadStoreError) -> Self {
        BusError::Publish(format!("Payload store error: {}", e))
    }
}

// ============================================================================
// Resolving Handler
// ============================================================================

/// Resolve external payload references in an EventBook.
///
/// Standalone function for use by `ResolvingHandler`. Fetches external payloads
/// from the store and replaces references with inline events.
async fn resolve_payloads_with_store<S: PayloadStore>(
    store: &S,
    book: &EventBook,
) -> Result<EventBook> {
    use crate::proto::event_page::Payload;

    let mut new_pages = Vec::with_capacity(book.pages.len());

    for page in &book.pages {
        if let Some(Payload::External(ref reference)) = page.payload {
            // H-03: `store.get` failure (or decode failure) must surface as
            // `Err`. The previous behaviour (warn! + pass the External page
            // through to the handler) silently delivered payloads the
            // business handler could not decode — events appeared to be
            // handled successfully while their contents were never
            // observed. Surfacing the error lets the bus transport (AMQP,
            // JetStream, etc.) nack / redeliver, and lets the handler
            // dispatch path see the failure. Matches the explicit-Err
            // idiom landed for C-12 in `sns_sqs/bus.rs`.
            let payload_bytes = store.get(reference).await.map_err(|e| {
                error!(
                    uri = %reference.uri,
                    error = %e,
                    "Failed to retrieve external payload; refusing to deliver \
                     unresolved External page to handler"
                );
                BusError::Publish(format!(
                    "Failed to retrieve external payload {}: {} \
                     (unresolved External page would be undecodable by handlers)",
                    reference.uri, e
                ))
            })?;

            let event = prost_types::Any::decode(payload_bytes.as_slice()).map_err(|e| {
                error!(
                    uri = %reference.uri,
                    error = %e,
                    "Failed to decode retrieved payload; refusing to deliver \
                     unresolved External page to handler"
                );
                BusError::Publish(format!(
                    "Failed to decode retrieved payload {}: {}",
                    reference.uri, e
                ))
            })?;

            new_pages.push(EventPage {
                header: page.header.clone(),
                created_at: page.created_at,
                payload: Some(Payload::Event(event)),
                no_commit: page.no_commit,
                cascade_id: page.cascade_id.clone(),
            });
            continue;
        }

        // Keep original page (no External reference)
        new_pages.push(page.clone());
    }

    Ok(EventBook {
        cover: book.cover.clone(),
        pages: new_pages,
        snapshot: book.snapshot.clone(),
        next_sequence: book.next_sequence,
    })
}

/// Handler wrapper that resolves external payload references before delegation.
///
/// When the `OffloadingEventBus` receives events with external payload references,
/// this handler fetches the actual payloads from the store before passing them
/// to the wrapped handler. This makes payload offloading transparent to consumers.
struct ResolvingHandler<S: PayloadStore> {
    inner: Arc<dyn EventHandler>,
    store: Arc<S>,
}

impl<S: PayloadStore + 'static> EventHandler for ResolvingHandler<S> {
    fn handle(
        &self,
        book: Arc<EventBook>,
    ) -> BoxFuture<'static, std::result::Result<(), BusError>> {
        let store = Arc::clone(&self.store);
        let inner = Arc::clone(&self.inner);

        Box::pin(async move {
            // Resolve external payloads before passing to the actual handler
            let resolved = resolve_payloads_with_store(store.as_ref(), &book).await?;
            inner.handle(Arc::new(resolved)).await
        })
    }
}

#[cfg(test)]
#[path = "offloading.test.rs"]
mod tests;
