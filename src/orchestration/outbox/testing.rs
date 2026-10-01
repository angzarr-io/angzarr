//! Test support: an outbox whose deliveries are recorded instead of sent.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{DeliveryResult, MemoryOutboxLog, Outbox, OutboxDeliverer, OutboxEntry, RetryPolicy};
use crate::storage::ProvenanceKind;

/// Records every entry it is asked to deliver; every delivery succeeds.
#[derive(Default)]
pub(crate) struct RecordingDeliverer {
    delivered: Mutex<Vec<OutboxEntry>>,
}

impl RecordingDeliverer {
    /// Every entry delivery was attempted for.
    pub(crate) fn attempted(&self) -> Vec<OutboxEntry> {
        self.delivered.lock().unwrap().clone()
    }

    /// The attempted entries of one kind.
    pub(crate) fn attempted_of(&self, kind: ProvenanceKind) -> Vec<OutboxEntry> {
        self.attempted()
            .into_iter()
            .filter(|e| e.kind == kind)
            .collect()
    }
}

#[async_trait]
impl OutboxDeliverer for RecordingDeliverer {
    async fn deliver(&self, entry: &OutboxEntry) -> DeliveryResult {
        self.delivered.lock().unwrap().push(entry.clone());
        DeliveryResult::Delivered
    }
}

/// An in-memory outbox delivering through a [`RecordingDeliverer`].
pub(crate) fn recording_outbox(name: &str) -> (Arc<Outbox>, Arc<RecordingDeliverer>) {
    let deliverer = Arc::new(RecordingDeliverer::default());
    let outbox = Outbox::new(
        name,
        "saga",
        Arc::new(MemoryOutboxLog),
        deliverer.clone(),
        RetryPolicy {
            jitter: false,
            ..RetryPolicy::default()
        },
    );
    (Arc::new(outbox), deliverer)
}
