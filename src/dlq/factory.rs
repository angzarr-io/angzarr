//! DLQ factory with self-registration pattern.
//!
//! Each DLQ backend registers itself via `inventory::submit!`.
//! The factory iterates registered backends to find one matching the configured type.

use std::sync::Arc;

use futures::future::BoxFuture;
use tracing::debug;

use super::chained::ChainedDlqPublisher;
use super::config::{DlqConfig, DlqTargetConfig};
use super::error::{errmsg, DlqError, Result};
use super::publishers::NoopDeadLetterPublisher;
use super::DeadLetterPublisher;

// ============================================================================
// Backend Registration
// ============================================================================

/// Function signature for creating DLQ publishers.
pub type CreateDlqFn =
    fn(&DlqTargetConfig) -> BoxFuture<'static, Option<Result<Arc<dyn DeadLetterPublisher>>>>;

/// Self-registering DLQ backend.
///
/// Backends register themselves via `inventory::submit!`:
///
/// ```ignore
/// inventory::submit! {
///     DlqBackend {
///         try_create: |config| {
///             let dlq_type = config.dlq_type.clone();
///             let amqp_config = config.amqp.clone();
///             Box::pin(async move {
///                 if dlq_type != "amqp" {
///                     return None;
///                 }
///                 let Some(amqp_config) = amqp_config else {
///                     return Some(Err(DlqError::NotConfigured));
///                 };
///                 match AmqpDeadLetterPublisher::new(&amqp_config.url).await {
///                     Ok(pub) => Some(Ok(Arc::new(pub) as Arc<dyn DeadLetterPublisher>)),
///                     Err(e) => Some(Err(e)),
///                 }
///             })
///         },
///     }
/// }
/// ```
pub struct DlqBackend {
    pub try_create: CreateDlqFn,
}

inventory::collect!(DlqBackend);

// ============================================================================
// Factory Functions
// ============================================================================

/// Create a single DLQ publisher for a target config.
async fn create_single_publisher(config: &DlqTargetConfig) -> Result<Arc<dyn DeadLetterPublisher>> {
    for backend in inventory::iter::<DlqBackend> {
        if let Some(result) = (backend.try_create)(config).await {
            return result;
        }
    }
    Err(DlqError::UnknownType(format!(
        "{}{}",
        errmsg::UNKNOWN_TYPE,
        config.dlq_type
    )))
}

/// Initialize DLQ publisher from priority list config.
///
/// Returns a ChainedDlqPublisher that tries targets in order.
/// Empty targets list returns NoopDeadLetterPublisher.
pub async fn init_dlq_publisher(
    config: &DlqConfig,
) -> std::result::Result<Arc<dyn DeadLetterPublisher>, Box<dyn std::error::Error>> {
    if config.targets.is_empty() {
        debug!("No DLQ targets configured, using noop publisher");
        return Ok(Arc::new(NoopDeadLetterPublisher));
    }

    let mut publishers = Vec::new();
    for target in &config.targets {
        publishers.push(create_single_publisher(target).await?);
    }

    if publishers.len() == 1 {
        // Single target, no need for chaining
        Ok(publishers.pop().unwrap())
    } else {
        Ok(Arc::new(ChainedDlqPublisher::new(publishers)))
    }
}

/// Initialize a DLQ **reader** from the operator's `dlq.audit` config.
///
/// Mirrors the [`init_dlq_publisher`] hard-fail contract for the read
/// side of R2-15:
///
/// - **`audit` is `None`**: return a `NoopDeadLetterReader`. Callers
///   (e.g. `angzarr_status.rs`) should log a `WARN` so operators know
///   the admin UI will report zero entries even when the publisher
///   side is wired.
/// - **`audit` is `Some` with `storage_type = "postgres"`**: connect a
///   `PostgresDlqReader` to `audit.postgres.uri`. Unreachable backend
///   surfaces as `Err` so the status binary exits at boot rather than
///   silently serving a noop reader.
/// - **`audit` is `Some` with `storage_type = "sqlite"`**: connect a
///   `SqliteDlqReader` to `audit.sqlite.uri()` (in-memory if
///   `path` is unset).
/// - **Any other `storage_type`**: hard-fail with `DlqError::UnknownType`.
///
/// The `audit` config is intentionally separate from `dlq.targets` so
/// operators can route delivery to a queue (AMQP, Pub/Sub) while still
/// reading from a query-friendly database. See R2-15 decision #5.
#[cfg(test)]
#[path = "factory.test.rs"]
mod tests;
