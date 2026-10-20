//! Logging-based DLQ publisher.
//!
//! Logs dead letters at WARN level with structured fields.
//! Useful for observability and as a last-resort fallback target.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use super::super::error::DlqError;
use super::super::factory::DlqBackend;
use super::super::{AngzarrDeadLetter, DeadLetterPublisher};

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    DlqBackend {
        try_create: |config| {
            let dlq_type = config.dlq_type.clone();
            Box::pin(async move {
                if dlq_type != "logging" {
                    return None;
                }
                Some(Ok(Arc::new(LoggingDeadLetterPublisher) as Arc<dyn DeadLetterPublisher>))
            })
        },
    }
}

/// Logging-based DLQ publisher.
///
/// Logs dead letters at WARN level with structured fields for observability.
/// Unlike NoopDeadLetterPublisher, this reports itself as configured.
pub struct LoggingDeadLetterPublisher;

#[async_trait]
impl DeadLetterPublisher for LoggingDeadLetterPublisher {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        let domain = dead_letter.domain().unwrap_or("unknown");
        let correlation_id = dead_letter.correlation_id().unwrap_or_default();

        warn!(
            domain = %domain,
            correlation_id = %correlation_id,
            reason = %dead_letter.rejection_reason,
            reason_type = %dead_letter.reason_type(),
            source_component = %dead_letter.source_component,
            source_component_kind = %dead_letter.source_component_kind.as_str_name(),
            "Dead letter published to logging DLQ"
        );

        #[cfg(feature = "otel")]
        {
            use crate::advice::metrics::{
                backend_attr, domain_attr, reason_type_attr, DLQ_PUBLISH_TOTAL,
            };
            DLQ_PUBLISH_TOTAL.add(
                1,
                &[
                    domain_attr(domain),
                    reason_type_attr(dead_letter.reason_type()),
                    backend_attr("logging"),
                ],
            );
        }

        Ok(())
    }

    fn is_configured(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[path = "logging.test.rs"]
mod tests;
