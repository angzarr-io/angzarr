//! AMQP-based DLQ publisher using RabbitMQ.
//!
//! Publishes dead letters to a topic exchange with routing key: `{domain}`.
//! Exchange name: `angzarr.dlq`. A durable catch-all queue
//! (`angzarr.dlq.catchall`, bound `#`) is declared at init so dead letters
//! are RETAINED even when no per-domain consumer queue exists — without it
//! an unroutable dead letter is discarded by the broker (D1).

use std::sync::Arc;

use async_trait::async_trait;
use deadpool_lapin::{Manager, Pool};
use lapin::{
    options::BasicPublishOptions, options::ConfirmSelectOptions, options::ExchangeDeclareOptions,
    options::QueueBindOptions, options::QueueDeclareOptions, publisher_confirm::Confirmation,
    types::FieldTable, BasicProperties, ExchangeKind,
};
use prost::Message;
use tracing::info;

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
            let amqp_config = config.amqp.clone();
            Box::pin(async move {
                if dlq_type != "amqp" {
                    return None;
                }
                let Some(amqp_config) = amqp_config else {
                    return Some(Err(DlqError::NotConfigured));
                };
                match AmqpDeadLetterPublisher::new(&amqp_config.url).await {
                    Ok(publisher) => Some(Ok(Arc::new(publisher) as Arc<dyn DeadLetterPublisher>)),
                    Err(e) => Some(Err(e)),
                }
            })
        },
    }
}

/// AMQP-based DLQ publisher using RabbitMQ.
///
/// Publishes dead letters to a topic exchange with routing key: `{domain}`.
/// Exchange name: `angzarr.dlq`
pub struct AmqpDeadLetterPublisher {
    pool: Pool,
    exchange: String,
}

impl AmqpDeadLetterPublisher {
    /// DLQ exchange name.
    const DLQ_EXCHANGE: &'static str = "angzarr.dlq";

    /// Durable catch-all queue bound `#` — retains every dead letter even
    /// when no per-domain consumer queue exists. The whole point of a DLQ
    /// target is surviving the absence of a consumer (D1).
    pub const DLQ_CATCHALL_QUEUE: &'static str = "angzarr.dlq.catchall";

    /// Create a new AMQP DLQ publisher.
    pub async fn new(amqp_url: &str) -> Result<Self, DlqError> {
        let manager = Manager::new(amqp_url.to_string(), Default::default());
        let pool = Pool::builder(manager)
            .max_size(5)
            .build()
            .map_err(|e| DlqError::Connection(format!("Failed to create AMQP pool: {}", e)))?;

        // Verify connection and declare exchange + catch-all queue.
        let conn = pool
            .get()
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to connect to AMQP: {}", e)))?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to create channel: {}", e)))?;

        channel
            .exchange_declare(
                Self::DLQ_EXCHANGE,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to declare DLQ exchange: {}", e)))?;

        // D1: without a bound queue, a topic exchange DISCARDS messages —
        // the dead letter is gone the moment publish "succeeds". The
        // catch-all guarantees retention; per-domain operator queues can
        // bind alongside it.
        channel
            .queue_declare(
                Self::DLQ_CATCHALL_QUEUE,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                DlqError::Connection(format!("Failed to declare DLQ catch-all queue: {}", e))
            })?;

        channel
            .queue_bind(
                Self::DLQ_CATCHALL_QUEUE,
                Self::DLQ_EXCHANGE,
                "#",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                DlqError::Connection(format!("Failed to bind DLQ catch-all queue: {}", e))
            })?;

        info!(
            exchange = %Self::DLQ_EXCHANGE,
            catchall = %Self::DLQ_CATCHALL_QUEUE,
            "AMQP DLQ publisher connected"
        );

        Ok(Self {
            pool,
            exchange: Self::DLQ_EXCHANGE.to_string(),
        })
    }
}

#[async_trait]
impl DeadLetterPublisher for AmqpDeadLetterPublisher {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let domain = dead_letter.domain().unwrap_or("unknown").to_string();
        let routing_key = domain.clone();
        #[cfg(feature = "otel")]
        let reason_type = dead_letter.reason_type();

        // Serialize to proto
        let proto = dead_letter.to_proto();
        let payload = proto.encode_to_vec();

        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to get connection: {}", e)))?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to create channel: {}", e)))?;

        // D1: without confirm_select the publish-confirmation await below
        // resolves immediately as `NotRequested` — broker receipt was never
        // actually verified and a dropped dead letter reported success.
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| {
                DlqError::Connection(format!("Failed to enable publisher confirms: {}", e))
            })?;

        let properties = BasicProperties::default()
            .with_content_type("application/protobuf".into())
            .with_delivery_mode(2); // persistent

        let confirmation = channel
            .basic_publish(
                &self.exchange,
                &routing_key,
                // D1: mandatory — if the catch-all binding is somehow gone,
                // surface the unroutable dead letter as a failure so the
                // chained publisher falls back to the next target instead
                // of dropping it.
                BasicPublishOptions {
                    mandatory: true,
                    ..Default::default()
                },
                &payload,
                properties,
            )
            .await
            .map_err(|e| DlqError::PublishFailed(format!("Failed to publish: {}", e)))?
            .await
            .map_err(|e| DlqError::PublishFailed(format!("Publish confirmation failed: {}", e)))?;

        match confirmation {
            Confirmation::Ack(None) => {}
            Confirmation::Ack(Some(returned)) => {
                return Err(DlqError::PublishFailed(format!(
                    "Dead letter UNROUTABLE (reply {} {}): no queue bound to {} — \
                     catch-all binding missing",
                    returned.reply_code, returned.reply_text, self.exchange
                )));
            }
            Confirmation::Nack(_) => {
                return Err(DlqError::PublishFailed(
                    "Dead letter publish nacked by broker".to_string(),
                ));
            }
            Confirmation::NotRequested => {
                return Err(DlqError::PublishFailed(
                    "Publisher confirms not active on DLQ channel (programmer error)".to_string(),
                ));
            }
        }

        info!(
            exchange = %self.exchange,
            routing_key = %routing_key,
            reason = %dead_letter.rejection_reason,
            "Published to AMQP DLQ"
        );

        #[cfg(feature = "otel")]
        {
            use crate::advice::metrics::{
                backend_attr, domain_attr, reason_type_attr, DLQ_PUBLISH_DURATION,
                DLQ_PUBLISH_TOTAL,
            };
            DLQ_PUBLISH_DURATION.record(start.elapsed().as_secs_f64(), &[backend_attr("amqp")]);
            DLQ_PUBLISH_TOTAL.add(
                1,
                &[
                    domain_attr(&domain),
                    reason_type_attr(reason_type),
                    backend_attr("amqp"),
                ],
            );
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "amqp.test.rs"]
mod tests;
