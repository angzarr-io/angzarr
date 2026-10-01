//! AWS SNS-based DLQ publisher.
//!
//! Publishes dead letters to SNS topics named `angzarr-dlq-{domain}`. SNS
//! keeps nothing itself, so each topic is created together with an SQS
//! queue of the same name subscribed to it: dead letters stay in that
//! queue (14-day retention) until an operator drains them.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_sns::types::MessageAttributeValue;
use aws_sdk_sns::Client;
use base64::prelude::*;
use prost::Message;
use tokio::sync::RwLock;
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
            let sns_config = config.sns_sqs.clone();
            Box::pin(async move {
                if dlq_type != "sns-sqs" && dlq_type != "sns_sqs" {
                    return None;
                }
                let sns_config = sns_config.unwrap_or_default();
                match SnsSqsDeadLetterPublisher::from_config(&sns_config).await {
                    Ok(publisher) => Some(Ok(Arc::new(publisher) as Arc<dyn DeadLetterPublisher>)),
                    Err(e) => Some(Err(e)),
                }
            })
        },
    }
}

/// Seconds an SQS retention queue keeps dead letters (the SQS maximum).
pub(crate) const RETENTION_SECONDS: u32 = 14 * 24 * 3600;

/// Queue policy letting `topic_arn` deliver into the queue `queue_arn`.
pub(crate) fn retention_queue_policy(queue_arn: &str, topic_arn: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "Service": "sns.amazonaws.com" },
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": { "ArnEquals": { "aws:SourceArn": topic_arn } }
        }]
    })
    .to_string()
}

/// AWS SNS-based DLQ publisher.
///
/// Publishes dead letters to SNS topics named `angzarr-dlq-{domain}`, each
/// with a subscribed SQS retention queue of the same name.
pub struct SnsSqsDeadLetterPublisher {
    sns: Client,
    sqs: aws_sdk_sqs::Client,
    topic_prefix: String,
    topic_arns: Arc<RwLock<HashMap<String, String>>>,
}

impl SnsSqsDeadLetterPublisher {
    /// Create a new SNS/SQS DLQ publisher.
    pub async fn new(region: Option<&str>, endpoint_url: Option<&str>) -> Result<Self, DlqError> {
        let mut config_builder = aws_config::defaults(BehaviorVersion::latest());

        if let Some(region) = region {
            config_builder = config_builder.region(aws_config::Region::new(region.to_string()));
        }

        if let Some(endpoint) = endpoint_url {
            config_builder = config_builder.endpoint_url(endpoint);
        }

        let aws_config = config_builder.load().await;
        let sns = Client::new(&aws_config);
        let sqs = aws_sdk_sqs::Client::new(&aws_config);

        info!(
            region = ?region,
            endpoint = ?endpoint_url,
            "SNS/SQS DLQ publisher connected"
        );

        Ok(Self {
            sns,
            sqs,
            topic_prefix: "angzarr-dlq".to_string(),
            topic_arns: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Create a new SNS/SQS DLQ publisher from config.
    pub async fn from_config(
        config: &super::super::config::SnsSqsDlqConfig,
    ) -> Result<Self, DlqError> {
        let mut config_builder = aws_config::defaults(BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            config_builder = config_builder.region(aws_config::Region::new(region.clone()));
        }

        if let Some(ref endpoint) = config.endpoint_url {
            config_builder = config_builder.endpoint_url(endpoint);
        }

        let aws_config = config_builder.load().await;
        let sns = Client::new(&aws_config);
        let sqs = aws_sdk_sqs::Client::new(&aws_config);

        info!(
            region = ?config.region,
            endpoint = ?config.endpoint_url,
            topic_prefix = %config.topic_prefix,
            "SNS/SQS DLQ publisher connected"
        );

        Ok(Self {
            sns,
            sqs,
            topic_prefix: config.topic_prefix.clone(),
            topic_arns: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Build DLQ topic name for a domain.
    fn topic_for_domain(&self, domain: &str) -> String {
        let sanitized = domain.replace('.', "-");
        format!("{}-{}", self.topic_prefix, sanitized)
    }

    /// Get or create an SNS topic ARN for a domain.
    async fn get_or_create_topic(&self, domain: &str) -> Result<String, DlqError> {
        let topic_name = self.topic_for_domain(domain);

        // Check cache
        {
            let arns = self.topic_arns.read().await;
            if let Some(arn) = arns.get(&topic_name) {
                return Ok(arn.clone());
            }
        }

        // Create topic (idempotent)
        let result = self
            .sns
            .create_topic()
            .name(&topic_name)
            .send()
            .await
            .map_err(|e| DlqError::PublishFailed(format!("Failed to create SNS topic: {}", e)))?;

        let arn = result
            .topic_arn()
            .ok_or_else(|| DlqError::PublishFailed("SNS create_topic returned no ARN".to_string()))?
            .to_string();

        self.ensure_retention_queue(&topic_name, &arn).await?;

        // Cache it
        {
            let mut arns = self.topic_arns.write().await;
            arns.insert(topic_name.clone(), arn.clone());
        }

        info!(topic = %topic_name, arn = %arn, "Created/found SNS DLQ topic");
        Ok(arn)
    }

    /// Create (idempotently) the SQS queue that retains the topic's dead
    /// letters and subscribe it to the topic. Without it SNS discards every
    /// message published to a topic that has no subscriber.
    async fn ensure_retention_queue(&self, name: &str, topic_arn: &str) -> Result<(), DlqError> {
        use aws_sdk_sqs::types::QueueAttributeName;

        let queue_url = self
            .sqs
            .create_queue()
            .queue_name(name)
            .attributes(
                QueueAttributeName::MessageRetentionPeriod,
                RETENTION_SECONDS.to_string(),
            )
            .send()
            .await
            .map_err(|e| DlqError::PublishFailed(format!("Failed to create SQS DLQ queue: {}", e)))?
            .queue_url()
            .ok_or_else(|| DlqError::PublishFailed("SQS create_queue returned no URL".into()))?
            .to_string();

        let queue_arn = self
            .sqs
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(|e| DlqError::PublishFailed(format!("Failed to read SQS DLQ queue: {}", e)))?
            .attributes()
            .and_then(|a| a.get(&QueueAttributeName::QueueArn).cloned())
            .ok_or_else(|| DlqError::PublishFailed("SQS DLQ queue has no ARN".into()))?;

        self.sqs
            .set_queue_attributes()
            .queue_url(&queue_url)
            .attributes(
                QueueAttributeName::Policy,
                retention_queue_policy(&queue_arn, topic_arn),
            )
            .send()
            .await
            .map_err(|e| {
                DlqError::PublishFailed(format!("Failed to set SQS DLQ queue policy: {}", e))
            })?;

        self.sns
            .subscribe()
            .topic_arn(topic_arn)
            .protocol("sqs")
            .endpoint(&queue_arn)
            .send()
            .await
            .map_err(|e| {
                DlqError::PublishFailed(format!("Failed to subscribe SQS DLQ queue: {}", e))
            })?;

        info!(queue = %name, "SQS DLQ retention queue subscribed");
        Ok(())
    }
}

#[async_trait]
impl DeadLetterPublisher for SnsSqsDeadLetterPublisher {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let domain = dead_letter.domain().unwrap_or("unknown").to_string();
        let topic_arn = self.get_or_create_topic(&domain).await?;
        #[cfg(feature = "otel")]
        let reason_type = dead_letter.reason_type();

        // Serialize to proto, then base64 encode
        let proto = dead_letter.to_proto();
        let payload = proto.encode_to_vec();
        let message = BASE64_STANDARD.encode(&payload);

        // Build message attributes
        let correlation_id = dead_letter
            .cover
            .as_ref()
            .map(|c| c.correlation_id.clone())
            .unwrap_or_default();

        let mut attrs = HashMap::new();
        attrs.insert(
            "domain".to_string(),
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value(&domain)
                .build()
                .map_err(|e| {
                    DlqError::PublishFailed(format!("Failed to build attribute: {}", e))
                })?,
        );
        attrs.insert(
            "correlation_id".to_string(),
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value(&correlation_id)
                .build()
                .map_err(|e| {
                    DlqError::PublishFailed(format!("Failed to build attribute: {}", e))
                })?,
        );

        self.sns
            .publish()
            .topic_arn(&topic_arn)
            .message(&message)
            .set_message_attributes(Some(attrs))
            .send()
            .await
            .map_err(|e| DlqError::PublishFailed(format!("Failed to publish to SNS: {}", e)))?;

        info!(
            topic_arn = %topic_arn,
            domain = %domain,
            reason = %dead_letter.rejection_reason,
            "Published to SNS DLQ"
        );

        #[cfg(feature = "otel")]
        {
            use crate::advice::metrics::{
                backend_attr, domain_attr, reason_type_attr, DLQ_PUBLISH_DURATION,
                DLQ_PUBLISH_TOTAL,
            };
            DLQ_PUBLISH_DURATION.record(start.elapsed().as_secs_f64(), &[backend_attr("sns_sqs")]);
            DLQ_PUBLISH_TOTAL.add(
                1,
                &[
                    domain_attr(&domain),
                    reason_type_attr(reason_type),
                    backend_attr("sns_sqs"),
                ],
            );
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "sns_sqs.test.rs"]
mod tests;
