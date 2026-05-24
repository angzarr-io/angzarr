//! AWS SNS/SQS event bus implementation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_sns::primitives::Blob;
use aws_sdk_sns::Client as SnsClient;
use aws_sdk_sqs::Client as SqsClient;
use prost::Message;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::bus::error::{BusError, Result};
use crate::bus::traits::{EventBus, EventHandler, PublishResult};
use crate::proto::EventBook;
use crate::proto_ext::{CoverExt, EventPageExt};

use super::config::SnsSqsConfig;
use super::consumer::consume_sqs_queue;
use super::{
    CORRELATION_ID_ATTR, DOMAIN_ATTR, MESSAGE_BODY_PLACEHOLDER, PAYLOAD_ATTR, ROOT_ID_ATTR,
};

/// AWS SNS/SQS event bus implementation.
///
/// Events are published to SNS topics named `{topic_prefix}-events-{domain}`.
/// Subscribers use SQS queues with configurable IDs.
pub struct SnsSqsEventBus {
    sns: SnsClient,
    sqs: SqsClient,
    pub(crate) config: SnsSqsConfig,
    handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    /// Cache of SNS topic ARNs by domain.
    topic_arns: Arc<RwLock<HashMap<String, String>>>,
    /// Cache of SQS queue URLs by domain.
    queue_urls: Arc<RwLock<HashMap<String, String>>>,
    /// Monotonic publish counter (per-bus-instance). Mixed into the SNS
    /// MessageDeduplicationId so legitimate retries of the same logical event
    /// (which would otherwise produce identical `{domain}-{root}-{max_seq}`
    /// dedup IDs) are not silently dropped by AWS's 5-minute FIFO dedup
    /// window. AWS dedup-id length cap is 128 chars — a u64 counter +
    /// the instance nonce both fit comfortably alongside the rest of the key.
    publish_counter: Arc<AtomicU64>,
    /// Per-bus-instance nonce mixed into the dedup_id alongside `publish_counter`.
    ///
    /// The counter alone resets to 0 on every `new()`, so any cross-restart
    /// republish (operator-driven replay, persist-and-publish retry after a
    /// crash) would re-emit the same `{domain}-{root}-{seq}-0` dedup_id that
    /// the original (pre-crash) publish used and silently lose the retry
    /// inside AWS's 5-minute FIFO dedup window. A fresh UUID per bus
    /// instance breaks that collision: P1's dedup_ids are `{...}-{u1}-{N}`,
    /// P2's are `{...}-{u2}-{M}`, and even if both processes restart within
    /// the dedup window the IDs no longer alias. Truncated to 12 hex chars to leave
    /// budget inside the 128-char cap; 48 bits of nonce + the counter is
    /// well past the birthday bound for the dedup window.
    instance_nonce: String,
}

/// Build the FIFO `(MessageGroupId, MessageDeduplicationId)` pair for an
/// SNS publish.
///
/// # Contract
///
/// **Root-less EventBooks are rejected with `BusError::Publish`.** FIFO
/// topics require a non-empty MessageGroupId. The only non-empty fallback
/// we could compute is a per-event UUID, which would guarantee every
/// root-less event lands in its OWN ordering group — i.e., no ordering at
/// all. That silently weakens the documented "ordering by aggregate root"
/// FIFO guarantee. AWS would reject an empty MessageGroupId on the wire
/// regardless; surfacing the misuse here as `BusError::Publish` produces a
/// clear, root-cause-naming error at the boundary instead of an opaque
/// AWS validation failure several layers down.
///
/// **MessageDeduplicationId includes a per-bus-instance nonce + monotonic
/// publish counter.** Without it, legitimate retries of the same logical
/// event (same `{domain}-{root}-{max_seq}` triple) would collide inside
/// AWS's 5-minute FIFO dedup window and be silently dropped — defeating
/// at-least-once republish flows (operator-driven replay,
/// persist-and-publish retry). The counter alone is process-local and
/// resets to 0 on restart, so an in-process-only counter would still alias
/// across a crash+restart within the 5-minute window. The instance nonce
/// (12 hex chars of a fresh UUID, ~48 bits) closes that gap: two distinct
/// bus instances cannot produce the same dedup_id even if their counters
/// happen to align. AWS allows up to 128 characters in the dedup_id; nonce
/// + u64 counter + the rest of the key fit comfortably.
///
/// # Purity
///
/// This helper is a pure function (no AWS SDK, no I/O, no time) so it can
/// be unit-tested directly without LocalStack / Floci / mocks. See
/// `bus.test.rs` for the C-12 regression suite.
pub(crate) fn build_fifo_attributes(
    book: &EventBook,
    instance_nonce: &str,
    publish_counter: u64,
) -> Result<(String, String)> {
    let root_id = book.root_id_hex().ok_or_else(|| {
        BusError::Publish(
            "EventBook missing root: SNS FIFO requires a non-empty MessageGroupId. \
             Falling back to a per-event UUID would silently disable ordering. \
             Provide a root on the EventBook cover, or route root-less events \
             through a non-FIFO transport."
                .to_string(),
        )
    })?;

    // Defense-in-depth: `root_id_hex` returns `Some` whenever a root
    // ProtoUuid is present, even if its `value` bytes are empty. An
    // empty hex string would still be rejected by AWS; reject it here
    // with the same explicit error so the framework's failure mode is
    // identical across both shapes of "no root".
    if root_id.is_empty() {
        return Err(BusError::Publish(
            "EventBook root_id is empty: SNS FIFO MessageGroupId must be non-empty".to_string(),
        ));
    }

    let domain = book.domain();
    let max_seq = book
        .pages
        .iter()
        .map(|p| p.sequence_num())
        .max()
        .unwrap_or(0);
    let dedup_id = format!(
        "{}-{}-{}-{}-{}",
        domain, root_id, max_seq, instance_nonce, publish_counter
    );

    Ok((root_id, dedup_id))
}

impl SnsSqsEventBus {
    /// Create a new SNS/SQS event bus.
    pub async fn new(config: SnsSqsConfig) -> Result<Self> {
        // Load AWS config
        let mut aws_config_builder = aws_config::defaults(BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            aws_config_builder = aws_config_builder.region(aws_config::Region::new(region.clone()));
        }

        if let Some(ref endpoint) = config.endpoint_url {
            aws_config_builder = aws_config_builder.endpoint_url(endpoint);
        }

        let aws_config = aws_config_builder.load().await;

        let sns = SnsClient::new(&aws_config);
        let sqs = SqsClient::new(&aws_config);

        info!(
            region = ?config.region,
            endpoint = ?config.endpoint_url,
            topic_prefix = %config.topic_prefix,
            "Connected to AWS SNS/SQS"
        );

        // 12 hex chars of a fresh UUID. See `instance_nonce` field doc for
        // why a process-local counter alone is insufficient against a
        // cross-restart republish inside AWS's 5-minute dedup window.
        let instance_nonce = format!("{:x}", Uuid::new_v4().as_u128() & 0xFFFF_FFFF_FFFF);

        Ok(Self {
            sns,
            sqs,
            config,
            handlers: Arc::new(RwLock::new(Vec::new())),
            topic_arns: Arc::new(RwLock::new(HashMap::new())),
            queue_urls: Arc::new(RwLock::new(HashMap::new())),
            publish_counter: Arc::new(AtomicU64::new(0)),
            instance_nonce,
        })
    }

    /// Get or create an SNS topic ARN for a domain.
    async fn get_or_create_topic(&self, domain: &str) -> Result<String> {
        let topic_name = self.config.topic_for_domain(domain);

        // Check cache
        {
            let arns = self.topic_arns.read().await;
            if let Some(arn) = arns.get(&topic_name) {
                return Ok(arn.clone());
            }
        }

        // Create FIFO topic (idempotent - returns existing if already exists)
        // FIFO topics enable message_group_id for aggregate root ordering
        let result = self
            .sns
            .create_topic()
            .name(&topic_name)
            .attributes("FifoTopic", "true")
            .attributes("ContentBasedDeduplication", "false") // We provide explicit dedup IDs
            .send()
            .await
            .map_err(|e| BusError::Publish(format!("Failed to create SNS topic: {}", e)))?;

        let arn = result
            .topic_arn()
            .ok_or_else(|| BusError::Publish("SNS create_topic returned no ARN".to_string()))?
            .to_string();

        // Cache it
        {
            let mut arns = self.topic_arns.write().await;
            arns.insert(topic_name.clone(), arn.clone());
        }

        info!(topic = %topic_name, arn = %arn, "Created/found SNS topic");
        Ok(arn)
    }

    /// Get or create an SQS queue URL for a domain.
    async fn get_or_create_queue(&self, domain: &str) -> Result<String> {
        let queue_name = self.config.queue_for_domain(domain);

        // Check cache
        {
            let urls = self.queue_urls.read().await;
            if let Some(url) = urls.get(&queue_name) {
                return Ok(url.clone());
            }
        }

        // Create FIFO queue (idempotent - returns existing if already exists)
        // FIFO queues maintain message ordering by message_group_id
        let result = self
            .sqs
            .create_queue()
            .queue_name(&queue_name)
            .attributes(
                aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout,
                self.config.visibility_timeout_secs.to_string(),
            )
            .attributes(
                aws_sdk_sqs::types::QueueAttributeName::FifoQueue,
                "true".to_string(),
            )
            .send()
            .await
            .map_err(|e| BusError::Subscribe(format!("Failed to create SQS queue: {}", e)))?;

        let url = result
            .queue_url()
            .ok_or_else(|| BusError::Subscribe("SQS create_queue returned no URL".to_string()))?
            .to_string();

        // Cache it
        {
            let mut urls = self.queue_urls.write().await;
            urls.insert(queue_name.clone(), url.clone());
        }

        info!(queue = %queue_name, url = %url, "Created/found SQS queue");
        Ok(url)
    }

    /// Subscribe an SQS queue to an SNS topic.
    async fn subscribe_queue_to_topic(&self, queue_url: &str, topic_arn: &str) -> Result<()> {
        // Get queue ARN
        let queue_attrs = self
            .sqs
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(|e| BusError::Subscribe(format!("Failed to get queue ARN: {}", e)))?;

        let queue_arn = queue_attrs
            .attributes()
            .and_then(|attrs| attrs.get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn))
            .ok_or_else(|| BusError::Subscribe("Queue has no ARN attribute".to_string()))?;

        // Subscribe queue to topic
        self.sns
            .subscribe()
            .topic_arn(topic_arn)
            .protocol("sqs")
            .endpoint(queue_arn)
            .attributes("RawMessageDelivery", "true")
            .send()
            .await
            .map_err(|e| {
                BusError::Subscribe(format!("Failed to subscribe queue to topic: {}", e))
            })?;

        debug!(queue_arn = %queue_arn, topic_arn = %topic_arn, "Subscribed queue to topic");
        Ok(())
    }
}

#[async_trait]
impl EventBus for SnsSqsEventBus {
    #[tracing::instrument(name = "bus.publish", skip_all, fields(domain = %book.domain()))]
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        let domain = book.domain();
        let correlation_id = book.correlation_id().to_string();

        // Compute FIFO ordering/dedup attributes BEFORE creating the topic.
        // Failing early on a root-less EventBook avoids the wasteful side
        // effect of provisioning a topic for a publish we cannot perform.
        let counter = self.publish_counter.fetch_add(1, Ordering::Relaxed);
        let (root_id, dedup_id) = build_fifo_attributes(&book, &self.instance_nonce, counter)?;

        let topic_arn = self.get_or_create_topic(domain).await?;

        // Serialize the event book. The protobuf bytes go into a BINARY
        // SNS MessageAttribute (`PAYLOAD_ATTR`) — not the SNS body — so
        // we don't burn 33% of the 256 KiB SNS/SQS budget on base64
        // overhead the way the previous body-encoded layout did (H-08).
        // With `RawMessageDelivery=true` on the SQS subscription the
        // binary attribute is forwarded to SQS verbatim, so the consumer
        // can read the bytes back with zero decoding.
        let data = book.encode_to_vec();

        // Build message attributes
        use aws_sdk_sns::types::MessageAttributeValue;

        let mut attrs = HashMap::new();
        attrs.insert(
            DOMAIN_ATTR.to_string(),
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value(domain)
                .build()
                .map_err(|e| BusError::Publish(format!("Failed to build attribute: {}", e)))?,
        );
        attrs.insert(
            CORRELATION_ID_ATTR.to_string(),
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value(&correlation_id)
                .build()
                .map_err(|e| BusError::Publish(format!("Failed to build attribute: {}", e)))?,
        );
        attrs.insert(
            ROOT_ID_ATTR.to_string(),
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value(&root_id)
                .build()
                .map_err(|e| BusError::Publish(format!("Failed to build attribute: {}", e)))?,
        );
        attrs.insert(
            PAYLOAD_ATTR.to_string(),
            MessageAttributeValue::builder()
                .data_type("Binary")
                .binary_value(Blob::new(data))
                .build()
                .map_err(|e| {
                    BusError::Publish(format!("Failed to build payload attribute: {}", e))
                })?,
        );

        #[cfg(feature = "otel")]
        super::otel::sns_inject_trace_context(&mut attrs);

        // Publish to SNS. FIFO ordering by aggregate root; dedup_id includes
        // a per-bus publish counter (see `build_fifo_attributes`) so
        // legitimate retries survive AWS's 5-minute dedup window. The
        // body is a short human-readable placeholder — SNS rejects an
        // empty `Message`, but the real payload travels in `PAYLOAD_ATTR`.
        self.sns
            .publish()
            .topic_arn(&topic_arn)
            .message(MESSAGE_BODY_PLACEHOLDER)
            .set_message_attributes(Some(attrs))
            .message_group_id(&root_id)
            .message_deduplication_id(&dedup_id)
            .send()
            .await
            .map_err(|e| BusError::Publish(format!("Failed to publish to SNS: {}", e)))?;

        debug!(
            domain = %domain,
            correlation_id = %correlation_id,
            topic_arn = %topic_arn,
            "Published event to SNS"
        );

        Ok(PublishResult::default())
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        let mut handlers = self.handlers.write().await;
        handlers.push(handler);
        Ok(())
    }

    async fn start_consuming(&self) -> Result<()> {
        let subscription_id = self.config.subscription_id.as_ref().ok_or_else(|| {
            BusError::Subscribe(
                "No subscription_id configured. Use SnsSqsConfig::subscriber()".to_string(),
            )
        })?;

        // Determine which domains to subscribe to
        let domains: Vec<String> = if self.config.domains.is_empty() {
            warn!("No domains specified. Subscribe-side filtering will be used.");
            vec!["events".to_string()]
        } else {
            self.config.domains.clone()
        };

        // Set up queues and subscriptions for each domain
        for domain in &domains {
            let topic_arn = self.get_or_create_topic(domain).await?;
            let queue_url = self.get_or_create_queue(domain).await?;
            self.subscribe_queue_to_topic(&queue_url, &topic_arn)
                .await?;
        }

        // Spawn consumer tasks for each domain's queue
        for domain in domains {
            let queue_url = {
                let urls = self.queue_urls.read().await;
                urls.get(&self.config.queue_for_domain(&domain))
                    .cloned()
                    .ok_or_else(|| {
                        BusError::Subscribe(format!("Queue URL not found for domain: {}", domain))
                    })?
            };

            tokio::spawn(consume_sqs_queue(
                queue_url,
                domain,
                self.sqs.clone(),
                self.handlers.clone(),
                self.config.domains.clone(),
                self.config.max_messages,
                self.config.wait_time_secs,
            ));
        }

        info!(subscription_id = %subscription_id, "Started SQS consumers");
        Ok(())
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        let mut config = match domain_filter {
            Some(d) => SnsSqsConfig::subscriber(name, vec![d.to_string()]),
            None => SnsSqsConfig::subscriber_all(name),
        };
        // Inherit region and endpoint from parent config
        config.region = self.config.region.clone();
        config.endpoint_url = self.config.endpoint_url.clone();
        let bus = SnsSqsEventBus::new(config).await?;
        Ok(Arc::new(bus))
    }

    fn max_message_size(&self) -> Option<usize> {
        // AWS SNS / SQS hard-cap message bodies at 256 KiB. Surface this so
        // OffloadingEventBus engages claim-check offload without explicit
        // operator config. See `super::MAX_MESSAGE_SIZE` for the citation.
        Some(super::MAX_MESSAGE_SIZE)
    }
}

#[cfg(test)]
#[path = "bus.test.rs"]
mod tests;
