//! Configuration for AWS SNS/SQS event bus.

/// Configuration for AWS SNS/SQS connection.
#[derive(Clone, Debug)]
pub struct SnsSqsConfig {
    /// AWS region (e.g., "us-east-1"). Uses default provider chain if not set.
    pub region: Option<String>,
    /// Custom endpoint URL (for LocalStack or testing).
    pub endpoint_url: Option<String>,
    /// Topic prefix for events (default: "angzarr").
    pub topic_prefix: String,
    /// Subscription ID suffix (consumer group equivalent).
    pub subscription_id: Option<String>,
    /// Domains to subscribe to (for consumers).
    /// Must be non-empty for consumers: each domain has its own topic, so
    /// there is no all-domains subscription (`start_consuming` rejects it).
    pub domains: Vec<String>,
    /// Visibility timeout in seconds for SQS messages (default: 30).
    pub visibility_timeout_secs: i32,
    /// Max number of messages to receive in one poll (default: 10).
    pub max_messages: i32,
    /// Wait time seconds for long polling (default: 20).
    pub wait_time_secs: i32,
}

impl SnsSqsConfig {
    /// Create config for publishing only.
    pub fn publisher() -> Self {
        Self {
            region: None,
            endpoint_url: None,
            topic_prefix: "angzarr".to_string(),
            subscription_id: None,
            domains: Vec::new(),
            visibility_timeout_secs: 30,
            max_messages: 10,
            wait_time_secs: 20,
        }
    }

    /// Create config for subscribing to specific domains.
    pub fn subscriber(subscription_id: impl Into<String>, domains: Vec<String>) -> Self {
        Self {
            region: None,
            endpoint_url: None,
            topic_prefix: "angzarr".to_string(),
            subscription_id: Some(subscription_id.into()),
            domains,
            visibility_timeout_secs: 30,
            max_messages: 10,
            wait_time_secs: 20,
        }
    }

    /// Derive a subscriber config that shares this config's region,
    /// endpoint, topic prefix and queue tuning, so a subscriber created
    /// from a publisher reads the topics that publisher writes.
    pub fn subscriber_config(&self, subscription_id: &str, domain: Option<&str>) -> Self {
        Self {
            subscription_id: Some(subscription_id.to_string()),
            domains: domain.map(|d| vec![d.to_string()]).unwrap_or_default(),
            ..self.clone()
        }
    }

    /// Set AWS region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set custom endpoint URL (for LocalStack or testing).
    pub fn with_endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint_url = Some(url.into());
        self
    }

    /// Set topic prefix.
    pub fn with_topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.topic_prefix = prefix.into();
        self
    }

    /// Set visibility timeout in seconds.
    pub fn with_visibility_timeout(mut self, secs: i32) -> Self {
        self.visibility_timeout_secs = secs;
        self
    }

    /// Build the SNS topic name for a domain.
    /// Uses dashes instead of dots for AWS compatibility.
    pub fn topic_for_domain(&self, domain: &str) -> String {
        let sanitized = domain.replace('.', "-");
        // Use .fifo suffix for FIFO topic support (message_group_id ordering)
        format!("{}-events-{}.fifo", self.topic_prefix, sanitized)
    }

    /// Build the SQS queue name for a domain.
    pub fn queue_for_domain(&self, domain: &str) -> String {
        let sanitized = domain.replace('.', "-");
        // Use .fifo suffix for FIFO queue support (matches FIFO topics)
        match &self.subscription_id {
            Some(sub_id) => format!("{}-{}-{}.fifo", self.topic_prefix, sub_id, sanitized),
            None => format!("{}-{}.fifo", self.topic_prefix, sanitized),
        }
    }
}
