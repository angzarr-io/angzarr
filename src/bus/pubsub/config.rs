//! Google Pub/Sub configuration.

/// Configuration for Google Pub/Sub connection.
#[derive(Clone, Debug)]
pub struct PubSubConfig {
    /// GCP project ID (used for topic/subscription path generation).
    pub project_id: String,
    /// Topic prefix for events (default: "angzarr").
    pub topic_prefix: String,
    /// Subscription ID suffix (consumer group equivalent).
    pub subscription_id: Option<String>,
    /// Domains to subscribe to (for consumers).
    /// Must be non-empty for consumers: each domain has its own topic, so
    /// there is no all-domains subscription (`start_consuming` rejects it).
    pub domains: Vec<String>,
}

impl PubSubConfig {
    /// Create config for publishing only.
    pub fn publisher(project_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            topic_prefix: "angzarr".to_string(),
            subscription_id: None,
            domains: Vec::new(),
        }
    }

    /// Create config for subscribing to specific domains.
    pub fn subscriber(
        project_id: impl Into<String>,
        subscription_id: impl Into<String>,
        domains: Vec<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            topic_prefix: "angzarr".to_string(),
            subscription_id: Some(subscription_id.into()),
            domains,
        }
    }

    /// Derive a subscriber config that shares this config's project and
    /// topic prefix, so a subscriber created from a publisher reads the
    /// topics that publisher writes.
    pub fn subscriber_config(&self, subscription_id: &str, domain: Option<&str>) -> Self {
        Self {
            subscription_id: Some(subscription_id.to_string()),
            domains: domain.map(|d| vec![d.to_string()]).unwrap_or_default(),
            ..self.clone()
        }
    }

    /// Set topic prefix.
    pub fn with_topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.topic_prefix = prefix.into();
        self
    }

    /// Build the topic name for a domain.
    /// Uses dashes instead of dots for Pub/Sub compatibility.
    pub fn topic_for_domain(&self, domain: &str) -> String {
        let sanitized = domain.replace('.', "-");
        format!("{}-events-{}", self.topic_prefix, sanitized)
    }

    /// Build the subscription name for a domain.
    pub fn subscription_for_domain(&self, domain: &str) -> String {
        let sanitized = domain.replace('.', "-");
        match &self.subscription_id {
            Some(sub_id) => format!("{}-{}-{}", self.topic_prefix, sub_id, sanitized),
            None => format!("{}-{}", self.topic_prefix, sanitized),
        }
    }
}
