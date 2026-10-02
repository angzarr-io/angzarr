//! Kafka event bus configuration.

use rdkafka::ClientConfig;

/// Configuration for Kafka connection.
#[derive(Clone, Debug)]
pub struct KafkaEventBusConfig {
    /// Kafka bootstrap servers (comma-separated).
    pub bootstrap_servers: String,
    /// Topic prefix for events (default: "angzarr").
    pub topic_prefix: String,
    /// Consumer group ID (required for subscribing).
    pub group_id: Option<String>,
    /// Domains to subscribe to (for consumers).
    pub domains: Option<Vec<String>>,
    /// SASL username (optional, for authenticated clusters).
    pub sasl_username: Option<String>,
    /// SASL password (optional, for authenticated clusters).
    pub sasl_password: Option<String>,
    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
    pub sasl_mechanism: Option<String>,
    /// Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL).
    pub security_protocol: Option<String>,
    /// SSL CA certificate path (for SSL connections).
    pub ssl_ca_location: Option<String>,
}

impl KafkaEventBusConfig {
    /// Create config for publishing only.
    pub fn publisher(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic_prefix: "angzarr".to_string(),
            group_id: None,
            domains: None,
            sasl_username: None,
            sasl_password: None,
            sasl_mechanism: None,
            security_protocol: None,
            ssl_ca_location: None,
        }
    }

    /// Create config for subscribing to specific domains.
    pub fn subscriber(
        bootstrap_servers: impl Into<String>,
        group_id: impl Into<String>,
        domains: Vec<String>,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic_prefix: "angzarr".to_string(),
            group_id: Some(group_id.into()),
            domains: Some(domains),
            sasl_username: None,
            sasl_password: None,
            sasl_mechanism: None,
            security_protocol: None,
            ssl_ca_location: None,
        }
    }

    /// Create config for subscribing to all domains.
    pub fn subscriber_all(
        bootstrap_servers: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic_prefix: "angzarr".to_string(),
            group_id: Some(group_id.into()),
            domains: None, // None means subscribe to all
            sasl_username: None,
            sasl_password: None,
            sasl_mechanism: None,
            security_protocol: None,
            ssl_ca_location: None,
        }
    }

    /// Derive a subscriber config that shares this config's brokers, topic
    /// prefix and security settings, so a subscriber created from a
    /// publisher reads the topics that publisher writes over the same
    /// authenticated connection. `None` subscribes to every domain.
    pub fn subscriber_config(&self, group_id: &str, domain: Option<&str>) -> Self {
        Self {
            group_id: Some(group_id.to_string()),
            domains: domain.map(|d| vec![d.to_string()]),
            ..self.clone()
        }
    }

    /// Add SASL authentication.
    pub fn with_sasl(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
        mechanism: impl Into<String>,
    ) -> Self {
        self.sasl_username = Some(username.into());
        self.sasl_password = Some(password.into());
        self.sasl_mechanism = Some(mechanism.into());
        self.security_protocol = Some("SASL_SSL".to_string());
        self
    }

    /// Set security protocol.
    pub fn with_security_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.security_protocol = Some(protocol.into());
        self
    }

    /// Set SSL CA certificate location.
    pub fn with_ssl_ca(mut self, ca_location: impl Into<String>) -> Self {
        self.ssl_ca_location = Some(ca_location.into());
        self
    }

    /// Set topic prefix.
    pub fn with_topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.topic_prefix = prefix.into();
        self
    }

    /// Build the topic name for a domain.
    pub fn topic_for_domain(&self, domain: &str) -> String {
        format!("{}.events.{}", self.topic_prefix, domain)
    }

    /// Build a ClientConfig for producers.
    pub(crate) fn build_producer_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);
        config.set("message.timeout.ms", "5000");
        config.set("acks", "all");
        config.set("enable.idempotence", "true");

        self.apply_security_config(&mut config);
        config
    }

    /// Build a ClientConfig for consumers.
    pub(crate) fn build_consumer_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);
        config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", "earliest");
        // SubscriberAll uses a regex subscription, and librdkafka only
        // discovers NEWLY CREATED matching topics on metadata refresh —
        // default 5 MINUTES. A domain that publishes its first event after
        // the consumer attached would be invisible for that long (contract:
        // test_multi_domain_subscription). 5s bounds discovery latency at
        // negligible metadata cost for this consumer count.
        config.set("topic.metadata.refresh.interval.ms", "5000");

        if let Some(ref group_id) = self.group_id {
            config.set("group.id", group_id);
        }

        self.apply_security_config(&mut config);
        config
    }

    /// Apply security settings to a ClientConfig.
    fn apply_security_config(&self, config: &mut ClientConfig) {
        if let Some(ref protocol) = self.security_protocol {
            config.set("security.protocol", protocol);
        }

        if let Some(ref mechanism) = self.sasl_mechanism {
            config.set("sasl.mechanism", mechanism);
        }

        if let Some(ref username) = self.sasl_username {
            config.set("sasl.username", username);
        }

        if let Some(ref password) = self.sasl_password {
            config.set("sasl.password", password);
        }

        if let Some(ref ca_location) = self.ssl_ca_location {
            config.set("ssl.ca.location", ca_location);
        }
    }
}
