use super::*;

// Test fixtures - not real credentials
// lgtm[rust/hardcoded-credentials]
const TEST_USER: &str = "test-user"; // codeql[rust/hard-coded-credentials]
                                     // lgtm[rust/hardcoded-credentials]
const TEST_PASSWORD: &str = "test-password"; // codeql[rust/hard-coded-credentials]

// NOTE: the old `test_message_key_generation` / `test_extract_domain*`
// tests were deleted: `KafkaEventBus::message_key` / `extract_domain`
// no longer exist. The partition-key boundary is now `validate_publish_key`,
// covered by the H-10 regression suite in bus.test.rs.

#[test]
fn test_topic_for_domain() {
    let config = KafkaEventBusConfig::publisher("localhost:9092");
    assert_eq!(config.topic_for_domain("orders"), "angzarr.events.orders");
}

#[test]
fn test_topic_with_custom_prefix() {
    let config = KafkaEventBusConfig::publisher("localhost:9092").with_topic_prefix("myapp");
    assert_eq!(config.topic_for_domain("orders"), "myapp.events.orders");
}

#[test]
fn test_publisher_config() {
    let config = KafkaEventBusConfig::publisher("localhost:9092");
    assert_eq!(config.bootstrap_servers, "localhost:9092");
    assert!(config.group_id.is_none());
    assert!(config.domains.is_none());
}

#[test]
fn test_subscriber_config() {
    let config = KafkaEventBusConfig::subscriber(
        "localhost:9092",
        "orders-projector",
        vec!["orders".to_string()],
    );
    assert_eq!(config.group_id, Some("orders-projector".to_string()));
    assert_eq!(config.domains, Some(vec!["orders".to_string()]));
}

#[test]
fn test_sasl_config() {
    // codeql[rust/hard-coded-cryptographic-value] - Test fixture, not real credentials
    let config = KafkaEventBusConfig::publisher("localhost:9092").with_sasl(
        TEST_USER,
        TEST_PASSWORD,
        "SCRAM-SHA-256",
    );
    assert_eq!(config.sasl_username, Some(TEST_USER.to_string()));
    assert_eq!(config.sasl_password, Some(TEST_PASSWORD.to_string()));
    assert_eq!(config.sasl_mechanism, Some("SCRAM-SHA-256".to_string()));
    assert_eq!(config.security_protocol, Some("SASL_SSL".to_string()));
}

#[test]
fn test_ssl_config() {
    let config = KafkaEventBusConfig::publisher("localhost:9092")
        .with_security_protocol("SSL")
        .with_ssl_ca("/path/to/ca.crt");
    assert_eq!(config.security_protocol, Some("SSL".to_string()));
    assert_eq!(config.ssl_ca_location, Some("/path/to/ca.crt".to_string()));
}

/// A subscriber derived from a publisher keeps the brokers, topic prefix
/// and SASL/SSL settings, so it reads the publisher's topics over the same
/// authenticated connection.
#[test]
fn test_subscriber_config_keeps_prefix_and_security() {
    let publisher = KafkaEventBusConfig::publisher("broker:9092")
        .with_topic_prefix("tenant-a")
        .with_sasl(TEST_USER, TEST_PASSWORD, "SCRAM-SHA-512")
        .with_ssl_ca("/ca.pem");
    let sub = publisher.subscriber_config("audit", Some("orders"));
    assert_eq!(sub.bootstrap_servers, "broker:9092");
    assert_eq!(sub.topic_prefix, "tenant-a");
    assert_eq!(sub.sasl_username.as_deref(), Some(TEST_USER));
    assert_eq!(sub.sasl_mechanism.as_deref(), Some("SCRAM-SHA-512"));
    assert_eq!(sub.ssl_ca_location.as_deref(), Some("/ca.pem"));
    assert_eq!(sub.group_id.as_deref(), Some("audit"));
    assert_eq!(sub.domains, Some(vec!["orders".to_string()]));
    assert_eq!(publisher.subscriber_config("all", None).domains, None);
}
