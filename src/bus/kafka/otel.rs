//! OTel trace context propagation for Kafka messages.

/// Kafka-specific injector that collects trace context key-value pairs.
/// Kafka's OwnedHeaders API requires building headers incrementally,
/// so we collect pairs first, then convert.
pub(super) struct KafkaInjector<'a>(pub &'a mut Vec<(String, String)>);

impl opentelemetry::propagation::Injector for KafkaInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.push((key.to_string(), value));
    }
}

/// Kafka-specific extractor for W3C trace context from message headers.
pub(super) struct KafkaExtractor<'a, H: rdkafka::message::Headers>(pub &'a H);

impl<H: rdkafka::message::Headers> opentelemetry::propagation::Extractor for KafkaExtractor<'_, H> {
    fn get(&self, key: &str) -> Option<&str> {
        for i in 0..self.0.count() {
            // rdkafka 0.36: get_as returns Result<Header<_>, _>, not Option.
            if let Ok(header) = self.0.get_as::<[u8]>(i) {
                if header.key == key {
                    return header.value.and_then(|v| std::str::from_utf8(v).ok());
                }
            }
        }
        None
    }
    fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::new();
        for i in 0..self.0.count() {
            if let Ok(header) = self.0.get_as::<[u8]>(i) {
                keys.push(header.key);
            }
        }
        keys
    }
}

/// Inject W3C trace context from the current span into Kafka message headers.
pub(super) fn kafka_inject_trace_context() -> rdkafka::message::OwnedHeaders {
    use rdkafka::message::OwnedHeaders;

    let mut pairs = Vec::new();
    crate::utils::tracing::inject_trace_context(&mut KafkaInjector(&mut pairs));

    let mut headers = OwnedHeaders::new();
    for (key, value) in pairs {
        headers = std::mem::take(&mut headers).insert(rdkafka::message::Header {
            key: &key,
            value: Some(value.as_bytes()),
        });
    }
    headers
}

/// Extract W3C trace context from Kafka message headers and set as parent on span.
pub(super) fn kafka_extract_trace_context<M: rdkafka::message::Message>(
    message: &M,
    span: &tracing::Span,
) {
    if let Some(headers) = message.headers() {
        crate::utils::tracing::extract_trace_context(&KafkaExtractor(headers), span);
    }
}
