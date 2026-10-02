//! gRPC utilities for correlation ID and trace context propagation.

use super::constants::CORRELATION_ID_HEADER;

/// Create a tonic Request with `x-correlation-id` gRPC metadata.
///
/// Propagates the correlation_id into gRPC request headers so that
/// server-side tower middleware can create tracing spans before
/// protobuf deserialization.
///
/// When the `otel` feature is enabled, also injects W3C `traceparent`
/// header for distributed trace context propagation.
pub fn correlated_request<T>(msg: T, correlation_id: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    if !correlation_id.is_empty() {
        if let Ok(val) = correlation_id.parse() {
            req.metadata_mut().insert(CORRELATION_ID_HEADER, val);
        }
    }

    #[cfg(feature = "otel")]
    {
        inject_trace_context(req.metadata_mut());
    }

    req
}

/// Inject W3C trace context into tonic metadata from the current tracing span.
#[cfg(feature = "otel")]
fn inject_trace_context(metadata: &mut tonic::metadata::MetadataMap) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let cx = tracing::Span::current().context();

    opentelemetry::global::get_text_map_propagator(|propagator| {
        let mut injector = MetadataInjector(metadata);
        propagator.inject_context(&cx, &mut injector);
    });
}

/// Adapter to inject OTel context into tonic gRPC metadata.
#[cfg(feature = "otel")]
struct MetadataInjector<'a>(&'a mut tonic::metadata::MetadataMap);

#[cfg(feature = "otel")]
impl opentelemetry::propagation::Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = value.parse() {
                self.0.insert(key, val);
            }
        }
    }
}

/// `google.rpc.Status`, the message tonic carries in a status's details.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<prost_types::Any>,
}

/// `google.rpc.ErrorInfo`: a machine-readable error reason.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ErrorInfo {
    #[prost(string, tag = "1")]
    pub reason: String,
    #[prost(string, tag = "2")]
    pub domain: String,
    #[prost(map = "string, string", tag = "3")]
    pub metadata: std::collections::HashMap<String, String>,
}

const ERROR_INFO_NAME: &str = "google.rpc.ErrorInfo";

/// Extension trait reading structured error details from a gRPC status.
pub trait StatusExt {
    /// The machine rejection code: `google.rpc.ErrorInfo.reason` in the
    /// status details (a `google.rpc.Status`), or empty when the status
    /// carries no ErrorInfo.
    fn error_info_reason(&self) -> String;
}

impl StatusExt for tonic::Status {
    fn error_info_reason(&self) -> String {
        use prost::Message;
        let Ok(status) = RpcStatus::decode(self.details()) else {
            return String::new();
        };
        status
            .details
            .iter()
            .filter(|any| super::type_url::fqn(&any.type_url) == ERROR_INFO_NAME)
            .find_map(|any| ErrorInfo::decode(any.value.as_slice()).ok())
            .map(|info| info.reason)
            .unwrap_or_default()
    }
}

#[cfg(test)]
#[path = "grpc.test.rs"]
mod tests;
