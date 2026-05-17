//! Bootstrap utilities for angzarr binaries.
//!
//! Shared initialization code for all angzarr sidecar binaries.

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::LOG_ENV_VAR;

/// Global handle to the TracerProvider so it stays alive for the process lifetime.
///
/// The batch exporter lives inside the provider — dropping it kills trace export.
#[cfg(feature = "otel")]
static TRACER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

/// Global handle to the LoggerProvider so it stays alive for the process lifetime.
///
/// The batch exporter lives inside the provider — dropping it kills log export.
#[cfg(feature = "otel")]
static LOG_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::logs::SdkLoggerProvider> =
    std::sync::OnceLock::new();

/// Initialize tracing and metrics with LOG_ENV_VAR environment variable.
///
/// Defaults to "info" level if LOG_ENV_VAR is not set.
///
/// When the `otel` feature is enabled, configures:
/// - OTLP trace exporter (spans → OTel Collector)
/// - OTLP log exporter (tracing events → OTel Collector)
/// - OTLP metrics exporter (counters/histograms → OTel Collector)
/// - W3C TraceContext propagator for distributed tracing
///
/// Configuration via environment variables:
/// - `OTEL_EXPORTER_OTLP_ENDPOINT` — Collector endpoint (default: `http://localhost:4317`)
/// - `OTEL_SERVICE_NAME` — Service name for resource attribution
/// - `OTEL_RESOURCE_ATTRIBUTES` — Additional resource key=value pairs
pub fn init_tracing() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_env(LOG_ENV_VAR)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer();

    #[cfg(feature = "otel")]
    {
        use opentelemetry::trace::TracerProvider;

        // W3C TraceContext propagator for distributed trace context
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        // OTLP trace exporter
        let trace_exporter = match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()
        {
            Ok(exporter) => exporter,
            Err(e) => {
                eprintln!("Failed to init OTLP trace exporter: {e}");
                // Fall back to non-OTel tracing
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt_layer)
                    .init();
                return;
            }
        };

        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(trace_exporter)
            .with_resource(otel_resource())
            .build();

        opentelemetry::global::set_tracer_provider(tracer_provider.clone());
        let _ = TRACER_PROVIDER.set(tracer_provider.clone());

        let otel_trace_layer =
            tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("angzarr"));

        // OTLP log exporter — provider stored in LOG_PROVIDER static to keep
        // the batch exporter alive for the process lifetime.
        let log_layer = match opentelemetry_otlp::LogExporter::builder()
            .with_tonic()
            .build()
        {
            Ok(log_exporter) => {
                let log_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
                    .with_batch_exporter(log_exporter)
                    .with_resource(otel_resource())
                    .build();
                let layer = opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
                    &log_provider,
                );
                let _ = LOG_PROVIDER.set(log_provider);
                Some(layer)
            }
            Err(e) => {
                eprintln!("Failed to init OTLP log exporter: {e}");
                None
            }
        };

        // OTLP metrics exporter
        match opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_temporality(opentelemetry_sdk::metrics::Temporality::Cumulative)
            .build()
        {
            Ok(metrics_exporter) => {
                let reader =
                    opentelemetry_sdk::metrics::PeriodicReader::builder(metrics_exporter).build();

                let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
                    .with_reader(reader)
                    .with_resource(otel_resource())
                    .build();

                opentelemetry::global::set_meter_provider(meter_provider);
            }
            Err(e) => {
                eprintln!("Failed to init OTLP metrics exporter: {e}");
            }
        }

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_trace_layer)
            .with(log_layer)
            .init();

        tracing::info!("OpenTelemetry tracing initialized");
    }

    #[cfg(not(feature = "otel"))]
    {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    }
}

/// Build the OpenTelemetry resource from environment variables.
///
/// Standard attributes:
/// - `service.name` — from `OTEL_SERVICE_NAME` (default: "angzarr")
/// - `service.version` — from `OTEL_SERVICE_VERSION` or Cargo package version
/// - `service.instance.id` — from `POD_NAME` (K8s) or `HOSTNAME`
/// - `deployment.environment` — from `OTEL_DEPLOYMENT_ENVIRONMENT` or `ENVIRONMENT`
/// - `service.namespace` — from `POD_NAMESPACE` (K8s only)
///
/// See: https://opentelemetry.io/docs/specs/semconv/resource/
#[cfg(feature = "otel")]
fn otel_resource() -> opentelemetry_sdk::Resource {
    use crate::config::OTEL_SERVICE_NAME_ENV_VAR;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::Resource;

    let service_name =
        std::env::var(OTEL_SERVICE_NAME_ENV_VAR).unwrap_or_else(|_| "angzarr".to_string());

    let mut attrs = vec![KeyValue::new("service.name", service_name)];

    // service.version: from env or compile-time package version
    let version = std::env::var("OTEL_SERVICE_VERSION")
        .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());
    attrs.push(KeyValue::new("service.version", version));

    // service.instance.id: K8s pod name or hostname
    if let Ok(instance_id) = std::env::var("POD_NAME").or_else(|_| std::env::var("HOSTNAME")) {
        attrs.push(KeyValue::new("service.instance.id", instance_id));
    }

    // deployment.environment: dev, staging, prod, etc.
    if let Ok(env) =
        std::env::var("OTEL_DEPLOYMENT_ENVIRONMENT").or_else(|_| std::env::var("ENVIRONMENT"))
    {
        attrs.push(KeyValue::new("deployment.environment", env));
    }

    // service.namespace: K8s namespace
    if let Ok(namespace) = std::env::var("POD_NAMESPACE") {
        attrs.push(KeyValue::new("service.namespace", namespace));
    }

    Resource::builder_empty().with_attributes(attrs).build()
}

/// Graceful shutdown of OpenTelemetry providers.
///
/// Call this before process exit to flush pending spans/logs/metrics.
pub fn shutdown_telemetry() {
    #[cfg(feature = "otel")]
    {
        if let Some(provider) = TRACER_PROVIDER.get() {
            if let Err(e) = provider.shutdown() {
                eprintln!("Failed to shut down tracer provider: {e}");
            }
        }
        if let Some(provider) = LOG_PROVIDER.get() {
            if let Err(e) = provider.shutdown() {
                eprintln!("Failed to shut down log provider: {e}");
            }
        }
        tracing::info!("OpenTelemetry providers shut down");
    }
}

/// Create a shutdown signal future that waits for SIGTERM (K8s) or SIGINT (Ctrl+C).
///
/// When the signal is received, calls `shutdown_telemetry()` to flush OTel buffers.
/// Use this with tonic's `with_graceful_shutdown()`.
///
/// # Example
/// ```ignore
/// Server::builder()
///     .add_service(my_service)
///     .serve_with_shutdown(addr, shutdown_signal())
///     .await?;
/// ```
pub async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received SIGINT, shutting down"),
        _ = terminate => tracing::info!("Received SIGTERM, shutting down"),
    }

    shutdown_telemetry();
}

/// Parse static endpoints from a comma-separated string.
///
/// Format: "domain=address,domain=address,..."
/// Example: "customer=/tmp/angzarr/aggregate-customer.sock,order=/tmp/angzarr/aggregate-order.sock"
pub fn parse_static_endpoints(endpoints_str: &str) -> Vec<(String, String)> {
    endpoints_str
        .split(',')
        .filter_map(|pair| {
            let parts: Vec<&str> = pair.trim().splitn(2, '=').collect();
            if parts.len() == 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        })
        .collect()
}

/// Parse configuration path from command-line arguments.
///
/// Looks for `--config` or `-c` followed by a path.
pub fn parse_config_path() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    parse_config_path_from_args(&args)
}

/// Internal: parse config path from a given args list (testable).
fn parse_config_path_from_args(args: &[String]) -> Option<String> {
    for i in 0..args.len() {
        if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

/// One-shot startup: initialize tracing and load configuration.
///
/// Combines [`init_tracing`], [`parse_config_path`], and
/// [`crate::config::Config::load`] into a single call. Used at the
/// top of every angzarr `main()` to replace the standard three-line
/// boilerplate.
///
/// # Example
///
/// ```rust,ignore
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = angzarr::utils::bootstrap::startup()?;
///     // ... build services, serve_with_transport ...
///     Ok(())
/// }
/// ```
///
/// Returns the loaded [`crate::config::Config`] on success, or the
/// underlying load error on failure.
pub fn startup() -> Result<crate::config::Config, Box<dyn std::error::Error>> {
    init_tracing();
    let path = parse_config_path();
    let config = crate::config::Config::load(path.as_deref())?;
    Ok(config)
}

#[cfg(test)]
#[path = "bootstrap.test.rs"]
mod tests;
