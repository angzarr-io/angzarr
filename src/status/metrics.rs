//! Self-observability for the `angzarr-status` ops console
//! (Phase 0 skeleton).
//!
//! Per the plan's resilience contract, the status console must be
//! itself observable: handlers should emit OTel metrics so operators
//! can spot the console misbehaving via the same dashboard tiles the
//! console exposes. This module owns the namespace and the meter
//! handle so individual handlers don't each re-derive it.
//!
//! ## Naming convention
//!
//! All metrics are prefixed `angzarr.status.*` — distinct from
//! `angzarr.storage.*` / `angzarr.bus.*` etc., so operators can
//! pivot Prom queries by component cleanly.
//!
//! ## Phase 0
//!
//! Just the meter + a single representative histogram for RPC
//! latency. More metrics land as their handlers do (replay attempts,
//! command-injection attempts, descriptor-pool rebuilds, audit-write
//! failures — all called out in the plan's self-observability bullet).

#[cfg(feature = "otel")]
use std::sync::LazyLock;

#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry::metrics::{Histogram, Meter};

/// Meter scope name. Matches the OTel-conventional pattern of one
/// meter per crate-component; pivoting on `otel.scope.name` in
/// PromQL surfaces all status-console metrics together.
#[cfg(feature = "otel")]
pub const METER_NAME: &str = "angzarr-status";

#[cfg(feature = "otel")]
static METER: LazyLock<Meter> = LazyLock::new(|| global::meter(METER_NAME));

/// Per-RPC handler latency. Tagged with `rpc.method` label at the
/// call site. Established in Phase 0 so the first handler that lands
/// (Phase 1 DLQ admin) has a metric to record against; pattern is
/// uniform across every subsequent handler.
#[cfg(feature = "otel")]
pub static RPC_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("angzarr.status.rpc.duration")
        .with_description("angzarr-status RPC handler duration")
        .with_unit("s")
        .build()
});

#[cfg(test)]
#[path = "metrics.test.rs"]
mod tests;
