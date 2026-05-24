//! IPC-based event bus for embedded multi-process mode.
//!
//! Substitutes for Kafka/RabbitMQ in embedded mode using UDS and named pipes.
//!
//! ## Trace Context Propagation
//!
//! Unlike distributed buses (AMQP, Kafka, SNS/SQS), IPC does **not** propagate
//! W3C TraceContext headers. Rationale:
//!
//! 1. **Same machine**: IPC runs on a single host where all processes share
//!    the same collector endpoint. Traces correlate via timestamps and
//!    correlation IDs without explicit context propagation.
//!
//! 2. **Protocol overhead**: Adding headers to the length-prefixed pipe protocol
//!    would require a breaking wire format change for minimal observability gain.
//!
//! 3. **Local-dev focus**: IPC is primarily for local development and testing
//!    where distributed tracing across services is less critical.
//!
//! For production distributed tracing, use AMQP, Kafka, or SNS/SQS buses which
//! implement full W3C TraceContext propagation via [`crate::utils::tracing`].
//!
//! ## Architecture (SNS/SQS-like)
//! ```text
//! ┌─────────────┐     ┌─────────────┐
//! │  Aggregate  │────▶│   Broker    │
//! │  (publish)  │ UDS │  (fanout)   │
//! └─────────────┘     └─────────────┘
//!                           │
//!          ┌────────────────┼────────────────┐
//!          ▼                ▼                ▼
//!    ┌──────────┐     ┌──────────┐     ┌──────────┐
//!    │ pipe-A   │     │ pipe-B   │     │ pipe-C   │
//!    │(projector)│    │ (saga)   │     │(projector)│
//!    └──────────┘     └──────────┘     └──────────┘
//! ```
//!
//! Usage:
//! 1. Orchestrator creates `IpcBroker` and registers subscribers
//! 2. Orchestrator calls `broker.run()` to start listening
//! 3. Aggregates use `IpcEventBus::publisher()` to connect and publish
//! 4. Projectors/sagas use `IpcEventBus::subscriber()` to read from pipes

use std::sync::Arc;

use tracing::info;

use super::config::EventBusMode;
use super::factory::BusBackend;
use super::traits::EventBus;
use crate::advice::InstrumentedBus;

mod broker;
pub(crate) mod checkpoint;
mod client;

pub use broker::{IpcBroker, IpcBrokerConfig, SubscriberInfo};
pub use client::{IpcConfig, IpcEventBus, SUBSCRIBERS_ENV_VAR};

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    BusBackend {
        try_create: |config, mode| {
            // Clone what we need before creating the 'static future
            let messaging_type = config.messaging_type.clone();
            let ipc_cfg = config.ipc.clone();
            Box::pin(async move {
                if messaging_type != "ipc" {
                    return None;
                }

                let ipc_config = match mode {
                    EventBusMode::Publisher => IpcConfig::publisher(&ipc_cfg.base_path),
                    EventBusMode::Subscriber { domain, .. } => {
                        let name = ipc_cfg
                            .subscriber_name
                            .clone()
                            .unwrap_or_else(|| format!("subscriber-{}", domain));
                        IpcConfig::subscriber(&ipc_cfg.base_path, name, vec![domain])
                    }
                    EventBusMode::SubscriberAll { queue } => {
                        let name = ipc_cfg.subscriber_name.clone().unwrap_or(queue);
                        let domains = ipc_cfg.get_domains();
                        IpcConfig::subscriber(&ipc_cfg.base_path, name, domains)
                    }
                };

                let bus = IpcEventBus::new(ipc_config);
                info!(messaging_type = "ipc", "Event bus initialized");
                // R2-WIRE-ADVICE: wrap with `InstrumentedBus` under "ipc".
                Some(Ok(
                    Arc::new(InstrumentedBus::new(bus, "ipc")) as Arc<dyn EventBus>
                ))
            })
        },
    }
}

/// Default base path for IPC sockets/pipes.
pub const DEFAULT_BASE_PATH: &str = "/tmp/angzarr";

/// Socket name for the broker.
pub const BROKER_SOCKET: &str = "event-broker.sock";

/// Pipe prefix for subscribers.
pub const SUBSCRIBER_PIPE_PREFIX: &str = "subscriber-";
