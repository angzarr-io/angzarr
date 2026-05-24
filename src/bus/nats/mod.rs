//! NATS JetStream EventBus implementation.
//!
//! Provides publish/subscribe for events using NATS JetStream consumers.
//!
//! ## Architecture
//!
//! - **Publishing**: Events published to `{prefix}.events.{domain}.{root}` (edition is
//!   in the EventBook payload, not the subject — see H-09 in
//!   `plans/deep-review-remediation.md` for the rationale)
//! - **Subscribing**: Durable pull consumers filter by domain
//! - **Consumer Groups**: Multiple subscribers with same name share workload
//!
//! # Configuration
//!
//! ```yaml
//! messaging:
//!   type: nats
//!   nats:
//!     url: "nats://localhost:4222"
//!     stream_prefix: "angzarr"
//!     consumer_name: "my-service"
//!     # JetStream-specific
//!     replicas: 3
//!     retention: "limits"  # limits, interest, workqueue
//!     max_age_hours: 168   # 7 days
//! ```

mod bus;
mod config;
mod consumer;
#[cfg(feature = "otel")]
mod otel;

use std::sync::Arc;

use tracing::info;

use super::config::EventBusMode;
use super::error::BusError;
use super::factory::BusBackend;
use super::traits::EventBus;
use crate::advice::InstrumentedBus;

pub use bus::NatsEventBus;
pub use config::NatsBusConfig;

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    BusBackend {
        try_create: |config, mode| {
            // Clone what we need before creating the 'static future
            let messaging_type = config.messaging_type.clone();
            let nats_url = config.nats.url.clone();
            let stream_prefix = config.nats.stream_prefix.clone();

            Box::pin(async move {
                if messaging_type != "nats" {
                    return None;
                }

                // Connect to NATS
                let client = match async_nats::connect(&nats_url).await {
                    Ok(c) => c,
                    Err(e) => {
                        return Some(Err(BusError::Connection(format!(
                            "NATS connect failed: {}",
                            e
                        ))))
                    }
                };

                let bus_config = match mode {
                    EventBusMode::Publisher => NatsBusConfig {
                        prefix: stream_prefix,
                        consumer_name: None,
                        domain_filter: None,
                    },
                    EventBusMode::Subscriber { queue, domain } => NatsBusConfig {
                        prefix: stream_prefix,
                        consumer_name: Some(queue),
                        domain_filter: Some(domain),
                    },
                    EventBusMode::SubscriberAll { queue } => NatsBusConfig {
                        prefix: stream_prefix,
                        consumer_name: Some(queue),
                        domain_filter: None,
                    },
                };

                match NatsEventBus::with_config(client, bus_config).await {
                    Ok(bus) => {
                        info!(messaging_type = "nats", "Event bus initialized");
                        // R2-WIRE-ADVICE: wrap with `InstrumentedBus` under "nats".
                        Some(Ok(
                            Arc::new(InstrumentedBus::new(bus, "nats")) as Arc<dyn EventBus>
                        ))
                    }
                    Err(e) => Some(Err(BusError::Connection(format!(
                        "NATS setup failed: {}",
                        e
                    )))),
                }
            })
        },
    }
}
