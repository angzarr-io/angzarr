//! Component discovery: the coordinator pulls its business process's
//! component descriptors (component.proto, `ComponentService.Describe`;
//! decision 14.10).
//!
//! - **When**: on the first connection to the business process, and again
//!   every time the channel comes back up after a disconnect. While a
//!   re-describe is pending, nothing is delivered.
//! - **No descriptor, no delivery**: the [`Registrar`] publishes readiness;
//!   until it holds a descriptor, synchronous callers are answered
//!   UNAVAILABLE and queued work stays queued.
//! - **A refused answer** (any descriptor with an empty `rules_version`):
//!   the whole answer is refused, nothing is held, readiness stays false, an
//!   error naming the business process and the component is logged and an
//!   alert raised. It is not asked again until the channel comes back up.
//! - **A failed call** (unreachable, or an error status): every failure is
//!   logged, and the call is retried with backoff (each retry is a
//!   reconnect attempt); an alert is raised once failures have lasted past
//!   the configured grace period.

use std::sync::Arc;
use std::time::{Duration, Instant};

/// A time source for the alert grace period (tests replace it).
pub type Now = Arc<dyn Fn() -> Instant + Send + Sync>;

/// Tracks one stretch of failing Describe calls: every failure is logged;
/// one alert is due once the stretch has lasted past the grace period
/// (decision 14.11).
#[derive(Debug, Default)]
pub struct FailureTracker {
    since: Option<Instant>,
    alerted: bool,
}

impl FailureTracker {
    /// Record a failure at `now`; true when the alert is due now.
    pub fn failed(&mut self, now: Instant, grace: Duration) -> bool {
        let since = *self.since.get_or_insert(now);
        if !self.alerted && now.duration_since(since) >= grace {
            self.alerted = true;
            return true;
        }
        false
    }

    /// The stretch ended (an answer arrived).
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// How long the current stretch has lasted.
    pub fn failing_for(&self, now: Instant) -> Duration {
        self.since
            .map(|s| now.duration_since(s))
            .unwrap_or_default()
    }
}

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use tokio::sync::watch;
use tonic::transport::Channel;
use tonic::Status;
use tracing::{error, info};

use crate::alerts::{Alert, Alerts};
use crate::proto::component_service_client::ComponentServiceClient;
use crate::proto::{ComponentDescriptor, ComponentKind, DescribeRequest};
use crate::registry::ComponentRegistry;

/// Where descriptors come from: the business process.
#[async_trait]
pub trait DescriptorSource: Send + Sync {
    /// Every component the business process hosts.
    async fn describe(&self) -> Result<Vec<ComponentDescriptor>, Status>;
}

/// [`DescriptorSource`] over gRPC: `ComponentService.Describe`.
#[derive(Clone)]
pub struct GrpcDescriptorSource {
    client: ComponentServiceClient<Channel>,
}

impl GrpcDescriptorSource {
    pub fn new(channel: Channel) -> Self {
        Self {
            client: ComponentServiceClient::new(channel),
        }
    }
}

#[async_trait]
impl DescriptorSource for GrpcDescriptorSource {
    async fn describe(&self) -> Result<Vec<ComponentDescriptor>, Status> {
        let mut client = self.client.clone();
        Ok(client
            .describe(DescribeRequest {})
            .await?
            .into_inner()
            .components)
    }
}

/// Which hosted component this coordinator serves.
#[derive(Debug, Clone)]
pub struct ComponentSelector {
    /// The component's kind.
    pub kind: ComponentKind,
    /// Its registered name, when the coordinator knows it. When `None`, the
    /// process must host exactly one component of `kind`.
    pub name: Option<String>,
}

impl ComponentSelector {
    /// The one descriptor this coordinator serves, or why there is none.
    pub fn select(
        &self,
        descriptors: &[ComponentDescriptor],
    ) -> Result<ComponentDescriptor, Status> {
        let chosen: Vec<_> = descriptors
            .iter()
            .filter(|d| d.kind == self.kind as i32)
            .filter(|d| self.name.as_ref().is_none_or(|n| &d.name == n))
            .collect();
        match chosen.as_slice() {
            [one] => Ok((*one).clone()),
            [] => Err(Status::not_found(format!(
                "the business process describes no {} component{}",
                self.kind.as_str_name(),
                self.name
                    .as_ref()
                    .map(|n| format!(" named '{n}'"))
                    .unwrap_or_default()
            ))),
            _ => Err(Status::failed_precondition(format!(
                "the business process describes several {} components; name the one to serve",
                self.kind.as_str_name()
            ))),
        }
    }
}

/// The result of one Describe.
#[derive(Debug, Clone)]
pub enum DescribeOutcome {
    /// The descriptor is held; the coordinator is ready.
    Registered,
    /// The answer was refused (a descriptor without a rules version, or no
    /// component this coordinator serves): nothing is held. Not retried
    /// until the channel comes back up.
    Refused(String),
    /// The call failed (unreachable, or an error status): retried.
    Failed(Status),
}

/// Keeps one instance's registration current and publishes readiness.
pub struct Registrar {
    source: Arc<dyn DescriptorSource>,
    selector: ComponentSelector,
    registry: Arc<ComponentRegistry>,
    domain: String,
    instance: String,
    /// The business process, for logs (its address).
    process: String,
    alerts: Arc<dyn Alerts>,
    alert_grace: Duration,
    now: Now,
    ready: watch::Sender<bool>,
}

impl Registrar {
    /// A registrar for `instance` of `domain`; readiness starts false.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: Arc<dyn DescriptorSource>,
        selector: ComponentSelector,
        registry: Arc<ComponentRegistry>,
        domain: impl Into<String>,
        instance: impl Into<String>,
        process: impl Into<String>,
        alerts: Arc<dyn Alerts>,
        alert_grace: Duration,
    ) -> Self {
        let (ready, _) = watch::channel(false);
        Self {
            source,
            selector,
            registry,
            domain: domain.into(),
            instance: instance.into(),
            process: process.into(),
            alerts,
            alert_grace,
            now: Arc::new(Instant::now),
            ready,
        }
    }

    /// Replace the time source (tests).
    pub fn with_now(mut self, now: Now) -> Self {
        self.now = now;
        self
    }

    /// Readiness: true while a descriptor is held.
    pub fn readiness(&self) -> watch::Receiver<bool> {
        self.ready.subscribe()
    }

    fn forget(&self) {
        self.registry.deregister(&self.domain, &self.instance);
        self.ready.send_replace(false);
    }

    /// One Describe: hold the selected descriptor, or hold nothing.
    pub async fn describe_once(&self) -> DescribeOutcome {
        let descriptors = match self.source.describe().await {
            Ok(descriptors) => descriptors,
            Err(status) => {
                self.forget();
                return DescribeOutcome::Failed(status);
            }
        };
        if let Some(unversioned) = descriptors.iter().find(|d| d.rules_version.is_empty()) {
            self.forget();
            let reason = format!(
                "business process {} describes component '{}' without a rules version; \
                 its answer is refused and nothing is delivered to it",
                self.process, unversioned.name
            );
            error!(process = %self.process, component = %unversioned.name, "{reason}");
            self.alerts.raise(Alert {
                component: unversioned.name.clone(),
                reason: reason.clone(),
            });
            return DescribeOutcome::Refused(reason);
        }
        let held = self.selector.select(&descriptors).and_then(|descriptor| {
            self.registry
                .register(&self.domain, &self.instance, descriptor)
        });
        match held {
            Ok(()) => {
                info!(process = %self.process, domain = %self.domain, "component registered");
                self.ready.send_replace(true);
                DescribeOutcome::Registered
            }
            Err(status) => {
                self.forget();
                let reason = format!("business process {}: {}", self.process, status.message());
                error!(process = %self.process, "{reason}");
                self.alerts.raise(Alert {
                    component: self.domain.clone(),
                    reason: reason.clone(),
                });
                DescribeOutcome::Refused(reason)
            }
        }
    }

    /// Describe until an answer arrives (retrying failed calls), then again
    /// each time `connections` reports a new connection, forever.
    pub async fn run(self: Arc<Self>, mut connections: watch::Receiver<u64>) {
        loop {
            self.describe_until_answered().await;
            // The connection Describe itself opened is not a reconnect.
            connections.borrow_and_update();
            if connections.changed().await.is_err() {
                return;
            }
            info!(process = %self.process, "channel came back up; describing again");
            // Nothing is delivered until the reconnected process answers.
            self.forget();
        }
    }

    /// One Describe within a failing stretch: a failure is logged, and
    /// alerted once the stretch has outlasted the grace period.
    pub async fn describe_tracking(&self, tracker: &mut FailureTracker) -> DescribeOutcome {
        let outcome = self.describe_once().await;
        match &outcome {
            DescribeOutcome::Failed(status) => {
                let now = (self.now)();
                error!(
                    process = %self.process,
                    %status,
                    failing_for = ?tracker.failing_for(now),
                    "describe failed; not serving"
                );
                if tracker.failed(now, self.alert_grace) {
                    self.alerts.raise(Alert {
                        component: self.domain.clone(),
                        reason: format!(
                            "business process {} has not answered Describe for {:?}: {}",
                            self.process,
                            tracker.failing_for(now),
                            status.message()
                        ),
                    });
                }
            }
            _ => tracker.reset(),
        }
        outcome
    }

    async fn describe_until_answered(&self) {
        let mut delays = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(200))
            .with_max_delay(Duration::from_secs(30))
            .without_max_times()
            .build();
        let mut tracker = FailureTracker::default();
        while let DescribeOutcome::Failed(_) = self.describe_tracking(&mut tracker).await {
            let delay = delays.next().unwrap_or(Duration::from_secs(30));
            tokio::time::sleep(delay).await;
        }
    }
}

#[cfg(test)]
#[path = "registration.test.rs"]
mod tests;
