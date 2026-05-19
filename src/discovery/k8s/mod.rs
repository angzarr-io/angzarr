//! K8s label-based service discovery.
//!
//! Discovers aggregate, projector, saga, and process-manager services by
//! watching K8s Service resources with appropriate labels. Service mesh
//! handles L7 gRPC load balancing—we just connect to Service DNS names.
//!
//! # Label Scheme
//!
//! ```yaml
//! # Aggregate coordinator
//! labels:
//!   app.kubernetes.io/component: aggregate
//!   angzarr.io/domain: cart
//!
//! # Projector coordinator
//! labels:
//!   app.kubernetes.io/component: projector
//!   angzarr.io/domain: cart
//!
//! # Saga coordinator (single source domain)
//! labels:
//!   app.kubernetes.io/component: saga
//!   angzarr.io/source-domain: tournament   # event source the saga subscribes to
//!
//! # Process manager coordinator (multiple source domains)
//! labels:
//!   app.kubernetes.io/component: process-manager
//!   angzarr.io/subscriptions: order,inventory,fulfillment
//! ```
//!
//! Saga services that are missing the source-domain label are skipped with
//! a warning — same for PM services missing subscriptions. The aggregate
//! sidecar's `call_sync_sagas`/`call_sync_pms` paths can only route by
//! source domain, so an unlabeled component can't be reached synchronously
//! and would silently degrade CASCADE mode if registered without the data.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};
use futures::TryStreamExt;
use k8s_openapi::api::core::v1::Service;
use kube::{
    api::{Api, ListParams},
    runtime::watcher::{self, Event},
    Client,
};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::config::{NAMESPACE_ENV_VAR, POD_NAMESPACE_ENV_VAR};
use crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use crate::proto::event_query_service_client::EventQueryServiceClient;
use crate::proto::projector_coordinator_service_client::ProjectorCoordinatorServiceClient;

use super::static_discovery::{PmService, SagaService, StaticServiceDiscovery};
use super::{DiscoveredService, DiscoveryError};

/// Label for component type.
const COMPONENT_LABEL: &str = "app.kubernetes.io/component";

/// Label for domain (aggregate and projector).
const DOMAIN_LABEL: &str = "angzarr.io/domain";

/// Label for the source domain a saga subscribes to (single value).
const SOURCE_DOMAIN_LABEL: &str = "angzarr.io/source-domain";

/// Label for the source domains a process manager subscribes to
/// (comma-separated list).
const SUBSCRIPTIONS_LABEL: &str = "angzarr.io/subscriptions";

/// Component values.
const COMPONENT_AGGREGATE: &str = "aggregate";
const COMPONENT_PROJECTOR: &str = "projector";
const COMPONENT_SAGA: &str = "saga";
const COMPONENT_PROCESS_MANAGER: &str = "process-manager";

/// Default gRPC port.
const DEFAULT_GRPC_PORT: u16 = 50051;

/// Minimum delay between watcher reconnect attempts.
const RECONNECT_MIN_DELAY: Duration = Duration::from_millis(100);

/// Maximum delay between watcher reconnect attempts.
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

/// Reconnect-backoff growth factor (exponential, no jitter so the loop is
/// deterministic for testing — jitter would only matter at fleet scale to
/// avoid thundering herd, and a single status console doesn't herd).
const RECONNECT_FACTOR: f32 = 2.0;

/// Per-watcher liveness window. If any watcher has been silent for longer
/// than this since its last observed event (or birth), `is_watcher_healthy`
/// reports false — the helm liveness probe can read this to restart the
/// pod rather than letting a silently-disconnected watcher keep serving a
/// stale cache.
pub const WATCHER_HEALTH_THRESHOLD: Duration = Duration::from_secs(30);

/// Tracks watcher liveness. The reconnect loop calls [`record_event`] on
/// every observed `Event` (including `Init`/`InitDone` markers — those
/// prove the apiserver is responding). [`is_healthy`] reads the last
/// observed timestamp and compares against the caller-supplied `now`.
///
/// Behind a `std::sync::Mutex` (not tokio's) because the critical section
/// is a single `Instant` swap — no `.await` inside the lock, and async
/// contention is irrelevant at the per-event update rate.
#[derive(Debug)]
pub(crate) struct WatcherHealth {
    last_event_at: Mutex<Instant>,
}

impl WatcherHealth {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            last_event_at: Mutex::new(now),
        }
    }

    pub(crate) fn record_event(&self, now: Instant) {
        let mut guard = self
            .last_event_at
            .lock()
            .expect("WatcherHealth mutex poisoned");
        *guard = now;
    }

    pub(crate) fn is_healthy(&self, threshold: Duration, now: Instant) -> bool {
        let last = *self
            .last_event_at
            .lock()
            .expect("WatcherHealth mutex poisoned");
        now.saturating_duration_since(last) <= threshold
    }
}

/// Compute the next reconnect delay and (optionally) reset the backoff
/// iterator.
///
/// Mirrors AMQP's `consume_with_reconnect` pattern, BUT fixes the H-07
/// anti-pattern: backoff resets ONLY after the stream actually delivered
/// at least one event in the previous cycle (`observed_any_event_in_cycle
/// = true`). A "subscribed → errored before first event" cycle (apiserver
/// rejects the watch immediately) must NOT reset the backoff, otherwise
/// the loop hammers the apiserver at the min delay forever.
fn next_reconnect_delay(
    observed_any_event_in_cycle: bool,
    backoff_iter: &mut ExponentialBackoff,
    builder: &ExponentialBuilder,
) -> Duration {
    if observed_any_event_in_cycle {
        *backoff_iter = builder.build();
    }
    backoff_iter.next().unwrap_or(RECONNECT_MAX_DELAY)
}

/// The standard reconnect-backoff builder for K8s watchers.
fn reconnect_builder() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(RECONNECT_MIN_DELAY)
        .with_max_delay(RECONNECT_MAX_DELAY)
        .with_factor(RECONNECT_FACTOR)
}

/// K8s label-based service discovery.
///
/// Mesh handles L7 load balancing—we just connect to Service names.
/// Delegates storage and client caching to `StaticServiceDiscovery`.
pub struct K8sServiceDiscovery {
    client: Option<Client>,
    namespace: String,
    /// Aggregates cache for K8s watcher updates.
    aggregates: Arc<RwLock<HashMap<String, DiscoveredService>>>,
    /// Projectors cache for K8s watcher updates.
    projectors: Arc<RwLock<HashMap<String, DiscoveredService>>>,
    /// Sagas cache: service name → SagaService (carries source_domain).
    sagas: Arc<RwLock<HashMap<String, SagaService>>>,
    /// PMs cache: service name → PmService (carries subscriptions list).
    pms: Arc<RwLock<HashMap<String, PmService>>>,
    /// Inner static discovery for storage and client caching.
    inner: StaticServiceDiscovery,
    /// Per-component watcher health. Populated by `start_watching` —
    /// `is_watcher_healthy()` reads them at the trait boundary.
    aggregate_health: Arc<WatcherHealth>,
    projector_health: Arc<WatcherHealth>,
    saga_health: Arc<WatcherHealth>,
    pm_health: Arc<WatcherHealth>,
}

impl K8sServiceDiscovery {
    /// Create a new service discovery instance.
    pub async fn new(namespace: impl Into<String>) -> Result<Self, DiscoveryError> {
        let client = Client::try_default().await?;
        let namespace = namespace.into();

        info!(namespace = %namespace, "Service discovery initialized");

        let now = Instant::now();
        Ok(Self {
            client: Some(client),
            namespace: namespace.clone(),
            aggregates: Arc::new(RwLock::new(HashMap::new())),
            projectors: Arc::new(RwLock::new(HashMap::new())),
            sagas: Arc::new(RwLock::new(HashMap::new())),
            pms: Arc::new(RwLock::new(HashMap::new())),
            inner: StaticServiceDiscovery::new(),
            aggregate_health: Arc::new(WatcherHealth::new(now)),
            projector_health: Arc::new(WatcherHealth::new(now)),
            saga_health: Arc::new(WatcherHealth::new(now)),
            pm_health: Arc::new(WatcherHealth::new(now)),
        })
    }

    /// Returns `true` only when every started watcher has observed an
    /// event within `WATCHER_HEALTH_THRESHOLD` (the `Init`/`InitDone`
    /// markers count). Returns `true` in static mode (no K8s client —
    /// nothing to watch). Surfaces the H-27 liveness signal for an
    /// external health check; if this returns `false` the cache may be
    /// drifting silently and the pod should be restarted.
    pub fn is_watcher_healthy(&self) -> bool {
        if self.client.is_none() {
            return true;
        }
        let now = Instant::now();
        let threshold = WATCHER_HEALTH_THRESHOLD;
        self.aggregate_health.is_healthy(threshold, now)
            && self.projector_health.is_healthy(threshold, now)
            && self.saga_health.is_healthy(threshold, now)
            && self.pm_health.is_healthy(threshold, now)
    }

    /// Create from environment variables.
    ///
    /// Reads namespace from NAMESPACE_ENV_VAR or POD_NAMESPACE_ENV_VAR env vars.
    pub async fn from_env() -> Result<Self, DiscoveryError> {
        let namespace = std::env::var(NAMESPACE_ENV_VAR)
            .or_else(|_| std::env::var(POD_NAMESPACE_ENV_VAR))
            .unwrap_or_else(|_| "default".to_string());

        Self::new(namespace).await
    }

    fn start_watching_component(
        &self,
        component: &'static str,
        cache: Arc<RwLock<HashMap<String, DiscoveredService>>>,
        health: Arc<WatcherHealth>,
    ) {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return,
        };
        let namespace = self.namespace.clone();

        tokio::spawn(async move {
            let builder = reconnect_builder();
            let mut backoff_iter = builder.build();

            loop {
                let services: Api<Service> = Api::namespaced(client.clone(), &namespace);
                let stream = watcher::watcher(
                    services,
                    watcher::Config::default()
                        .labels(&format!("{}={}", COMPONENT_LABEL, component)),
                );

                info!(component = component, "Starting service watcher");

                // `observed_event` toggles to true the first time the
                // stream yields anything — used to decide whether the
                // next reconnect-cycle should reset its backoff.
                let observed_event = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let outcome = {
                    let observed_event = observed_event.clone();
                    let cache = cache.clone();
                    let health = health.clone();
                    let namespace = namespace.clone();
                    stream
                        .try_for_each(|event| {
                            let cache = cache.clone();
                            let health = health.clone();
                            let observed_event = observed_event.clone();
                            let namespace = namespace.clone();
                            async move {
                                observed_event.store(true, std::sync::atomic::Ordering::Release);
                                health.record_event(Instant::now());
                                Self::handle_event(component, &cache, &namespace, event).await;
                                Ok(())
                            }
                        })
                        .await
                };

                let observed = observed_event.load(std::sync::atomic::Ordering::Acquire);
                match outcome {
                    Ok(()) => info!(
                        component = component,
                        observed_events = observed,
                        "Watcher stream ended; reconnecting"
                    ),
                    Err(e) => warn!(
                        component = component,
                        error = %e,
                        observed_events = observed,
                        "Service watcher error; will reconnect after backoff"
                    ),
                }

                let delay = next_reconnect_delay(observed, &mut backoff_iter, &builder);
                tokio::time::sleep(delay).await;
            }
        });
    }

    /// Start a watcher for saga services. Same shape as
    /// [`start_watching_component`] but extracts the source-domain label
    /// into a [`SagaService`] so [`get_saga_endpoints_for_domain`] can
    /// filter by source domain without re-reading metadata on every call.
    fn start_watching_sagas(
        &self,
        cache: Arc<RwLock<HashMap<String, SagaService>>>,
        health: Arc<WatcherHealth>,
    ) {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return,
        };
        let namespace = self.namespace.clone();

        tokio::spawn(async move {
            let builder = reconnect_builder();
            let mut backoff_iter = builder.build();

            loop {
                let services: Api<Service> = Api::namespaced(client.clone(), &namespace);
                let stream = watcher::watcher(
                    services,
                    watcher::Config::default()
                        .labels(&format!("{}={}", COMPONENT_LABEL, COMPONENT_SAGA)),
                );

                info!(component = COMPONENT_SAGA, "Starting saga watcher");

                let observed_event = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let outcome = {
                    let observed_event = observed_event.clone();
                    let cache = cache.clone();
                    let health = health.clone();
                    let namespace = namespace.clone();
                    stream
                        .try_for_each(|event| {
                            let cache = cache.clone();
                            let health = health.clone();
                            let observed_event = observed_event.clone();
                            let namespace = namespace.clone();
                            async move {
                                observed_event.store(true, std::sync::atomic::Ordering::Release);
                                health.record_event(Instant::now());
                                Self::handle_saga_event(&cache, &namespace, event).await;
                                Ok(())
                            }
                        })
                        .await
                };

                let observed = observed_event.load(std::sync::atomic::Ordering::Acquire);
                match outcome {
                    Ok(()) => info!(
                        component = COMPONENT_SAGA,
                        observed_events = observed,
                        "Saga watcher stream ended; reconnecting"
                    ),
                    Err(e) => warn!(
                        component = COMPONENT_SAGA,
                        error = %e,
                        observed_events = observed,
                        "Saga watcher error; will reconnect after backoff"
                    ),
                }

                let delay = next_reconnect_delay(observed, &mut backoff_iter, &builder);
                tokio::time::sleep(delay).await;
            }
        });
    }

    /// Start a watcher for process-manager services.
    fn start_watching_pms(
        &self,
        cache: Arc<RwLock<HashMap<String, PmService>>>,
        health: Arc<WatcherHealth>,
    ) {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return,
        };
        let namespace = self.namespace.clone();

        tokio::spawn(async move {
            let builder = reconnect_builder();
            let mut backoff_iter = builder.build();

            loop {
                let services: Api<Service> = Api::namespaced(client.clone(), &namespace);
                let stream = watcher::watcher(
                    services,
                    watcher::Config::default().labels(&format!(
                        "{}={}",
                        COMPONENT_LABEL, COMPONENT_PROCESS_MANAGER
                    )),
                );

                info!(
                    component = COMPONENT_PROCESS_MANAGER,
                    "Starting process-manager watcher"
                );

                let observed_event = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let outcome = {
                    let observed_event = observed_event.clone();
                    let cache = cache.clone();
                    let health = health.clone();
                    let namespace = namespace.clone();
                    stream
                        .try_for_each(|event| {
                            let cache = cache.clone();
                            let health = health.clone();
                            let observed_event = observed_event.clone();
                            let namespace = namespace.clone();
                            async move {
                                observed_event.store(true, std::sync::atomic::Ordering::Release);
                                health.record_event(Instant::now());
                                Self::handle_pm_event(&cache, &namespace, event).await;
                                Ok(())
                            }
                        })
                        .await
                };

                let observed = observed_event.load(std::sync::atomic::Ordering::Acquire);
                match outcome {
                    Ok(()) => info!(
                        component = COMPONENT_PROCESS_MANAGER,
                        observed_events = observed,
                        "PM watcher stream ended; reconnecting"
                    ),
                    Err(e) => warn!(
                        component = COMPONENT_PROCESS_MANAGER,
                        error = %e,
                        observed_events = observed,
                        "PM watcher error; will reconnect after backoff"
                    ),
                }

                let delay = next_reconnect_delay(observed, &mut backoff_iter, &builder);
                tokio::time::sleep(delay).await;
            }
        });
    }

    async fn handle_saga_event(
        cache: &RwLock<HashMap<String, SagaService>>,
        namespace: &str,
        event: Event<Service>,
    ) {
        match event {
            Event::Apply(svc) | Event::InitApply(svc) => {
                if let Some(saga) = Self::extract_saga_with_namespace(&svc, namespace) {
                    debug!(
                        service = %saga.service.name,
                        source_domain = %saga.source_domain,
                        "Saga discovered/updated"
                    );
                    cache.write().await.insert(saga.service.name.clone(), saga);
                }
            }
            Event::Delete(svc) => {
                if let Some(name) = svc.metadata.name {
                    debug!(service = %name, "Saga deleted");
                    cache.write().await.remove(&name);
                }
            }
            Event::Init => debug!(component = COMPONENT_SAGA, "Watcher initialized"),
            Event::InitDone => debug!(component = COMPONENT_SAGA, "Watcher init done"),
        }
    }

    async fn handle_pm_event(
        cache: &RwLock<HashMap<String, PmService>>,
        namespace: &str,
        event: Event<Service>,
    ) {
        match event {
            Event::Apply(svc) | Event::InitApply(svc) => {
                if let Some(pm) = Self::extract_pm_with_namespace(&svc, namespace) {
                    debug!(
                        service = %pm.service.name,
                        subscriptions = ?pm.subscriptions,
                        "PM discovered/updated"
                    );
                    cache.write().await.insert(pm.service.name.clone(), pm);
                }
            }
            Event::Delete(svc) => {
                if let Some(name) = svc.metadata.name {
                    debug!(service = %name, "PM deleted");
                    cache.write().await.remove(&name);
                }
            }
            Event::Init => debug!(component = COMPONENT_PROCESS_MANAGER, "Watcher initialized"),
            Event::InitDone => debug!(component = COMPONENT_PROCESS_MANAGER, "Watcher init done"),
        }
    }

    fn extract_saga(&self, svc: &Service) -> Option<SagaService> {
        Self::extract_saga_with_namespace(svc, &self.namespace)
    }

    fn extract_saga_with_namespace(svc: &Service, namespace: &str) -> Option<SagaService> {
        let service = Self::extract_service_with_namespace(svc, namespace)?;
        let labels = svc.metadata.labels.as_ref();
        let source_domain = labels.and_then(|l| l.get(SOURCE_DOMAIN_LABEL)).cloned();
        let Some(source_domain) = source_domain else {
            tracing::warn!(
                service = %service.name,
                "Saga service missing {SOURCE_DOMAIN_LABEL} label — skipping registration"
            );
            return None;
        };
        Some(SagaService {
            service,
            source_domain,
        })
    }

    fn extract_pm(&self, svc: &Service) -> Option<PmService> {
        Self::extract_pm_with_namespace(svc, &self.namespace)
    }

    fn extract_pm_with_namespace(svc: &Service, namespace: &str) -> Option<PmService> {
        let service = Self::extract_service_with_namespace(svc, namespace)?;
        let labels = svc.metadata.labels.as_ref();
        let raw = labels.and_then(|l| l.get(SUBSCRIPTIONS_LABEL)).cloned();
        let Some(raw) = raw else {
            tracing::warn!(
                service = %service.name,
                "PM service missing {SUBSCRIPTIONS_LABEL} label — skipping registration"
            );
            return None;
        };
        let subscriptions: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if subscriptions.is_empty() {
            tracing::warn!(
                service = %service.name,
                raw = %raw,
                "PM service has empty {SUBSCRIPTIONS_LABEL} label — skipping registration"
            );
            return None;
        }
        Some(PmService {
            service,
            subscriptions,
        })
    }

    async fn handle_event(
        component: &str,
        cache: &RwLock<HashMap<String, DiscoveredService>>,
        namespace: &str,
        event: Event<Service>,
    ) {
        match event {
            Event::Apply(svc) | Event::InitApply(svc) => {
                if let Some(discovered) = Self::extract_service_with_namespace(&svc, namespace) {
                    debug!(
                        component = component,
                        service = %discovered.name,
                        domain = ?discovered.domain,
                        "Service discovered/updated"
                    );
                    cache
                        .write()
                        .await
                        .insert(discovered.name.clone(), discovered);
                }
            }
            Event::Delete(svc) => {
                if let Some(name) = svc.metadata.name {
                    debug!(component = component, service = %name, "Service deleted");
                    cache.write().await.remove(&name);
                }
            }
            Event::Init => {
                debug!(component = component, "Watcher initialized");
            }
            Event::InitDone => {
                debug!(component = component, "Watcher init done");
            }
        }
    }

    fn extract_service(&self, svc: &Service) -> Option<DiscoveredService> {
        Self::extract_service_with_namespace(svc, &self.namespace)
    }

    fn extract_service_with_namespace(svc: &Service, namespace: &str) -> Option<DiscoveredService> {
        let name = svc.metadata.name.as_ref()?;
        let labels = svc.metadata.labels.as_ref();

        let domain = labels.and_then(|l| l.get(DOMAIN_LABEL)).cloned();

        // Find grpc port
        let port = svc
            .spec
            .as_ref()
            .and_then(|s| s.ports.as_ref())
            .and_then(|ports| {
                ports
                    .iter()
                    .find(|p| p.name.as_deref() == Some("grpc"))
                    .or_else(|| ports.first())
            })
            .and_then(|p| u16::try_from(p.port).ok())
            .unwrap_or(DEFAULT_GRPC_PORT);

        let service_address = format!("{}.{}.svc.cluster.local", name, namespace);

        info!(
            service = %name,
            address = %service_address,
            port = port,
            domain = ?domain,
            "Extracted service"
        );

        Some(DiscoveredService {
            name: name.clone(),
            service_address,
            port,
            domain,
        })
    }

    /// Register a discovered service with inner for client caching.
    async fn sync_to_inner(&self, component: &str, service: &DiscoveredService) {
        if let Some(domain) = &service.domain {
            if component == COMPONENT_AGGREGATE {
                self.inner
                    .register_aggregate(domain, &service.service_address, service.port)
                    .await;
            } else if component == COMPONENT_PROJECTOR {
                self.inner
                    .register_projector(
                        &service.name,
                        domain,
                        &service.service_address,
                        service.port,
                    )
                    .await;
            }
        }
    }
}

use super::ServiceDiscovery;

#[async_trait::async_trait]
impl ServiceDiscovery for K8sServiceDiscovery {
    async fn register_aggregate(&self, domain: &str, address: &str, port: u16) {
        // Store in local cache for K8s compatibility
        let service = DiscoveredService {
            name: format!("{}-aggregate", domain),
            service_address: address.to_string(),
            port,
            domain: Some(domain.to_string()),
        };
        self.aggregates
            .write()
            .await
            .insert(service.name.clone(), service);

        // Delegate to inner for client caching
        self.inner.register_aggregate(domain, address, port).await;
    }

    async fn register_projector(&self, name: &str, domain: &str, address: &str, port: u16) {
        // Store in local cache for K8s compatibility
        let service = DiscoveredService {
            name: name.to_string(),
            service_address: address.to_string(),
            port,
            domain: Some(domain.to_string()),
        };
        self.projectors
            .write()
            .await
            .insert(service.name.clone(), service);

        // Delegate to inner for client caching
        self.inner
            .register_projector(name, domain, address, port)
            .await;
    }

    async fn get_aggregate(
        &self,
        domain: &str,
    ) -> Result<CommandHandlerCoordinatorServiceClient<Channel>, DiscoveryError> {
        // Sync any unsynced services from local cache to inner
        let aggregates = self.aggregates.read().await;
        for service in aggregates.values() {
            if let Some(d) = &service.domain {
                // This is idempotent - inner will skip if already registered
                self.inner
                    .register_aggregate(d, &service.service_address, service.port)
                    .await;
            }
        }
        drop(aggregates);

        // Delegate to inner
        self.inner.get_aggregate(domain).await
    }

    async fn get_event_query(
        &self,
        domain: &str,
    ) -> Result<EventQueryServiceClient<Channel>, DiscoveryError> {
        // Sync any unsynced services from local cache to inner
        let aggregates = self.aggregates.read().await;
        for service in aggregates.values() {
            if let Some(d) = &service.domain {
                self.inner
                    .register_aggregate(d, &service.service_address, service.port)
                    .await;
            }
        }
        drop(aggregates);

        // Delegate to inner
        self.inner.get_event_query(domain).await
    }

    async fn get_all_projectors(
        &self,
    ) -> Result<Vec<ProjectorCoordinatorServiceClient<Channel>>, DiscoveryError> {
        // Sync any unsynced services from local cache to inner
        let projectors = self.projectors.read().await;
        for service in projectors.values() {
            if let Some(d) = &service.domain {
                self.inner
                    .register_projector(&service.name, d, &service.service_address, service.port)
                    .await;
            }
        }
        drop(projectors);

        // Delegate to inner
        self.inner.get_all_projectors().await
    }

    async fn get_projector_by_name(
        &self,
        name: &str,
    ) -> Result<ProjectorCoordinatorServiceClient<Channel>, DiscoveryError> {
        // Sync any unsynced services from local cache to inner
        let projectors = self.projectors.read().await;
        for service in projectors.values() {
            if let Some(d) = &service.domain {
                self.inner
                    .register_projector(&service.name, d, &service.service_address, service.port)
                    .await;
            }
        }
        drop(projectors);

        // Delegate to inner
        self.inner.get_projector_by_name(name).await
    }

    async fn aggregate_domains(&self) -> Vec<String> {
        // Use local cache - it has the authoritative list from K8s
        self.aggregates
            .read()
            .await
            .values()
            .filter_map(|s| s.domain.clone())
            .collect()
    }

    async fn has_aggregates(&self) -> bool {
        !self.aggregates.read().await.is_empty()
    }

    async fn has_projectors(&self) -> bool {
        !self.projectors.read().await.is_empty()
    }

    async fn register_saga(&self, name: &str, source_domain: &str, address: &str, port: u16) {
        // Mirror into the K8s cache so future calls hit the same source
        // even if the watcher hasn't observed the service yet.
        let saga_service = SagaService {
            service: DiscoveredService {
                name: name.to_string(),
                service_address: address.to_string(),
                port,
                domain: Some(source_domain.to_string()),
            },
            source_domain: source_domain.to_string(),
        };
        self.sagas
            .write()
            .await
            .insert(name.to_string(), saga_service);
        // Also register with inner for client caching used by the
        // SagaCoordinator gRPC clients.
        self.inner
            .register_saga(name, source_domain, address, port)
            .await;
    }

    async fn register_pm(&self, name: &str, subscriptions: &[&str], address: &str, port: u16) {
        let pm_service = PmService {
            service: DiscoveredService {
                name: name.to_string(),
                service_address: address.to_string(),
                port,
                domain: None,
            },
            subscriptions: subscriptions.iter().map(|s| s.to_string()).collect(),
        };
        self.pms.write().await.insert(name.to_string(), pm_service);
        self.inner
            .register_pm(name, subscriptions, address, port)
            .await;
    }

    async fn get_saga_endpoints_for_domain(&self, source_domain: &str) -> Vec<DiscoveredService> {
        // Read directly from the K8s cache — the watcher is the source of
        // truth in K8s mode. Inner is only consulted for client caching
        // via `register_saga`, not for routing decisions.
        self.sagas
            .read()
            .await
            .values()
            .filter(|s| s.source_domain == source_domain)
            .map(|s| s.service.clone())
            .collect()
    }

    async fn get_pm_endpoints_for_domain(&self, domain: &str) -> Vec<DiscoveredService> {
        self.pms
            .read()
            .await
            .values()
            .filter(|pm| pm.subscriptions.iter().any(|sub| sub == domain))
            .map(|pm| pm.service.clone())
            .collect()
    }

    async fn has_sagas(&self) -> bool {
        !self.sagas.read().await.is_empty()
    }

    async fn has_pms(&self) -> bool {
        !self.pms.read().await.is_empty()
    }

    async fn initial_sync(&self) -> Result<(), DiscoveryError> {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return Ok(()), // Static mode - no K8s sync
        };

        info!("Performing initial service sync");

        let services: Api<Service> = Api::namespaced(client, &self.namespace);

        // Sync aggregates
        let aggregate_list = services
            .list(
                &ListParams::default()
                    .labels(&format!("{}={}", COMPONENT_LABEL, COMPONENT_AGGREGATE)),
            )
            .await?;
        for svc in aggregate_list {
            if let Some(discovered) = self.extract_service(&svc) {
                self.aggregates
                    .write()
                    .await
                    .insert(discovered.name.clone(), discovered.clone());
                // Also register with inner
                self.sync_to_inner(COMPONENT_AGGREGATE, &discovered).await;
            }
        }

        // Sync projectors
        let projector_list = services
            .list(
                &ListParams::default()
                    .labels(&format!("{}={}", COMPONENT_LABEL, COMPONENT_PROJECTOR)),
            )
            .await?;
        for svc in projector_list {
            if let Some(discovered) = self.extract_service(&svc) {
                self.projectors
                    .write()
                    .await
                    .insert(discovered.name.clone(), discovered.clone());
                // Also register with inner
                self.sync_to_inner(COMPONENT_PROJECTOR, &discovered).await;
            }
        }

        // Sync sagas
        let saga_list = services
            .list(&ListParams::default().labels(&format!("{}={}", COMPONENT_LABEL, COMPONENT_SAGA)))
            .await?;
        for svc in saga_list {
            if let Some(saga) = self.extract_saga(&svc) {
                self.inner
                    .register_saga(
                        &saga.service.name,
                        &saga.source_domain,
                        &saga.service.service_address,
                        saga.service.port,
                    )
                    .await;
                self.sagas
                    .write()
                    .await
                    .insert(saga.service.name.clone(), saga);
            }
        }

        // Sync PMs
        let pm_list = services
            .list(&ListParams::default().labels(&format!(
                "{}={}",
                COMPONENT_LABEL, COMPONENT_PROCESS_MANAGER
            )))
            .await?;
        for svc in pm_list {
            if let Some(pm) = self.extract_pm(&svc) {
                let subs: Vec<&str> = pm.subscriptions.iter().map(String::as_str).collect();
                self.inner
                    .register_pm(
                        &pm.service.name,
                        &subs,
                        &pm.service.service_address,
                        pm.service.port,
                    )
                    .await;
                self.pms.write().await.insert(pm.service.name.clone(), pm);
            }
        }

        let aggregates = self.aggregates.read().await;
        let projectors = self.projectors.read().await;
        let sagas = self.sagas.read().await;
        let pms = self.pms.read().await;

        info!(
            aggregates = aggregates.len(),
            projectors = projectors.len(),
            sagas = sagas.len(),
            pms = pms.len(),
            "Initial sync complete"
        );

        Ok(())
    }

    fn start_watching(&self) {
        if self.client.is_none() {
            return; // Static mode - no K8s watching
        }
        self.start_watching_component(
            COMPONENT_AGGREGATE,
            self.aggregates.clone(),
            self.aggregate_health.clone(),
        );
        self.start_watching_component(
            COMPONENT_PROJECTOR,
            self.projectors.clone(),
            self.projector_health.clone(),
        );
        self.start_watching_sagas(self.sagas.clone(), self.saga_health.clone());
        self.start_watching_pms(self.pms.clone(), self.pm_health.clone());
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
