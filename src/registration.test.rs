//! Tests for pulling the registration.
//!
//! Why this matters (decisions 14.10, 14.11; one coordinator serves one
//! component): the coordinator delivers nothing until it holds its
//! component's descriptor, reports ready only after a successful pull,
//! refuses a descriptor without a version or of another kind (alerted, not
//! asked again until the channel comes back up), and re-describes on
//! reconnect.

use std::sync::Mutex;

use super::*;
use crate::alerts::RecordingAlerts;
use crate::test_utils::aggregate_descriptor;

struct Scripted(Mutex<Vec<Result<ComponentDescriptor, Status>>>);

#[async_trait]
impl DescriptorSource for Scripted {
    async fn describe(&self) -> Result<ComponentDescriptor, Status> {
        self.0.lock().unwrap().remove(0)
    }
}

fn order(version: &str) -> ComponentDescriptor {
    aggregate_descriptor("order", "/orders.v1.OrderState", version, false)
}

fn projector(name: &str) -> ComponentDescriptor {
    ComponentDescriptor {
        name: name.into(),
        kind: ComponentKind::Projector as i32,
        rules_version: "1.0.0".into(),
        ..Default::default()
    }
}

fn unavailable() -> Status {
    Status::unavailable("process restarting")
}

fn registrar(
    answers: Vec<Result<ComponentDescriptor, Status>>,
    alerts: Arc<RecordingAlerts>,
    registry: Arc<ComponentRegistry>,
) -> Registrar {
    Registrar::new(
        Arc::new(Scripted(Mutex::new(answers))),
        ComponentKind::Aggregate,
        registry,
        "order",
        "order-business:50101",
        alerts,
        Duration::from_millis(0),
    )
}

#[tokio::test]
async fn test_registered_then_dropped_on_failure_then_reregistered() {
    let registry = Arc::new(ComponentRegistry::new());
    let r = registrar(
        vec![Ok(order("1.4.2")), Err(unavailable()), Ok(order("1.5.0"))],
        Arc::new(RecordingAlerts::new()),
        registry.clone(),
    );
    let ready = r.readiness();
    assert!(!*ready.borrow());

    assert!(matches!(
        r.describe_once().await,
        DescribeOutcome::Registered
    ));
    assert!(*ready.borrow());
    assert_eq!(registry.get().unwrap().rules_version(), "1.4.2");

    assert!(matches!(
        r.describe_once().await,
        DescribeOutcome::Failed(_)
    ));
    assert!(!*ready.borrow());
    assert!(registry.get().is_none());

    assert!(matches!(
        r.describe_once().await,
        DescribeOutcome::Registered
    ));
    assert_eq!(registry.get().unwrap().rules_version(), "1.5.0");
}

/// A versionless descriptor is refused, with an error naming the process
/// and component, and an alert (C-0564).
#[tokio::test]
async fn test_versionless_descriptor_is_refused_and_alerted() {
    let registry = Arc::new(ComponentRegistry::new());
    let alerts = Arc::new(RecordingAlerts::new());
    let r = registrar(vec![Ok(order(""))], alerts.clone(), registry.clone());
    let DescribeOutcome::Refused(reason) = r.describe_once().await else {
        panic!("expected a refusal");
    };
    assert!(
        reason.contains("order-business:50101") && reason.contains("'order'"),
        "{reason}"
    );
    assert!(registry.get().is_none());
    assert!(!*r.readiness().borrow());
    assert_eq!(alerts.raised().len(), 1);
}

/// A descriptor of another kind than the coordinator's is a refused
/// registration: nothing held, not ready, alerted.
#[tokio::test]
async fn test_descriptor_of_another_kind_is_refused_and_alerted() {
    let registry = Arc::new(ComponentRegistry::new());
    let alerts = Arc::new(RecordingAlerts::new());
    let r = registrar(
        vec![Ok(projector("view"))],
        alerts.clone(),
        registry.clone(),
    );
    let DescribeOutcome::Refused(reason) = r.describe_once().await else {
        panic!("expected a refusal");
    };
    assert!(
        reason.contains("COMPONENT_KIND_PROJECTOR") && reason.contains("COMPONENT_KIND_AGGREGATE"),
        "{reason}"
    );
    assert!(registry.get().is_none());
    assert!(!*r.readiness().borrow());
    assert_eq!(alerts.raised().len(), 1);
}

/// A failing call is retried, and alerted once past the grace period.
#[tokio::test]
async fn test_failed_calls_retry_and_alert_after_grace() {
    let registry = Arc::new(ComponentRegistry::new());
    let alerts = Arc::new(RecordingAlerts::new());
    let r = registrar(
        vec![Err(unavailable()), Err(unavailable()), Ok(order("1.4.2"))],
        alerts.clone(),
        registry,
    );
    r.describe_until_answered().await;
    assert!(*r.readiness().borrow());
    assert_eq!(
        alerts.raised().len(),
        1,
        "one alert past a zero grace period"
    );
}

/// A new connection triggers a re-describe; until it answers nothing is
/// held (C-0563).
#[tokio::test]
async fn test_reconnect_redescribes() {
    let registry = Arc::new(ComponentRegistry::new());
    let r = Arc::new(registrar(
        vec![Ok(order("1.4.2")), Ok(order("1.5.0"))],
        Arc::new(RecordingAlerts::new()),
        registry.clone(),
    ));
    let (connections, watched) = watch::channel(0u64);
    let mut ready = r.readiness();
    let task = tokio::spawn(r.clone().run(watched));
    ready.wait_for(|r| *r).await.unwrap();
    assert_eq!(registry.get().unwrap().rules_version(), "1.4.2");
    connections.send_modify(|n| *n += 1);
    for _ in 0..100 {
        if registry.get().map(|g| g.rules_version().to_string()) == Some("1.5.0".into()) {
            task.abort();
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("not re-described after reconnect");
}

/// A coordinator configured with a component name refuses a descriptor of
/// another name, like a kind mismatch: nothing held, not ready, alerted.
#[tokio::test]
async fn test_descriptor_of_another_name_is_refused_and_alerted() {
    let registry = Arc::new(ComponentRegistry::new());
    let alerts = Arc::new(RecordingAlerts::new());
    let r = Registrar::new(
        Arc::new(Scripted(Mutex::new(vec![
            Ok(projector("projector-other")),
            Ok(projector("projector-table-view")),
        ]))),
        ComponentKind::Projector,
        registry.clone(),
        "projector-table-view",
        "view-process:50201",
        alerts.clone(),
        Duration::from_millis(0),
    )
    .with_expected_name("projector-table-view");
    let DescribeOutcome::Refused(reason) = r.describe_once().await else {
        panic!("expected a refusal");
    };
    assert!(
        reason.contains("'projector-other'") && reason.contains("'projector-table-view'"),
        "{reason}"
    );
    assert!(registry.get().is_none());
    assert!(!*r.readiness().borrow());
    assert_eq!(alerts.raised().len(), 1);
    assert!(matches!(
        r.describe_once().await,
        DescribeOutcome::Registered
    ));
}

/// A descriptor whose state does not support a configured conflict
/// resolution is a refused registration (types.proto: configuration naming
/// an unsupported resolution is refused when the descriptor arrives).
#[tokio::test]
async fn test_descriptor_failing_the_configuration_check_is_refused() {
    let registry = Arc::new(ComponentRegistry::new());
    let alerts = Arc::new(RecordingAlerts::new());
    let policy = crate::conflict_resolution::ConflictPolicy::from_config(
        &serde_yaml::from_str("default: COMMUTATIVE\n").unwrap(),
    )
    .unwrap();
    let r = registrar(vec![Ok(order("1.4.2"))], alerts.clone(), registry.clone())
        .with_descriptor_check(Arc::new(move |d| policy.check_supported(d)));
    let DescribeOutcome::Refused(reason) = r.describe_once().await else {
        panic!("expected a refusal");
    };
    assert!(reason.contains("COMMUTATIVE"), "{reason}");
    assert!(registry.get().is_none());
    assert_eq!(alerts.raised().len(), 1);
}
