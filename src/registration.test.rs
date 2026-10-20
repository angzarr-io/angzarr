//! Tests for pulling registrations.
//!
//! Why this matters (owner ruling 2026-10-19): the coordinator must deliver
//! nothing to a component it holds no descriptor for, and must report
//! ready only after a successful pull; a business process that disappears
//! must stop being served until it is described again.

use std::sync::Mutex;

use super::*;
use crate::alerts::RecordingAlerts;
use crate::test_utils::aggregate_descriptor;

struct Scripted(Mutex<Vec<Result<Vec<ComponentDescriptor>, Status>>>);

#[async_trait]
impl DescriptorSource for Scripted {
    async fn describe(&self) -> Result<Vec<ComponentDescriptor>, Status> {
        self.0.lock().unwrap().remove(0)
    }
}

fn selector() -> ComponentSelector {
    ComponentSelector {
        kind: ComponentKind::Aggregate,
        name: None,
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

#[test]
fn test_select_by_kind_and_name() {
    let all = vec![order("1.0.0"), projector("view-a"), projector("view-b")];
    assert_eq!(selector().select(&all).unwrap().name, "order");
    let named = ComponentSelector {
        kind: ComponentKind::Projector,
        name: Some("view-b".into()),
    };
    assert_eq!(named.select(&all).unwrap().name, "view-b");
    let ambiguous = ComponentSelector {
        kind: ComponentKind::Projector,
        name: None,
    };
    assert_eq!(
        ambiguous.select(&all).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let missing = ComponentSelector {
        kind: ComponentKind::Saga,
        name: None,
    };
    assert_eq!(
        missing.select(&all).unwrap_err().code(),
        tonic::Code::NotFound
    );
}

fn registrar(
    source: Arc<Scripted>,
    alerts: Arc<RecordingAlerts>,
    registry: Arc<ComponentRegistry>,
) -> Registrar {
    Registrar::new(
        source,
        selector(),
        registry,
        "order",
        "local",
        "order-business:50101",
        alerts,
        Duration::from_millis(0),
    )
}

#[tokio::test]
async fn test_registered_then_dropped_on_failure_then_reregistered() {
    let registry = Arc::new(ComponentRegistry::new());
    let source = Arc::new(Scripted(Mutex::new(vec![
        Ok(vec![order("1.4.2")]),
        Err(Status::unavailable("process restarting")),
        Ok(vec![order("1.5.0")]),
    ])));
    let r = registrar(source, Arc::new(RecordingAlerts::new()), registry.clone());
    let ready = r.readiness();
    assert!(!*ready.borrow());

    assert!(matches!(
        r.describe_once().await,
        DescribeOutcome::Registered
    ));
    assert!(*ready.borrow());
    assert_eq!(
        registry.get("order", "local").unwrap().rules_version(),
        "1.4.2"
    );

    assert!(matches!(
        r.describe_once().await,
        DescribeOutcome::Failed(_)
    ));
    assert!(!*ready.borrow());
    assert!(registry.get("order", "local").is_none());

    assert!(matches!(
        r.describe_once().await,
        DescribeOutcome::Registered
    ));
    assert_eq!(
        registry.get("order", "local").unwrap().rules_version(),
        "1.5.0"
    );
}

/// An answer with any unversioned descriptor is refused as a whole, with an
/// error naming the process and component, and an alert (C-0564).
#[tokio::test]
async fn test_unversioned_answer_is_refused_whole_and_alerted() {
    let registry = Arc::new(ComponentRegistry::new());
    let alerts = Arc::new(RecordingAlerts::new());
    let mut unversioned = projector("view");
    unversioned.rules_version = String::new();
    let source = Arc::new(Scripted(Mutex::new(vec![Ok(vec![
        order("1.4.2"),
        unversioned,
    ])])));
    let r = registrar(source, alerts.clone(), registry.clone());
    let outcome = r.describe_once().await;
    let DescribeOutcome::Refused(reason) = outcome else {
        panic!("expected a refusal");
    };
    assert!(
        reason.contains("order-business:50101") && reason.contains("'view'"),
        "{reason}"
    );
    assert!(registry.get("order", "local").is_none());
    assert!(!*r.readiness().borrow());
    assert_eq!(alerts.raised().len(), 1);
}

/// A failing call is retried, and alerted once past the grace period.
#[tokio::test]
async fn test_failed_calls_retry_and_alert_after_grace() {
    let registry = Arc::new(ComponentRegistry::new());
    let alerts = Arc::new(RecordingAlerts::new());
    let source = Arc::new(Scripted(Mutex::new(vec![
        Err(Status::unavailable("down")),
        Err(Status::unavailable("down")),
        Ok(vec![order("1.4.2")]),
    ])));
    let r = registrar(source, alerts.clone(), registry.clone());
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
    let source = Arc::new(Scripted(Mutex::new(vec![
        Ok(vec![order("1.4.2")]),
        Ok(vec![order("1.5.0")]),
    ])));
    let r = Arc::new(registrar(
        source,
        Arc::new(RecordingAlerts::new()),
        registry.clone(),
    ));
    let (connections, watched) = watch::channel(0u64);
    let mut ready = r.readiness();
    let task = tokio::spawn(r.clone().run(watched));
    ready.wait_for(|r| *r).await.unwrap();
    assert_eq!(
        registry.get("order", "local").unwrap().rules_version(),
        "1.4.2"
    );
    connections.send_modify(|n| *n += 1);
    for _ in 0..100 {
        if registry
            .get("order", "local")
            .map(|g| g.rules_version().to_string())
            == Some("1.5.0".into())
        {
            task.abort();
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("not re-described after reconnect");
}
