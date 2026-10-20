//! Tests for the projector bus consumer.
//!
//! Why this matters: a terminal projector failure must reach the DLQ and be
//! acknowledged (redelivery would fail the same way), a retryable one must
//! be redelivered, and a dead letter that cannot be written must not be
//! acknowledged, or the event is lost.

use std::sync::Mutex;

use async_trait::async_trait;
use tonic::Status;

use super::*;
use crate::dlq::{ChannelDeadLetterPublisher, DlqError};
use crate::proto::{Cover, Projection};

struct Scripted {
    result: Result<Projection, Status>,
    seen: Mutex<Vec<String>>,
}

impl Scripted {
    fn new(result: Result<Projection, Status>) -> Arc<Self> {
        Arc::new(Self {
            result,
            seen: Mutex::new(Vec::new()),
        })
    }
    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait]
impl ProjectorHandler for Scripted {
    async fn handle(&self, events: &EventBook) -> Result<Projection, Status> {
        self.seen.lock().unwrap().push(events.domain().to_string());
        self.result.clone()
    }
}

struct FailingDlq;

#[async_trait]
impl DeadLetterPublisher for FailingDlq {
    async fn publish(&self, _: AngzarrDeadLetter) -> Result<(), DlqError> {
        Err(DlqError::PublishFailed("down".into()))
    }
}

fn book(domain: &str) -> Arc<EventBook> {
    Arc::new(EventBook {
        cover: Some(Cover {
            domain: domain.into(),
            root: Some(uuid::Uuid::from_u128(1).into()),
            edition: None,
        }),
        ..Default::default()
    })
}

#[tokio::test]
async fn test_projects_every_non_infrastructure_domain_by_default() {
    let projector = Scripted::new(Ok(Projection::default()));
    let handler = ProjectorEventHandler::from_handler(projector.clone(), "p".into());
    handler.handle(book("order")).await.unwrap();
    handler.handle(book("_angzarr")).await.unwrap();
    assert_eq!(projector.seen(), ["order"]);
}

#[tokio::test]
async fn test_domain_filter_keeps_only_listed_domains() {
    let projector = Scripted::new(Ok(Projection::default()));
    let handler = ProjectorEventHandler::from_handler(projector.clone(), "p".into())
        .with_domains(vec!["table".into()]);
    handler.handle(book("order")).await.unwrap();
    handler.handle(book("table")).await.unwrap();
    assert_eq!(projector.seen(), ["table"]);
}

#[tokio::test]
async fn test_terminal_failure_is_dead_lettered_and_acked() {
    let projector = Scripted::new(Err(Status::failed_precondition("bad event")));
    let (dlq, mut received) = ChannelDeadLetterPublisher::new();
    let handler = ProjectorEventHandler::from_handler(projector, "p".into())
        .with_dlq_publisher(Arc::new(dlq));
    handler.handle(book("order")).await.unwrap();
    let dead_letter = received.recv().await.unwrap();
    assert_eq!(dead_letter.source_component, "p");
    assert_eq!(dead_letter.source_component_kind, ComponentKind::Projector);
}

#[tokio::test]
async fn test_retryable_failure_is_redelivered() {
    let projector = Scripted::new(Err(Status::unavailable("projection db down")));
    let (dlq, mut received) = ChannelDeadLetterPublisher::new();
    let handler = ProjectorEventHandler::from_handler(projector, "p".into())
        .with_dlq_publisher(Arc::new(dlq));
    assert!(handler.handle(book("order")).await.is_err());
    assert!(received.try_recv().is_err());
}

#[tokio::test]
async fn test_failed_dead_letter_is_not_acked() {
    let projector = Scripted::new(Err(Status::failed_precondition("bad event")));
    let handler = ProjectorEventHandler::from_handler(projector, "p".into())
        .with_dlq_publisher(Arc::new(FailingDlq));
    assert!(handler.handle(book("order")).await.is_err());
}

#[tokio::test]
async fn test_deliveries_wait_until_registered() {
    let projector = Scripted::new(Ok(Projection::default()));
    let (ready, readiness) = tokio::sync::watch::channel(false);
    let handler = ProjectorEventHandler::from_handler(projector.clone(), "p".into())
        .with_readiness(readiness);
    let pending = tokio::spawn(handler.handle(book("order")));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(projector.seen().is_empty(), "delivered before registration");
    assert!(!pending.is_finished());
    ready.send_replace(true);
    pending.await.unwrap().unwrap();
    assert_eq!(projector.seen(), ["order"]);
}
