//! Tests for the projector coordinator service.
//!
//! Why this matters: the aggregate coordinator's HandleSync calls
//! HandleSync here for every synchronous projector and reports each by
//! name; a projection must come back named, and a failure must surface as
//! the projector's own status.

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

use super::*;

struct Fixed(Result<Projection, Status>, AtomicUsize);

#[async_trait]
impl ProjectorHandler for Fixed {
    async fn handle(&self, _events: &EventBook) -> Result<Projection, Status> {
        self.1.fetch_add(1, Ordering::SeqCst);
        self.0.clone()
    }
}

#[tokio::test]
async fn test_handle_sync_names_the_projection() {
    let coord = ProjectorCoord::new(
        "projector-order-view",
        Arc::new(Fixed(Ok(Projection::default()), AtomicUsize::new(0))),
    );
    let projection = coord
        .handle_sync(Request::new(EventBook::default()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(projection.projector, "projector-order-view");
}

#[tokio::test]
async fn test_handle_sync_keeps_a_named_projection() {
    let named = Projection {
        projector: "own-name".into(),
        ..Default::default()
    };
    let coord = ProjectorCoord::new("p", Arc::new(Fixed(Ok(named), AtomicUsize::new(0))));
    let projection = coord
        .handle_sync(Request::new(EventBook::default()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(projection.projector, "own-name");
}

#[tokio::test]
async fn test_handle_sync_surfaces_the_failure() {
    let coord = ProjectorCoord::new(
        "p",
        Arc::new(Fixed(
            Err(Status::failed_precondition("bad")),
            AtomicUsize::new(0),
        )),
    );
    let err = coord
        .handle_sync(Request::new(EventBook::default()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn test_handle_async_returns_before_projecting_and_projects() {
    let handler = Arc::new(Fixed(Ok(Projection::default()), AtomicUsize::new(0)));
    let coord = ProjectorCoord::new("p", handler.clone());
    coord
        .handle_async(Request::new(EventBook::default()))
        .await
        .unwrap();
    for _ in 0..50 {
        if handler.1.load(Ordering::SeqCst) == 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the asynchronous projection never ran");
}

#[tokio::test]
async fn test_unregistered_projector_is_unavailable() {
    let (_ready, readiness) = tokio::sync::watch::channel(false);
    let coord = ProjectorCoord::new(
        "p",
        Arc::new(Fixed(Ok(Projection::default()), AtomicUsize::new(0))),
    )
    .with_readiness(readiness);
    let err = coord
        .handle_sync(Request::new(EventBook::default()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable);
    let err = coord
        .handle_async(Request::new(EventBook::default()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable);
}
