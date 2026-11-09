//! `grpc.health.v1.Health`: SERVING once bound, NOT_SERVING as shutdown
//! begins.

mod support;

use angzarr_coordinator_grpc::{Listener, Server};
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

#[tokio::test]
async fn health_is_serving_once_bound_and_not_serving_after_shutdown() {
    let running = Server::builder()
        .listen(Listener::tcp("127.0.0.1:0".parse().unwrap()))
        .bind()
        .await
        .unwrap();
    let addr = match &running.bound().endpoints()[0] {
        angzarr_coordinator_grpc::BoundEndpoint::Tcp { addr, .. } => *addr,
        other => panic!("{other:?}"),
    };
    let mut client = HealthClient::new(support::tcp(addr).await);
    let overall = HealthCheckRequest {
        service: String::new(),
    };

    // Once bound: serving, on the Check and as the first message of a Watch.
    let status = client
        .check(overall.clone())
        .await
        .unwrap()
        .into_inner()
        .status;
    assert_eq!(status, ServingStatus::Serving as i32);
    let mut watch = client.watch(overall.clone()).await.unwrap().into_inner();
    let first = watch.message().await.unwrap().unwrap().status;
    assert_eq!(first, ServingStatus::Serving as i32);

    // As shutdown begins, the open Watch is told before the server goes.
    let stopping = tokio::spawn(running.shutdown());
    let next = watch.message().await.unwrap().unwrap().status;
    assert_eq!(next, ServingStatus::NotServing as i32);
    // Graceful: the open call keeps the shutdown waiting until it ends.
    assert!(!stopping.is_finished());
    drop(watch);
    drop(client);
    stopping.await.unwrap().expect("shuts down");
}
