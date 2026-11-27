//! `grpc.health.v1.Health`: SERVING once bound, NOT_SERVING as shutdown
//! begins.

mod support;

use angzarr_coordinator_grpc::{HealthStatus, Listener, Server};
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

// The composition knows whether its service is really working, which the
// listener cannot: it sets the status through the handle while the server
// runs, and a Check (and an open Watch) hears each change.
#[tokio::test]
async fn the_composition_flips_health_while_the_server_runs() {
    let running = Server::builder()
        .listen(Listener::tcp("127.0.0.1:0".parse().unwrap()))
        .bind()
        .await
        .unwrap();
    let addr = match &running.bound().endpoints()[0] {
        angzarr_coordinator_grpc::BoundEndpoint::Tcp { addr, .. } => *addr,
        other => panic!("{other:?}"),
    };
    let health = running.health();
    let mut client = HealthClient::new(support::tcp(addr).await);
    let overall = HealthCheckRequest {
        service: String::new(),
    };
    let check = |mut client: HealthClient<_>, service: &str| {
        let request = HealthCheckRequest {
            service: service.to_string(),
        };
        async move { client.check(request).await.map(|r| r.into_inner().status) }
    };
    let mut watch = client.watch(overall).await.unwrap().into_inner();
    assert_eq!(
        watch.message().await.unwrap().unwrap().status,
        ServingStatus::Serving as i32
    );

    health.set("", HealthStatus::NotServing).await;
    assert_eq!(
        check(client.clone(), "").await.unwrap(),
        ServingStatus::NotServing as i32
    );
    assert_eq!(
        watch.message().await.unwrap().unwrap().status,
        ServingStatus::NotServing as i32
    );

    health.set("", HealthStatus::Serving).await;
    assert_eq!(
        check(client.clone(), "").await.unwrap(),
        ServingStatus::Serving as i32
    );

    // A served name has a status of its own; one never set is unknown.
    assert!(check(client.clone(), "ctxloom.v1.Ctxloom").await.is_err());
    health
        .set("ctxloom.v1.Ctxloom", HealthStatus::NotServing)
        .await;
    assert_eq!(
        check(client.clone(), "ctxloom.v1.Ctxloom").await.unwrap(),
        ServingStatus::NotServing as i32
    );
    assert_eq!(
        check(client.clone(), "").await.unwrap(),
        ServingStatus::Serving as i32,
        "the overall status is its own"
    );

    drop(watch);
    drop(client);
    running.shutdown().await.expect("shuts down");
}
