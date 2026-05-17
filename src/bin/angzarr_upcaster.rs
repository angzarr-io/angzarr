//! angzarr-upcaster: Default no-op upcaster
//!
//! A passthrough upcaster that returns events unchanged. Used for testing,
//! configuration validation, and as a template for custom upcaster implementations.
//!
//! ## Architecture
//! ```text
//! [angzarr-aggregate] --(UpcasterService gRPC)--> [angzarr-upcaster]
//!                                                        |
//!                                                        v
//!                                                 events unchanged
//! ```
//!
//! ## Usage
//! Deploy this alongside client logic as a fallback when no event version
//! transformation is needed, or use it as a starting point for custom upcasters.
//!
//! ## Configuration
//! - transport.type: "tcp" or "uds"
//! - transport.tcp.port: Port for TCP (default: 50051)
//! - transport.uds.base_path: Base path for UDS sockets

use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tonic_health::server::health_reporter;
use tracing::{debug, info};

use angzarr::proto::upcaster_service_server::{UpcasterService, UpcasterServiceServer};
use angzarr::proto::{UpcastRequest, UpcastResponse};
use angzarr::proto_reflect;
use angzarr::transport::{grpc_trace_layer, serve_with_transport};
use angzarr::utils::bootstrap::startup;

/// No-op upcaster service that passes events through unchanged.
#[derive(Debug, Default)]
pub struct NoOpUpcaster;

#[tonic::async_trait]
impl UpcasterService for NoOpUpcaster {
    async fn upcast(
        &self,
        request: Request<UpcastRequest>,
    ) -> Result<Response<UpcastResponse>, Status> {
        let req = request.into_inner();

        debug!(
            domain = %req.domain,
            event_count = req.events.len(),
            "Upcaster passthrough (no transformation)"
        );

        // Return events unchanged
        Ok(Response::new(UpcastResponse { events: req.events }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = startup()?;

    let upcaster_service = NoOpUpcaster;

    // Health reporter
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<UpcasterServiceServer<NoOpUpcaster>>()
        .await;

    info!("angzarr-upcaster started (no-op passthrough mode)");

    let router = Server::builder()
        .layer(grpc_trace_layer())
        .add_service(health_service)
        .add_service(proto_reflect::reflection_service())
        .add_service(UpcasterServiceServer::new(upcaster_service));

    serve_with_transport(router, &config.transport, "upcaster", None).await?;

    Ok(())
}
