//! `angzarr-status` — operations console backend.
//!
//! Phase 0 skeleton: tonic server with `grpc.health.v1.Health/Check`
//! only. No DLQ admin, no descriptor pulling, no handlers — those land
//! in later phases per `plans/virtual-spinning-flute.md`. Bringing this
//! up validates the cross-cutting infrastructure (Helm chart, Skaffold
//! target, envoy sidecar, frontend init-container, port wiring).
//!
//! ## Multi-instance / HA
//!
//! Designed to run as multiple replicas behind a Kubernetes Service.
//! Per the plan's HA contract, instances are stateless w.r.t. each
//! other — no inter-pod coordination — so the LB can route any request
//! to any pod without session affinity. Pod-level state (descriptor
//! pool, health cache) is rebuilt independently per pod on startup
//! and converges within seconds.
//!
//! ## Architecture
//! ```text
//! [Browser] --REST--> [envoy sidecar] --gRPC--> [angzarr-status]
//!                          (transcoder)               (this bin)
//! [grpcurl] --gRPC--------- direct ----------------> [angzarr-status]
//! ```

use std::sync::Arc;

use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_health::ServingStatus;
use tracing::info;

use angzarr::dlq::NoopDeadLetterReader;
use angzarr::proto::status::dlq_admin_service_server::DlqAdminServiceServer;
use angzarr::proto_reflect;
use angzarr::status::descriptors;
use angzarr::status::handlers::dlq::DlqAdminHandler;
use angzarr::transport::{grpc_trace_layer, serve_with_transport};
use angzarr::utils::bootstrap::startup;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = startup()?;

    // Initialize the descriptor pool: framework descriptors plus any
    // operator-mounted `.protoset` files under
    // `ANGZARR_STATUS_DESCRIPTORS_DIR` (typically a Helm-managed
    // ConfigMap at /etc/angzarr/descriptors/). Tolerant per the
    // resilience contract: any failure here logs but doesn't kill the
    // binary — the DLQ admin handler falls back to raw bytes when the
    // pool can't decode a payload.
    let extras = match descriptors::descriptors_dir_from_env() {
        Some(dir) => {
            let files = descriptors::load_protoset_files(&dir);
            tracing::info!(
                path = %dir.display(),
                count = files.len(),
                "loaded pre-staged descriptor files"
            );
            files
        }
        None => {
            tracing::debug!(
                "no ANGZARR_STATUS_DESCRIPTORS_DIR mount; using framework descriptors only"
            );
            Vec::new()
        }
    };
    if let Err(e) = proto_reflect::init_from_embedded_with_extras(&extras) {
        tracing::warn!(
            error = %e,
            "descriptor pool init failed — payload_view will be empty until P3 reflection-pull lands"
        );
    }

    // Health reporter — Phase 0 reports SERVING at the **overall server**
    // level (empty service name, gRPC health-protocol convention). The
    // plan's HA contract is explicit: liveness ≠ aggregate health.
    // ClusterHealthService (Phase 2) will roll up downstream sidecars.
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_service_status("", ServingStatus::Serving)
        .await;

    // Phase 1.1: wire DLQ admin with a Noop reader. Real DB-backed
    // readers ship in P1.2; the Noop path exists to validate the
    // gRPC surface + envelope shape end-to-end via grpcurl today.
    // Operators see a `state.degraded` ProblemDetails per the plan's
    // tolerance contract until a backend is configured.
    let dlq_handler = DlqAdminHandler::new(Arc::new(NoopDeadLetterReader));

    info!(
        "angzarr-status started (DLQ admin: Noop reader — \
         configure a DB-backed publisher to surface real entries)"
    );

    let router = Server::builder()
        .layer(grpc_trace_layer())
        .add_service(health_service)
        .add_service(proto_reflect::reflection_service())
        .add_service(DlqAdminServiceServer::new(dlq_handler));

    // `None` qualifier: framework-level service, not per-domain. UDS
    // socket path resolves to `{base}/status.sock`, TCP binds to
    // `config.transport.tcp.port` (Helm chart pins to 1390 per
    // `status::DEFAULT_GRPC_PORT`).
    serve_with_transport(router, &config.transport, "status", None).await?;

    Ok(())
}
