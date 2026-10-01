//! `angzarr-status` — operations console backend.
//!
//! Serves `DlqAdminService` (list / get / delete / replay dead letters),
//! gRPC health and the public reflection subset.
//!
//! - Dead letters are read from the `dlq.audit` store; replays are fenced
//!   and recorded in the same store's `dlq_replay_audit` table.
//! - Replays are re-submitted to the aggregate of the command's domain,
//!   addressed through `ANGZARR_STATIC_ENDPOINTS` (`domain=address,...`);
//!   without it, replay reports "not configured".
//! - `dlq.retention_days` deletes older dead letters once a day.
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

use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_health::ServingStatus;
use tracing::{error, info, warn};

use std::sync::Arc;

use angzarr::config::STATIC_ENDPOINTS_ENV_VAR;
use angzarr::dlq::{
    init_dlq_reader, init_replay_audit_writer, DeadLetterReader, GrpcReplayPublisher,
    NoopReplayPublisher, ReplayPublisher,
};
use angzarr::proto::status::dlq_admin_service_server::DlqAdminServiceServer;
use angzarr::proto_reflect;
use angzarr::status::descriptors;
use angzarr::status::handlers::dlq::DlqAdminHandler;
use angzarr::transport::{grpc_trace_layer, serve_with_transport};
use angzarr::utils::bootstrap::{parse_static_endpoints, startup};

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

    // R2-15 step 8: wire the DLQ admin reader from `dlq.audit` config.
    // - audit unset -> noop reader + WARN (UI returns zero entries; the
    //   admin gRPC surface still works for shape verification).
    // - audit set + unreachable -> hard-fail at boot, mirrors the
    //   publisher-side contract from steps 3-4 so operators get a loud
    //   failure rather than a silent always-empty list.
    let dlq_reader = init_dlq_reader(config.dlq.audit.as_ref())
        .await
        .map_err(|e| {
            error!("DLQ audit reader init failed (boot abort): {}", e);
            e
        })?;
    if config.dlq.audit.is_none() {
        warn!(
            "dlq.audit is unset; status admin DLQ listing will be empty. \
             Set dlq.audit.storage_type + connection in config.yaml to \
             surface published dead letters."
        );
    } else {
        info!(
            storage_type = %config.dlq.audit.as_ref().map(|a| a.storage_type.as_str()).unwrap_or(""),
            "DLQ audit reader initialized"
        );
    }
    let audit_writer = init_replay_audit_writer(config.dlq.audit.as_ref())
        .await
        .map_err(|e| {
            error!("DLQ replay-audit writer init failed (boot abort): {}", e);
            e
        })?;
    let replay: Arc<dyn ReplayPublisher> = match std::env::var(STATIC_ENDPOINTS_ENV_VAR) {
        Ok(endpoints) if !endpoints.trim().is_empty() => {
            info!("DLQ replay re-submits commands to aggregates from {STATIC_ENDPOINTS_ENV_VAR}");
            Arc::new(GrpcReplayPublisher::new(parse_static_endpoints(&endpoints)))
        }
        _ => {
            warn!(
                "{STATIC_ENDPOINTS_ENV_VAR} is unset; DLQ replay is disabled \
                 (ReplayDeadLetter reports not-configured)"
            );
            Arc::new(NoopReplayPublisher)
        }
    };
    if let Some(days) = config.dlq.retention_days {
        spawn_dlq_retention(dlq_reader.clone(), days);
    }
    let dlq_handler = DlqAdminHandler::new_with_audit(dlq_reader, replay, audit_writer);

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

/// Delete dead letters older than `days` from the audit store, at start
/// and then once a day.
fn spawn_dlq_retention(reader: Arc<dyn DeadLetterReader>, days: u32) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(24 * 3600));
        loop {
            ticker.tick().await;
            let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(days));
            match reader.delete_older_than(cutoff).await {
                Ok(0) => {}
                Ok(removed) => info!(
                    removed,
                    retention_days = days,
                    "expired dead letters deleted"
                ),
                Err(e) => warn!(error = %e, "DLQ retention pass failed"),
            }
        }
    });
}
