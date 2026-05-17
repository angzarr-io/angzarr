//! `angzarr-status` — operations console backend (Phase 0 skeleton).
//!
//! Framework-level service exposing operator surfaces over gRPC:
//! DLQ admin, cluster health, event browsing, projection viewing,
//! command injection, PromQL proxy, and proto-descriptor registry.
//! Paired in-pod with an envoy sidecar that transcodes the gRPC
//! surface to REST/JSON for the SPA web console.
//!
//! See `/home/babbitt/.claude/plans/virtual-spinning-flute.md` for the
//! design contract.
//!
//! ## Phase 0 (current)
//!
//! Compiles and serves `grpc.health.v1.Health/Check`. Everything else
//! lands in later phases — no DB, no descriptor pulling, no handlers
//! yet. The bin's purpose at this phase is to validate the cross-
//! cutting infrastructure (Helm chart, Skaffold target, envoy sidecar,
//! frontend init-container, port wiring) before adding feature code.

pub mod descriptors;
pub mod handlers;
pub mod metrics;

/// Default gRPC bind port for the status binary.
///
/// 1390 is the framework-level service slot in CLAUDE.md's Port
/// Standards table — distinct from the 1300-aggregate domain range.
pub const DEFAULT_GRPC_PORT: u16 = 1390;

/// Default HTTP (REST + SPA) port served by the envoy sidecar.
///
/// The status binary itself does not bind 8080 — the sidecar does,
/// terminating HTTP and transcoding to gRPC on `localhost:1390`.
/// Pinned here so Helm templates and tests can reference one constant.
pub const DEFAULT_HTTP_PORT: u16 = 8080;
