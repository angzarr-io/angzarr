//! Constants used across the proto extensions.

/// gRPC metadata key for correlation ID propagation.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

/// Fallback domain when cover is missing or has no domain set.
pub const UNKNOWN_DOMAIN: &str = "unknown";

/// Domain prefix for synthetic projection event books.
///
/// Projector output is published as `_projection.{projector_name}.{domain}`.
pub const PROJECTION_DOMAIN_PREFIX: &str = "_projection";

/// Protobuf type URL for serialized Projection messages in synthetic event books.
///
/// Canonical bare form (`/` + FQN); produced and matched by the
/// projector path, so an exact comparison is sufficient.
pub const PROJECTION_TYPE_URL: &str = "/io.angzarr.v1.Projection";

/// Wildcard domain for catch-all routing (matches any domain).
pub const WILDCARD_DOMAIN: &str = "*";

/// The meta domain for angzarr infrastructure.
pub const META_ANGZARR_DOMAIN: &str = "_angzarr";

/// Type URL prefix for googleapis.com protobuf Any messages.
///
/// Used by `decode_typed` to match type URLs in Event/Command payloads.
pub const TYPE_URL_PREFIX: &str = "type.googleapis.com/";
