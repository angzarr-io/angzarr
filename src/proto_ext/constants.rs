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

/// Default edition name for the main timeline.
///
/// The canonical timeline is named "angzarr". Empty edition names are treated
/// as equivalent to this value.
pub const DEFAULT_EDITION: &str = "angzarr";
