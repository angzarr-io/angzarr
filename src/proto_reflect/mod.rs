//! Protobuf reflection utilities.
//!
//! Provides runtime introspection of Any-packed protobuf messages
//! using a shared DescriptorPool. Used for:
//! - State diff (commutative merge detection)
//! - Logging projectors (human-readable event/state logging)
//! - Debug tooling (inspect Any-packed messages)
//! - **Operator-facing payload rendering** ([`decode_to_json`],
//!   [`decode_any_to_json`], [`dynamic_to_json`]) — single reusable
//!   primitive for any consumer that needs to surface decoded
//!   payloads to humans. Used by:
//!     - DLQ admin (`StoredDeadLetter.payload_view`, P1.2.5)
//!     - Event-store browsing (planned, P3 / GraphQL gateway)
//!     - Future user-domain command / projection viewers
//!
//!   Tolerance contract: every JSON decoder returns an empty string
//!   on failure (pool not initialized, type unknown, bad bytes,
//!   serialization error). Callers fall back to raw bytes — no panic,
//!   no failed RPC.
//!
//! Descriptor advertisement to peers uses the **standard**
//! `grpc.reflection.v1.ServerReflection` service, wired via
//! [`reflection_service`]. See `plans/virtual-spinning-flute.md` P8.

use prost_reflect::ReflectMessage;
use prost_types::Any;
use std::collections::HashSet;
use std::sync::OnceLock;

/// Global descriptor pool, initialized at startup.
static DESCRIPTOR_POOL: OnceLock<prost_reflect::DescriptorPool> = OnceLock::new();

/// Error message constants for proto reflection operations.
pub mod errmsg {
    pub const NOT_INITIALIZED: &str = "Descriptor pool not initialized";
    pub const ALREADY_INITIALIZED: &str = "Descriptor pool already initialized";
    pub const INVALID_TYPE_URL: &str = "Invalid type URL: ";
    pub const UNKNOWN_TYPE: &str = "Unknown message type: ";
    pub const DECODE_ERROR: &str = "Decode error: ";
    pub const POOL_DECODE_ERROR: &str = "Pool decode error: ";
}

/// Errors that can occur during proto reflection.
#[derive(Debug, thiserror::Error)]
pub enum ReflectError {
    #[error("{}", errmsg::NOT_INITIALIZED)]
    NotInitialized,

    #[error("{}", errmsg::ALREADY_INITIALIZED)]
    AlreadyInitialized,

    #[error("{}{}", errmsg::INVALID_TYPE_URL, .0)]
    InvalidTypeUrl(String),

    #[error("{}{}", errmsg::UNKNOWN_TYPE, .0)]
    UnknownType(String),

    #[error("{}{}", errmsg::DECODE_ERROR, .0)]
    Decode(#[from] prost::DecodeError),

    #[error("{}{}", errmsg::POOL_DECODE_ERROR, .0)]
    PoolDecode(String),
}

/// Initialize the descriptor pool from file descriptor set bytes.
///
/// Call once at coordinator/runtime startup.
///
/// # Errors
///
/// Returns error if pool is already initialized or if bytes are invalid.
pub fn init_pool(fds_bytes: &[u8]) -> Result<(), ReflectError> {
    let pool = prost_reflect::DescriptorPool::decode(fds_bytes)
        .map_err(|e| ReflectError::PoolDecode(e.to_string()))?;
    DESCRIPTOR_POOL
        .set(pool)
        .map_err(|_| ReflectError::AlreadyInitialized)
}

/// Get the global descriptor pool.
///
/// # Errors
///
/// Returns error if pool is not initialized.
pub fn pool() -> Result<&'static prost_reflect::DescriptorPool, ReflectError> {
    DESCRIPTOR_POOL.get().ok_or(ReflectError::NotInitialized)
}

/// Check if the descriptor pool is initialized.
pub fn is_initialized() -> bool {
    DESCRIPTOR_POOL.get().is_some()
}

/// Embedded descriptor set from build time.
///
/// Contains all proto message definitions compiled into the binary.
/// Use `init_from_embedded()` to initialize the pool with this data.
pub const EMBEDDED_DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.bin"));

/// Initialize the descriptor pool from the embedded descriptor set.
///
/// This is the recommended way to initialize the pool at startup.
/// Call this once during application initialization.
///
/// # Example
///
/// ```ignore
/// if let Err(e) = angzarr::proto_reflect::init_from_embedded() {
///     tracing::warn!(error = %e, "Failed to initialize proto reflection");
/// }
/// ```
///
/// # Errors
///
/// Returns error if pool is already initialized or if descriptor is invalid.
pub fn init_from_embedded() -> Result<(), ReflectError> {
    init_pool(EMBEDDED_DESCRIPTOR)
}

/// Initialize the pool from the embedded descriptor set, idempotently.
///
/// Calls [`init_from_embedded`] but treats the
/// already-initialized case as success. Suitable as the very first
/// thing a binary does after `startup()` when payload decoding is
/// needed (DLQ admin handler, GraphQL gateway, etc.).
pub fn ensure_initialized() -> Result<(), ReflectError> {
    if is_initialized() {
        return Ok(());
    }
    match init_from_embedded() {
        Ok(()) => Ok(()),
        // Already-init race: someone else won the OnceLock between
        // our check and our set. Still a success outcome.
        Err(ReflectError::AlreadyInitialized) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Decode proto-encoded bytes of a fully-qualified message type to a
/// JSON string using the global [`DescriptorPool`].
///
/// Returns the empty string when:
///   - the pool isn't initialized,
///   - the requested type isn't in the pool,
///   - the bytes don't decode against that descriptor,
///   - JSON serialization fails.
///
/// This is the tolerance contract from the plan's resilience section:
/// payload rendering must NEVER fail an entire row — callers fall
/// back to the raw bytes when decode returns empty. Nested
/// `google.protobuf.Any` payloads are auto-decoded when their
/// `type_url` is in the pool (prost-reflect follows the canonical
/// proto3 JSON encoding for Any).
///
/// `full_type_name` is the dotted protobuf type name, e.g.
/// `"angzarr_client.proto.angzarr.AngzarrDeadLetter"`.
pub fn decode_to_json(full_type_name: &str, encoded: &[u8]) -> String {
    let Ok(pool) = pool() else {
        return String::new();
    };
    let Some(descriptor) = pool.get_message_by_name(full_type_name) else {
        return String::new();
    };
    let Ok(msg) = prost_reflect::DynamicMessage::decode(descriptor, encoded) else {
        return String::new();
    };
    dynamic_to_json(&msg)
}

/// Serialize an already-decoded [`prost_reflect::DynamicMessage`] to
/// JSON using the canonical proto3 JSON encoding. Returns the empty
/// string on serialization failure.
///
/// Reusable primitive: callers who already hold a `DynamicMessage`
/// (e.g. obtained from [`decode_any`]) use this directly without
/// going through the bytes → decode round trip.
pub fn dynamic_to_json(msg: &prost_reflect::DynamicMessage) -> String {
    use serde::Serialize as _;
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    let mut serializer = serde_json::Serializer::new(&mut buf);
    if msg.serialize(&mut serializer).is_err() {
        return String::new();
    }
    String::from_utf8(buf).unwrap_or_default()
}

/// Decode a [`prost_types::Any`] to a JSON string via the global
/// pool.
///
/// The convenience entry point for the most common operator-rendering
/// case: event-store payloads (`EventPage.event`), command payloads
/// (`CommandPage.command`), projection state, etc. — every framework
/// payload site uses `Any`, and operators want JSON.
///
/// Equivalent to `decode_to_json(type_name_from_url(&any.type_url), &any.value)`
/// but cheaper to call: no string allocation for the type name.
/// Same tolerance contract as the other decoders: empty string when
/// the type isn't in the pool or decoding fails.
pub fn decode_any_to_json(any: &Any) -> String {
    let Ok(msg) = decode_any(any) else {
        return String::new();
    };
    dynamic_to_json(&msg)
}

/// Build a `tonic-reflection` v1 service over the framework's embedded
/// `FileDescriptorSet`.
///
/// Adds `grpc.reflection.v1.ServerReflection` to a tonic server so
/// `grpcurl` and the envoy `grpc_json_transcoder` filter's reflection
/// path work without shipping descriptor files separately.
///
/// Returns the configured server. Caller chains it into
/// `Server::builder().add_service(reflection_service())`.
///
/// # Panics
///
/// Panics if the embedded descriptor bytes are malformed — that would
/// indicate a broken `build.rs`, which should fail compilation long
/// before a `cargo run`.
///
/// # Example
///
/// ```rust,ignore
/// let router = Server::builder()
///     .layer(grpc_trace_layer())
///     .add_service(health_service)
///     .add_service(proto_reflect::reflection_service());
/// ```
pub fn reflection_service() -> tonic_reflection::server::v1::ServerReflectionServer<
    impl tonic_reflection::server::v1::ServerReflection,
> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(EMBEDDED_DESCRIPTOR)
        // Standard cross-cutting services every angzarr bin exposes.
        // Their descriptors live in their own crates; we splice them
        // into the reflection registry so `grpcurl :PORT list` shows
        // everything that can be called on the server, not just
        // framework protos. Without this, callers that resolve via
        // reflection ("grpcurl ... grpc.health.v1.Health/Check") get
        // "target server does not expose service" even though the
        // service is wired up — they couldn't *find* the proto.
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("framework + health descriptor sets are well-formed")
}

/// Extract type name from Any.type_url.
///
/// "type.googleapis.com/examples.PlayerState" -> "examples.PlayerState"
fn extract_type_name(type_url: &str) -> Result<&str, ReflectError> {
    type_url
        .rsplit('/')
        .next()
        .ok_or_else(|| ReflectError::InvalidTypeUrl(type_url.to_string()))
}

/// Decode an Any into a DynamicMessage for reflection.
///
/// # Errors
///
/// Returns error if pool is not initialized, type is unknown, or decode fails.
pub fn decode_any(any: &Any) -> Result<prost_reflect::DynamicMessage, ReflectError> {
    let pool = pool()?;
    let type_name = extract_type_name(&any.type_url)?;
    let descriptor = pool
        .get_message_by_name(type_name)
        .ok_or_else(|| ReflectError::UnknownType(type_name.to_string()))?;
    Ok(prost_reflect::DynamicMessage::decode(
        descriptor,
        &any.value[..],
    )?)
}

/// Compare two Any-packed messages, return changed field paths.
///
/// Handles scalar fields and map fields with key-based paths.
/// Returns field paths like "bankroll", "seats[1]", "table_reservations[table_a]".
///
/// # Errors
///
/// Returns error if either message cannot be decoded.
pub fn diff_fields(before: &Any, after: &Any) -> Result<HashSet<String>, ReflectError> {
    let before_msg = decode_any(before)?;
    let after_msg = decode_any(after)?;
    Ok(diff_dynamic_messages(&before_msg, &after_msg))
}

/// Compare two DynamicMessages and return changed field paths.
fn diff_dynamic_messages(
    before: &prost_reflect::DynamicMessage,
    after: &prost_reflect::DynamicMessage,
) -> HashSet<String> {
    let mut changed = HashSet::new();

    for field in before.descriptor().fields() {
        let before_val = before.get_field(&field);
        let after_val = after.get_field(&field);

        if field.is_map() {
            // Map field: check each key individually
            diff_map_field(&field, &before_val, &after_val, &mut changed);
        } else if before_val != after_val {
            changed.insert(field.name().to_string());
        }
    }

    // Note: We assume both messages are the same type.
    // If types differ, the descriptor iteration above handles all fields.

    changed
}

/// Diff a map field, adding "field[key]" paths for changed entries.
fn diff_map_field(
    field: &prost_reflect::FieldDescriptor,
    before: &prost_reflect::Value,
    after: &prost_reflect::Value,
    changed: &mut HashSet<String>,
) {
    use prost_reflect::Value;

    let before_map = match before {
        Value::Map(m) => m.clone(),
        _ => return,
    };
    let after_map = match after {
        Value::Map(m) => m.clone(),
        _ => return,
    };

    // Keys in after but not before, or with different values
    for (key, after_val) in after_map.iter() {
        match before_map.get(key) {
            Some(before_val) if before_val == after_val => {}
            _ => {
                // Changed or added
                let key_str = format_map_key(key);
                changed.insert(format!("{}[{}]", field.name(), key_str));
            }
        }
    }

    // Keys removed (in before but not after)
    for key in before_map.keys() {
        if !after_map.contains_key(key) {
            let key_str = format_map_key(key);
            changed.insert(format!("{}[{}]", field.name(), key_str));
        }
    }
}

/// Format a map key for display in field paths.
fn format_map_key(key: &prost_reflect::MapKey) -> String {
    use prost_reflect::MapKey;
    match key {
        MapKey::Bool(b) => b.to_string(),
        MapKey::I32(n) => n.to_string(),
        MapKey::I64(n) => n.to_string(),
        MapKey::U32(n) => n.to_string(),
        MapKey::U64(n) => n.to_string(),
        MapKey::String(s) => s.clone(),
    }
}

/// Check if two sets of changed fields are disjoint (no overlap).
///
/// Used for commutative merge detection.
pub fn fields_are_disjoint(a: &HashSet<String>, b: &HashSet<String>) -> bool {
    a.is_disjoint(b)
}

/// Format a DynamicMessage as human-readable string (for logging).
pub fn format_message(msg: &prost_reflect::DynamicMessage) -> String {
    format!("{:?}", msg)
}

/// Format an Any as human-readable string.
///
/// # Errors
///
/// Returns error if message cannot be decoded.
pub fn format_any(any: &Any) -> Result<String, ReflectError> {
    let msg = decode_any(any)?;
    Ok(format_message(&msg))
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
