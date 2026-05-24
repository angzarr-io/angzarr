//! Storage error types.

use uuid::Uuid;

/// Result type for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;

/// Error message constants for storage operations.
pub mod errmsg {
    pub const NOT_FOUND: &str = "Event not found: ";
    pub const SEQUENCE_CONFLICT: &str = "Sequence conflict: ";
    pub const INVALID_TIMESTAMP: &str = "Invalid timestamp: ";
    pub const INVALID_TIMESTAMP_FORMAT: &str = "Invalid timestamp format: ";
    pub const INVALID_DIVERGENCE_POINT: &str = "Invalid divergence point: ";
    pub const INVALID_UUID: &str = "Invalid UUID: ";
    pub const DATABASE_ERROR: &str = "Database error: ";
    pub const PROTOBUF_DECODE_ERROR: &str = "Protobuf decode error: ";
    pub const MISSING_COVER: &str = "Cover missing from EventBook";
    pub const MISSING_ROOT: &str = "Root UUID missing from Cover";
    pub const REDIS_ERROR: &str = "Redis error: ";
    pub const NOT_IMPLEMENTED: &str = "Not implemented: ";
    pub const BACKEND_ERROR: &str = "Backend error: ";
    pub const NATS_ERROR: &str = "NATS error: ";
    pub const UNKNOWN_TYPE: &str = "Unknown storage type: ";
    pub const MAIN_TIMELINE_PROTECTED: &str = "Main timeline is protected: ";
}

/// Errors that can occur during storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{}domain={domain}, root={root}", errmsg::NOT_FOUND)]
    NotFound { domain: String, root: Uuid },

    #[error("{}expected {expected}, got {actual}", errmsg::SEQUENCE_CONFLICT)]
    SequenceConflict { expected: u32, actual: u32 },

    #[error("{}seconds={seconds}, nanos={nanos}", errmsg::INVALID_TIMESTAMP)]
    InvalidTimestamp { seconds: i64, nanos: i32 },

    #[error("{}{}", errmsg::INVALID_TIMESTAMP_FORMAT, .0)]
    InvalidTimestampFormat(String),

    #[error("{}{}", errmsg::INVALID_DIVERGENCE_POINT, .0)]
    InvalidDivergencePoint(String),

    #[error("{}{}", errmsg::INVALID_UUID, .0)]
    InvalidUuid(#[from] uuid::Error),

    // SQLite is always compiled; postgres and immudb are optional
    #[error("{}{}", errmsg::DATABASE_ERROR, .0)]
    Database(#[from] sqlx::Error),

    #[error("{}{}", errmsg::PROTOBUF_DECODE_ERROR, .0)]
    ProtobufDecode(#[from] prost::DecodeError),

    #[error("{}", errmsg::MISSING_COVER)]
    MissingCover,

    #[error("{}", errmsg::MISSING_ROOT)]
    MissingRoot,

    #[cfg(feature = "redis")]
    #[error("{}{}", errmsg::REDIS_ERROR, .0)]
    Redis(#[from] ::redis::RedisError),

    #[error("{}{}", errmsg::NOT_IMPLEMENTED, .0)]
    NotImplemented(String),

    /// Backend-specific runtime error that isn't covered by a typed variant.
    ///
    /// Use for cloud-SDK errors (DynamoDB, Bigtable, ImmuDB) where the
    /// underlying error type doesn't implement `From` into a typed variant.
    /// Distinct from `NotImplemented` — the backend IS implemented and the
    /// trait method exists; the runtime call just failed.
    #[error("{}{}", errmsg::BACKEND_ERROR, .0)]
    Backend(String),

    #[cfg(feature = "nats")]
    #[error("{}{}", errmsg::NATS_ERROR, .0)]
    Nats(String),

    #[error("{}{}", errmsg::UNKNOWN_TYPE, .0)]
    UnknownType(String),

    /// Caller attempted a destructive operation against the main timeline.
    ///
    /// The main timeline (`""` or `"angzarr"` at the API layer; SQL NULL in
    /// the column) is append-only. `delete_edition_events` on a main-timeline
    /// sentinel raises this. Both client-side and stored-procedure guards
    /// enforce it for SQL backends (C-15).
    #[error("{}{}", errmsg::MAIN_TIMELINE_PROTECTED, .0)]
    MainTimelineProtected(String),
}
