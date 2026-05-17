//! DLQ error types.

/// Error message constants for DLQ operations.
pub mod errmsg {
    pub const NOT_CONFIGURED: &str = "DLQ not configured";
    pub const SERIALIZATION_FAILED: &str = "Failed to serialize message: ";
    pub const PUBLISH_FAILED: &str = "Failed to publish to DLQ: ";
    pub const CONNECTION_ERROR: &str = "Connection error: ";
    pub const INVALID_DEAD_LETTER: &str = "Invalid dead letter: ";
    pub const UNKNOWN_TYPE: &str = "Unknown DLQ backend type: ";
    pub const QUERY_FAILED: &str = "DLQ query failed: ";
    pub const INVALID_ARGUMENT: &str = "Invalid DLQ query argument: ";
    pub const CONFLICT: &str = "Conflict: ";
}

/// Errors that can occur during DLQ operations.
#[derive(Debug, thiserror::Error)]
pub enum DlqError {
    #[error("{}", errmsg::NOT_CONFIGURED)]
    NotConfigured,

    #[error("{}{}", errmsg::SERIALIZATION_FAILED, .0)]
    Serialization(String),

    #[error("{}{}", errmsg::PUBLISH_FAILED, .0)]
    PublishFailed(String),

    #[error("{}{}", errmsg::CONNECTION_ERROR, .0)]
    Connection(String),

    #[error("{}{}", errmsg::INVALID_DEAD_LETTER, .0)]
    InvalidDeadLetter(String),

    #[error("{}{}", errmsg::UNKNOWN_TYPE, .0)]
    UnknownType(String),

    #[error("{}{}", errmsg::QUERY_FAILED, .0)]
    QueryFailed(String),

    #[error("{}{}", errmsg::INVALID_ARGUMENT, .0)]
    InvalidArgument(String),

    /// A two-phase-replay conflict: a pending audit row already exists
    /// for the same `idempotency_key`. Surfaced when a second replica
    /// or a double-clicking operator attempts to replay the same DLQ
    /// entry while the first attempt is still in flight (or already
    /// committed under the same key).
    #[error("{}{}", errmsg::CONFLICT, .0)]
    Conflict(String),
}

/// Result type for DLQ operations.
pub type Result<T> = std::result::Result<T, DlqError>;
