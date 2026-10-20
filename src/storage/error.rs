//! Storage error types.

pub type Result<T> = std::result::Result<T, StorageError>;

/// Error message constants for storage operations.
pub mod errmsg {
    pub const SEQUENCE_CONFLICT: &str = "Sequence conflict: ";
    pub const INVALID_TIMESTAMP: &str = "Invalid timestamp: ";
    pub const INVALID_UUID: &str = "Invalid UUID: ";
    pub const DATABASE_ERROR: &str = "Database error: ";
    pub const PROTOBUF_DECODE_ERROR: &str = "Protobuf decode error: ";
    pub const NOT_IMPLEMENTED: &str = "Not implemented: ";
    pub const UNKNOWN_TYPE: &str = "Unknown storage type: ";
}

/// Errors from storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The append's first sequence was not the stream's next sequence:
    /// another write committed first.
    #[error("{}expected {expected}, got {actual}", errmsg::SEQUENCE_CONFLICT)]
    SequenceConflict { expected: u64, actual: u64 },

    #[error("{}seconds={seconds}, nanos={nanos}", errmsg::INVALID_TIMESTAMP)]
    InvalidTimestamp { seconds: i64, nanos: i32 },

    #[error("{}{}", errmsg::INVALID_UUID, .0)]
    InvalidUuid(#[from] uuid::Error),

    #[error("{}{}", errmsg::DATABASE_ERROR, .0)]
    Database(#[from] sqlx::Error),

    #[error("{}{}", errmsg::PROTOBUF_DECODE_ERROR, .0)]
    ProtobufDecode(#[from] prost::DecodeError),

    #[error("{}{}", errmsg::NOT_IMPLEMENTED, .0)]
    NotImplemented(String),

    #[error("{}{}", errmsg::UNKNOWN_TYPE, .0)]
    UnknownType(String),
}
