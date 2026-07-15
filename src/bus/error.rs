//! Bus error types.

use tonic::Status;

/// Result type for bus operations.
pub type Result<T> = std::result::Result<T, BusError>;

/// Error message constants for bus operations.
pub mod errmsg {
    pub const CONNECTION_FAILED: &str = "Connection failed: ";
    pub const PUBLISH_FAILED: &str = "Publish failed: ";
    pub const SUBSCRIBE_FAILED: &str = "Subscribe failed: ";
    pub const PROJECTOR_FAILED: &str = "Projector failed";
    pub const SAGA_FAILED: &str = "Saga failed";
    pub const GRPC_ERROR: &str = "gRPC error: ";
    pub const SUBSCRIBE_NOT_SUPPORTED: &str = "Subscribe not supported for this bus type";
    pub const UNKNOWN_TYPE: &str = "Unknown messaging type: ";
    pub const MISSING_TYPE: &str =
        "No messaging transport configured; set messaging.type to one of: amqp, kafka, pubsub, sns-sqs";
}

/// Errors that can occur during bus operations.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("{}{}", errmsg::CONNECTION_FAILED, .0)]
    Connection(String),

    #[error("{}{}", errmsg::PUBLISH_FAILED, .0)]
    Publish(String),

    #[error("{}{}", errmsg::SUBSCRIBE_FAILED, .0)]
    Subscribe(String),

    #[error("{} '{name}': {message}", errmsg::PROJECTOR_FAILED)]
    ProjectorFailed { name: String, message: String },

    #[error("{} '{name}': {message}", errmsg::SAGA_FAILED)]
    SagaFailed { name: String, message: String },

    #[error("{}{}", errmsg::GRPC_ERROR, .0)]
    Grpc(#[from] Status),

    #[error("{}", errmsg::SUBSCRIBE_NOT_SUPPORTED)]
    SubscribeNotSupported,

    #[error("{}{}", errmsg::UNKNOWN_TYPE, .0)]
    UnknownType(String),

    /// `messaging.type` was left unset (C14: there is no in-process
    /// default transport to fall back to). Distinct from `UnknownType` so
    /// operators immediately see "you forgot to configure this" rather
    /// than a message naming a phantom `"channel"` backend.
    #[error("{}", errmsg::MISSING_TYPE)]
    MissingType,
}
