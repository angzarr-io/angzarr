//! DLQ publisher implementations.
//!
//! Each publisher handles one backend. Backends not yet ported to the latest
//! proto (kafka, pubsub, sns-sqs, database, the offload stores) are absent:
//! a configured target of such a type is refused at boot as unknown.

mod channel;
mod filesystem;
mod logging;
mod noop;

#[cfg(feature = "amqp")]
mod amqp;

pub use channel::ChannelDeadLetterPublisher;
pub use filesystem::FilesystemDeadLetterPublisher;
pub use logging::LoggingDeadLetterPublisher;
pub use noop::NoopDeadLetterPublisher;

#[cfg(feature = "amqp")]
pub use amqp::AmqpDeadLetterPublisher;
