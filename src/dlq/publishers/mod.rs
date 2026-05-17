//! DLQ publisher implementations.
//!
//! Each publisher handles a specific transport backend (AMQP, Kafka, etc.).

mod channel;
mod filesystem;
mod logging;
mod noop;
mod offload;

// Database DLQ always available (sqlite always compiled)
mod database;

// Read-side counterpart to `database` — counterpart `DeadLetterReader`
// impls for the same `dlq_entries` schema. See `database_reader.rs`.
pub mod database_reader;

// Replay-audit writers + migration runners (P1.4). Same backends as
// `database` / `database_reader`; same pool-ownership conventions.
pub mod audit_writer;

#[cfg(feature = "amqp")]
mod amqp;
#[cfg(feature = "kafka")]
mod kafka;
#[cfg(feature = "pubsub")]
mod pubsub;
#[cfg(feature = "sns-sqs")]
mod sns_sqs;

pub use channel::ChannelDeadLetterPublisher;
pub use filesystem::FilesystemDeadLetterPublisher;
pub use logging::LoggingDeadLetterPublisher;
pub use noop::NoopDeadLetterPublisher;
pub use offload::OffloadFilesystemDlqPublisher;

#[cfg(feature = "gcs")]
pub use offload::OffloadGcsDlqPublisher;
#[cfg(feature = "s3")]
pub use offload::OffloadS3DlqPublisher;

#[cfg(feature = "postgres")]
pub use database::PostgresDlqPublisher;
// SQLite is always compiled
pub use database::SqliteDlqPublisher;

#[cfg(feature = "postgres")]
pub use database_reader::PostgresDlqReader;
pub use database_reader::SqliteDlqReader;

#[cfg(feature = "postgres")]
pub use audit_writer::{run_postgres_migrations, PostgresReplayAuditWriter};
pub use audit_writer::{run_sqlite_migrations, SqliteReplayAuditWriter};

#[cfg(feature = "amqp")]
pub use amqp::AmqpDeadLetterPublisher;
#[cfg(feature = "kafka")]
pub use kafka::KafkaDeadLetterPublisher;
#[cfg(feature = "pubsub")]
pub use pubsub::PubSubDeadLetterPublisher;
#[cfg(feature = "sns-sqs")]
pub use sns_sqs::SnsSqsDeadLetterPublisher;
