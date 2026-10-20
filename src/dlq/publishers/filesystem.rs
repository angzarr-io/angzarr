//! Filesystem-based DLQ publisher.
//!
//! Writes dead letters to files in a configured directory.
//! Useful for development, debugging, and local persistence.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use prost::Message;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tracing::info;

use super::super::config::FilesystemDlqConfig;
use super::super::error::DlqError;
use super::super::factory::DlqBackend;
use super::super::{AngzarrDeadLetter, DeadLetterPublisher};

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    DlqBackend {
        try_create: |config| {
            let dlq_type = config.dlq_type.clone();
            let fs_config = config.filesystem.clone();
            Box::pin(async move {
                if dlq_type != "filesystem" {
                    return None;
                }
                let fs_config = fs_config.unwrap_or_default();
                match FilesystemDeadLetterPublisher::new(&fs_config).await {
                    Ok(publisher) => Some(Ok(Arc::new(publisher) as Arc<dyn DeadLetterPublisher>)),
                    Err(e) => Some(Err(e)),
                }
            })
        },
    }
}

/// Filesystem-based DLQ publisher.
///
/// Writes dead letters to files in a configured directory.
/// File naming: `{timestamp}_{domain}_{correlation_id}.{format}`
pub struct FilesystemDeadLetterPublisher {
    path: PathBuf,
    format: String,
}

impl FilesystemDeadLetterPublisher {
    /// Create a new filesystem DLQ publisher.
    pub async fn new(config: &FilesystemDlqConfig) -> Result<Self, DlqError> {
        let path = PathBuf::from(&config.path);

        // Ensure directory exists
        fs::create_dir_all(&path).await.map_err(|e| {
            DlqError::Connection(format!("Failed to create DLQ directory {:?}: {}", path, e))
        })?;

        info!(path = %config.path, format = %config.format, "Filesystem DLQ publisher initialized");

        Ok(Self {
            path,
            format: config.format.clone(),
        })
    }

    /// Generate filename for a dead letter.
    fn generate_filename(&self, dead_letter: &AngzarrDeadLetter) -> String {
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%3f");
        let domain = dead_letter.domain().unwrap_or("unknown");
        // UUID text is already filename-safe.
        let correlation_id = dead_letter
            .correlation_id()
            .unwrap_or_else(|| "no-correlation".to_string());

        let ext = match self.format.as_str() {
            "protobuf" | "proto" => "pb",
            _ => "json",
        };

        format!(
            "{}_{}_{}_{}.{}",
            timestamp,
            domain,
            correlation_id,
            uuid::Uuid::new_v4().as_simple(),
            ext
        )
    }
}

#[async_trait]
impl DeadLetterPublisher for FilesystemDeadLetterPublisher {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        let filename = self.generate_filename(&dead_letter);
        let file_path = self.path.join(&filename);

        let data = match self.format.as_str() {
            "protobuf" | "proto" => {
                let proto = dead_letter.to_proto();
                proto.encode_to_vec()
            }
            _ => {
                // JSON format - serialize as human-readable JSON
                // Proto messages don't have serde derives, so we create a JSON manually
                let proto_bytes = dead_letter.to_proto().encode_to_vec();
                let payload_base64 = base64::engine::general_purpose::STANDARD.encode(&proto_bytes);

                let json = serde_json::json!({
                    "domain": dead_letter.domain().unwrap_or("unknown"),
                    "correlation_id": dead_letter.correlation_id(),
                    "rejection_reason": dead_letter.rejection_reason,
                    "reason_type": dead_letter.reason_type(),
                    "source_component": dead_letter.source_component,
                    "source_component_kind": dead_letter.source_component_kind.as_str_name(),
                    "metadata": dead_letter.metadata,
                    "occurred_at": dead_letter.occurred_at.as_ref().map(|ts| {
                        chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                            .map(|dt| dt.to_rfc3339())
                    }),
                    // Include proto bytes as base64 for recovery
                    "payload_base64": payload_base64,
                });
                serde_json::to_vec_pretty(&json).map_err(|e| {
                    DlqError::Serialization(format!("Failed to serialize to JSON: {}", e))
                })?
            }
        };

        let mut file = File::create(&file_path).await.map_err(|e| {
            DlqError::PublishFailed(format!("Failed to create file {:?}: {}", file_path, e))
        })?;

        file.write_all(&data).await.map_err(|e| {
            DlqError::PublishFailed(format!("Failed to write to file {:?}: {}", file_path, e))
        })?;

        file.flush().await.map_err(|e| {
            DlqError::PublishFailed(format!("Failed to flush file {:?}: {}", file_path, e))
        })?;

        info!(
            path = %file_path.display(),
            domain = %dead_letter.domain().unwrap_or("unknown"),
            reason = %dead_letter.rejection_reason,
            "Dead letter written to filesystem"
        );

        #[cfg(feature = "otel")]
        {
            use crate::advice::metrics::{
                backend_attr, domain_attr, reason_type_attr, DLQ_PUBLISH_TOTAL,
            };
            DLQ_PUBLISH_TOTAL.add(
                1,
                &[
                    domain_attr(dead_letter.domain().unwrap_or("unknown")),
                    reason_type_attr(dead_letter.reason_type()),
                    backend_attr("filesystem"),
                ],
            );
        }

        Ok(())
    }

    fn is_configured(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[path = "filesystem.test.rs"]
mod tests;
