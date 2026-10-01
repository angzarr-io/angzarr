//! Replay publisher that re-submits a dead-lettered command to the
//! aggregate coordinator of the command's domain over gRPC.

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use super::error::DlqError;
use super::replay::{ReplayMetadata, ReplayMode, ReplayPublisher};
use crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use crate::proto::event_query_service_client::EventQueryServiceClient;
use crate::proto::page_header::SequenceType;
use crate::proto::{CommandBook, CommandRequest, Query, SyncMode};
use crate::proto_ext::correlated_request;
use crate::transport::{connect_to_address, GrpcMessageLimits};

/// Request metadata naming the DLQ row a replayed command came from.
pub const REPLAYED_FROM_DLQ_ID_HEADER: &str = "x-angzarr-replayed-from-dlq-id";
/// Request metadata carrying the dead letter's original correlation id.
pub const ORIGINAL_CORRELATION_ID_HEADER: &str = "x-angzarr-original-correlation-id";

/// Re-submits replayed commands to `{domain}`'s aggregate coordinator,
/// addressed through a static `domain -> address` map (the
/// `ANGZARR_STATIC_ENDPOINTS` the saga/PM sidecars use).
///
/// `FreshSequence` re-stamps explicit page sequences from the aggregate's
/// current `next_sequence` (read through its EventQueryService) before
/// submitting; `AsIs` submits the command unchanged.
pub struct GrpcReplayPublisher {
    endpoints: HashMap<String, String>,
    channels: Mutex<HashMap<String, Channel>>,
}

impl GrpcReplayPublisher {
    /// Publisher over `(domain, address)` endpoints.
    pub fn new(endpoints: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            endpoints: endpoints.into_iter().collect(),
            channels: Mutex::new(HashMap::new()),
        }
    }

    async fn channel(&self, domain: &str) -> Result<Channel, DlqError> {
        let address = self.endpoints.get(domain).ok_or_else(|| {
            DlqError::InvalidArgument(format!(
                "no aggregate endpoint for domain {:?}; add it to ANGZARR_STATIC_ENDPOINTS",
                domain
            ))
        })?;
        let mut channels = self.channels.lock().await;
        if let Some(channel) = channels.get(domain) {
            return Ok(channel.clone());
        }
        let channel = connect_to_address(address)
            .await
            .map_err(|e| DlqError::Connection(format!("{}: {}", address, e)))?;
        channels.insert(domain.to_string(), channel.clone());
        Ok(channel)
    }
}

/// Re-stamp explicit page sequences consecutively from `next_sequence`.
/// Deferred (saga/external) pages are stamped by the framework and left
/// as they are.
pub(crate) fn restamp_sequences(command: &mut CommandBook, next_sequence: u32) {
    let mut sequence = next_sequence;
    for page in &mut command.pages {
        if let Some(header) = page.header.as_mut() {
            if let Some(SequenceType::Sequence(seq)) = header.sequence_type.as_mut() {
                *seq = sequence;
                sequence = sequence.saturating_add(1);
            }
        }
    }
}

#[async_trait]
impl ReplayPublisher for GrpcReplayPublisher {
    async fn replay(
        &self,
        mut command: CommandBook,
        metadata: ReplayMetadata,
    ) -> Result<(), DlqError> {
        let cover = command
            .cover
            .clone()
            .ok_or_else(|| DlqError::InvalidDeadLetter("command has no cover".to_string()))?;
        let channel = self.channel(&cover.domain).await?;

        if metadata.mode == ReplayMode::FreshSequence {
            // Look the aggregate up by domain + root only: a correlation
            // id in the query would select by correlation instead.
            let query = Query {
                cover: Some(crate::proto::Cover {
                    correlation_id: String::new(),
                    ..cover.clone()
                }),
                selection: None,
            };
            let book = EventQueryServiceClient::new(channel.clone())
                .with_message_limits()
                .get_event_book(query)
                .await
                .map_err(|s| {
                    DlqError::PublishFailed(format!("reading current sequence: {}", s.message()))
                })?
                .into_inner();
            restamp_sequences(&mut command, book.next_sequence);
        }

        let mut request = correlated_request(
            CommandRequest {
                command: Some(command),
                sync_mode: SyncMode::Simple.into(),
                ..Default::default()
            },
            &cover.correlation_id,
        );
        let headers = request.metadata_mut();
        if let Ok(v) = metadata.replayed_from_dlq_id.to_string().parse() {
            headers.insert(REPLAYED_FROM_DLQ_ID_HEADER, v);
        }
        if let Ok(v) = metadata.original_correlation_id.parse() {
            headers.insert(ORIGINAL_CORRELATION_ID_HEADER, v);
        }

        CommandHandlerCoordinatorServiceClient::new(channel)
            .with_message_limits()
            .handle_command(request)
            .await
            .map_err(|s| {
                DlqError::PublishFailed(format!(
                    "aggregate rejected replay ({:?}): {}",
                    s.code(),
                    s.message()
                ))
            })?;
        Ok(())
    }

    fn source_id(&self) -> &'static str {
        "grpc-aggregate"
    }
}

#[cfg(test)]
#[path = "replay_grpc.test.rs"]
mod tests;
