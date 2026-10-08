//! Durable logs behind an [`Outbox`](super::Outbox).
//!
//! The log is append-only: a record, failed attempts, and a close. An entry
//! is open until its close is appended.

use std::sync::Arc;

use async_trait::async_trait;
use prost::{Message, Name};

use crate::proto::{event_page, page_header::SequenceType, CommandBook, EventPage, PageHeader};
use crate::proto_ext::type_url;
use crate::storage::{AddMeta, EventStore, StorageError};

use super::{OutboxEntry, OutboxError};

/// Append-only persistence of outbox obligations.
#[async_trait]
pub trait OutboxLog: Send + Sync {
    /// Persist a new obligation. Recording an existing key is a no-op.
    async fn append_record(&self, entry: &OutboxEntry) -> Result<(), OutboxError>;
    /// Persist a failed delivery attempt.
    async fn append_attempt(&self, key: &str, error: &str) -> Result<(), OutboxError>;
    /// Persist that the obligation is settled (delivered or dead-lettered).
    async fn append_close(&self, key: &str) -> Result<(), OutboxError>;
    /// Every obligation recorded and not closed, with its attempts so far.
    async fn open_entries(&self) -> Result<Vec<OutboxEntry>, OutboxError>;
}

/// A log that keeps nothing: obligations live only in the outbox's working
/// set and do not survive a restart.
#[derive(Default)]
pub struct MemoryOutboxLog;

#[async_trait]
impl OutboxLog for MemoryOutboxLog {
    async fn append_record(&self, _entry: &OutboxEntry) -> Result<(), OutboxError> {
        Ok(())
    }
    async fn append_attempt(&self, _key: &str, _error: &str) -> Result<(), OutboxError> {
        Ok(())
    }
    async fn append_close(&self, _key: &str) -> Result<(), OutboxError> {
        Ok(())
    }
    #[crate::trivial_delegation]
    async fn open_entries(&self) -> Result<Vec<OutboxEntry>, OutboxError> {
        Ok(Vec::new())
    }
}

/// Type URL of an attempt page: the error text as a StringValue.
const ATTEMPT_TYPE_URL: &str = "/google.protobuf.StringValue";
/// Type URL of the close page.
const CLOSE_TYPE_URL: &str = "/google.protobuf.Empty";

/// An outbox log kept in a coordinator's event store.
///
/// Each obligation is its own stream in the framework-owned domain
/// `_angzarr_outbox.{name}` (never a business stream), rooted at a UUIDv5 of
/// the entry key on the main timeline: page 0 records the book (a
/// CommandBook), each failed attempt appends its error, and a final empty
/// page closes it. Closed streams stay as the delivery history.
pub struct EventStoreOutboxLog {
    store: Arc<dyn EventStore>,
    domain: String,
}

impl EventStoreOutboxLog {
    /// A log for the outbox named `name` in `store`.
    pub fn new(store: Arc<dyn EventStore>, name: &str) -> Self {
        Self {
            store,
            domain: format!("_angzarr_outbox.{name}"),
        }
    }

    /// The framework-owned domain holding this outbox's streams.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    fn root(key: &str) -> uuid::Uuid {
        uuid::Uuid::new_v5(
            &crate::orchestration::correlation::ANGZARR_UUID_NAMESPACE,
            key.as_bytes(),
        )
    }

    fn page(sequence: u32, type_url: &str, value: Vec<u8>) -> EventPage {
        EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(sequence)),
            }),
            created_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
            payload: Some(event_page::Payload::Event(prost_types::Any {
                type_url: type_url.to_string(),
                value,
            })),
        }
    }

    async fn append(&self, key: &str, type_url: &str, value: Vec<u8>) -> Result<(), OutboxError> {
        let root = Self::root(key);
        // A concurrent writer (another replica draining the same entry) can
        // take the next sequence; retry once at the new head.
        for _ in 0..2 {
            let next = self
                .store
                .get_next_sequence(&self.domain, "", root)
                .await
                .map_err(log_error)?;
            match self
                .store
                .add(
                    &self.domain,
                    "",
                    root,
                    vec![Self::page(next, type_url, value.clone())],
                    &AddMeta::default(),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(StorageError::SequenceConflict { .. }) => continue,
                Err(e) => return Err(log_error(e)),
            }
        }
        Err(OutboxError::Log(format!(
            "concurrent writes kept moving outbox stream {key}"
        )))
    }

    fn entry_from_pages(pages: &[EventPage]) -> Option<OutboxEntry> {
        let mut entry: Option<OutboxEntry> = None;
        for page in pages {
            let Some(event_page::Payload::Event(any)) = page.payload.as_ref() else {
                continue;
            };
            match type_url::fqn(&any.type_url) {
                name if name == CommandBook::full_name() => {
                    let book = CommandBook::decode(any.value.as_slice()).ok()?;
                    entry = Some(OutboxEntry::from_recorded(book));
                }
                "google.protobuf.StringValue" => {
                    if let Some(entry) = entry.as_mut() {
                        entry.attempts += 1;
                        entry.last_error = String::decode(any.value.as_slice()).unwrap_or_default();
                    }
                }
                "google.protobuf.Empty" => return None,
                _ => {}
            }
        }
        entry
    }
}

fn log_error(e: StorageError) -> OutboxError {
    OutboxError::Log(e.to_string())
}

#[async_trait]
impl OutboxLog for EventStoreOutboxLog {
    async fn append_record(&self, entry: &OutboxEntry) -> Result<(), OutboxError> {
        let record = Self::page(0, type_url::COMMAND_BOOK, entry.book.encode_to_vec());
        match self
            .store
            .add(
                &self.domain,
                "",
                Self::root(&entry.key),
                vec![record],
                &AddMeta::default(),
            )
            .await
        {
            // An existing record is the same obligation.
            Ok(_) | Err(StorageError::SequenceConflict { .. }) => Ok(()),
            Err(e) => Err(log_error(e)),
        }
    }

    async fn append_attempt(&self, key: &str, error: &str) -> Result<(), OutboxError> {
        self.append(key, ATTEMPT_TYPE_URL, error.to_string().encode_to_vec())
            .await
    }

    async fn append_close(&self, key: &str) -> Result<(), OutboxError> {
        self.append(key, CLOSE_TYPE_URL, Vec::new()).await
    }

    async fn open_entries(&self) -> Result<Vec<OutboxEntry>, OutboxError> {
        let roots = self
            .store
            .list_roots(&self.domain, "")
            .await
            .map_err(log_error)?;
        let mut open = Vec::new();
        for root in roots {
            let pages = self
                .store
                .get(&self.domain, "", root)
                .await
                .map_err(log_error)?;
            if let Some(entry) = Self::entry_from_pages(&pages) {
                open.push(entry);
            }
        }
        Ok(open)
    }
}

#[cfg(test)]
#[path = "log.test.rs"]
mod tests;
