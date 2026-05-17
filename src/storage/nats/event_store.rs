//! NATS JetStream EventStore implementation.

use async_nats::jetstream::{
    self,
    consumer::pull::Config as ConsumerConfig,
    stream::{Config as StreamConfig, RetentionPolicy, StorageType},
    Context,
};
use async_trait::async_trait;
use futures::StreamExt;
use prost::Message;
use tracing::debug;
use uuid::Uuid;

use crate::orchestration::aggregate::DEFAULT_EDITION;
use crate::proto::{Cover, Edition, EventBook, EventPage, Uuid as ProtoUuid};
use crate::proto_ext::EventPageExt;
use crate::storage::helpers::is_main_timeline;
use crate::storage::{
    AddOutcome, CascadeParticipant, EventStore, Result, SourceInfo, StorageError,
};

use super::DEFAULT_PREFIX;

/// Header name for angzarr sequence number.
///
/// # Why Custom Sequence Headers?
///
/// JetStream assigns global sequence numbers to all messages in a stream,
/// but event sourcing requires per-aggregate sequences (aggregate A has seq 0,1,2
/// while aggregate B has seq 0,1,2 — completely independent). We store the
/// angzarr sequence in a header so we can query "what's the max sequence for
/// aggregate X" without loading and decoding all message payloads.
const HEADER_SEQUENCE: &str = "Angzarr-Sequence";

/// Header name for correlation ID.
const HEADER_CORRELATION: &str = "Angzarr-Correlation";

/// Header name for cascade ID (for cascade stream).
const HEADER_CASCADE_ID: &str = "Angzarr-Cascade-Id";

/// Header name for committed status (for cascade stream).
const HEADER_COMMITTED: &str = "Angzarr-Committed";

/// Header name for created_at timestamp (for cascade stream).
const HEADER_CREATED_AT: &str = "Angzarr-Created-At";

/// Header name for domain (for cascade stream participant identification).
const HEADER_DOMAIN: &str = "Angzarr-Domain";

/// Header name for edition (for cascade stream participant identification).
const HEADER_EDITION: &str = "Angzarr-Edition";

/// Header name for root UUID (for cascade stream participant identification).
const HEADER_ROOT: &str = "Angzarr-Root";

/// Header name for fact-injection idempotency claim (C-18).
const HEADER_EXTERNAL_ID: &str = "Angzarr-External-Id";

/// Header names for saga-source provenance (C-18).
const HEADER_SOURCE_EDITION: &str = "Angzarr-Source-Edition";
const HEADER_SOURCE_DOMAIN: &str = "Angzarr-Source-Domain";
const HEADER_SOURCE_ROOT: &str = "Angzarr-Source-Root";
const HEADER_SOURCE_SEQ: &str = "Angzarr-Source-Seq";

/// Cascade stream suffix.
const CASCADE_STREAM_SUFFIX: &str = "CASCADE";

/// Default query timeout in milliseconds.
const DEFAULT_QUERY_TIMEOUT_MS: u64 = 100;

/// EventStore backed by NATS JetStream streams.
///
/// Events are stored in per-domain streams with subjects:
/// `{prefix}.events.{domain}.{root}.{edition}`
///
/// # Why One Stream Per Domain (Not Per Aggregate)?
///
/// Per-aggregate streams would explode the stream count (millions of orders =
/// millions of streams). Per-domain streams keep count manageable while still
/// allowing efficient per-aggregate queries via subject filtering.
///
/// # Why Store EventBooks (Not Individual Events)?
///
/// Consistency with EventBus — when events are published to the bus, they travel
/// as EventBooks. Storing the same format in the EventStore means:
/// 1. No format translation between storage and bus
/// 2. Batch publishing is natural (multiple events in one message)
/// 3. Headers like correlation_id are preserved end-to-end
pub struct NatsEventStore {
    jetstream: Context,
    prefix: String,
    /// Timeout for consuming messages from streams.
    query_timeout: std::time::Duration,
}

impl NatsEventStore {
    /// Create a new NATS EventStore.
    ///
    /// # Arguments
    /// * `client` - Connected NATS client
    /// * `prefix` - Optional subject prefix (defaults to "angzarr")
    pub async fn new(
        client: async_nats::Client,
        prefix: Option<&str>,
    ) -> std::result::Result<Self, async_nats::Error> {
        let jetstream = jetstream::new(client);
        Ok(Self {
            jetstream,
            prefix: prefix.unwrap_or(DEFAULT_PREFIX).to_string(),
            query_timeout: std::time::Duration::from_millis(DEFAULT_QUERY_TIMEOUT_MS),
        })
    }

    /// Set custom query timeout for consuming messages from streams.
    pub fn with_query_timeout(mut self, timeout_ms: u64) -> Self {
        self.query_timeout = std::time::Duration::from_millis(timeout_ms);
        self
    }

    /// Get the stream name for a domain.
    fn stream_name(&self, domain: &str) -> String {
        format!("{}_{}", self.prefix.to_uppercase(), domain.to_uppercase())
    }

    /// Get the subject for an aggregate.
    fn subject(&self, domain: &str, root: Uuid, edition: &str) -> String {
        format!(
            "{}.events.{}.{}.{}",
            self.prefix,
            domain,
            root.as_hyphenated(),
            edition
        )
    }

    /// Build deduplication message ID for batch (EventBook format).
    ///
    /// Format: `{domain}.{root}.{edition}.{first_seq}-{last_seq}`
    ///
    /// # Why This Format?
    ///
    /// JetStream deduplication uses `Nats-Msg-Id` to reject duplicate publishes.
    /// The ID must be deterministic (same input → same ID) so retried publishes
    /// resolve to the same message. We include:
    /// - `domain.root.edition`: Identifies the aggregate (namespace)
    /// - `first_seq-last_seq`: Identifies the exact event range being published
    ///
    /// This means if a publisher crashes and retries, the second publish with the
    /// same events gets deduplicated — no duplicate events in storage.
    fn msg_id_batch(
        domain: &str,
        root: Uuid,
        edition: &str,
        first_seq: u32,
        last_seq: u32,
    ) -> String {
        format!(
            "{}.{}.{}.{}-{}",
            domain,
            root.as_hyphenated(),
            edition,
            first_seq,
            last_seq
        )
    }

    /// Ensure the stream exists for a domain.
    async fn ensure_stream(&self, domain: &str) -> Result<()> {
        let stream_name = self.stream_name(domain);
        let subjects = format!("{}.events.{}.>", self.prefix, domain);

        match self.jetstream.get_stream(&stream_name).await {
            Ok(_) => Ok(()),
            Err(_) => {
                self.jetstream
                    .create_stream(StreamConfig {
                        name: stream_name,
                        subjects: vec![subjects],
                        retention: RetentionPolicy::Limits,
                        storage: StorageType::File,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| StorageError::Nats(format!("Failed to create stream: {}", e)))?;
                Ok(())
            }
        }
    }

    /// Get the cascade stream name.
    ///
    /// All cascade tracking events go to a single stream for efficient cross-domain queries.
    fn cascade_stream_name(&self) -> String {
        format!("{}_{}", self.prefix.to_uppercase(), CASCADE_STREAM_SUFFIX)
    }

    /// Get the subject for a cascade event.
    ///
    /// Subject format: `{prefix}.cascade.{cascade_id}.{domain}.{root}.{edition}`
    ///
    /// This allows filtering by cascade_id prefix to find all participants.
    fn cascade_subject(&self, cascade_id: &str, domain: &str, root: Uuid, edition: &str) -> String {
        format!(
            "{}.cascade.{}.{}.{}.{}",
            self.prefix,
            cascade_id,
            domain,
            root.as_hyphenated(),
            edition
        )
    }

    /// Ensure the cascade stream exists.
    ///
    /// The cascade stream captures all events with cascade_id for 2PC queries.
    async fn ensure_cascade_stream(&self) -> Result<()> {
        let stream_name = self.cascade_stream_name();
        let subjects = format!("{}.cascade.>", self.prefix);

        match self.jetstream.get_stream(&stream_name).await {
            Ok(_) => Ok(()),
            Err(_) => {
                self.jetstream
                    .create_stream(StreamConfig {
                        name: stream_name,
                        subjects: vec![subjects],
                        retention: RetentionPolicy::Limits,
                        storage: StorageType::File,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| {
                        StorageError::Nats(format!("Failed to create cascade stream: {}", e))
                    })?;
                Ok(())
            }
        }
    }

    /// Get the current max sequence for an aggregate.
    /// Decodes the last EventBook and finds the max sequence from its pages.
    async fn get_max_sequence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> Result<Option<u32>> {
        let stream_name = self.stream_name(domain);
        let subject = self.subject(domain, root, edition);

        let stream = match self.jetstream.get_stream(&stream_name).await {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        // Get last message for this specific subject
        match stream.get_last_raw_message_by_subject(&subject).await {
            Ok(msg) => {
                // Decode as EventBook and find max sequence from pages
                if let Ok(book) = EventBook::decode(msg.payload.as_ref()) {
                    let max_seq = book.pages.iter().map(Self::get_sequence).max();
                    return Ok(max_seq);
                }
                Ok(None)
            }
            Err(_) => Ok(None), // No message found or other error
        }
    }

    /// Query events from a specific subject starting at a given sequence.
    /// Reads EventBook messages and extracts pages (same format as EventBus).
    async fn query_events(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let stream_name = self.stream_name(domain);
        let subject = self.subject(domain, root, edition);

        let stream = match self.jetstream.get_stream(&stream_name).await {
            Ok(s) => s,
            Err(_) => return Ok(Vec::new()),
        };

        // Create ephemeral consumer for this query
        let consumer_name = format!("query-{}", Uuid::new_v4());
        let consumer = stream
            .create_consumer(ConsumerConfig {
                name: Some(consumer_name),
                filter_subject: subject,
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::None,
                ..Default::default()
            })
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to create consumer: {}", e)))?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to get message stream: {}", e)))?;

        let mut events = Vec::new();

        // Fetch all messages (EventBooks) and extract pages
        while let Ok(Some(msg)) = tokio::time::timeout(self.query_timeout, messages.next()).await {
            let msg =
                msg.map_err(|e| StorageError::Nats(format!("Failed to receive message: {}", e)))?;

            // Decode as EventBook (unified format with EventBus)
            let book =
                EventBook::decode(msg.payload.as_ref()).map_err(StorageError::ProtobufDecode)?;

            // Extract pages and filter by sequence
            for page in book.pages {
                let seq = Self::get_sequence(&page);
                if seq >= from {
                    events.push(page);
                }
            }
        }

        // Sort by sequence to ensure order
        events.sort_by_key(Self::get_sequence);

        Ok(events)
    }

    /// Extract sequence number from an EventPage.
    fn get_sequence(event: &EventPage) -> u32 {
        event.sequence_num()
    }

    /// C-18 helper: scan the aggregate's subject history for a message
    /// whose `Angzarr-External-Id` header matches `external_id`, returning
    /// the message's payload (the persisted EventBook) and its pages on
    /// match. Used by both `add()`'s idempotency precheck and
    /// `find_by_external_id`.
    ///
    /// We use a per-call ephemeral consumer filtered to the aggregate's
    /// exact subject — i.e. the scan is bounded by the aggregate's
    /// `add()` history (typically tens of messages), not the whole
    /// stream. The lookup is O(N) in that history because NATS
    /// JetStream has no native secondary index; for high-volume saga
    /// pipelines an operator may want to add a `{prefix}.claims` KV
    /// bucket — same pattern documented for `get_by_correlation`.
    async fn scan_aggregate_books_with_header<F>(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        consumer_prefix: &str,
        mut matcher: F,
    ) -> Result<Vec<EventBook>>
    where
        F: FnMut(&async_nats::HeaderMap) -> bool,
    {
        let stream_name = self.stream_name(domain);
        let subject = self.subject(domain, root, edition);

        let stream = match self.jetstream.get_stream(&stream_name).await {
            Ok(s) => s,
            Err(_) => return Ok(Vec::new()),
        };

        let consumer_name = format!("{}-{}", consumer_prefix, Uuid::new_v4());
        let consumer = stream
            .create_consumer(ConsumerConfig {
                name: Some(consumer_name),
                filter_subject: subject,
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::None,
                ..Default::default()
            })
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to create scan consumer: {}", e)))?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to get message stream: {}", e)))?;

        let mut matches = Vec::new();
        while let Ok(Some(msg)) = tokio::time::timeout(self.query_timeout, messages.next()).await {
            let Ok(msg) = msg else { continue };
            let headers_match = msg.headers.as_ref().map(&mut matcher).unwrap_or(false);
            if !headers_match {
                continue;
            }
            let Ok(book) = EventBook::decode(msg.payload.as_ref()) else {
                continue;
            };
            matches.push(book);
        }
        Ok(matches)
    }

    /// C-18: check for an existing external_id claim on this aggregate.
    /// Returns `Some((first_seq, last_seq))` of the prior batch when
    /// found, `None` otherwise. Used by `add()` to short-circuit
    /// duplicate publishes (the SQL backends do the same via the
    /// `check_idempotency` helper).
    async fn find_external_id_claim(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<(u32, u32)>> {
        let books = self
            .scan_aggregate_books_with_header(domain, edition, root, "extid-check", |headers| {
                headers
                    .get(HEADER_EXTERNAL_ID)
                    .map(|v| v.as_str() == external_id)
                    .unwrap_or(false)
            })
            .await?;
        // Collect sequences across all matching books — under normal
        // operation there is exactly one match (the idempotency check
        // runs at add-time), but defensive code handles multi-match.
        let mut seqs: Vec<u32> = books
            .iter()
            .flat_map(|b| b.pages.iter().map(Self::get_sequence))
            .collect();
        if seqs.is_empty() {
            return Ok(None);
        }
        seqs.sort_unstable();
        Ok(Some((*seqs.first().unwrap(), *seqs.last().unwrap())))
    }

    /// Extract (root UUID, edition name) from a Cover, returning None if invalid.
    fn extract_root_edition(cover: &Cover) -> Option<(Uuid, String)> {
        let root = cover.root.as_ref()?;
        let uuid = Uuid::from_slice(&root.value).ok()?;
        let edition = cover
            .edition
            .as_ref()
            .map(|e| e.name.clone())
            .unwrap_or_else(|| DEFAULT_EDITION.to_string());
        Some((uuid, edition))
    }

    /// Build an EventBook from grouped pages.
    fn build_correlation_book(
        domain: &str,
        correlation_id: &str,
        root: Uuid,
        edition: String,
        mut pages: Vec<EventPage>,
    ) -> EventBook {
        pages.sort_by_key(Self::get_sequence);
        let next_seq = pages.last().map(Self::get_sequence).unwrap_or(0) + 1;

        EventBook {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: correlation_id.to_string(),
                edition: Some(Edition {
                    name: edition,
                    divergences: vec![],
                }),
            }),
            pages,
            snapshot: None,
            next_sequence: next_seq,
        }
    }

    /// Scan a stream for events matching a correlation ID.
    /// Returns events grouped by (root, edition).
    async fn scan_stream_for_correlation(
        &self,
        stream: &jetstream::stream::Stream,
        correlation_id: &str,
    ) -> std::collections::HashMap<(Uuid, String), Vec<EventPage>> {
        use std::collections::HashMap;

        let mut events_by_root: HashMap<(Uuid, String), Vec<EventPage>> = HashMap::new();

        // Create temporary consumer to scan all messages
        let consumer_name = format!("correlation-{}", Uuid::new_v4());
        let consumer = match stream
            .create_consumer(ConsumerConfig {
                name: Some(consumer_name),
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::None,
                ..Default::default()
            })
            .await
        {
            Ok(c) => c,
            Err(_) => return events_by_root,
        };

        let mut messages = match consumer.messages().await {
            Ok(m) => m,
            Err(_) => return events_by_root,
        };

        // Scan messages with timeout
        while let Ok(Some(msg)) = tokio::time::timeout(self.query_timeout, messages.next()).await {
            let Ok(msg) = msg else { continue };
            let Ok(book) = EventBook::decode(msg.payload.as_ref()) else {
                continue;
            };

            // Check correlation from Cover
            let book_correlation = book
                .cover
                .as_ref()
                .map(|c| c.correlation_id.as_str())
                .unwrap_or("");

            if book_correlation != correlation_id {
                continue;
            }

            // Extract root and edition, skip if invalid
            let Some(cover) = &book.cover else { continue };
            let Some((root, edition)) = Self::extract_root_edition(cover) else {
                continue;
            };

            events_by_root
                .entry((root, edition))
                .or_default()
                .extend(book.pages);
        }

        events_by_root
    }

    /// Perform composite read for editions (main timeline up to divergence + edition events).
    async fn composite_read(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        // Get edition events first to find divergence point
        let edition_events = self.query_events(domain, edition, root, 0).await?;

        let divergence = edition_events
            .first()
            .map(Self::get_sequence)
            .unwrap_or(u32::MAX);

        if divergence == u32::MAX {
            // No edition events, just return main timeline
            return self.query_events(domain, DEFAULT_EDITION, root, from).await;
        }

        let mut result = Vec::new();

        // Main timeline events: [from, divergence)
        if from < divergence {
            let main_events = self
                .query_events(domain, DEFAULT_EDITION, root, from)
                .await?;
            for event in main_events {
                if Self::get_sequence(&event) < divergence {
                    result.push(event);
                }
            }
        }

        // Edition events: [max(from, divergence), ∞)
        let edition_from = from.max(divergence);
        for event in edition_events {
            if Self::get_sequence(&event) >= edition_from {
                result.push(event);
            }
        }

        Ok(result)
    }
}

#[async_trait]
impl EventStore for NatsEventStore {
    async fn add(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        events: Vec<EventPage>,
        correlation_id: &str,
        external_id: Option<&str>,
        source_info: Option<&SourceInfo>,
    ) -> Result<AddOutcome> {
        if events.is_empty() {
            return Ok(AddOutcome::Added {
                first_sequence: 0,
                last_sequence: 0,
            });
        }

        self.ensure_stream(domain).await?;

        let external_id = external_id.unwrap_or("");

        // C-18: external_id idempotency check. Pre-fix `add()` ignored
        // external_id entirely; now we scan the aggregate's per-message
        // history (already filtered by subject = single domain/root/edition)
        // and check the `Angzarr-External-Id` header on each message
        // before re-persisting. Returns `Duplicate` with the original
        // sequence range if a prior call recorded this external_id.
        if !external_id.is_empty() {
            if let Some((first, last)) = self
                .find_external_id_claim(domain, edition, root, external_id)
                .await?
            {
                return Ok(AddOutcome::Duplicate {
                    first_sequence: first,
                    last_sequence: last,
                });
            }
        }

        // Get current max sequence for validation
        let max_seq = self.get_max_sequence(domain, edition, root).await?;
        let expected_next = max_seq.map(|s| s + 1).unwrap_or(0);
        let first_seq = Self::get_sequence(&events[0]);

        if first_seq != expected_next {
            return Err(StorageError::SequenceConflict {
                expected: expected_next,
                actual: first_seq,
            });
        }

        let subject = self.subject(domain, root, edition);
        let last_seq = events.last().map(Self::get_sequence).unwrap_or(first_seq);

        // Build EventBook (same format as EventBus.publish)
        let book = EventBook {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: correlation_id.to_string(),
                edition: Some(Edition {
                    name: edition.to_string(),
                    divergences: vec![],
                }),
            }),
            pages: events,
            snapshot: None,
            next_sequence: last_seq + 1,
        };

        let payload = book.encode_to_vec();

        // Build headers (aligned with EventBus format)
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(HEADER_SEQUENCE, first_seq.to_string().as_str());
        if !correlation_id.is_empty() {
            headers.insert(HEADER_CORRELATION, correlation_id);
        }

        // C-18: persist external_id + source_info as headers on the
        // batch message so `find_by_external_id` and `find_by_source`
        // can discover them by scanning the aggregate's subject history.
        // SQL backends store per-event; NATS stores per-EventBook (one
        // publish == one batch). For the trait contract, what matters is
        // that a claim recorded at `add()` is discoverable on lookup —
        // and the lookup returns the events that were persisted under
        // that claim, which is the whole book's pages here.
        if !external_id.is_empty() {
            headers.insert(HEADER_EXTERNAL_ID, external_id);
        }
        if let Some(info) = source_info.filter(|s| !s.is_empty()) {
            headers.insert(HEADER_SOURCE_EDITION, info.edition.as_str());
            headers.insert(HEADER_SOURCE_DOMAIN, info.domain.as_str());
            let source_root_str = info.root.as_hyphenated().to_string();
            headers.insert(HEADER_SOURCE_ROOT, source_root_str.as_str());
            let source_seq_str = info.seq.to_string();
            headers.insert(HEADER_SOURCE_SEQ, source_seq_str.as_str());
        }

        // Deduplication ID: batch format for idempotency with EventBus
        let msg_id = Self::msg_id_batch(domain, root, edition, first_seq, last_seq);
        headers.insert("Nats-Msg-Id", msg_id.as_str());

        self.jetstream
            .publish_with_headers(subject.clone(), headers, payload.clone().into())
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to publish: {}", e)))?
            .await
            .map_err(|e| StorageError::Nats(format!("Publish ack failed: {}", e)))?;

        debug!(
            domain = %domain,
            root = %root,
            first_seq = first_seq,
            last_seq = last_seq,
            msg_id = %msg_id,
            "Published EventBook to NATS"
        );

        // Dual-write to cascade stream for 2PC tracking
        // Check if any event in the book has a cascade_id
        for page in &book.pages {
            if let Some(ref cascade_id) = page.cascade_id {
                self.ensure_cascade_stream().await?;

                let seq = Self::get_sequence(page);
                let cascade_subject = self.cascade_subject(cascade_id, domain, root, edition);

                // Build cascade tracking headers
                let mut cascade_headers = async_nats::HeaderMap::new();
                cascade_headers.insert(HEADER_CASCADE_ID, cascade_id.as_str());
                cascade_headers.insert(HEADER_COMMITTED, (!page.no_commit).to_string().as_str());
                cascade_headers.insert(HEADER_DOMAIN, domain);
                cascade_headers.insert(HEADER_EDITION, edition);
                cascade_headers.insert(HEADER_ROOT, root.as_hyphenated().to_string().as_str());
                cascade_headers.insert(HEADER_SEQUENCE, seq.to_string().as_str());

                // Add created_at timestamp if present
                if let Some(ref ts) = page.created_at {
                    let ts_str = format!("{}.{}", ts.seconds, ts.nanos);
                    cascade_headers.insert(HEADER_CREATED_AT, ts_str.as_str());
                }

                // Deduplication ID for cascade event
                let cascade_msg_id = format!(
                    "cascade.{}.{}.{}.{}.{}",
                    cascade_id, domain, root, edition, seq
                );
                cascade_headers.insert("Nats-Msg-Id", cascade_msg_id.as_str());

                // Publish page to cascade stream (payload needed for sequence extraction)
                let page_payload = page.encode_to_vec();
                self.jetstream
                    .publish_with_headers(cascade_subject, cascade_headers, page_payload.into())
                    .await
                    .map_err(|e| StorageError::Nats(format!("Failed to publish cascade: {}", e)))?
                    .await
                    .map_err(|e| {
                        StorageError::Nats(format!("Cascade publish ack failed: {}", e))
                    })?;

                debug!(
                    cascade_id = %cascade_id,
                    domain = %domain,
                    root = %root,
                    seq = seq,
                    committed = !page.no_commit,
                    "Published to cascade stream"
                );
            }
        }

        Ok(AddOutcome::Added {
            first_sequence: first_seq,
            last_sequence: last_seq,
        })
    }

    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Vec<EventPage>> {
        self.get_from(domain, edition, root, 0).await
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        if is_main_timeline(edition) {
            return self.query_events(domain, DEFAULT_EDITION, root, from).await;
        }

        self.composite_read(domain, edition, root, from).await
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<Vec<EventPage>> {
        let events = self.get_from(domain, edition, root, from).await?;
        Ok(events
            .into_iter()
            .filter(|e| Self::get_sequence(e) < to)
            .collect())
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>> {
        let stream_name = self.stream_name(domain);
        let subject_prefix = format!("{}.events.{}.", self.prefix, domain);
        let subject_suffix = format!(".{}", edition);

        let stream = match self.jetstream.get_stream(&stream_name).await {
            Ok(s) => s,
            Err(_) => return Ok(Vec::new()),
        };

        let mut roots = std::collections::HashSet::new();

        // Create consumer to iterate subjects
        let consumer_name = format!("list-roots-{}", Uuid::new_v4());
        let filter = format!("{}.events.{}.*.{}", self.prefix, domain, edition);

        let consumer = match stream
            .create_consumer(ConsumerConfig {
                name: Some(consumer_name),
                filter_subject: filter,
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::None,
                ..Default::default()
            })
            .await
        {
            Ok(c) => c,
            Err(_) => return Ok(Vec::new()),
        };

        let mut messages = match consumer.messages().await {
            Ok(m) => m,
            Err(_) => return Ok(Vec::new()),
        };

        // Collect unique roots from message subjects
        while let Ok(Some(msg_result)) =
            tokio::time::timeout(self.query_timeout, messages.next()).await
        {
            if let Ok(msg) = msg_result {
                // Parse subject: {prefix}.events.{domain}.{root}.{edition}
                if let Some(rest) = msg.subject.as_str().strip_prefix(&subject_prefix) {
                    if let Some(root_str) = rest.strip_suffix(&subject_suffix) {
                        if let Ok(root) = Uuid::parse_str(root_str) {
                            roots.insert(root);
                        }
                    }
                }
            }
        }

        Ok(roots.into_iter().collect())
    }

    async fn list_domains(&self) -> Result<Vec<String>> {
        // List all streams with our prefix
        let mut domains = Vec::new();
        let prefix = self.prefix.to_uppercase();

        let mut streams = self.jetstream.streams();
        while let Some(stream) = streams.next().await {
            if let Ok(info) = stream {
                if let Some(domain) = info.config.name.strip_prefix(&format!("{}_", prefix)) {
                    domains.push(domain.to_lowercase());
                }
            }
        }

        Ok(domains)
    }

    async fn get_next_sequence(&self, domain: &str, edition: &str, root: Uuid) -> Result<u32> {
        // For non-main editions, check edition first then fall back to main
        if !is_main_timeline(edition) {
            if let Some(seq) = self.get_max_sequence(domain, edition, root).await? {
                return Ok(seq + 1);
            }
        }

        // Check main timeline
        let target = if is_main_timeline(edition) {
            edition
        } else {
            DEFAULT_EDITION
        };

        let max_seq = self.get_max_sequence(domain, target, root).await?;
        Ok(max_seq.map(|s| s + 1).unwrap_or(0))
    }

    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &str,
    ) -> Result<Vec<EventPage>> {
        let until_dt = chrono::DateTime::parse_from_rfc3339(until)
            .map_err(|e| StorageError::InvalidTimestampFormat(e.to_string()))?;

        let all_events = self.get(domain, edition, root).await?;

        Ok(all_events
            .into_iter()
            .filter(|e| {
                if let Some(ref ts) = e.created_at {
                    if let Some(dt) = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                    {
                        return dt <= until_dt;
                    }
                }
                false
            })
            .collect())
    }

    async fn get_by_correlation(&self, correlation_id: &str) -> Result<Vec<EventBook>> {
        if correlation_id.is_empty() {
            return Ok(vec![]);
        }

        // # Why This Is Expensive (And Why We Accept It)
        //
        // Without a secondary index, finding events by correlation_id requires
        // scanning every message in every stream. This is O(total events) — slow
        // for large deployments.
        //
        // However:
        // 1. get_by_correlation is rare — used mainly by PMs, not hot paths
        // 2. A proper fix (KV index mapping correlation_id → roots) adds complexity
        // 3. Most workflows have few events per correlation_id
        //
        // For production at scale: add a `{prefix}.correlations` KV bucket that maps
        // correlation_id → [(domain, root, edition)] and update it on each publish.
        // This makes lookup O(1) but adds write overhead and eventual consistency.
        let domains = self.list_domains().await?;
        let mut books = Vec::new();

        for domain in domains {
            let stream_name = self.stream_name(&domain);
            let Ok(stream) = self.jetstream.get_stream(&stream_name).await else {
                continue;
            };

            let events_by_root = self
                .scan_stream_for_correlation(&stream, correlation_id)
                .await;

            for ((root, edition), pages) in events_by_root {
                books.push(Self::build_correlation_book(
                    &domain,
                    correlation_id,
                    root,
                    edition,
                    pages,
                ));
            }
        }

        Ok(books)
    }

    async fn delete_edition_events(&self, domain: &str, edition: &str) -> Result<u32> {
        // NATS JetStream doesn't support targeted deletion by subject filter easily
        // We need to purge messages matching our subject pattern
        let stream_name = self.stream_name(domain);
        let subject_pattern = format!("{}.events.{}.*.{}", self.prefix, domain, edition);

        let mut stream = match self.jetstream.get_stream(&stream_name).await {
            Ok(s) => s,
            Err(_) => return Ok(0),
        };

        // Get count before purge
        let count_before = stream
            .info()
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to get stream info: {}", e)))?
            .state
            .messages;

        // Purge messages matching subject
        stream
            .purge()
            .filter(&subject_pattern)
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to purge: {}", e)))?;

        let count_after = stream
            .info()
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to get stream info: {}", e)))?
            .state
            .messages;

        let deleted = (count_before - count_after) as u32;

        debug!(
            domain = %domain,
            edition = %edition,
            deleted = deleted,
            "Purged edition events from NATS"
        );

        Ok(deleted)
    }

    async fn find_by_source(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        source_info: &SourceInfo,
    ) -> Result<Option<Vec<EventPage>>> {
        // C-18: Saga idempotency. Pre-fix this returned `Ok(None)`
        // unconditionally, silently violating the trait contract.
        // We scan the aggregate's subject history (already filtered to a
        // single domain/root/edition) and match on the four source
        // headers persisted by `add()`. NATS has no native secondary
        // index; the scan is bounded by the aggregate's `add()` history.
        if source_info.is_empty() {
            return Ok(None);
        }
        let target_root_str = source_info.root.as_hyphenated().to_string();
        let target_seq_str = source_info.seq.to_string();
        let target_edition = source_info.edition.clone();
        let target_domain = source_info.domain.clone();
        let books = self
            .scan_aggregate_books_with_header(
                domain,
                edition,
                root,
                "find-by-source",
                move |headers| {
                    let h_edition = headers.get(HEADER_SOURCE_EDITION).map(|v| v.as_str());
                    let h_domain = headers.get(HEADER_SOURCE_DOMAIN).map(|v| v.as_str());
                    let h_root = headers.get(HEADER_SOURCE_ROOT).map(|v| v.as_str());
                    let h_seq = headers.get(HEADER_SOURCE_SEQ).map(|v| v.as_str());
                    h_edition == Some(target_edition.as_str())
                        && h_domain == Some(target_domain.as_str())
                        && h_root == Some(target_root_str.as_str())
                        && h_seq == Some(target_seq_str.as_str())
                },
            )
            .await?;
        let pages: Vec<EventPage> = books.into_iter().flat_map(|b| b.pages).collect();
        if pages.is_empty() {
            Ok(None)
        } else {
            let mut sorted = pages;
            sorted.sort_by_key(Self::get_sequence);
            Ok(Some(sorted))
        }
    }

    async fn find_by_external_id(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<Vec<EventPage>>> {
        // C-18: fact-injection idempotency. Pre-fix this returned
        // `Ok(None)` unconditionally, silently violating the trait
        // contract. We scan the aggregate's subject history (already
        // filtered to a single domain/root/edition) and match on the
        // `Angzarr-External-Id` header persisted by `add()`. Empty
        // external_id returns None per contract.
        if external_id.is_empty() {
            return Ok(None);
        }
        let target = external_id.to_string();
        let books = self
            .scan_aggregate_books_with_header(
                domain,
                edition,
                root,
                "find-by-extid",
                move |headers| {
                    headers
                        .get(HEADER_EXTERNAL_ID)
                        .map(|v| v.as_str() == target.as_str())
                        .unwrap_or(false)
                },
            )
            .await?;
        let pages: Vec<EventPage> = books.into_iter().flat_map(|b| b.pages).collect();
        if pages.is_empty() {
            Ok(None)
        } else {
            let mut sorted = pages;
            sorted.sort_by_key(Self::get_sequence);
            Ok(Some(sorted))
        }
    }

    async fn query_stale_cascades(&self, threshold: &str) -> Result<Vec<String>> {
        use std::collections::{HashMap, HashSet};

        // Parse threshold timestamp
        let threshold_dt = chrono::DateTime::parse_from_rfc3339(threshold)
            .map_err(|e| StorageError::InvalidTimestampFormat(e.to_string()))?;

        let stream_name = self.cascade_stream_name();
        let stream = match self.jetstream.get_stream(&stream_name).await {
            Ok(s) => s,
            Err(_) => return Ok(Vec::new()), // No cascade stream yet
        };

        // Create ephemeral consumer to scan cascade events
        let consumer_name = format!("stale-cascades-{}", Uuid::new_v4());
        let consumer = stream
            .create_consumer(ConsumerConfig {
                name: Some(consumer_name),
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::None,
                ..Default::default()
            })
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to create cascade consumer: {}", e)))?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| StorageError::Nats(format!("Failed to get cascade messages: {}", e)))?;

        // Track committed vs uncommitted cascade_ids with their oldest timestamps
        let mut cascade_committed: HashSet<String> = HashSet::new();
        let mut cascade_uncommitted: HashMap<String, chrono::DateTime<chrono::FixedOffset>> =
            HashMap::new();

        while let Ok(Some(msg)) = tokio::time::timeout(self.query_timeout, messages.next()).await {
            let Ok(msg) = msg else { continue };

            // Extract headers
            let Some(ref headers) = msg.headers else {
                continue;
            };

            let cascade_id = headers
                .get(HEADER_CASCADE_ID)
                .map(|v| v.to_string())
                .unwrap_or_default();

            if cascade_id.is_empty() {
                continue;
            }

            let committed = headers
                .get(HEADER_COMMITTED)
                .map(|v| v.as_str() == "true")
                .unwrap_or(false);

            if committed {
                // This cascade has at least one committed event - mark as committed
                cascade_committed.insert(cascade_id);
            } else {
                // Parse created_at timestamp
                if let Some(ts_val) = headers.get(HEADER_CREATED_AT) {
                    let ts_str = ts_val.to_string();
                    if let Some((secs_str, nanos_str)) = ts_str.split_once('.') {
                        if let (Ok(secs), Ok(nanos)) =
                            (secs_str.parse::<i64>(), nanos_str.parse::<u32>())
                        {
                            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                                let dt_fixed = dt.fixed_offset();
                                // Track oldest timestamp for this cascade
                                cascade_uncommitted
                                    .entry(cascade_id)
                                    .and_modify(|existing| {
                                        if dt_fixed < *existing {
                                            *existing = dt_fixed;
                                        }
                                    })
                                    .or_insert(dt_fixed);
                            }
                        }
                    }
                }
            }
        }

        // Find stale cascades: uncommitted AND older than threshold AND never committed
        let stale: Vec<String> = cascade_uncommitted
            .into_iter()
            .filter(|(cascade_id, oldest_ts)| {
                !cascade_committed.contains(cascade_id) && *oldest_ts < threshold_dt
            })
            .map(|(cascade_id, _)| cascade_id)
            .collect();

        Ok(stale)
    }

    async fn query_cascade_participants(
        &self,
        cascade_id: &str,
    ) -> Result<Vec<CascadeParticipant>> {
        use std::collections::HashMap;

        let stream_name = self.cascade_stream_name();
        let stream = match self.jetstream.get_stream(&stream_name).await {
            Ok(s) => s,
            Err(_) => return Ok(Vec::new()),
        };

        // Filter by cascade_id prefix in subject: {prefix}.cascade.{cascade_id}.>
        let filter_subject = format!("{}.cascade.{}.>", self.prefix, cascade_id);

        let consumer_name = format!("cascade-participants-{}", Uuid::new_v4());
        let consumer = stream
            .create_consumer(ConsumerConfig {
                name: Some(consumer_name),
                filter_subject,
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::None,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                StorageError::Nats(format!(
                    "Failed to create cascade participant consumer: {}",
                    e
                ))
            })?;

        let mut messages = consumer.messages().await.map_err(|e| {
            StorageError::Nats(format!("Failed to get cascade participant messages: {}", e))
        })?;

        // Group by (domain, edition, root) and collect sequences
        let mut participants: HashMap<(String, String, Uuid), Vec<u32>> = HashMap::new();

        while let Ok(Some(msg)) = tokio::time::timeout(self.query_timeout, messages.next()).await {
            let Ok(msg) = msg else { continue };

            let Some(ref headers) = msg.headers else {
                continue;
            };

            let domain = headers
                .get(HEADER_DOMAIN)
                .map(|v| v.to_string())
                .unwrap_or_default();
            let edition = headers
                .get(HEADER_EDITION)
                .map(|v| v.to_string())
                .unwrap_or_default();
            let root_str = headers
                .get(HEADER_ROOT)
                .map(|v| v.to_string())
                .unwrap_or_default();
            let seq_str = headers
                .get(HEADER_SEQUENCE)
                .map(|v| v.to_string())
                .unwrap_or_default();

            if domain.is_empty() || root_str.is_empty() {
                continue;
            }

            let Ok(root) = Uuid::parse_str(&root_str) else {
                continue;
            };
            let Ok(seq) = seq_str.parse::<u32>() else {
                continue;
            };

            participants
                .entry((domain, edition, root))
                .or_default()
                .push(seq);
        }

        // Convert to CascadeParticipant structs
        let result: Vec<CascadeParticipant> = participants
            .into_iter()
            .map(|((domain, edition, root), sequences)| CascadeParticipant {
                domain,
                edition,
                root,
                sequences,
            })
            .collect();

        Ok(result)
    }
}
