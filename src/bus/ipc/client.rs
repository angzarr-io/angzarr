//! IPC event bus client - same interface as AMQP/Kafka.
//!
//! Publishers: Read subscriber list from env var, write directly to pipes.
//! Subscribers: Read from their named pipe.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use nix::libc;

use async_trait::async_trait;
use prost::Message;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::broker::SubscriberInfo;
use super::checkpoint::{Checkpoint, CheckpointConfig};
use super::{DEFAULT_BASE_PATH, SUBSCRIBER_PIPE_PREFIX};
use crate::bus::{BusError, EventBus, EventHandler, PublishResult, Result};
use crate::proto::EventBook;
use crate::proto_ext::{CoverExt, EventPageExt};

// ============================================================================
// Consumer Helper Functions
// ============================================================================

/// Result of reading a message from a pipe.
#[derive(Debug)]
enum ReadResult {
    /// Message data read successfully.
    Message(Vec<u8>),
    /// Pipe closed (EOF) - should reopen.
    Eof,
    /// Message too large - should skip to next message.
    TooLarge(usize),
    /// Fatal error - should exit.
    Error(std::io::Error),
}

/// Read a length-prefixed message from a file.
///
/// Protocol: 4-byte big-endian length, then message body.
fn read_length_prefixed_message(file: &mut File) -> ReadResult {
    // Read length prefix (4 bytes, big-endian)
    let mut len_buf = [0u8; 4];
    match file.read_exact(&mut len_buf) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return ReadResult::Eof;
        }
        Err(e) => {
            return ReadResult::Error(e);
        }
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
    if len > MAX_MESSAGE_SIZE {
        return ReadResult::TooLarge(len);
    }

    // Read message body
    let mut buf = vec![0u8; len];
    match file.read_exact(&mut buf) {
        Ok(_) => ReadResult::Message(buf),
        Err(e) => ReadResult::Error(e),
    }
}

/// Process a decoded EventBook with domain filtering, checkpoint, and handler dispatch.
///
/// Returns true if handlers should be called, false if message was filtered/skipped.
fn should_process_message(
    book: &EventBook,
    domains: &[String],
    checkpoint: &Checkpoint,
    rt: &tokio::runtime::Handle,
) -> bool {
    let routing_key = book.routing_key();

    // Check domain filter using routing key
    if !matches_domain_filter(&routing_key, domains) {
        return false;
    }

    // Extract root and max sequence for checkpoint
    let root_bytes = book
        .cover
        .as_ref()
        .and_then(|c| c.root.as_ref())
        .map(|r| r.value.as_slice());
    let max_sequence = max_page_sequence(book);

    // Skip if already processed (checkpoint deduplication)
    if let (Some(root), Some(seq)) = (root_bytes, max_sequence) {
        let dominated =
            rt.block_on(async { !checkpoint.should_process(&routing_key, root, seq).await });
        if dominated {
            debug!(routing_key = %routing_key, sequence = seq, "Skipping checkpointed event");
            return false;
        }
    }

    true
}

/// Dispatch an EventBook to handlers and (conditionally) advance the checkpoint.
///
/// C-10 contract: the checkpoint advances ONLY when every handler returns
/// `Ok`. A failing handler must leave the checkpoint at its prior value
/// so that, on consumer restart, the event is re-delivered rather than
/// silently skipped. This matches Kafka's `commit_message`-on-success
/// pattern (`src/bus/kafka/bus.rs:149`).
///
/// Note that, unlike the broker-backed transports, the IPC pipe has no
/// "redeliver this message" semantic on its own — the kernel pipe is a
/// one-shot byte stream. Holding back the checkpoint here means that
/// after a crash/restart, the consumer relies on the broker side (or
/// the upstream EventStore) to replay the event from its persisted
/// position. In practice, for in-process IPC the checkpoint IS the
/// retry boundary: not advancing on failure is the only mechanism we
/// have to avoid silent loss, but it does NOT cause immediate
/// re-delivery from the current pipe stream — that's a broker
/// limitation, not a bug in this fix.
fn dispatch_to_handlers(
    book: Arc<EventBook>,
    handlers: &Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    checkpoint: &Checkpoint,
    rt: &tokio::runtime::Handle,
) {
    let routing_key = book.routing_key();
    let root_bytes = book
        .cover
        .as_ref()
        .and_then(|c| c.root.as_ref())
        .map(|r| r.value.clone());
    let max_sequence = max_page_sequence(&book);

    rt.block_on(async {
        // Delegate to the shared dispatch helper so the success bool is
        // computed the same way every transport computes it.
        let all_succeeded = crate::bus::dispatch::dispatch_to_handlers(handlers, &book).await;

        // Advance the checkpoint only on full success. Any handler
        // failure leaves the checkpoint at its prior value so the event
        // is eligible for re-processing.
        if !all_succeeded {
            warn!(
                routing_key = %routing_key,
                sequence = ?max_sequence,
                "Handler failure: leaving checkpoint unadvanced for redelivery (C-10)"
            );
            return;
        }

        if let (Some(root), Some(seq)) = (&root_bytes, max_sequence) {
            checkpoint.update(&routing_key, root, seq).await;
        }
    });
}

/// Action to take after processing a message.
#[derive(Debug, PartialEq)]
enum MessageAction {
    /// Continue reading from the pipe.
    Continue,
    /// Break inner loop, reopen pipe (EOF or recoverable error).
    Reopen,
    /// Exit consumer entirely (fatal error).
    Exit,
}

/// Handle a successfully read message buffer.
///
/// Decodes, filters, and dispatches the message to handlers.
fn handle_message_buffer(
    buf: Vec<u8>,
    domains: &[String],
    handlers: &Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    checkpoint: &Checkpoint,
    rt: &tokio::runtime::Handle,
) {
    let book = match EventBook::decode(&buf[..]) {
        Ok(b) => Arc::new(b),
        Err(e) => {
            error!(error = %e, "Failed to decode EventBook");
            return;
        }
    };

    if !should_process_message(&book, domains, checkpoint, rt) {
        return;
    }

    debug!(routing_key = %book.routing_key(), "Received event via pipe");
    dispatch_to_handlers(book, handlers, checkpoint, rt);
}

/// Flush checkpoint on pipe EOF.
fn flush_checkpoint_on_eof(checkpoint: &Checkpoint, rt: &tokio::runtime::Handle) {
    rt.block_on(async {
        if let Err(e) = checkpoint.flush().await {
            warn!(error = %e, "Failed to flush checkpoint on pipe EOF");
        }
    });
}

/// Process a single read result from the pipe.
///
/// Handles message decoding, filtering, and dispatching to handlers.
/// Returns an action indicating what the consumer loop should do next.
fn process_read_result(
    result: ReadResult,
    pipe_path: &std::path::Path,
    domains: &[String],
    handlers: &Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    checkpoint: &Checkpoint,
    rt: &tokio::runtime::Handle,
) -> MessageAction {
    match result {
        ReadResult::Message(buf) => {
            handle_message_buffer(buf, domains, handlers, checkpoint, rt);
            MessageAction::Continue
        }
        ReadResult::Eof => {
            flush_checkpoint_on_eof(checkpoint, rt);
            debug!(pipe = %pipe_path.display(), "Pipe EOF, reopening");
            MessageAction::Reopen
        }
        ReadResult::TooLarge(len) => {
            error!(len, "Message too large");
            MessageAction::Reopen
        }
        ReadResult::Error(e) => {
            error!(error = %e, "Pipe read error");
            MessageAction::Exit
        }
    }
}

/// Read messages from a pipe connection until EOF or error.
///
/// Returns true to continue outer loop (reopen pipe), false to exit entirely.
fn handle_pipe_connection(
    file: &mut File,
    pipe_path: &std::path::Path,
    domains: &[String],
    handlers: &Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    checkpoint: &Checkpoint,
    rt: &tokio::runtime::Handle,
) -> bool {
    loop {
        let result = read_length_prefixed_message(file);
        match process_read_result(result, pipe_path, domains, handlers, checkpoint, rt) {
            MessageAction::Continue => continue,
            MessageAction::Reopen => return true,
            MessageAction::Exit => return false,
        }
    }
}

/// Run the IPC consumer loop with reconnection logic.
///
/// This is the main consumer loop that:
/// 1. Opens the pipe (blocks until a writer connects)
/// 2. Reads messages until EOF
/// 3. Reopens the pipe and repeats (unless shutdown or fatal error)
fn run_consumer_loop(
    pipe_path: &std::path::Path,
    domains: &[String],
    handlers: &Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    checkpoint: &Checkpoint,
    shutdown: &AtomicBool,
) {
    let rt = tokio::runtime::Handle::current();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            debug!(pipe = %pipe_path.display(), "IPC consumer shutting down");
            return;
        }

        let mut file = match File::open(pipe_path) {
            Ok(f) => f,
            Err(e) => {
                error!(pipe = %pipe_path.display(), error = %e, "Failed to open pipe");
                return;
            }
        };

        // Check shutdown after unblocking from open
        if shutdown.load(Ordering::Relaxed) {
            debug!(pipe = %pipe_path.display(), "IPC consumer shutting down");
            return;
        }

        info!(pipe = %pipe_path.display(), "IPC consumer connected");

        if !handle_pipe_connection(&mut file, pipe_path, domains, handlers, checkpoint, &rt) {
            return; // Fatal error, exit entirely
        }
        // Otherwise continue loop to reopen pipe
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Env var name for subscriber list (set by orchestrator).
pub const SUBSCRIBERS_ENV_VAR: &str = "ANGZARR_IPC_SUBSCRIBERS";

/// Configuration for IPC event bus.
#[derive(Debug, Clone)]
pub struct IpcConfig {
    /// Base path for pipes.
    pub base_path: PathBuf,
    /// Subscriber name (for subscriber mode only).
    pub subscriber_name: Option<String>,
    /// Domains to subscribe to (for subscriber mode only).
    pub domains: Vec<String>,
    /// Subscriber list (for publisher mode, loaded from env var).
    pub subscribers: Vec<SubscriberInfo>,
    /// Enable checkpoint persistence for subscribers.
    /// Tracks last-processed sequence per (domain, root) for crash recovery.
    pub checkpoint_enabled: bool,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from(DEFAULT_BASE_PATH),
            subscriber_name: None,
            domains: Vec::new(),
            subscribers: Vec::new(),
            checkpoint_enabled: false,
        }
    }
}

impl IpcConfig {
    /// Create publisher config, loading subscribers from env var.
    pub fn publisher(base_path: impl Into<PathBuf>) -> Self {
        let subscribers = load_subscribers_from_env();
        Self {
            base_path: base_path.into(),
            subscriber_name: None,
            domains: Vec::new(),
            subscribers,
            checkpoint_enabled: false,
        }
    }

    /// Create publisher config with explicit subscriber list.
    pub fn publisher_with_subscribers(
        base_path: impl Into<PathBuf>,
        subscribers: Vec<SubscriberInfo>,
    ) -> Self {
        Self {
            base_path: base_path.into(),
            subscriber_name: None,
            domains: Vec::new(),
            subscribers,
            checkpoint_enabled: false,
        }
    }

    /// Create subscriber config with checkpointing enabled.
    pub fn subscriber(
        base_path: impl Into<PathBuf>,
        name: impl Into<String>,
        domains: Vec<String>,
    ) -> Self {
        Self {
            base_path: base_path.into(),
            subscriber_name: Some(name.into()),
            domains,
            subscribers: Vec::new(),
            checkpoint_enabled: true,
        }
    }

    /// Get the subscriber pipe path.
    pub fn subscriber_pipe(&self) -> Option<PathBuf> {
        self.subscriber_name.as_ref().map(|name| {
            self.base_path
                .join(format!("{}{}.pipe", SUBSCRIBER_PIPE_PREFIX, name))
        })
    }
}

/// Load subscriber list from env var.
fn load_subscribers_from_env() -> Vec<SubscriberInfo> {
    match std::env::var(SUBSCRIBERS_ENV_VAR) {
        Ok(json) => serde_json::from_str(&json).unwrap_or_else(|e| {
            warn!(error = %e, "Failed to parse {}", SUBSCRIBERS_ENV_VAR);
            Vec::new()
        }),
        Err(_) => {
            debug!("{} not set, no subscribers configured", SUBSCRIBERS_ENV_VAR);
            Vec::new()
        }
    }
}

/// IPC event bus - same interface as AMQP/Kafka.
pub struct IpcEventBus {
    config: IpcConfig,
    /// Live subscriber routing list.
    ///
    /// Seeded from `config.subscribers` at construction time. The publish
    /// path snapshots this list before fanning out, and a `BrokenPipe` on
    /// `write_all` to any subscriber prunes that subscriber from this list
    /// so subsequent publishes don't re-target a dead pipe. See H-04 in
    /// plans/deep-review-remediation.md.
    ///
    /// `config.subscribers` is kept as the immutable bootstrap snapshot
    /// (for inspection by tests and the publisher constructor); this field
    /// is the mutable runtime view.
    subscribers: Arc<RwLock<Vec<SubscriberInfo>>>,
    /// Handlers for subscriber mode.
    handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    /// Consumer task handle.
    consumer_task: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Tracks last-processed sequence for crash recovery.
    checkpoint: Arc<Checkpoint>,
    /// Shutdown signal for the consumer task.
    shutdown: Arc<AtomicBool>,
    /// Per-subscriber-pipe write mutex.
    ///
    /// Serializes the length-prefix + body write so intra-process publishers
    /// can't interleave their frames. POSIX guarantees atomic pipe writes only
    /// for buffers ≤ PIPE_BUF (4 KiB on Linux); event books routinely exceed
    /// that. Holding this mutex around a single-buffer `write_all` (length
    /// prefix concatenated with body) ensures that intra-process publishers
    /// emit complete frames atomically with respect to each other and that a
    /// partial-failure (WouldBlock) cannot leave a phantom length prefix in
    /// the pipe. See C-09 in plans/deep-review-remediation.md.
    ///
    /// Per-subscriber granularity means publishes to different subscribers
    /// stay parallel; only writes to the SAME pipe are serialized.
    ///
    /// **Runtime-safety**: the actual blocking syscalls (open + fcntl +
    /// write_all) run inside `tokio::task::spawn_blocking` so a slow reader
    /// on a full FIFO blocks a blocking-pool thread, never a runtime worker.
    /// The tokio Mutex is held across the spawn_blocking's `.await`; tokio
    /// Mutex is designed for this and the contract still serializes writes
    /// to the SAME pipe.
    ///
    /// **Cross-process scope**: this mutex serializes ONLY publishers within
    /// the same process. When multiple processes publish to the same FIFO,
    /// the kernel still guarantees atomicity only for writes ≤ PIPE_BUF.
    /// In practice the framework's in-process bus is the dominant contention
    /// model; cross-process publishers should use a transport with native
    /// fan-out semantics (AMQP, Kafka, NATS).
    pipe_locks: Arc<RwLock<HashMap<PathBuf, Arc<Mutex<()>>>>>,
}

impl IpcEventBus {
    /// Create a new IPC event bus.
    pub fn new(config: IpcConfig) -> Self {
        let checkpoint_config = match (&config.subscriber_name, config.checkpoint_enabled) {
            (Some(name), true) => CheckpointConfig::for_subscriber(&config.base_path, name),
            _ => CheckpointConfig::disabled(),
        };
        let subscribers = Arc::new(RwLock::new(config.subscribers.clone()));
        Self {
            checkpoint: Arc::new(Checkpoint::new(checkpoint_config)),
            config,
            subscribers,
            handlers: Arc::new(RwLock::new(Vec::new())),
            consumer_task: Arc::new(RwLock::new(None)),
            shutdown: Arc::new(AtomicBool::new(false)),
            pipe_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Return a snapshot of the live subscriber routing list.
    ///
    /// Used by tests to verify the publisher's view of who is currently
    /// routable. Subscribers are pruned from this list when a publish
    /// fails with `BrokenPipe` (the kernel-level equivalent of an
    /// unannounced unregister). See H-04.
    pub async fn live_subscribers(&self) -> Vec<SubscriberInfo> {
        self.subscribers.read().await.clone()
    }

    /// Drop subscribers whose names appear in `names` from the routing
    /// list.
    ///
    /// Idempotent: names not present are silently ignored. This is the
    /// same retain-by-name operation the publish path performs when a
    /// per-subscriber `write_all` returns `BrokenPipe`; exposing it
    /// here (a) lets tests pin the pruning bookkeeping without racing
    /// on a real kernel EPIPE and (b) gives an operator-visible seam
    /// if the broker ever wants to push an explicit unregister into
    /// the publisher.
    pub(crate) async fn prune_subscribers(&self, names: &[String]) {
        if names.is_empty() {
            return;
        }
        let mut live = self.subscribers.write().await;
        live.retain(|s| !names.iter().any(|n| n == &s.name));
    }

    /// Get (or lazily create) the per-pipe write mutex for `pipe_path`.
    ///
    /// Used to serialize length-prefix + body writes within this process so
    /// concurrent publishers cannot interleave their frames. See C-09.
    async fn pipe_lock(&self, pipe_path: &Path) -> Arc<Mutex<()>> {
        // Fast path: read lock — the common case is "lock exists, just return
        // a clone of the Arc". Slow path on first publish to this pipe takes
        // a write lock to insert.
        if let Some(lock) = self.pipe_locks.read().await.get(pipe_path) {
            return lock.clone();
        }
        let mut map = self.pipe_locks.write().await;
        map.entry(pipe_path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Create a publisher bus (loads subscribers from env var).
    pub fn publisher(base_path: impl Into<PathBuf>) -> Self {
        Self::new(IpcConfig::publisher(base_path))
    }

    /// Create a subscriber bus.
    pub fn subscriber(
        base_path: impl Into<PathBuf>,
        name: impl Into<String>,
        domains: Vec<String>,
    ) -> Self {
        Self::new(IpcConfig::subscriber(base_path, name, domains))
    }

    /// Stop the consumer and clean up.
    ///
    /// Sets the shutdown flag and unblocks the consumer if it's stuck
    /// waiting for a writer on the pipe.
    pub async fn stop(&self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Open the pipe for writing to unblock consumer's blocking File::open().
        // The consumer will see the shutdown flag after the open returns.
        if let Some(pipe_path) = self.config.subscriber_pipe() {
            let _ = OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&pipe_path);
        }

        if let Some(handle) = self.consumer_task.write().await.take() {
            handle.abort();
        }
    }

    /// Start consuming from the pipe (for subscribers).
    pub async fn start_consuming(&self) -> Result<()> {
        let pipe_path = match self.config.subscriber_pipe() {
            Some(p) => p,
            None => {
                return Err(BusError::Subscribe(
                    "No subscriber name configured".to_string(),
                ))
            }
        };

        // Check if already consuming
        {
            let task = self.consumer_task.read().await;
            if task.is_some() {
                return Ok(());
            }
        }

        // Load persisted checkpoint positions before starting consumer
        if let Err(e) = self.checkpoint.load().await {
            warn!(error = %e, "Failed to load checkpoint, starting fresh");
        }

        let handlers = self.handlers.clone();
        let domains = self.config.domains.clone();
        let checkpoint = self.checkpoint.clone();
        let shutdown = self.shutdown.clone();

        info!(pipe = %pipe_path.display(), "Starting IPC consumer");

        // Spawn blocking task for pipe reading (pipes are blocking I/O)
        let handle = tokio::task::spawn_blocking(move || {
            run_consumer_loop(&pipe_path, &domains, &handlers, &checkpoint, &shutdown);
        });

        *self.consumer_task.write().await = Some(handle);

        Ok(())
    }
}

#[async_trait]
impl EventBus for IpcEventBus {
    /// Publish events directly to subscriber pipes.
    #[tracing::instrument(name = "bus.publish", skip_all, fields(domain = %book.domain()))]
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        // Snapshot the live routing list. We iterate over the snapshot so
        // a concurrent prune doesn't perturb iteration; any subscriber
        // pruned during this publish is collected separately and applied
        // to the shared list at the end.
        let snapshot: Vec<SubscriberInfo> = {
            let guard = self.subscribers.read().await;
            if guard.is_empty() {
                debug!("No subscribers configured, event not published");
                return Ok(PublishResult::default());
            }
            guard.clone()
        };

        let routing_key = book.routing_key();

        // Serialize once.
        //
        // To prevent length+body splits from interleaving with concurrent
        // publishers on the same pipe (C-09 framing-interleave bug), and to
        // prevent a `WouldBlock` between the prefix and body writes from
        // leaving a phantom 4-byte prefix in the pipe (C-09 half-written
        // frame variant), build ONE buffer containing the length prefix
        // concatenated with the body and write it under a per-pipe mutex
        // with a single `write_all` call. A partial failure now leaves
        // either zero bytes (no kernel write progressed) or all bytes in
        // the pipe — never a stranded length prefix.
        let serialized = book.encode_to_vec();
        let mut framed: Vec<u8> = Vec::with_capacity(4 + serialized.len());
        framed.extend_from_slice(&(serialized.len() as u32).to_be_bytes());
        framed.extend_from_slice(&serialized);
        // Wrap in Arc so each subscriber's spawn_blocking closure can hold a
        // cheap clone instead of copying the whole frame.
        let framed = Arc::new(framed);

        // Names of subscribers to prune from the routing list after this
        // publish. Collected during the fan-out loop and applied in a
        // single write-lock acquisition at the end (H-04).
        let mut to_prune: Vec<String> = Vec::new();

        for subscriber in &snapshot {
            // Check domain filter using routing key
            if !matches_domain_filter(&routing_key, &subscriber.domains) {
                continue;
            }

            // Acquire the per-pipe write mutex BEFORE the blocking I/O so
            // that the open() + write_all() pair is sequenced with respect
            // to any other publisher within this process targeting the
            // same FIFO. Holding it across the spawn_blocking is fine —
            // tokio::Mutex is designed for await-spanning critical
            // sections, and the actual blocking syscalls now run on a
            // blocking-pool thread (not on a runtime worker), so a slow
            // reader on the FIFO can no longer steal a worker thread or
            // deadlock current-thread runtimes.
            let pipe_lock = self.pipe_lock(&subscriber.pipe_path).await;
            let _write_guard = pipe_lock.lock().await;

            // Run the open + clear_nonblock + write_all in a
            // spawn_blocking closure. The framed buffer is small (few KiB
            // typical, ≤10 MiB cap from the reader side) so cloning into
            // the closure is cheap relative to the syscall path. We open
            // O_NONBLOCK so the open returns ENXIO when no reader is
            // attached (instead of hanging); once it succeeds we clear
            // O_NONBLOCK so write_all BLOCKS on a full pipe (back-pressure)
            // rather than returning WouldBlock mid-frame — that's the
            // original C-09 "half-written frame" desync.
            let pipe_path = subscriber.pipe_path.clone();
            let framed_for_write = Arc::clone(&framed);
            let write_result: std::io::Result<WriteOutcome> =
                tokio::task::spawn_blocking(move || {
                    let mut file = match OpenOptions::new()
                        .write(true)
                        .custom_flags(libc::O_NONBLOCK)
                        .open(&pipe_path)
                    {
                        Ok(f) => f,
                        Err(e) if e.raw_os_error() == Some(libc::ENXIO) => {
                            // ENXIO = no reader yet; benign during bring-up.
                            return Ok(WriteOutcome::NoReader);
                        }
                        Err(e) => return Err(e),
                    };
                    clear_nonblock(&file)?;
                    match file.write_all(framed_for_write.as_slice()) {
                        Ok(()) => Ok(WriteOutcome::Wrote),
                        Err(e) => Err(e),
                    }
                })
                .await
                .map_err(|join_err| {
                    BusError::Publish(format!(
                        "IPC publish blocking task panicked for subscriber '{}': {}",
                        subscriber.name, join_err
                    ))
                })?;

            match write_result {
                Ok(WriteOutcome::Wrote) => {
                    debug!(
                        subscriber = %subscriber.name,
                        routing_key = %routing_key,
                        "Published event to pipe"
                    );
                }
                Ok(WriteOutcome::NoReader) => {
                    // Bring-up race; leave subscriber in the routing list.
                }
                Err(e) => match decide_write_error(&e) {
                    WriteErrorOutcome::Prune => {
                        // H-04: BrokenPipe means the subscriber's reader
                        // closed its end of the FIFO. Treat as an
                        // unannounced unregister: drop the subscriber from
                        // the routing list and continue with the rest of
                        // the fan-out. Surviving subscribers still get
                        // their copies.
                        debug!(
                            subscriber = %subscriber.name,
                            "Subscriber pipe closed (broken pipe); pruning from routing list"
                        );
                        to_prune.push(subscriber.name.clone());
                    }
                    WriteErrorOutcome::Err => {
                        // Apply any prunes we accumulated before the failure
                        // so we don't keep targeting dead subscribers on the
                        // next publish.
                        drop(_write_guard);
                        self.prune_subscribers(&to_prune).await;
                        return Err(BusError::Publish(format!(
                            "Failed to write to IPC pipe for subscriber '{}': {}",
                            subscriber.name, e
                        )));
                    }
                },
            }
            // _write_guard drops here, releasing the per-pipe mutex.
        }

        // Apply any pending prunes in a single write-lock acquisition.
        // Idempotent: a concurrent caller that already pruned the same
        // name is safe.
        self.prune_subscribers(&to_prune).await;

        Ok(PublishResult::default())
    }

    /// Subscribe to events from the named pipe.
    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        if self.config.subscriber_name.is_none() {
            return Err(BusError::Subscribe(
                "Cannot subscribe without subscriber_name".to_string(),
            ));
        }

        let count = {
            let mut handlers = self.handlers.write().await;
            handlers.push(handler);
            handlers.len()
        };

        info!(handler_count = count, "Handler subscribed to IPC bus");

        Ok(())
    }

    /// Start consuming from the pipe (IPC requires explicit start).
    async fn start_consuming(&self) -> Result<()> {
        IpcEventBus::start_consuming(self).await
    }

    /// Create a new subscriber bus sharing the same base path.
    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        let domains = match domain_filter {
            Some(d) => vec![d.to_string()],
            None => vec![],
        };
        let config = IpcConfig::subscriber(&self.config.base_path, name, domains);
        Ok(Arc::new(IpcEventBus::new(config)))
    }
}

/// Extract the highest sequence number from an EventBook's pages.
fn max_page_sequence(book: &EventBook) -> Option<u32> {
    book.pages.iter().map(|p| p.sequence_num()).max()
}

/// Clear `O_NONBLOCK` on an open file descriptor.
///
/// The publish path opens the FIFO with `O_NONBLOCK` so the `open()` call
/// doesn't hang when no reader is attached (instead returns `ENXIO`). Once
/// the open succeeds we know a reader IS attached on the *kernel* side, and
/// we want subsequent `write_all` calls to BLOCK on a full pipe (back
/// pressure) rather than returning `WouldBlock` mid-frame. Returning
/// mid-frame is what created the C-09 "half-written frame" desync.
fn clear_nonblock(file: &File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: fd is owned by `file` for the duration of this call.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let new_flags = flags & !libc::O_NONBLOCK;
    // SAFETY: same as above.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, new_flags) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Domain filter helper for testing - checks if routing_key matches domain list.
///
/// Returns true if:
/// - domains is empty (accept all)
/// - domains contains "#" (wildcard)
/// - domains contains the routing_key
fn matches_domain_filter(routing_key: &str, domains: &[String]) -> bool {
    domains.is_empty() || domains.iter().any(|d| d == "#" || d == routing_key)
}

/// Outcome of a successful per-subscriber publish attempt against a FIFO.
///
/// `Wrote` is the happy path. `NoReader` is the orchestration bring-up race
/// — open returned `ENXIO` because no reader is attached yet; not an error,
/// and not a prune trigger.
#[derive(Debug, PartialEq, Eq)]
enum WriteOutcome {
    Wrote,
    NoReader,
}

/// Outcome of classifying a per-subscriber `write_all` failure.
///
/// See H-04: `BrokenPipe` means the subscriber's reader closed its
/// end of the FIFO — the broker supports register/unregister, so this
/// is semantically an unannounced unregister and the publisher should
/// drop the subscriber from its routing list. Any other I/O error is
/// a transport-level failure that must propagate up to the caller as
/// `BusError::Publish`.
#[derive(Debug, PartialEq, Eq)]
enum WriteErrorOutcome {
    /// Drop the subscriber from the routing list and continue.
    Prune,
    /// Propagate the error to the caller.
    Err,
}

/// Classify an `io::Error` from `write_all` to a subscriber FIFO.
///
/// `BrokenPipe` (EPIPE) → `Prune` — the subscriber's reader closed,
/// treat as an unannounced unregister; all *surviving* subscribers
/// still receive this publish, so the call returns `Ok` from the
/// caller's perspective.
///
/// Everything else → `Err` — a real transport failure (permission
/// denied, disk full on the FIFO's filesystem, interrupted by a
/// signal we can't recover from cleanly, etc.). These propagate to
/// the caller so they can retry, dead-letter, or abort.
fn decide_write_error(err: &std::io::Error) -> WriteErrorOutcome {
    if err.kind() == std::io::ErrorKind::BrokenPipe {
        WriteErrorOutcome::Prune
    } else {
        WriteErrorOutcome::Err
    }
}

#[cfg(test)]
#[path = "client.test.rs"]
mod tests;
