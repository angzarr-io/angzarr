//! Coordinator outbox: durable at-least-once delivery of coordinator
//! obligations.
//!
//! A coordinator records an obligation here before it acknowledges the work
//! that raised it, then delivers it at least once:
//!
//! - a process manager's command that failed transiently after the PM's
//!   events were persisted (the handler cannot be re-run to re-produce it);
//! - a compensation Notification (compensation_delivery.feature): a
//!   RejectionNotification for a rejected saga/PM command, addressed to the
//!   command's `angzarr_deferred.source`, or a Compensate for a reaction
//!   command a failed CASCADE_ERROR_COMPENSATE request had executed,
//!   addressed to that command's target.
//!
//! Every entry is a CommandBook: the command itself, or the Notification
//! delivery envelope (see [`crate::orchestration::compensation`]).
//!
//! # Delivery
//!
//! [`Outbox::submit`] records an entry and attempts it at once;
//! [`Outbox::drain_due`] (run periodically by [`Outbox::spawn_drain`])
//! retries pending entries whose backoff has elapsed. Failed attempts back
//! off exponentially ([`RetryPolicy`]); once the attempt budget is spent the
//! entry is dead-lettered and closed. Some failures end delivery at once:
//! a permanent command rejection (which also raises the command's
//! RejectionNotification), and an UNIMPLEMENTED answer to a notification
//! (the target has no handler for it).
//!
//! # Durability
//!
//! Entries are written to an [`OutboxLog`] before they count as recorded.
//! [`EventStoreOutboxLog`] keeps them in the coordinator's event store, so
//! [`Outbox::recover`] reloads open obligations after a restart;
//! [`MemoryOutboxLog`] keeps nothing (tests, and coordinators without
//! storage).

pub mod config;
pub mod delivery;
pub mod log;
#[cfg(test)]
pub(crate) mod testing;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher};
use crate::proto::CommandBook;
use crate::proto_ext::{AngzarrDeferredSequenceExt, CommandBookExt, CoverExt, PageHeaderExt};
use crate::storage::ProvenanceKind;

pub use config::{OutboxConfig, OutboxOverride};
pub use delivery::{
    CompensationSender, CoordinatorDeliverer, DiscoveryCompensationSender, RevocationHandling,
};
pub use log::{EventStoreOutboxLog, MemoryOutboxLog, OutboxLog};

/// Error raised by an outbox.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    /// The outbox log could not be read or written.
    #[error("outbox log error: {0}")]
    Log(String),
    /// The book is not something the outbox can deliver.
    #[error("outbox entry rejected: {0}")]
    InvalidEntry(String),
}

/// One obligation awaiting delivery.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxEntry {
    /// Stable key: the kind plus the book's provenance tuple. Recording the
    /// same obligation twice collapses onto one entry.
    pub key: String,
    /// What the book is: a command, or a notification envelope.
    pub kind: ProvenanceKind,
    /// The command, or the Notification delivery envelope.
    pub book: CommandBook,
    /// Delivery attempts made so far.
    pub attempts: u32,
    /// Error of the most recent failed attempt.
    pub last_error: String,
}

impl OutboxEntry {
    /// An entry delivering `command` to its target aggregate.
    pub fn command(command: CommandBook) -> Self {
        Self::of_kind(ProvenanceKind::Command, command)
    }

    /// An entry delivering a Notification envelope to its target's
    /// HandleCompensation. Fails when the book is not an envelope.
    pub fn notification(envelope: CommandBook) -> Result<Self, OutboxError> {
        let kind =
            crate::orchestration::compensation::notification_kind(&envelope).ok_or_else(|| {
                OutboxError::InvalidEntry("not a Notification delivery envelope".to_string())
            })?;
        Ok(Self::of_kind(kind, envelope))
    }

    /// Rebuild an entry from a recorded book: an envelope is a
    /// notification, anything else a command.
    pub fn from_recorded(book: CommandBook) -> Self {
        match crate::orchestration::compensation::notification_kind(&book) {
            Some(kind) => Self::of_kind(kind, book),
            None => Self::command(book),
        }
    }

    fn of_kind(kind: ProvenanceKind, book: CommandBook) -> Self {
        Self {
            key: entry_key(kind, &book),
            kind,
            book,
            attempts: 0,
            last_error: String::new(),
        }
    }

    /// Whether this entry delivers a notification (not a command).
    pub fn is_notification(&self) -> bool {
        self.kind != ProvenanceKind::Command
    }
}

/// The stable key of an obligation: the kind, then the book's deferred
/// provenance tuple — `{source_key}#{source_component}#{command_index}`,
/// the destinations' idempotency key — or, for a book without one, its
/// target and sequence.
pub fn entry_key(kind: ProvenanceKind, book: &CommandBook) -> String {
    let deferred = book
        .first_command()
        .and_then(|page| page.header.as_ref())
        .and_then(|header| header.angzarr_deferred());
    match deferred {
        Some(deferred) => format!(
            "{}#{}#{}#{}",
            kind.as_str(),
            deferred.idempotency_key(),
            deferred.source_component,
            deferred.command_index,
        ),
        None => format!(
            "{}#{}:{}:{}:{}",
            kind.as_str(),
            book.domain(),
            book.root_id_hex().unwrap_or_default(),
            book.correlation_id(),
            book.command_sequence(),
        ),
    }
}

/// Retry schedule of an outbox.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Delivery attempts (the first included) before an entry is
    /// dead-lettered.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_backoff: Duration,
    /// Upper bound of any delay.
    pub max_backoff: Duration,
    /// Randomize each delay within its upper half.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(30),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Delay after the `attempt`-th failed attempt (1-based): the initial
    /// backoff doubled per further attempt, capped at the maximum. With
    /// jitter the delay is drawn from the upper half of that value, so a
    /// later retry never waits less than an earlier one (until the cap).
    pub fn delay(&self, attempt: u32) -> Duration {
        let doublings = attempt.saturating_sub(1).min(31);
        let base = self
            .initial_backoff
            .saturating_mul(1u32 << doublings)
            .min(self.max_backoff);
        if !self.jitter {
            return base;
        }
        let half = base / 2;
        let fraction = (uuid::Uuid::new_v4().as_u128() % 1_000_001) as f64 / 1_000_000.0;
        half + half.mul_f64(fraction)
    }
}

/// Outcome of one delivery attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryResult {
    /// The target accepted the book.
    Delivered,
    /// A transient failure; retry after the backoff.
    Retryable(String),
    /// A failure retrying cannot fix.
    Rejected {
        /// gRPC code of the failure.
        code: tonic::Code,
        /// Error message.
        message: String,
        /// Machine rejection code (ErrorInfo.reason); empty when none.
        error_code: String,
    },
}

/// Delivers outbox entries to their targets.
#[async_trait]
pub trait OutboxDeliverer: Send + Sync {
    /// Attempt one delivery of `entry`.
    async fn deliver(&self, entry: &OutboxEntry) -> DeliveryResult;
}

/// Tally of one drain pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainStats {
    /// Entries delivered and closed.
    pub delivered: u32,
    /// Entries that failed again and stay pending.
    pub retried: u32,
    /// Entries dead-lettered and closed.
    pub dead_lettered: u32,
}

impl DrainStats {
    fn add(&mut self, other: DrainStats) {
        self.delivered += other.delivered;
        self.retried += other.retried;
        self.dead_lettered += other.dead_lettered;
    }
}

struct Pending {
    entry: OutboxEntry,
    next_due: Instant,
    in_flight: bool,
}

/// A coordinator's outbox: a durable log of obligations plus the working set
/// of those still open.
pub struct Outbox {
    name: String,
    component_type: String,
    log: Arc<dyn OutboxLog>,
    deliverer: Arc<dyn OutboxDeliverer>,
    dead_letters: Option<Arc<dyn DeadLetterPublisher>>,
    policy: RetryPolicy,
    pending: Mutex<HashMap<String, Pending>>,
}

impl Outbox {
    /// An outbox named after its coordinator component (`name`, of
    /// `component_type` "aggregate" / "saga" / "process_manager"; both appear
    /// on its dead letters).
    pub fn new(
        name: impl Into<String>,
        component_type: impl Into<String>,
        log: Arc<dyn OutboxLog>,
        deliverer: Arc<dyn OutboxDeliverer>,
        policy: RetryPolicy,
    ) -> Self {
        Self {
            name: name.into(),
            component_type: component_type.into(),
            log,
            deliverer,
            dead_letters: None,
            policy,
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// A coordinator's running outbox: configured from `config` (the
    /// schedule for `name`), dead-lettering to `dead_letters`, with the
    /// obligations `log` still holds open reloaded and a drain loop started.
    pub async fn start(
        name: &str,
        component_type: &str,
        log: Arc<dyn OutboxLog>,
        deliverer: Arc<dyn OutboxDeliverer>,
        config: &OutboxConfig,
        dead_letters: Arc<dyn DeadLetterPublisher>,
    ) -> Result<Arc<Self>, OutboxError> {
        let outbox = Arc::new(
            Self::new(name, component_type, log, deliverer, config.policy(name))
                .with_dead_letters(dead_letters),
        );
        let recovered = outbox.recover().await?;
        if recovered > 0 {
            tracing::info!(outbox = %name, recovered, "outbox recovered open obligations");
        }
        outbox.spawn_drain(config.drain_interval());
        Ok(outbox)
    }

    /// Dead-letter exhausted and permanently failed entries to `publisher`.
    pub fn with_dead_letters(mut self, publisher: Arc<dyn DeadLetterPublisher>) -> Self {
        self.dead_letters = Some(publisher);
        self
    }

    /// The retry schedule.
    pub fn policy(&self) -> RetryPolicy {
        self.policy
    }

    /// Load the obligations the log still holds open (after a restart).
    /// Returns how many were loaded; they are due at once.
    pub async fn recover(&self) -> Result<usize, OutboxError> {
        let open = self.log.open_entries().await?;
        let count = open.len();
        let now = Instant::now();
        let mut pending = self.pending.lock().await;
        for entry in open {
            pending.entry(entry.key.clone()).or_insert(Pending {
                entry,
                next_due: now,
                in_flight: false,
            });
        }
        Ok(count)
    }

    /// Record an obligation durably. Idempotent on the entry key: recording
    /// an obligation already open keeps its progress.
    pub async fn record(&self, entry: OutboxEntry) -> Result<(), OutboxError> {
        if self.pending.lock().await.contains_key(&entry.key) {
            return Ok(());
        }
        self.log.append_record(&entry).await?;
        self.pending
            .lock()
            .await
            .entry(entry.key.clone())
            .or_insert(Pending {
                entry,
                next_due: Instant::now(),
                in_flight: false,
            });
        Ok(())
    }

    /// Record an obligation, then attempt its delivery once. A failed
    /// attempt leaves it pending for the drain loop; only a failure to
    /// record is an error.
    pub async fn submit(&self, entry: OutboxEntry) -> Result<DrainStats, OutboxError> {
        let key = entry.key.clone();
        self.record(entry).await?;
        let Some(entry) = self.claim(&key, None).await else {
            return Ok(DrainStats::default());
        };
        self.attempt(entry, Instant::now()).await
    }

    /// Attempt every open entry whose backoff has elapsed at `now`.
    pub async fn drain_due(&self, now: Instant) -> Result<DrainStats, OutboxError> {
        let due: Vec<String> = {
            let pending = self.pending.lock().await;
            pending
                .iter()
                .filter(|(_, p)| !p.in_flight && p.next_due <= now)
                .map(|(key, _)| key.clone())
                .collect()
        };
        let mut stats = DrainStats::default();
        for key in due {
            if let Some(entry) = self.claim(&key, Some(now)).await {
                stats.add(self.attempt(entry, now).await?);
            }
        }
        Ok(stats)
    }

    /// Attempt every entry due now.
    pub async fn drain_once(&self) -> Result<DrainStats, OutboxError> {
        self.drain_due(Instant::now()).await
    }

    /// Keys of the obligations still open.
    pub async fn open_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.pending.lock().await.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// The open entry under `key`, if any.
    pub async fn open_entry(&self, key: &str) -> Option<OutboxEntry> {
        self.pending.lock().await.get(key).map(|p| p.entry.clone())
    }

    /// When the open entry under `key` is next due.
    pub async fn next_due(&self, key: &str) -> Option<Instant> {
        self.pending.lock().await.get(key).map(|p| p.next_due)
    }

    /// Run [`Self::drain_once`] every `interval` until the task is dropped.
    pub fn spawn_drain(self: &Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        let outbox = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                match outbox.drain_once().await {
                    Ok(stats) if stats != DrainStats::default() => debug!(
                        outbox = %outbox.name,
                        delivered = stats.delivered,
                        retried = stats.retried,
                        dead_lettered = stats.dead_lettered,
                        "outbox drain pass"
                    ),
                    Ok(_) => {}
                    Err(e) => warn!(outbox = %outbox.name, error = %e, "outbox drain failed"),
                }
            }
        })
    }

    /// Mark the entry under `key` in flight and return it; `None` when it is
    /// absent, already in flight, or (with `due_at`) not yet due.
    async fn claim(&self, key: &str, due_at: Option<Instant>) -> Option<OutboxEntry> {
        let mut pending = self.pending.lock().await;
        let slot = pending.get_mut(key)?;
        if slot.in_flight || due_at.is_some_and(|now| slot.next_due > now) {
            return None;
        }
        slot.in_flight = true;
        Some(slot.entry.clone())
    }

    async fn attempt(&self, entry: OutboxEntry, now: Instant) -> Result<DrainStats, OutboxError> {
        let result = self.deliverer.deliver(&entry).await;
        let outcome = self.settle(&entry, result, now).await;
        if outcome.is_err() {
            if let Some(slot) = self.pending.lock().await.get_mut(&entry.key) {
                slot.in_flight = false;
            }
        }
        outcome
    }

    async fn settle(
        &self,
        entry: &OutboxEntry,
        result: DeliveryResult,
        now: Instant,
    ) -> Result<DrainStats, OutboxError> {
        let attempts = entry.attempts + 1;
        let (message, permanent, rejection_code) = match result {
            DeliveryResult::Delivered => {
                debug!(outbox = %self.name, key = %entry.key, "outbox entry delivered");
                self.close(&entry.key).await?;
                return Ok(DrainStats {
                    delivered: 1,
                    ..Default::default()
                });
            }
            DeliveryResult::Retryable(message) => (message, false, String::new()),
            DeliveryResult::Rejected {
                code,
                message,
                error_code,
            } => {
                // A notification target without a handler answers
                // UNIMPLEMENTED; any other rejection of a notification is
                // retried. A command's rejection is final.
                let permanent = !entry.is_notification() || code == tonic::Code::Unimplemented;
                (message, permanent, error_code)
            }
        };

        if !permanent && attempts < self.policy.max_attempts {
            self.log.append_attempt(&entry.key, &message).await?;
            let mut pending = self.pending.lock().await;
            if let Some(slot) = pending.get_mut(&entry.key) {
                slot.entry.attempts = attempts;
                slot.entry.last_error = message;
                slot.next_due = now + self.policy.delay(attempts);
                slot.in_flight = false;
            }
            return Ok(DrainStats {
                retried: 1,
                ..Default::default()
            });
        }

        error!(
            outbox = %self.name,
            key = %entry.key,
            attempts,
            error = %message,
            "outbox entry dead-lettered"
        );
        self.dead_letter(entry, attempts, &message, !permanent)
            .await;
        if permanent && !entry.is_notification() {
            self.raise_rejection(entry, &message, &rejection_code)
                .await?;
        }
        self.close(&entry.key).await?;
        Ok(DrainStats {
            dead_lettered: 1,
            ..Default::default()
        })
    }

    /// A command rejected on redelivery reaches its source like any other
    /// rejection: record its RejectionNotification.
    async fn raise_rejection(
        &self,
        entry: &OutboxEntry,
        reason: &str,
        code: &str,
    ) -> Result<(), OutboxError> {
        let Some(envelope) =
            crate::orchestration::compensation::rejection_envelope(&entry.book, reason, code)
        else {
            return Ok(());
        };
        self.record(OutboxEntry::notification(envelope)?).await
    }

    async fn dead_letter(&self, entry: &OutboxEntry, attempts: u32, error: &str, transient: bool) {
        let Some(publisher) = &self.dead_letters else {
            return;
        };
        let dead_letter = if entry.is_notification() {
            AngzarrDeadLetter::from_compensation_delivery_failure(
                &entry.book,
                attempts,
                error,
                &self.name,
                &self.component_type,
            )
        } else {
            AngzarrDeadLetter::from_pm_command_rejection(
                &entry.book,
                error,
                attempts,
                transient,
                &self.name,
            )
        };
        if let Err(e) = publisher.publish(dead_letter).await {
            error!(outbox = %self.name, key = %entry.key, error = %e, "failed to publish outbox dead letter");
        }
    }

    async fn close(&self, key: &str) -> Result<(), OutboxError> {
        self.log.append_close(key).await?;
        self.pending.lock().await.remove(key);
        Ok(())
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
