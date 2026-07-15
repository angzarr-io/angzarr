//! Command outbox for at-least-once redelivery of PM commands that fail
//! transiently AFTER the PM persist boundary (C04).
//!
//! # Why this exists
//!
//! `execute_pm_commands` dispatches PM-produced commands *after* the PM's own
//! event book is persisted — the module doc calls this "the point of no
//! return". A command cannot be retried in place there: re-running the PM
//! handler would re-emit (and duplicate) the already-persisted PM events. So
//! before this outbox, a transient `CommandOutcome::Retryable` on a
//! non-Decision command fell into a warn-only `else` and was silently dropped
//! — the destination aggregate never saw the command, the workflow stalled
//! forever, and no operator signal was emitted (finding #5).
//!
//! # Design (reviewer decision: outbox, not plain DLQ)
//!
//! The failed command is captured into a [`CommandOutbox`]. A drain loop
//! ([`drain_once`]) redelivers pending entries via the same
//! [`CommandExecutor`] transport used for the in-line dispatch (the
//! transport-backed command path C13 formalizes). Redelivery repeats up to a
//! `max_attempts` budget; only on exhaustion (or a *permanent* `Rejected`
//! during drain) does the entry move to the DLQ as the terminal sink. This
//! gives at-least-once delivery with auto-recovery from transient blips,
//! rather than parking every transient failure on a human.
//!
//! # Idempotency / dedup
//!
//! Entries are keyed by [`OutboxEntry::dedup_key`], derived from the command's
//! stamped `angzarr_deferred` provenance (the existing O1 idempotency tuple:
//! source key + source component + command index). Re-enqueueing the same
//! post-persist command — e.g. when the PM handler re-runs after an H-13
//! persist restart — collapses onto the existing entry instead of double
//! booking, and destinations dedupe redeliveries on the same stamped key.
//!
//! # Persistence sub-decision (flagged)
//!
//! [`CommandOutbox`] abstracts persistence. The shipped
//! [`InMemoryCommandOutbox`] gives at-least-once *within a process lifetime*.
//! A durable impl reusing PM storage (append outbox records to the PM's own
//! correlation-id-rooted event stream, so drain resumes after a restart) is
//! the recommended production default — deferred, coordinated with C13.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::{debug, error};

use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher};
use crate::proto::{CommandBook, SyncMode};
use crate::proto_ext::{AngzarrDeferredSequenceExt, CommandBookExt, CoverExt, PageHeaderExt};

use crate::orchestration::command::{CommandExecutor, CommandOutcome};

/// Error raised by a [`CommandOutbox`] backend.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    /// The underlying storage/queue failed.
    #[error("outbox backend error: {0}")]
    Backend(String),
}

/// A command captured for at-least-once redelivery after the PM persist
/// boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxEntry {
    /// Stable dedup key derived from the command's `angzarr_deferred`
    /// provenance. Two enqueues of the same post-persist command collapse to
    /// one entry.
    pub dedup_key: String,
    /// The full command to redeliver — carries target domain, sequence
    /// stamping, payload, and correlation id. Replayable as-is.
    pub command: CommandBook,
    /// Redelivery attempts already made by the drain loop. `0` at first
    /// enqueue: the in-line dispatch that failed is NOT counted here — the
    /// drainer owns the retry budget.
    pub attempts: u32,
    /// The most recent transient error, kept for DLQ context on exhaustion.
    pub last_error: String,
}

impl OutboxEntry {
    /// Build an entry for a command that failed its in-line post-persist
    /// dispatch and must be redelivered.
    pub fn for_redelivery(command: &CommandBook, reason: &str) -> Self {
        Self {
            dedup_key: dedup_key_for(command),
            command: command.clone(),
            attempts: 0,
            last_error: reason.to_string(),
        }
    }
}

/// Compute the stable dedup key for a command.
///
/// Preference order:
/// 1. The command's `angzarr_deferred` provenance — the O1 idempotency tuple
///    `{source_key}#{source_component}#{command_index}`. This is what
///    `execute_pm_commands` stamps on every PM command that does not carry an
///    explicit destination sequence, and it is exactly the key destinations
///    dedupe on, so redeliveries are idempotent end to end.
/// 2. Fallback for commands stamped with an explicit destination sequence
///    (D-5) or no header at all: `{domain}:{correlation_id}:{sequence}`.
///
/// Two distinct commands never collide (command_index disambiguates siblings
/// of one invocation; domain/correlation/sequence disambiguates the fallback).
pub fn dedup_key_for(command: &CommandBook) -> String {
    if let Some(deferred) = command
        .first_command()
        .and_then(|page| page.header.as_ref())
        .and_then(|header| header.angzarr_deferred())
    {
        return format!(
            "{}#{}#{}",
            deferred.idempotency_key(),
            deferred.source_component,
            deferred.command_index,
        );
    }
    format!(
        "{}:{}:{}",
        command.domain(),
        command.correlation_id(),
        command.command_sequence(),
    )
}

/// Durable (or in-memory) store of commands awaiting redelivery.
///
/// Implementations MUST be idempotent on [`OutboxEntry::dedup_key`]: a second
/// `enqueue` of an already-present key is a no-op (it must NOT reset the
/// attempt counter or overwrite the entry).
#[async_trait]
pub trait CommandOutbox: Send + Sync {
    /// Capture a command for redelivery. Idempotent on `dedup_key`.
    async fn enqueue(&self, entry: OutboxEntry) -> Result<(), OutboxError>;

    /// Return every entry currently awaiting redelivery.
    async fn pending(&self) -> Result<Vec<OutboxEntry>, OutboxError>;

    /// Record a failed redelivery attempt: increment `attempts` and store the
    /// latest error. No-op if the key is absent (already settled).
    async fn record_attempt(&self, dedup_key: &str, error: &str) -> Result<(), OutboxError>;

    /// Remove a settled entry (delivered, or moved to the DLQ). Terminal.
    async fn remove(&self, dedup_key: &str) -> Result<(), OutboxError>;
}

/// In-memory [`CommandOutbox`]. At-least-once within a process lifetime; entries
/// are lost on process exit (see module persistence note).
#[derive(Default)]
pub struct InMemoryCommandOutbox {
    entries: Mutex<HashMap<String, OutboxEntry>>,
}

impl InMemoryCommandOutbox {
    /// Create an empty outbox.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CommandOutbox for InMemoryCommandOutbox {
    async fn enqueue(&self, entry: OutboxEntry) -> Result<(), OutboxError> {
        let mut guard = self.entries.lock().await;
        // Idempotent: keep the existing entry (and its attempt count) on a
        // duplicate key so a handler re-run never resets progress.
        guard.entry(entry.dedup_key.clone()).or_insert(entry);
        Ok(())
    }

    async fn pending(&self) -> Result<Vec<OutboxEntry>, OutboxError> {
        Ok(self.entries.lock().await.values().cloned().collect())
    }

    async fn record_attempt(&self, dedup_key: &str, error: &str) -> Result<(), OutboxError> {
        if let Some(entry) = self.entries.lock().await.get_mut(dedup_key) {
            entry.attempts += 1;
            entry.last_error = error.to_string();
        }
        Ok(())
    }

    async fn remove(&self, dedup_key: &str) -> Result<(), OutboxError> {
        self.entries.lock().await.remove(dedup_key);
        Ok(())
    }
}

/// Outcome tally of one [`drain_once`] pass. Useful for metrics and tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainStats {
    /// Entries redelivered successfully and removed.
    pub delivered: u32,
    /// Entries that failed transiently again and stay pending for a later pass.
    pub retried: u32,
    /// Entries moved to the DLQ (budget exhausted or permanently rejected) and
    /// removed.
    pub dead_lettered: u32,
}

/// Attempt one redelivery pass over every pending outbox entry.
///
/// Per entry:
/// - `Success` -> remove (delivered).
/// - `Retryable` and the attempt budget is NOT yet spent -> record the attempt,
///   leave pending for the next pass (at-least-once redelivery).
/// - `Retryable` and the budget IS spent (`attempts + 1 >= max_attempts`) ->
///   publish to the DLQ (`is_transient = true`) and remove (terminal).
/// - `Rejected` -> a permanent rejection that redelivery cannot fix; publish to
///   the DLQ (`is_transient = false`) and remove immediately.
///
/// `dlq` is the terminal sink; when `None`, exhausted/rejected entries are
/// dropped from the outbox with only an error log (no publisher wired).
pub async fn drain_once(
    outbox: &dyn CommandOutbox,
    executor: &dyn CommandExecutor,
    dlq: Option<&Arc<dyn DeadLetterPublisher>>,
    component: &str,
    max_attempts: u32,
    sync_mode: SyncMode,
) -> Result<DrainStats, OutboxError> {
    let pending = outbox.pending().await?;
    let mut stats = DrainStats::default();

    for entry in pending {
        match executor.execute(entry.command.clone(), sync_mode).await {
            CommandOutcome::Success(_) => {
                debug!(dedup_key = %entry.dedup_key, "outbox command redelivered");
                outbox.remove(&entry.dedup_key).await?;
                stats.delivered += 1;
            }
            CommandOutcome::Retryable { reason, .. } => {
                let attempts_after = entry.attempts + 1;
                if attempts_after >= max_attempts {
                    error!(
                        dedup_key = %entry.dedup_key,
                        attempts = attempts_after,
                        error = %reason,
                        "outbox command exhausted redelivery budget; moving to DLQ"
                    );
                    publish_outbox_dlq(dlq, component, &entry.command, &reason, attempts_after, true)
                        .await;
                    outbox.remove(&entry.dedup_key).await?;
                    stats.dead_lettered += 1;
                } else {
                    outbox.record_attempt(&entry.dedup_key, &reason).await?;
                    stats.retried += 1;
                }
            }
            CommandOutcome::Rejected { message, .. } => {
                error!(
                    dedup_key = %entry.dedup_key,
                    error = %message,
                    "outbox command permanently rejected on redelivery; moving to DLQ"
                );
                publish_outbox_dlq(dlq, component, &entry.command, &message, entry.attempts, false)
                    .await;
                outbox.remove(&entry.dedup_key).await?;
                stats.dead_lettered += 1;
            }
        }
    }

    Ok(stats)
}

/// Publish an outbox entry to the DLQ terminal sink. No-op when no publisher is
/// wired.
async fn publish_outbox_dlq(
    dlq: Option<&Arc<dyn DeadLetterPublisher>>,
    component: &str,
    command: &CommandBook,
    error: &str,
    retry_count: u32,
    is_transient: bool,
) {
    let Some(publisher) = dlq else {
        return;
    };
    let dead_letter =
        AngzarrDeadLetter::from_pm_command_rejection(command, error, retry_count, is_transient, component);
    let domain = command.domain().to_string();
    if let Err(e) = publisher.publish(dead_letter).await {
        error!(%domain, error = %e, "failed to publish outbox-exhaustion DLQ entry");
    }
}

#[cfg(test)]
#[path = "outbox.test.rs"]
mod tests;
