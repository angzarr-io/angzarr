//! Tests for IPC bus checkpoint/position tracking.
//!
//! Checkpoints track the last processed sequence per (domain, root) pair.
//! This enables at-least-once delivery with deduplication: if a subscriber
//! crashes and restarts, it can skip events already processed.
//!
//! Key behaviors:
//! - Higher sequences replace lower ones (high-water mark)
//! - should_process() returns false for already-processed events
//! - State persists to disk and survives restarts
//! - Disabled mode makes all operations no-ops
//!
//! Without checkpointing, restarts would reprocess all events from the
//! beginning, potentially causing duplicate side effects.

use super::*;
use tempfile::TempDir;

fn test_config(dir: &TempDir, name: &str) -> CheckpointConfig {
    CheckpointConfig {
        file_path: dir.path().join(format!("checkpoint-{}.json", name)),
        flush_interval: Duration::from_millis(100),
        enabled: true,
    }
}

// ============================================================================
// Core Checkpoint Operations
// ============================================================================

/// Checkpoint tracks high-water mark per (domain, root).
///
/// Lower sequence updates are ignored — we only advance, never regress.
/// This models "I've seen up to sequence N" semantics.
#[tokio::test]
async fn test_checkpoint_get_set() {
    let dir = TempDir::new().unwrap();
    let checkpoint = Checkpoint::new(test_config(&dir, "test"));

    let root = uuid::Uuid::new_v4().as_bytes().to_vec();

    // Initially none
    assert_eq!(checkpoint.get("orders", &root).await, None);

    // Update
    checkpoint.update("orders", &root, 5).await;
    assert_eq!(checkpoint.get("orders", &root).await, Some(5));

    // Update to higher value
    checkpoint.update("orders", &root, 10).await;
    assert_eq!(checkpoint.get("orders", &root).await, Some(10));

    // Lower value ignored
    checkpoint.update("orders", &root, 7).await;
    assert_eq!(checkpoint.get("orders", &root).await, Some(10));
}

/// should_process enables deduplication during message handling.
///
/// Events with sequence <= checkpoint are skipped. This prevents duplicate
/// processing after subscriber restart when replaying from a pipe.
#[tokio::test]
async fn test_checkpoint_should_process() {
    let dir = TempDir::new().unwrap();
    let checkpoint = Checkpoint::new(test_config(&dir, "test"));

    let root = uuid::Uuid::new_v4().as_bytes().to_vec();

    // All events should process initially
    assert!(checkpoint.should_process("orders", &root, 1).await);
    assert!(checkpoint.should_process("orders", &root, 5).await);

    // Mark sequence 5 as processed
    checkpoint.update("orders", &root, 5).await;

    // Events <= 5 should not process
    assert!(!checkpoint.should_process("orders", &root, 1).await);
    assert!(!checkpoint.should_process("orders", &root, 5).await);

    // Events > 5 should process
    assert!(checkpoint.should_process("orders", &root, 6).await);
    assert!(checkpoint.should_process("orders", &root, 10).await);
}

// ============================================================================
// Persistence Tests
// ============================================================================

/// Checkpoint state survives process restarts.
///
/// flush() writes to disk; load() restores on startup. Without persistence,
/// subscribers would lose their position on crash and reprocess everything.
#[tokio::test]
async fn test_checkpoint_persistence() {
    let dir = TempDir::new().unwrap();
    let config = test_config(&dir, "persist");

    let root = uuid::Uuid::new_v4().as_bytes().to_vec();

    // Create and populate checkpoint
    {
        let checkpoint = Checkpoint::new(config.clone());
        checkpoint.update("orders", &root, 10).await;
        checkpoint.update("products", &root, 20).await;
        checkpoint.flush().await.unwrap();
    }

    // Load in new instance
    {
        let checkpoint = Checkpoint::new(config);
        checkpoint.load().await.unwrap();
        assert_eq!(checkpoint.get("orders", &root).await, Some(10));
        assert_eq!(checkpoint.get("products", &root).await, Some(20));
    }
}

// ============================================================================
// Configuration Tests
// ============================================================================

/// Disabled checkpoint makes all operations no-ops.
///
/// Used when deduplication isn't needed (e.g., idempotent handlers) or
/// when you want to force reprocessing of all events.
#[tokio::test]
async fn test_checkpoint_disabled() {
    let checkpoint = Checkpoint::new(CheckpointConfig::disabled());

    let root = uuid::Uuid::new_v4().as_bytes().to_vec();

    // All operations are no-ops when disabled
    assert_eq!(checkpoint.get("orders", &root).await, None);
    checkpoint.update("orders", &root, 10).await;
    assert_eq!(checkpoint.get("orders", &root).await, None);
    assert!(checkpoint.should_process("orders", &root, 1).await);
}

/// Stats report checkpoint health metrics.
///
/// position_count shows how many (domain, root) pairs are tracked.
/// dirty indicates unsaved changes pending flush — after H-05, `update()`
/// flushes on every advancement, so the post-update `dirty` invariant is
/// `false` (the on-disk state is up to date). The earlier `true` assertion
/// reflected the pre-H-05 batched-flush semantics.
#[tokio::test]
async fn test_checkpoint_stats() {
    let dir = TempDir::new().unwrap();
    let checkpoint = Checkpoint::new(test_config(&dir, "stats"));

    let root1 = uuid::Uuid::new_v4().as_bytes().to_vec();
    let root2 = uuid::Uuid::new_v4().as_bytes().to_vec();

    checkpoint.update("orders", &root1, 5).await;
    checkpoint.update("products", &root2, 10).await;

    let stats = checkpoint.stats().await;
    assert_eq!(stats.position_count, 2);
    // Post-H-05: update() flushes synchronously, so dirty is cleared.
    assert!(!stats.dirty);
}

// ============================================================================
// H-05: Concurrent update atomicity + crash-recovery durability
// ============================================================================

/// H-05 (a): Concurrent `update()` calls must not lose the high-water mark.
///
/// The bug: `update()` does a `get()` under a read-lock, compares, then takes
/// a write-lock to `set()`. Between those two locks a writer that observed a
/// stale `current` can overwrite a higher value committed by a concurrent
/// task. The end-state then reflects an arbitrary write order rather than
/// `max(seqs)`.
///
/// We spawn N tasks each pushing a distinct sequence for the SAME
/// (domain, root) key. After all complete, the stored value must equal
/// `max(seqs)`. Without locking across read-then-write this assertion can
/// fail because a slow task observing `current = 0` can come in after a
/// faster task wrote `N-1` and clobber it back to a lower sequence.
///
/// Note: this is a stress-style test — the race is timing-dependent, but
/// with enough tasks/iterations it reproduces reliably on the baseline.
/// Even if a single run happens to win the race, the assertion encodes the
/// invariant: the checkpoint is a high-water mark, period. Any version of
/// `update()` that respects that invariant under concurrency passes.
#[tokio::test]
async fn test_checkpoint_concurrent_update_preserves_high_water_mark() {
    let dir = TempDir::new().unwrap();
    let checkpoint = Arc::new(Checkpoint::new(test_config(&dir, "concurrent")));
    let root = Arc::new(uuid::Uuid::new_v4().as_bytes().to_vec());

    // N tasks each writing a distinct sequence. Higher N raises the chance
    // of catching the TOCTOU window.
    const N: u32 = 64;
    let mut handles = Vec::with_capacity(N as usize);
    for seq in 1..=N {
        let cp = Arc::clone(&checkpoint);
        let r = Arc::clone(&root);
        handles.push(tokio::spawn(async move {
            cp.update("orders", &r, seq).await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // The high-water mark MUST be max(seqs). Any value < N means a
    // concurrent writer with a stale `current` clobbered a higher write.
    assert_eq!(
        checkpoint.get("orders", &root).await,
        Some(N),
        "expected final checkpoint to be max(seqs) = {N}, got something lower — \
         a concurrent update() observed a stale `current` and overwrote a higher \
         sequence (H-05: update() is not locked across read-then-write)"
    );
}

/// H-05 (b): Crash-recovery durability — `update()` must persist the new
/// sequence before returning, so a crash between updates cannot lose
/// progress.
///
/// The bug: `update()` flushes only when `flush_interval` (default 5s) has
/// elapsed since the last flush. An update that comes in within that
/// window writes ONLY to the in-memory `data` map. If the process crashes
/// before the next flush, the on-disk file is stale and the consumer
/// re-processes events whose checkpoint was already advanced in memory —
/// re-running side-effecting handlers without an idempotency contract.
///
/// We simulate this by:
///   1. creating a Checkpoint with a long flush_interval (1 hour) so the
///      opportunistic flush definitely does NOT fire,
///   2. calling `update()` with seq=5,
///   3. dropping the Checkpoint without calling `flush()` (the "crash"),
///   4. creating a fresh Checkpoint pointing at the same file, calling
///      `load()`, and reading back.
///
/// Post-fix (flush-on-every-update): the reloaded value MUST be Some(5)
/// — the in-memory advance was made durable before `update()` returned.
/// Pre-fix: the file is empty/absent, the reload returns None, and on
/// real consumer restart the broker/upstream replayer re-emits seq <= 5
/// to handlers that may not be idempotent.
#[tokio::test]
async fn test_checkpoint_update_is_durable_without_explicit_flush() {
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("checkpoint-durability.json");
    let root = uuid::Uuid::new_v4().as_bytes().to_vec();

    // flush_interval of 1 hour guarantees the opportunistic flush does NOT
    // fire during this test — the only way the disk reflects seq=5 is if
    // update() itself made it durable.
    let config = CheckpointConfig {
        file_path: file_path.clone(),
        flush_interval: Duration::from_secs(3600),
        enabled: true,
    };

    {
        let checkpoint = Checkpoint::new(config.clone());
        checkpoint.update("orders", &root, 5).await;
        // NO explicit flush() call — simulate a crash between update()
        // and the next opportunistic flush.
        drop(checkpoint);
    }

    // Reload from disk. If update() didn't persist, the file is missing
    // (or was never written) and `get` returns None.
    let reloaded = Checkpoint::new(config);
    reloaded
        .load()
        .await
        .expect("load() of post-crash checkpoint file");
    assert_eq!(
        reloaded.get("orders", &root).await,
        Some(5),
        "expected reloaded checkpoint to reflect seq=5; got None or stale value \
         — update() advanced the in-memory state but did NOT persist to disk \
         within the flush_interval, so a crash here loses checkpoint progress \
         (H-05: delayed-flush durability gap)"
    );
}
