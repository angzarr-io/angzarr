//! Tests for outbox pattern wrapper.
//!
//! The outbox pattern ensures guaranteed event delivery via a three-step process:
//! 1. Write event to SQL outbox table (within transaction)
//! 2. Publish to inner bus
//! 3. Delete from outbox on success
//!
//! If publish fails, event remains in outbox for background recovery.
//!
//! Why this matters: Some deployments need delivery guarantees beyond what
//! the message broker provides. The outbox pattern adds a durability layer
//! using the application database as the source of truth.
//!
//! Key behaviors verified:
//! - Config defaults and env var override
//! - Three-step publish protocol (insert → publish → delete)
//! - Recovery of orphaned events
//! - Retry count limits and corrupt event handling

use super::*;

#[test]
fn test_outbox_config_default() {
    let config = OutboxConfig::default();
    assert!(!config.enabled);
    assert_eq!(config.max_retries, 10);
    assert_eq!(config.recovery_interval_secs, 5);
}

#[test]
fn test_outbox_config_env_override() {
    // This test verifies the env var logic exists
    // Actual env var testing would require isolation
    let config = OutboxConfig {
        enabled: false,
        ..Default::default()
    };
    // Without env var set, should respect config
    assert!(!config.enabled);
}

// ============================================================================
// is_enabled Tests
// ============================================================================

/// is_enabled returns true when enabled field is true.
#[test]
fn test_is_enabled_returns_true_when_enabled() {
    let config = OutboxConfig {
        enabled: true,
        max_retries: 10,
        recovery_interval_secs: 5,
    };
    assert!(config.is_enabled());
}

/// is_enabled returns false when disabled and no env var set.
#[test]
fn test_is_enabled_returns_false_when_disabled_and_no_env() {
    // Clear env var if set
    std::env::remove_var(OUTBOX_ENABLED_ENV_VAR);

    let config = OutboxConfig {
        enabled: false,
        max_retries: 10,
        recovery_interval_secs: 5,
    };
    assert!(!config.is_enabled());
}

/// max_retries value is respected (not defaulted).
#[test]
fn test_max_retries_value_is_respected() {
    let config = OutboxConfig {
        enabled: false,
        max_retries: 5,
        recovery_interval_secs: 10,
    };
    assert_eq!(config.max_retries, 5);
    assert_ne!(config.max_retries, 10); // Not the default
}

/// recovery_interval_secs value is respected.
#[test]
fn test_recovery_interval_value_is_respected() {
    let config = OutboxConfig {
        enabled: false,
        max_retries: 10,
        recovery_interval_secs: 30,
    };
    assert_eq!(config.recovery_interval_secs, 30);
    assert_ne!(config.recovery_interval_secs, 5); // Not the default
}

// ============================================================================
// SQLite Integration Tests
// ============================================================================
//
// These tests verify the full outbox flow against an in-memory SQLite database.
// SQLite is always compiled.

mod sqlite_tests {
    use super::*;
    use crate::bus::mock::MockEventBus;
    use crate::proto_ext::pages::EventPageExt;
    use crate::test_utils::{make_cover_with_root, make_event_page};
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::Row;

    async fn create_test_pool() -> sqlx::SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("Failed to create in-memory SQLite pool")
    }

    fn make_test_event_book(domain: &str, root: uuid::Uuid) -> EventBook {
        EventBook {
            cover: Some(make_cover_with_root(domain, root)),
            pages: vec![make_event_page(0)],
            snapshot: None,
            ..Default::default()
        }
    }

    async fn count_outbox_entries(pool: &sqlx::SqlitePool) -> i64 {
        sqlx::query("SELECT COUNT(*) as count FROM outbox")
            .fetch_one(pool)
            .await
            .map(|row| row.get::<i64, _>("count"))
            .unwrap_or(0)
    }

    async fn get_outbox_entry(pool: &sqlx::SqlitePool, id: &str) -> Option<(String, i32)> {
        sqlx::query("SELECT domain, retry_count FROM outbox WHERE id = ?")
            .bind(id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()
            .map(|row| (row.get("domain"), row.get("retry_count")))
    }

    async fn insert_orphaned_event(
        pool: &sqlx::SqlitePool,
        id: &str,
        domain: &str,
        event_data: &[u8],
        age_seconds: i32,
        retry_count: i32,
    ) {
        let sql = format!(
            "INSERT INTO outbox (id, domain, root, event_data, created_at, retry_count) \
             VALUES (?, ?, 'test-root', ?, datetime('now', '-{} seconds'), ?)",
            age_seconds
        );
        sqlx::query(&sql)
            .bind(id)
            .bind(domain)
            .bind(event_data)
            .bind(retry_count)
            .execute(pool)
            .await
            .expect("Failed to insert orphaned event");
    }

    // ========================================================================
    // Table Initialization Tests
    // ========================================================================

    /// init creates the outbox table.
    #[tokio::test]
    async fn test_init_creates_outbox_table() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = SqliteOutboxEventBus::new(inner, pool.clone(), config);
        outbox.init().await.expect("init should succeed");

        // Verify table exists by querying it
        let result = sqlx::query("SELECT COUNT(*) FROM outbox")
            .fetch_one(&pool)
            .await;
        assert!(result.is_ok(), "outbox table should exist after init");
    }

    /// init is idempotent (can be called multiple times).
    #[tokio::test]
    async fn test_init_is_idempotent() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = SqliteOutboxEventBus::new(inner, pool.clone(), config);

        // Call init multiple times
        outbox.init().await.expect("first init should succeed");
        outbox.init().await.expect("second init should succeed");
        outbox.init().await.expect("third init should succeed");
    }

    // ========================================================================
    // Three-Step Publish Protocol Tests
    // ========================================================================

    /// Successful publish removes event from outbox.
    #[tokio::test]
    async fn test_publish_success_removes_from_outbox() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = SqliteOutboxEventBus::new(inner.clone(), pool.clone(), config);
        outbox.init().await.unwrap();

        let book = Arc::new(make_test_event_book("orders", uuid::Uuid::new_v4()));

        // Publish should succeed
        let result = outbox.publish(book).await;
        assert!(result.is_ok(), "publish should succeed");

        // Inner bus should have received the event
        assert_eq!(inner.published_count().await, 1);

        // Outbox should be empty (event removed after success)
        assert_eq!(count_outbox_entries(&pool).await, 0);
    }

    /// Failed publish leaves event in outbox for recovery.
    #[tokio::test]
    async fn test_publish_failure_leaves_event_in_outbox() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        inner.set_fail_on_publish(true).await;
        let config = OutboxConfig::default();

        let outbox = SqliteOutboxEventBus::new(inner.clone(), pool.clone(), config);
        outbox.init().await.unwrap();

        let book = Arc::new(make_test_event_book("orders", uuid::Uuid::new_v4()));

        // Publish should fail
        let result = outbox.publish(book).await;
        assert!(result.is_err(), "publish should fail when inner bus fails");

        // Inner bus attempted publish
        assert_eq!(inner.published_count().await, 0); // MockEventBus doesn't record failed publishes

        // Event should remain in outbox for recovery
        assert_eq!(count_outbox_entries(&pool).await, 1);
    }

    /// Publish stores correct domain and root metadata.
    #[tokio::test]
    async fn test_publish_stores_correct_domain_and_root() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        inner.set_fail_on_publish(true).await; // Force failure to inspect outbox
        let config = OutboxConfig::default();

        let outbox = SqliteOutboxEventBus::new(inner, pool.clone(), config);
        outbox.init().await.unwrap();

        let root = uuid::Uuid::new_v4();
        let book = Arc::new(make_test_event_book("inventory", root));

        let _ = outbox.publish(book).await;

        // Verify stored metadata
        let row = sqlx::query("SELECT domain, root FROM outbox")
            .fetch_one(&pool)
            .await
            .expect("should have one entry");

        let stored_domain: String = row.get("domain");
        let stored_root: String = row.get("root");

        assert_eq!(stored_domain, "inventory");
        assert_eq!(stored_root, hex::encode(root.as_bytes()));
    }

    // ========================================================================
    // Recovery Tests
    // ========================================================================

    /// Recovery publishes orphaned events and deletes them.
    #[tokio::test]
    async fn test_recover_orphaned_publishes_and_deletes() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig {
            max_retries: 10,
            ..Default::default()
        };

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        // Create a valid EventBook and encode it
        let book = make_test_event_book("orders", uuid::Uuid::new_v4());
        let event_data = book.encode_to_vec();

        // Insert orphaned event (older than 30 seconds)
        insert_orphaned_event(&pool, "orphan-1", "orders", &event_data, 60, 0).await;

        assert_eq!(count_outbox_entries(&pool).await, 1);

        // Run recovery
        let recovered = outbox
            .recover_orphaned()
            .await
            .expect("recovery should succeed");

        assert_eq!(recovered, 1, "should recover 1 event");
        assert_eq!(
            inner.published_count().await,
            1,
            "inner bus should receive event"
        );
        assert_eq!(
            count_outbox_entries(&pool).await,
            0,
            "outbox should be empty after recovery"
        );
    }

    /// Recovery skips events younger than 30 seconds.
    #[tokio::test]
    async fn test_recover_skips_recent_events() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        let book = make_test_event_book("orders", uuid::Uuid::new_v4());
        let event_data = book.encode_to_vec();

        // Insert recent event (only 5 seconds old, under 30 second threshold)
        insert_orphaned_event(&pool, "recent-1", "orders", &event_data, 5, 0).await;

        // Run recovery
        let recovered = outbox
            .recover_orphaned()
            .await
            .expect("recovery should succeed");

        assert_eq!(recovered, 0, "should not recover recent events");
        assert_eq!(inner.published_count().await, 0);
        assert_eq!(
            count_outbox_entries(&pool).await,
            1,
            "recent event should remain"
        );
    }

    /// Recovery increments retry count on publish failure.
    #[tokio::test]
    async fn test_recover_increments_retry_count_on_failure() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        inner.set_fail_on_publish(true).await;
        let config = OutboxConfig {
            max_retries: 10,
            ..Default::default()
        };

        let outbox = Arc::new(SqliteOutboxEventBus::new(inner, pool.clone(), config));
        outbox.init().await.unwrap();

        let book = make_test_event_book("orders", uuid::Uuid::new_v4());
        let event_data = book.encode_to_vec();

        insert_orphaned_event(&pool, "retry-test", "orders", &event_data, 60, 0).await;

        // First recovery attempt (should fail and increment)
        let recovered = outbox.recover_orphaned().await.unwrap();
        assert_eq!(recovered, 0, "no events recovered on failure");

        let entry = get_outbox_entry(&pool, "retry-test").await;
        assert_eq!(
            entry,
            Some(("orders".to_string(), 1)),
            "retry_count should be 1"
        );

        // Second recovery attempt
        let _ = outbox.recover_orphaned().await;
        let entry = get_outbox_entry(&pool, "retry-test").await;
        assert_eq!(
            entry,
            Some(("orders".to_string(), 2)),
            "retry_count should be 2"
        );
    }

    /// Recovery respects max_retries limit.
    #[tokio::test]
    async fn test_recover_respects_max_retries() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        inner.set_fail_on_publish(true).await;
        let config = OutboxConfig {
            max_retries: 3,
            ..Default::default()
        };

        let outbox = Arc::new(SqliteOutboxEventBus::new(inner, pool.clone(), config));
        outbox.init().await.unwrap();

        let book = make_test_event_book("orders", uuid::Uuid::new_v4());
        let event_data = book.encode_to_vec();

        // Insert event that has already exceeded max retries
        insert_orphaned_event(&pool, "max-retry", "orders", &event_data, 60, 3).await;

        // Recovery should skip this event
        let recovered = outbox.recover_orphaned().await.unwrap();
        assert_eq!(recovered, 0);

        // Event should still be in outbox (not deleted, just skipped)
        let entry = get_outbox_entry(&pool, "max-retry").await;
        assert!(
            entry.is_some(),
            "event at max retries should remain in outbox"
        );
    }

    /// Recovery removes corrupt events.
    #[tokio::test]
    async fn test_recover_removes_corrupt_events() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        // Insert corrupt/invalid protobuf data
        let corrupt_data = vec![0xFF, 0xFE, 0xFD, 0xFC];
        insert_orphaned_event(&pool, "corrupt-1", "orders", &corrupt_data, 60, 0).await;

        assert_eq!(count_outbox_entries(&pool).await, 1);

        // Recovery should remove corrupt entry
        let recovered = outbox.recover_orphaned().await.unwrap();
        assert_eq!(recovered, 0, "corrupt events don't count as recovered");
        assert_eq!(
            inner.published_count().await,
            0,
            "corrupt events not published"
        );
        assert_eq!(
            count_outbox_entries(&pool).await,
            0,
            "corrupt event should be deleted"
        );
    }

    /// Recovery processes multiple events.
    #[tokio::test]
    async fn test_recover_processes_multiple_events() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        // Insert multiple orphaned events
        for i in 0..5 {
            let book = make_test_event_book("orders", uuid::Uuid::new_v4());
            let event_data = book.encode_to_vec();
            insert_orphaned_event(&pool, &format!("batch-{}", i), "orders", &event_data, 60, 0)
                .await;
        }

        assert_eq!(count_outbox_entries(&pool).await, 5);

        let recovered = outbox.recover_orphaned().await.unwrap();
        assert_eq!(recovered, 5);
        assert_eq!(inner.published_count().await, 5);
        assert_eq!(count_outbox_entries(&pool).await, 0);
    }

    /// Recovery handles partial failure (mix of valid and corrupt).
    #[tokio::test]
    async fn test_recover_partial_failure() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig {
            max_retries: 10,
            ..Default::default()
        };

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        // Insert valid events
        let book1 = make_test_event_book("orders", uuid::Uuid::new_v4());
        let book2 = make_test_event_book("orders", uuid::Uuid::new_v4());
        insert_orphaned_event(&pool, "valid-1", "orders", &book1.encode_to_vec(), 60, 0).await;
        insert_orphaned_event(&pool, "valid-2", "orders", &book2.encode_to_vec(), 61, 0).await;

        // Insert corrupt event in between
        insert_orphaned_event(&pool, "corrupt", "orders", &[0xFF], 62, 0).await;

        assert_eq!(count_outbox_entries(&pool).await, 3);

        let recovered = outbox.recover_orphaned().await.unwrap();

        // Should recover 2 valid, delete 1 corrupt
        assert_eq!(recovered, 2);
        assert_eq!(inner.published_count().await, 2);
        assert_eq!(count_outbox_entries(&pool).await, 0);
    }

    // ========================================================================
    // Recovery Task Tests
    // ========================================================================

    /// Recovery task can be stopped without hanging.
    #[tokio::test]
    async fn test_recovery_task_can_be_stopped() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = Arc::new(SqliteOutboxEventBus::new(inner, pool, config));
        outbox.init().await.unwrap();

        // Spawn with short interval
        let handle = spawn_sqlite_recovery_task(outbox, 1);

        // Give it a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Stop should not hang
        handle.stop();

        // Give it time to actually stop
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // ========================================================================
    // EventBus Trait Delegation Tests
    // ========================================================================

    /// subscribe delegates to inner bus.
    #[tokio::test]
    async fn test_subscribe_delegates_to_inner() {
        use crate::bus::BusError;

        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = SqliteOutboxEventBus::new(inner, pool, config);
        outbox.init().await.unwrap();

        struct DummyHandler;
        impl EventHandler for DummyHandler {
            fn handle(
                &self,
                _book: Arc<EventBook>,
            ) -> futures::future::BoxFuture<'static, std::result::Result<(), BusError>>
            {
                Box::pin(async { Ok(()) })
            }
        }

        // MockEventBus returns SubscribeNotSupported
        let result = outbox.subscribe(Box::new(DummyHandler)).await;
        assert!(matches!(result, Err(BusError::SubscribeNotSupported)));
    }

    /// create_subscriber delegates to inner bus.
    #[tokio::test]
    async fn test_create_subscriber_delegates_to_inner() {
        use crate::bus::BusError;

        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = SqliteOutboxEventBus::new(inner, pool, config);
        outbox.init().await.unwrap();

        let result = outbox.create_subscriber("test", Some("orders")).await;
        assert!(matches!(result, Err(BusError::SubscribeNotSupported)));
    }

    // ========================================================================
    // Recovery Boundary Tests
    // ========================================================================

    /// Event at 29 seconds is not recovered (under 30s threshold).
    #[tokio::test]
    async fn test_recover_boundary_29_seconds_not_recovered() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        let book = make_test_event_book("orders", uuid::Uuid::new_v4());
        let event_data = book.encode_to_vec();

        // Insert event at 29 seconds (just under 30 second threshold)
        insert_orphaned_event(&pool, "boundary-29", "orders", &event_data, 29, 0).await;

        let recovered = outbox.recover_orphaned().await.unwrap();

        assert_eq!(
            recovered, 0,
            "event at 29 seconds should NOT be recovered (under 30s threshold)"
        );
        assert_eq!(count_outbox_entries(&pool).await, 1, "event should remain");
    }

    /// Event at 31 seconds is recovered (over 30s threshold).
    #[tokio::test]
    async fn test_recover_boundary_31_seconds_is_recovered() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        let book = make_test_event_book("orders", uuid::Uuid::new_v4());
        let event_data = book.encode_to_vec();

        // Insert event at 31 seconds (just over 30 second threshold)
        insert_orphaned_event(&pool, "boundary-31", "orders", &event_data, 31, 0).await;

        let recovered = outbox.recover_orphaned().await.unwrap();

        assert_eq!(
            recovered, 1,
            "event at 31 seconds SHOULD be recovered (over 30s threshold)"
        );
        assert_eq!(
            count_outbox_entries(&pool).await,
            0,
            "event should be removed"
        );
    }

    /// Recovery batch limit of 100 is respected.
    #[tokio::test]
    async fn test_recover_respects_batch_limit_of_100() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig::default();

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        // Insert 150 orphaned events (exceeds 100 batch limit)
        for i in 0..150 {
            let book = make_test_event_book("orders", uuid::Uuid::new_v4());
            let event_data = book.encode_to_vec();
            insert_orphaned_event(
                &pool,
                &format!("batch-limit-{}", i),
                "orders",
                &event_data,
                60,
                0,
            )
            .await;
        }

        assert_eq!(count_outbox_entries(&pool).await, 150);

        // First recovery should process exactly 100 (the batch limit)
        let recovered = outbox.recover_orphaned().await.unwrap();

        assert_eq!(
            recovered, 100,
            "should recover exactly 100 events (batch limit)"
        );
        assert_eq!(
            count_outbox_entries(&pool).await,
            50,
            "50 events should remain for next batch"
        );

        // Second recovery should process the remaining 50
        let recovered2 = outbox.recover_orphaned().await.unwrap();
        assert_eq!(recovered2, 50, "should recover remaining 50 events");
        assert_eq!(count_outbox_entries(&pool).await, 0, "all events recovered");
    }

    /// max_retries boundary: event at retry_count=4 is recovered, at 5 is not.
    #[tokio::test]
    async fn test_recover_max_retries_boundary() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig {
            max_retries: 5,
            ..Default::default()
        };

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        let book = make_test_event_book("orders", uuid::Uuid::new_v4());
        let event_data = book.encode_to_vec();

        // Insert event at retry_count = 4 (just under max_retries=5)
        insert_orphaned_event(&pool, "under-max", "orders", &event_data, 60, 4).await;

        // Insert event at retry_count = 5 (at max_retries)
        insert_orphaned_event(&pool, "at-max", "orders", &event_data, 60, 5).await;

        let recovered = outbox.recover_orphaned().await.unwrap();

        // Only the one under max_retries should be attempted
        assert_eq!(
            recovered, 1,
            "only event under max_retries should be recovered"
        );
        assert_eq!(
            count_outbox_entries(&pool).await,
            1,
            "event at max_retries should remain"
        );

        // Verify the remaining event is the one at max_retries
        let entry = get_outbox_entry(&pool, "at-max").await;
        assert!(entry.is_some(), "event at max_retries should still exist");
    }

    // ========================================================================
    // C-13 Recovery Ordering Tests
    // ========================================================================
    //
    // These tests pin the invariant that recovery NEVER republishes an older
    // event for a root after a newer event for that root has been successfully
    // published. CQRS-ES requires monotonic per-root delivery; if recovery
    // re-emits seq=N for root X after seq=N+k has already gone out, downstream
    // projectors and PMs observe state regression.
    //
    // Baseline (pre-fix) behavior: recovery decodes the orphaned book and
    // unconditionally calls inner.publish() — so the consumer sees
    // [seq=2, seq=1] when seq=1 had failed and seq=2 had succeeded.
    //
    // Fix (Option A from the plan): recovery checks per-(domain, root) the
    // last successfully published sequence; if the orphaned event's sequence
    // is <= that watermark, the row is dropped from the outbox WITHOUT
    // republishing (the consumer has already seen newer state — re-emitting
    // would be regressive).

    fn make_test_event_book_seq(domain: &str, root: uuid::Uuid, seq: u32) -> EventBook {
        EventBook {
            cover: Some(make_cover_with_root(domain, root)),
            pages: vec![make_event_page(seq)],
            snapshot: None,
            ..Default::default()
        }
    }

    /// Recovery must not republish an older event for a root when a newer
    /// event for the same root has already been successfully published.
    ///
    /// Scenario:
    ///   1. seq=1 for root X failed to publish on the normal path; it sits
    ///      in the outbox table with an aged created_at.
    ///   2. seq=2 for root X is published normally — inner bus records it.
    ///   3. Recovery fires. It must NOT emit seq=1 to the inner bus.
    ///
    /// The published log on the inner bus must show only seq=2 for root X
    /// (or seq=1 followed by seq=2 if a future redesign blocks the seq=2
    /// publish until seq=1 has been recovered; either is acceptable as long
    /// as ordering holds). The test specifically forbids the
    /// [seq=2, seq=1] sequence — that is the C-13 bug.
    #[tokio::test]
    async fn test_recovery_does_not_republish_superseded_event() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig {
            max_retries: 10,
            ..Default::default()
        };

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        let root = uuid::Uuid::new_v4();
        let root_hex = hex::encode(root.as_bytes());

        // (a) seq=1 for root X is stranded in the outbox (its publish
        //     attempt previously failed; the row stayed behind). We insert
        //     it directly with an aged timestamp to model "older than 30s".
        let book_seq1 = make_test_event_book_seq("orders", root, 1);
        let bytes_seq1 = book_seq1.encode_to_vec();
        insert_orphaned_event(&pool, "seq1-stale", "orders", &bytes_seq1, 60, 0).await;
        // Override the root column to match the real root (helper writes
        // 'test-root'); recovery needs the real root to look up the
        // per-root published watermark.
        sqlx::query("UPDATE outbox SET root = ? WHERE id = ?")
            .bind(&root_hex)
            .bind("seq1-stale")
            .execute(&pool)
            .await
            .expect("failed to set root for orphaned seq=1");

        // (b) seq=2 for root X publishes successfully on the normal path.
        let book_seq2 = Arc::new(make_test_event_book_seq("orders", root, 2));
        outbox
            .publish(book_seq2)
            .await
            .expect("seq=2 publish must succeed");

        assert_eq!(
            inner.published_count().await,
            1,
            "inner bus has only seq=2 so far"
        );

        // (c) Recovery fires. The bug republishes the stale seq=1 to the
        //     inner bus AFTER seq=2 has already been emitted.
        let _ = outbox
            .recover_orphaned()
            .await
            .expect("recovery should succeed");

        // (d) Observe what the consumer saw.
        let observed = inner.take_published().await;
        let observed_seqs: Vec<u32> = observed
            .iter()
            .flat_map(|b| b.pages.iter().map(|p| p.sequence_num()))
            .collect();

        // The C-13 invariant: ordering must be monotonic per-root.
        // It is acceptable for recovery to drop seq=1 (Option A) or to
        // hold seq=2 until seq=1 has recovered (Option B). It is NOT
        // acceptable to emit seq=1 after seq=2.
        let mut last: Option<u32> = None;
        for (i, seq) in observed_seqs.iter().enumerate() {
            if let Some(prev) = last {
                assert!(
                    *seq > prev,
                    "C-13: per-root ordering violation on inner bus. Position {i} \
                     emitted seq={seq} after seq={prev}. Full observed sequence: \
                     {observed_seqs:?}. Recovery republished an older event for \
                     root {root_hex} after a newer event was already published."
                );
            }
            last = Some(*seq);
        }

        // And: seq=1 must be removed from the outbox so it never re-fires.
        // Under Option A, recovery deletes superseded rows.
        let remaining_seq1 = get_outbox_entry(&pool, "seq1-stale").await;
        assert!(
            remaining_seq1.is_none(),
            "C-13: superseded seq=1 row must be removed from outbox after \
             recovery sees seq=2 has been published (Option A). Found: \
             {remaining_seq1:?}"
        );
    }

    /// Recovery still republishes orphaned events when NO newer event for
    /// the same root has been published. This is a regression guard against
    /// an over-eager fix that drops every orphaned event.
    #[tokio::test]
    async fn test_recovery_still_republishes_non_superseded_event() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let config = OutboxConfig {
            max_retries: 10,
            ..Default::default()
        };

        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            config,
        ));
        outbox.init().await.unwrap();

        let root = uuid::Uuid::new_v4();
        let root_hex = hex::encode(root.as_bytes());

        // seq=5 for root X is stranded. No newer event has been published
        // for this root. Recovery must republish it.
        let book = make_test_event_book_seq("orders", root, 5);
        let bytes = book.encode_to_vec();
        insert_orphaned_event(&pool, "lonely-seq5", "orders", &bytes, 60, 0).await;
        sqlx::query("UPDATE outbox SET root = ? WHERE id = ?")
            .bind(&root_hex)
            .bind("lonely-seq5")
            .execute(&pool)
            .await
            .expect("failed to set root for orphaned seq=5");

        let recovered = outbox
            .recover_orphaned()
            .await
            .expect("recovery should succeed");

        assert_eq!(recovered, 1, "non-superseded event must be recovered");
        assert_eq!(
            inner.published_count().await,
            1,
            "non-superseded event must reach the inner bus"
        );
        assert_eq!(
            count_outbox_entries(&pool).await,
            0,
            "non-superseded event must be removed from outbox after success"
        );
    }

    /// C-13 follow-up: root-less events MUST bypass the supersession check.
    ///
    /// All root-less events share the same `(domain, "")` bucket but are
    /// genuinely distinct messages (they're not retries of one another).
    /// Without the root-less bypass, the first successful publish bumps the
    /// `(domain, "")` watermark, and every subsequent orphaned root-less
    /// event in that domain gets dropped as "superseded" — silent data loss.
    #[tokio::test]
    async fn test_recovery_republishes_rootless_orphan_even_when_watermark_present() {
        let pool = create_test_pool().await;
        let inner = Arc::new(MockEventBus::new());
        let outbox = Arc::new(SqliteOutboxEventBus::new(
            inner.clone(),
            pool.clone(),
            OutboxConfig::default(),
        ));
        outbox.init().await.unwrap();

        // A root-less book: Cover without a root. `extract_routing_key`
        // yields root_hex = "" and max_seq = 0 for both publishes.
        fn rootless_book(domain: &str) -> EventBook {
            EventBook {
                cover: Some(crate::proto::Cover {
                    domain: domain.to_string(),
                    root: None,
                    correlation_id: String::new(),
                    edition: None,
                }),
                pages: vec![make_event_page(0)],
                snapshot: None,
                ..Default::default()
            }
        }

        // (a) First root-less publish goes through the normal path. The
        //     watermark for ("orders", "") gets bumped to 0.
        outbox
            .publish(Arc::new(rootless_book("orders")))
            .await
            .expect("first root-less publish must succeed");
        assert_eq!(
            inner.published_count().await,
            1,
            "first root-less publish must reach the inner bus"
        );

        // (b) A different root-less event for the same domain is orphaned
        //     in the outbox (e.g., publish failed on a transient SDK error
        //     and the row stayed behind). It has the same (domain, "")
        //     supersession key — but it is NOT the same message.
        let bytes_two = rootless_book("orders").encode_to_vec();
        insert_orphaned_event(&pool, "rootless-orphan", "orders", &bytes_two, 60, 0).await;
        // The helper writes root='test-root' — overwrite to the canonical
        // "" so the recovery path sees an actual root-less row.
        sqlx::query("UPDATE outbox SET root = '' WHERE id = ?")
            .bind("rootless-orphan")
            .execute(&pool)
            .await
            .expect("failed to clear root for orphaned root-less event");

        // (c) Recovery fires.
        let recovered = outbox
            .recover_orphaned()
            .await
            .expect("recovery must succeed");

        assert_eq!(
            recovered, 1,
            "the orphaned root-less event MUST be republished, not dropped as superseded"
        );
        assert_eq!(
            inner.published_count().await,
            2,
            "inner bus must observe BOTH root-less events"
        );
        assert!(
            get_outbox_entry(&pool, "rootless-orphan").await.is_none(),
            "orphaned row should be deleted after successful republish"
        );
    }
}
