//! Tests for chained DLQ publisher with priority fallback.
//!
//! The chained publisher tries each target in order until one succeeds.
//! This enables resilient DLQ patterns: try message queue first, fall back
//! to database, then filesystem, then logging. If all fail, the dead letter
//! is lost (but logged).
//!
//! Why this matters: DLQ is the safety net for failed messages. If the
//! primary DLQ (e.g., RabbitMQ) is down, we need fallback options to
//! prevent data loss.
//!
//! Key behaviors:
//! - First success short-circuits — remaining targets not tried
//! - Failures are logged and continue to next target
//! - Empty chain returns NotConfigured error
//! - is_configured() true only if chain is non-empty

use super::*;
use crate::dlq::publishers::NoopDeadLetterPublisher;
use crate::dlq::DeadLetterPayload;
use crate::proto::{ComponentKind, EventBook};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

// ============================================================================
// Test Doubles
// ============================================================================

/// Publisher that always fails — simulates infrastructure outage.
struct FailingPublisher {
    call_count: AtomicUsize,
}

impl FailingPublisher {
    fn new() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl DeadLetterPublisher for FailingPublisher {
    async fn publish(&self, _dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Err(DlqError::PublishFailed("intentional failure".to_string()))
    }
}

/// Publisher that always succeeds — simulates healthy target.
struct SuccessPublisher {
    call_count: AtomicUsize,
}

impl SuccessPublisher {
    fn new() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl DeadLetterPublisher for SuccessPublisher {
    async fn publish(&self, _dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn make_test_dead_letter() -> AngzarrDeadLetter {
    AngzarrDeadLetter {
        payload: DeadLetterPayload::Events(EventBook::default()),
        rejection_reason: "test".to_string(),
        rejection_details: None,
        occurred_at: None,
        metadata: HashMap::new(),
        source_component: "test".to_string(),
        source_component_kind: ComponentKind::Unspecified,
    }
}

// ============================================================================
// Chaining Behavior Tests
// ============================================================================

/// First target succeeds → remaining targets not called.
///
/// Optimization: don't waste resources on fallback targets if primary works.
#[tokio::test]
async fn test_chained_first_succeeds() {
    let success = Arc::new(SuccessPublisher::new());
    let failing = Arc::new(FailingPublisher::new());

    let chained = ChainedDlqPublisher::new(vec![
        success.clone() as Arc<dyn DeadLetterPublisher>,
        failing.clone() as Arc<dyn DeadLetterPublisher>,
    ]);

    let result = chained.publish(make_test_dead_letter()).await;
    assert!(result.is_ok());
    assert_eq!(success.call_count.load(Ordering::SeqCst), 1);
    assert_eq!(failing.call_count.load(Ordering::SeqCst), 0);
}

/// First target fails → fallback to second target.
///
/// Primary use case: message queue unavailable, database accepts dead letter.
#[tokio::test]
async fn test_chained_fallback_to_second() {
    let failing = Arc::new(FailingPublisher::new());
    let success = Arc::new(SuccessPublisher::new());

    let chained = ChainedDlqPublisher::new(vec![
        failing.clone() as Arc<dyn DeadLetterPublisher>,
        success.clone() as Arc<dyn DeadLetterPublisher>,
    ]);

    let result = chained.publish(make_test_dead_letter()).await;
    assert!(result.is_ok());
    assert_eq!(failing.call_count.load(Ordering::SeqCst), 1);
    assert_eq!(success.call_count.load(Ordering::SeqCst), 1);
}

/// All targets fail → returns last error.
///
/// Total DLQ failure is critical but recoverable via logging at call site.
/// Last error preserved for diagnostics.
#[tokio::test]
async fn test_chained_all_fail() {
    let failing1 = Arc::new(FailingPublisher::new());
    let failing2 = Arc::new(FailingPublisher::new());

    let chained = ChainedDlqPublisher::new(vec![
        failing1.clone() as Arc<dyn DeadLetterPublisher>,
        failing2.clone() as Arc<dyn DeadLetterPublisher>,
    ]);

    let result = chained.publish(make_test_dead_letter()).await;
    assert!(result.is_err());
    assert_eq!(failing1.call_count.load(Ordering::SeqCst), 1);
    assert_eq!(failing2.call_count.load(Ordering::SeqCst), 1);
}

/// Empty chain reports not configured and fails publish.
///
/// No targets = no DLQ. Caller should handle gracefully (log and continue).
#[tokio::test]
async fn test_chained_empty_not_configured() {
    let chained = ChainedDlqPublisher::new(vec![]);
    assert!(!chained.is_configured());

    let result = chained.publish(make_test_dead_letter()).await;
    assert!(result.is_err());
}

/// Non-empty chain reports configured.
#[test]
fn test_chained_is_configured() {
    let chained = ChainedDlqPublisher::new(vec![Arc::new(NoopDeadLetterPublisher)]);
    assert!(chained.is_configured());
}
