//! Tests for handler instrumentation wrappers.
//!
//! Handler wrappers add metrics to projector, saga, and PM handlers:
//! - Duration histograms by component, name, domain, outcome
//! - No overhead when otel feature is disabled
//!
//! Why this matters: Handler metrics reveal slow projectors, failing sagas,
//! and PM bottlenecks without modifying handler implementations.
//!
//! Key behaviors verified:
//! - Wrappers delegate to inner handlers
//! - Results pass through unchanged
//!
//! Note: Metric emission tests require integration tests with OTel collector.

use super::*;

// ============================================================================
// Projector Handler Tests
// ============================================================================

struct MockProjectorHandler;

#[async_trait]
impl ProjectorHandler for MockProjectorHandler {
    async fn handle(
        &self,
        _events: &EventBook,
        _mode: ProjectionMode,
    ) -> Result<Projection, Status> {
        Ok(Projection::default())
    }
}

/// InstrumentedProjectorHandler delegates to inner handler.
#[tokio::test]
async fn test_instrumented_projector_delegates() {
    let inner = MockProjectorHandler;
    let handler = InstrumentedProjectorHandler::new(inner, "test-projector");

    let events = EventBook::default();
    let result = handler.handle(&events, ProjectionMode::Execute).await;
    assert!(result.is_ok());
}

// ============================================================================
// Saga Handler Tests
// ============================================================================

struct MockSagaHandler;

#[async_trait]
impl SagaHandler for MockSagaHandler {
    async fn handle(&self, _source: &EventBook) -> Result<SagaResponse, Status> {
        Ok(SagaResponse::default())
    }
}

/// InstrumentedSagaHandler delegates to inner handler.
#[tokio::test]
async fn test_instrumented_saga_delegates() {
    let inner = MockSagaHandler;
    let handler = InstrumentedSagaHandler::new(inner, "test-saga");

    let source = EventBook::default();
    let result = handler.handle(&source).await;
    assert!(result.is_ok());
}

// ============================================================================
// Process Manager Handler Tests
// ============================================================================

struct MockPMHandler;

impl ProcessManagerHandler for MockPMHandler {
    fn handle(
        &self,
        _trigger: &EventBook,
        _process_state: Option<&EventBook>,
    ) -> ProcessManagerHandleResult {
        ProcessManagerHandleResult::default()
    }
}

/// InstrumentedPMHandler delegates to inner handler.
#[test]
fn test_instrumented_pm_delegates() {
    let inner = MockPMHandler;
    let handler = InstrumentedPMHandler::new(inner, "test-pm");

    let trigger = EventBook::default();
    let result = handler.handle(&trigger, None);
    assert!(result.commands.is_empty());
    assert!(result.process_events.is_empty());
    assert!(result.facts.is_empty());
}
