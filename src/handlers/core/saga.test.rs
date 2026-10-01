//! Tests for saga event handler construction.
//!
//! The saga handler orchestrates saga execution across event bus events:
//! - Two-phase saga protocol (prepare → fetch destinations → execute)
//! - Retry with backoff on sequence conflicts
//! - Output domain validation
//!
//! Unit tests verify:
//! - Constructor patterns store correct fields
//! - Handler correctly initializes with/without optional components
//! - Backoff configuration is applied

use super::*;
use crate::orchestration::command::{CommandExecutor, CommandOutcome};
use crate::orchestration::destination::DestinationFetcher;
use crate::orchestration::saga::{SagaContextFactory, SagaRetryContext};
use crate::orchestration::FactExecutor;
use crate::proto::{CommandBook, Cover, EventBook, SyncMode};
use async_trait::async_trait;
use std::sync::Arc;

// ============================================================================
// Mock Implementations
// ============================================================================

struct MockSagaContextFactory {
    name: String,
}

impl MockSagaContextFactory {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

impl SagaContextFactory for MockSagaContextFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _events: Arc<EventBook>) -> Box<dyn SagaRetryContext> {
        unimplemented!("Not needed for constructor tests")
    }
}

struct MockCommandExecutor;

#[async_trait]
impl CommandExecutor for MockCommandExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        unimplemented!("Not needed for constructor tests")
    }
}

struct MockDestinationFetcher;

#[async_trait]
impl DestinationFetcher for MockDestinationFetcher {
    async fn fetch(&self, _cover: &Cover) -> Result<Option<EventBook>, tonic::Status> {
        unimplemented!("Not needed for constructor tests")
    }

    async fn fetch_by_correlation(
        &self,
        _domain: &str,
        _correlation_id: &str,
    ) -> Result<Option<EventBook>, tonic::Status> {
        unimplemented!("Not needed for constructor tests")
    }
}

struct MockFactExecutor;

#[async_trait]
impl FactExecutor for MockFactExecutor {
    async fn inject(
        &self,
        _fact: EventBook,
        _delivery: crate::orchestration::FactDelivery,
    ) -> Result<(), crate::orchestration::FactInjectionError> {
        unimplemented!("Not needed for constructor tests")
    }
}

// ============================================================================
// from_factory Tests
// ============================================================================

/// from_factory stores context factory reference.
#[test]
fn test_from_factory_stores_context_factory() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("test-saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = SagaEventHandler::from_factory(factory.clone(), executor, None);

    assert_eq!(handler.context_factory.name(), "test-saga");
}

/// from_factory stores command executor.
#[test]
fn test_from_factory_stores_command_executor() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = SagaEventHandler::from_factory(factory, executor, None);

    // Executor is stored - we can't easily verify but construction succeeds
    assert!(handler.command_bus.is_none());
}

/// from_factory with destination fetcher stores it.
#[test]
fn test_from_factory_with_destination_fetcher() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);

    let handler = SagaEventHandler::from_factory(factory, executor, Some(fetcher));

    assert!(handler.destination_fetcher.is_some());
}

/// from_factory without destination fetcher has None.
#[test]
fn test_from_factory_without_destination_fetcher() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = SagaEventHandler::from_factory(factory, executor, None);

    assert!(handler.destination_fetcher.is_none());
}

/// from_factory sets default backoff.
#[test]
fn test_from_factory_sets_default_backoff() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = SagaEventHandler::from_factory(factory, executor, None);

    // Can't directly inspect backoff, but we can verify it builds
    let _ = handler.backoff;
}

/// D-3 regression (review decision): sagas are at-least-once by DEFAULT.
/// `propagate_errors` must default to true so a transient orchestration
/// failure NACKs the source event and the bus redelivers — the old `false`
/// default acked-and-lost the cross-domain translation, the one job a saga
/// has. PM and aggregate handlers already defaulted to propagation.
#[test]
fn test_default_error_propagation_is_at_least_once() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = SagaEventHandler::from_factory(factory.clone(), executor.clone(), None);
    assert!(
        handler.propagate_errors,
        "from_factory must default to propagate_errors=true (at-least-once, D-3)"
    );

    let handler = SagaEventHandler::from_factory_with_validator(
        factory,
        executor,
        None,
        None,
        None,
        None,
        crate::utils::retry::saga_backoff(),
    );
    assert!(
        handler.propagate_errors,
        "from_factory_with_validator must default to propagate_errors=true (D-3)"
    );
}

/// from_factory initializes optional fields to None.
#[test]
fn test_from_factory_optional_fields_none() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = SagaEventHandler::from_factory(factory, executor, None);

    assert!(handler.command_bus.is_none());
    assert!(handler.fact_executor.is_none());
    assert!(handler.output_domain_validator.is_none());
}

// ============================================================================
// from_factory_with_validator Tests
// ============================================================================

/// from_factory_with_validator stores fact executor.
#[test]
fn test_from_factory_with_validator_stores_fact_executor() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);
    let fact_executor: Arc<dyn FactExecutor> = Arc::new(MockFactExecutor);

    let handler = SagaEventHandler::from_factory_with_validator(
        factory,
        executor,
        None,
        None,
        Some(fact_executor),
        None,
        saga_backoff(),
    );

    assert!(handler.fact_executor.is_some());
}

/// from_factory_with_validator stores output domain validator.
#[test]
fn test_from_factory_with_validator_stores_validator() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);
    // OutputDomainValidator is a type alias for a closure
    let validator: Arc<OutputDomainValidator> = Arc::new(|_cmd: &CommandBook| Ok(()));

    let handler = SagaEventHandler::from_factory_with_validator(
        factory,
        executor,
        None,
        None,
        None,
        Some(validator),
        saga_backoff(),
    );

    assert!(handler.output_domain_validator.is_some());
}

/// from_factory_with_validator with all options.
#[test]
fn test_from_factory_with_validator_all_options() {
    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("full-saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let fact_executor: Arc<dyn FactExecutor> = Arc::new(MockFactExecutor);
    // OutputDomainValidator is a type alias for a closure
    let validator: Arc<OutputDomainValidator> = Arc::new(|_cmd: &CommandBook| Ok(()));

    let handler = SagaEventHandler::from_factory_with_validator(
        factory,
        executor,
        None, // command_bus
        Some(fetcher),
        Some(fact_executor),
        Some(validator),
        saga_backoff(),
    );

    assert!(handler.destination_fetcher.is_some());
    assert!(handler.fact_executor.is_some());
    assert!(handler.output_domain_validator.is_some());
    assert_eq!(handler.context_factory.name(), "full-saga");
}

/// from_factory_with_validator respects custom backoff.
#[test]
fn test_from_factory_with_validator_custom_backoff() {
    use backon::ExponentialBuilder;

    let factory: Arc<dyn SagaContextFactory> = Arc::new(MockSagaContextFactory::new("saga"));
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);
    let custom_backoff = ExponentialBuilder::default().with_max_times(5);

    let handler = SagaEventHandler::from_factory_with_validator(
        factory,
        executor,
        None,
        None,
        None,
        None,
        custom_backoff,
    );

    // Can't directly compare backoffs, but construction succeeds
    let _ = handler.backoff;
}
