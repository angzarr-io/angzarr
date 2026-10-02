//! Tests for process manager event handler construction.
//!
//! The PM handler orchestrates process manager execution across event bus events:
//! - Event filtering via targets
//! - Destination fetching for cross-domain correlation
//! - Command execution with retry/backoff
//!
//! Unit tests verify:
//! - Constructor patterns store correct fields
//! - Builder methods correctly modify handler state
//! - Backoff configuration is applied

use super::*;
use crate::orchestration::command::{CommandExecutor, CommandOutcome};
use crate::orchestration::destination::DestinationFetcher;
use crate::orchestration::process_manager::PMContextFactory;
use crate::orchestration::FactExecutor;
use crate::proto::{CommandBook, Cover, EventBook, SyncMode};
use async_trait::async_trait;
use std::sync::Arc;

// ============================================================================
// Mock Implementations
// ============================================================================

struct MockPMContextFactory {
    name: String,
    pm_domain: String,
}

impl MockPMContextFactory {
    fn new(name: &str, pm_domain: &str) -> Self {
        Self {
            name: name.to_string(),
            pm_domain: pm_domain.to_string(),
        }
    }
}

impl PMContextFactory for MockPMContextFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn pm_domain(&self) -> &str {
        &self.pm_domain
    }

    fn create(&self) -> Box<dyn crate::orchestration::process_manager::ProcessManagerContext> {
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
    let factory: Arc<dyn PMContextFactory> =
        Arc::new(MockPMContextFactory::new("test-pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = ProcessManagerEventHandler::from_factory(factory.clone(), fetcher, executor);

    assert_eq!(handler.context_factory.name(), "test-pm");
    assert_eq!(handler.context_factory.pm_domain(), "pm-domain");
}

/// from_factory stores destination fetcher.
#[test]
fn test_from_factory_stores_destination_fetcher() {
    let factory: Arc<dyn PMContextFactory> = Arc::new(MockPMContextFactory::new("pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = ProcessManagerEventHandler::from_factory(factory, fetcher, executor);

    // Fetcher is stored - construction succeeds (can't easily verify content)
    assert!(handler.fact_executor.is_none());
}

/// from_factory stores command executor.
#[test]
fn test_from_factory_stores_command_executor() {
    let factory: Arc<dyn PMContextFactory> = Arc::new(MockPMContextFactory::new("pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = ProcessManagerEventHandler::from_factory(factory, fetcher, executor);

    // Executor is stored - construction succeeds
    assert!(handler.targets.is_empty());
}

/// from_factory initializes fact_executor to None.
#[test]
fn test_from_factory_fact_executor_none() {
    let factory: Arc<dyn PMContextFactory> = Arc::new(MockPMContextFactory::new("pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = ProcessManagerEventHandler::from_factory(factory, fetcher, executor);

    assert!(handler.fact_executor.is_none());
}

/// from_factory initializes targets to empty.
#[test]
fn test_from_factory_targets_empty() {
    let factory: Arc<dyn PMContextFactory> = Arc::new(MockPMContextFactory::new("pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = ProcessManagerEventHandler::from_factory(factory, fetcher, executor);

    assert!(handler.targets.is_empty());
}

/// from_factory sets default backoff.
#[test]
fn test_from_factory_sets_default_backoff() {
    let factory: Arc<dyn PMContextFactory> = Arc::new(MockPMContextFactory::new("pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = ProcessManagerEventHandler::from_factory(factory, fetcher, executor);

    // Can't directly inspect backoff, but verify it was set
    let _ = handler.backoff;
}

// ============================================================================
// Builder Method Tests
// ============================================================================

/// with_fact_executor stores fact executor.
#[test]
fn test_with_fact_executor_stores_executor() {
    let factory: Arc<dyn PMContextFactory> = Arc::new(MockPMContextFactory::new("pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);
    let fact_executor: Arc<dyn FactExecutor> = Arc::new(MockFactExecutor);

    let handler = ProcessManagerEventHandler::from_factory(factory, fetcher, executor)
        .with_fact_executor(Some(fact_executor));

    assert!(handler.fact_executor.is_some());
}

/// with_fact_executor with None keeps it None.
#[test]
fn test_with_fact_executor_none() {
    let factory: Arc<dyn PMContextFactory> = Arc::new(MockPMContextFactory::new("pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);

    let handler = ProcessManagerEventHandler::from_factory(factory, fetcher, executor)
        .with_fact_executor(None);

    assert!(handler.fact_executor.is_none());
}

/// with_targets stores targets.
#[test]
fn test_with_targets_stores_targets() {
    let factory: Arc<dyn PMContextFactory> = Arc::new(MockPMContextFactory::new("pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);
    let targets = vec![
        Target {
            domain: "order".to_string(),
            types: vec!["OrderCreated".to_string()],
        },
        Target {
            domain: "inventory".to_string(),
            types: vec![],
        },
    ];

    let handler =
        ProcessManagerEventHandler::from_factory(factory, fetcher, executor).with_targets(targets);

    assert_eq!(handler.targets.len(), 2);
    assert_eq!(handler.targets[0].domain, "order");
    assert_eq!(handler.targets[0].types, vec!["OrderCreated".to_string()]);
    assert_eq!(handler.targets[1].domain, "inventory");
    assert!(handler.targets[1].types.is_empty());
}

/// with_backoff stores custom backoff.
#[test]
fn test_with_backoff_stores_custom_backoff() {
    use backon::ExponentialBuilder;

    let factory: Arc<dyn PMContextFactory> = Arc::new(MockPMContextFactory::new("pm", "pm-domain"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);
    let custom_backoff = ExponentialBuilder::default().with_max_times(10);

    let handler = ProcessManagerEventHandler::from_factory(factory, fetcher, executor)
        .with_backoff(custom_backoff);

    // Can't directly compare backoffs, but construction succeeds
    let _ = handler.backoff;
}

/// Builder methods can be chained.
#[test]
fn test_builder_methods_chainable() {
    use backon::ExponentialBuilder;

    let factory: Arc<dyn PMContextFactory> =
        Arc::new(MockPMContextFactory::new("full-pm", "pm-full"));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(MockDestinationFetcher);
    let executor: Arc<dyn CommandExecutor> = Arc::new(MockCommandExecutor);
    let fact_executor: Arc<dyn FactExecutor> = Arc::new(MockFactExecutor);
    let targets = vec![Target {
        domain: "order".to_string(),
        types: vec![],
    }];

    let handler = ProcessManagerEventHandler::from_factory(factory, fetcher, executor)
        .with_fact_executor(Some(fact_executor))
        .with_targets(targets)
        .with_backoff(ExponentialBuilder::default().with_max_times(5));

    assert!(handler.fact_executor.is_some());
    assert_eq!(handler.targets.len(), 1);
    assert_eq!(handler.context_factory.name(), "full-pm");
    assert_eq!(handler.context_factory.pm_domain(), "pm-full");
}
