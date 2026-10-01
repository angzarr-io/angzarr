//! How a coordinator delivers outbox entries and classifies the outcome.

use super::*;

use std::sync::Mutex as StdMutex;

use crate::bus::MockEventBus;
use crate::proto::{
    business_response, command_page, page_header::SequenceType, AngzarrDeferredSequence,
    CommandPage, CommandResponse, Cover, PageHeader, RevocationResponse, Uuid as ProtoUuid,
};

fn reserve_stock() -> CommandBook {
    let cover = |domain: &str, root: u8| Cover {
        domain: domain.to_string(),
        root: Some(ProtoUuid {
            value: vec![root; 16],
        }),
        correlation_id: "corr-1".to_string(),
        edition: None,
        ext: None,
    };
    CommandBook {
        cover: Some(cover("inventory", 2)),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                    source: Some(cover("order", 1)),
                    source_seq: 0,
                    source_component: "OrderFulfillment".to_string(),
                    command_index: 0,
                })),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "/inventory.ReserveStock".to_string(),
                value: vec![],
            })),
            merge_strategy: 0,
        }],
    }
}

fn rejection_entry() -> OutboxEntry {
    OutboxEntry::notification(
        crate::orchestration::compensation::rejection_envelope(&reserve_stock(), "out of stock")
            .unwrap(),
    )
    .unwrap()
}

/// Answers HandleCompensation with a fixed result and records the envelopes.
struct FixedSender {
    result: Result<BusinessResponse, Status>,
    sent: StdMutex<Vec<CommandBook>>,
}

impl FixedSender {
    fn new(result: Result<BusinessResponse, Status>) -> Arc<Self> {
        Arc::new(Self {
            result,
            sent: StdMutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl CompensationSender for FixedSender {
    async fn handle_compensation(&self, envelope: CommandBook) -> Result<BusinessResponse, Status> {
        self.sent.lock().unwrap().push(envelope);
        self.result.clone()
    }
}

struct FixedExecutor(fn() -> CommandOutcome, StdMutex<Vec<SyncMode>>);

#[async_trait]
impl CommandExecutor for FixedExecutor {
    async fn execute(&self, _command: CommandBook, sync_mode: SyncMode) -> CommandOutcome {
        self.1.lock().unwrap().push(sync_mode);
        (self.0)()
    }
}

#[tokio::test]
async fn notification_delivered_on_success() {
    let sender = FixedSender::new(Ok(BusinessResponse::default()));
    let deliverer = CoordinatorDeliverer::new(sender.clone());
    let entry = rejection_entry();

    assert_eq!(deliverer.deliver(&entry).await, DeliveryResult::Delivered);
    assert_eq!(sender.sent.lock().unwrap().clone(), vec![entry.book]);
}

/// UNIMPLEMENTED is the one notification failure that is not retried.
#[tokio::test]
async fn notification_unimplemented_is_rejected() {
    let deliverer = CoordinatorDeliverer::new(FixedSender::new(Err(Status::unimplemented(
        "no undo handler",
    ))));
    assert_eq!(
        deliverer.deliver(&rejection_entry()).await,
        DeliveryResult::Rejected {
            code: tonic::Code::Unimplemented,
            message: "no undo handler".to_string(),
        }
    );
}

#[tokio::test]
async fn notification_other_failures_are_retryable() {
    let deliverer =
        CoordinatorDeliverer::new(FixedSender::new(Err(Status::unavailable("order down"))));
    assert_eq!(
        deliverer.deliver(&rejection_entry()).await,
        DeliveryResult::Retryable("order down".to_string())
    );
}

#[tokio::test]
async fn command_outcomes_map_to_delivery_results() {
    let sender = FixedSender::new(Ok(BusinessResponse::default()));
    let entry = OutboxEntry::command(reserve_stock());

    let success = Arc::new(FixedExecutor(
        || CommandOutcome::Success(CommandResponse::default()),
        StdMutex::new(Vec::new()),
    ));
    let deliverer = CoordinatorDeliverer::new(sender.clone())
        .with_commands(success.clone(), SyncMode::Decision);
    assert_eq!(deliverer.deliver(&entry).await, DeliveryResult::Delivered);
    assert_eq!(success.1.lock().unwrap().clone(), vec![SyncMode::Decision]);
    assert!(
        sender.sent.lock().unwrap().is_empty(),
        "commands never go to HandleCompensation"
    );

    let retryable = Arc::new(FixedExecutor(
        || CommandOutcome::Retryable {
            reason: "busy".to_string(),
            current_state: None,
        },
        StdMutex::new(Vec::new()),
    ));
    let deliverer =
        CoordinatorDeliverer::new(sender.clone()).with_commands(retryable, SyncMode::Simple);
    assert_eq!(
        deliverer.deliver(&entry).await,
        DeliveryResult::Retryable("busy".to_string())
    );

    let rejected = Arc::new(FixedExecutor(
        || CommandOutcome::Rejected {
            code: tonic::Code::FailedPrecondition,
            message: "out of stock".to_string(),
        },
        StdMutex::new(Vec::new()),
    ));
    let deliverer = CoordinatorDeliverer::new(sender).with_commands(rejected, SyncMode::Simple);
    assert_eq!(
        deliverer.deliver(&entry).await,
        DeliveryResult::Rejected {
            code: tonic::Code::FailedPrecondition,
            message: "out of stock".to_string(),
        }
    );
}

/// Without an executor a command entry stays pending.
#[tokio::test]
async fn command_without_executor_is_retryable() {
    let deliverer = CoordinatorDeliverer::new(FixedSender::new(Ok(BusinessResponse::default())));
    assert!(matches!(
        deliverer
            .deliver(&OutboxEntry::command(reserve_stock()))
            .await,
        DeliveryResult::Retryable(_)
    ));
}

/// A RevocationResponse asking for a system revocation publishes the
/// SagaCompensationFailed event.
#[tokio::test]
async fn revocation_response_is_acted_on() {
    let bus = Arc::new(MockEventBus::new());
    let sender = FixedSender::new(Ok(BusinessResponse {
        result: Some(business_response::Result::Revocation(RevocationResponse {
            emit_system_revocation: true,
            reason: "cannot cancel".to_string(),
            ..Default::default()
        })),
    }));
    let deliverer =
        CoordinatorDeliverer::new(sender).with_revocation_handling(RevocationHandling {
            event_bus: bus.clone(),
            config: SagaCompensationConfig::default(),
        });

    assert_eq!(
        deliverer.deliver(&rejection_entry()).await,
        DeliveryResult::Delivered
    );
    assert_eq!(bus.published_count().await, 1);
}

/// Revocation handling applies only to delivered rejections.
#[tokio::test]
async fn compensate_responses_are_not_revocations() {
    let bus = Arc::new(MockEventBus::new());
    let sender = FixedSender::new(Ok(BusinessResponse {
        result: Some(business_response::Result::Revocation(RevocationResponse {
            emit_system_revocation: true,
            ..Default::default()
        })),
    }));
    let deliverer =
        CoordinatorDeliverer::new(sender).with_revocation_handling(RevocationHandling {
            event_bus: bus.clone(),
            config: SagaCompensationConfig::default(),
        });
    let compensate = OutboxEntry::notification(
        crate::orchestration::compensation::compensate_envelope(&reserve_stock(), None, "r"),
    )
    .unwrap();

    assert_eq!(
        deliverer.deliver(&compensate).await,
        DeliveryResult::Delivered
    );
    assert_eq!(bus.published_count().await, 0);
}

/// The compensation request carries the envelope and its correlation id.
#[test]
fn compensation_request_wraps_the_envelope() {
    let envelope = rejection_entry().book;
    let request = compensation_request(envelope.clone());
    assert_eq!(request.get_ref().command, Some(envelope));
    assert_eq!(request.get_ref().sync_mode, SyncMode::Async as i32);
}
