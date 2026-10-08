---
title: Event Bus
sidebar_label: Event Bus
---

# Event Bus

The EventBus decouples event producers from consumers. Aggregates don't know who's listening; handlers don't know where events originate. This loose coupling enables the reactive architecture that makes event sourcing powerful.

## Contract

The EventBus contract that all transport backends must satisfy:

```gherkin
Feature: EventBus

  The EventBus provides publish/subscribe messaging for events.
  All implementations must satisfy this contract.

  Scenario: Publish event reaches subscriber
    Given a subscriber listening to domain "player"
    When an event is published to domain "player"
    Then the subscriber receives the event

  Scenario: Domain filtering isolates subscribers
    Given a subscriber listening to domain "player"
    When an event is published to domain "hand"
    Then the subscriber does NOT receive the event

  Scenario: Fan-out to multiple subscribers
    Given two subscribers listening to domain "player"
    When an event is published to domain "player"
    Then both subscribers receive the event

  Scenario: At-least-once delivery
    Given a subscriber listening to domain "player"
    When a subscriber fails to process an event
    Then the event is redelivered
```

## Why Pub/Sub

After an aggregate persists events, interested parties need to react:

- **Projectors** build read models (denormalized views for queries)
- **Sagas** translate events into commands for other domains
- **Process managers** correlate events across multiple domains

Without pub/sub, the aggregate would need explicit knowledge of every consumer. Adding a new projector would require modifying aggregate code. Pub/sub inverts this—consumers declare interest, producers remain unchanged.

## Why At-Least-Once Delivery

The bus guarantees every published event reaches every subscriber at least once. Not exactly once:

- **Network partitions**: A consumer may process an event, then fail before acknowledging. The bus redelivers.
- **Crash recovery**: A subscriber might restart mid-processing. The bus redelivers from the last checkpoint.

This design choice pushes idempotency responsibility to handlers. Handlers must tolerate duplicate events. This is intentional:

- **Simplicity**: At-least-once is achievable without distributed transactions
- **Correctness**: Handlers already need idempotency for replay scenarios
- **Performance**: No two-phase commit overhead

Handlers achieve idempotency through the PositionStore. Before processing, check if this sequence was already handled. If so, skip.

## Why Domain Filtering

Subscribers declare which domains they care about:

```
subscribe(domain="player") → receives PlayerRegistered, FundsDeposited, ...
subscribe(domain="hand") → receives CardsDealt, ActionTaken, HandComplete, ...
```

This filtering prevents:

- **Wasted processing**: A player projector doesn't need hand events
- **Coupling**: Handlers explicitly declare dependencies, visible in code
- **Scaling**: Partition load by domain when needed

### Why Not Event Type Filtering

You can filter by event type within a domain, but domain filtering is primary. Domains represent bounded contexts with cohesive event vocabularies. Cross-cutting concerns (logging, metrics) subscribe to all domains.

## Why Fan-Out

Multiple handlers receive copies of the same event:

```
HandComplete → output-projector (updates game display)
            → hand-player-saga (transfers winnings to players)
            → hand-table-saga (signals table to end hand)
```

Each handler processes independently. One failing doesn't block others. This isolation enables:

- **Independent deployment**: Deploy projector changes without touching sagas
- **Graceful degradation**: Analytics can lag while order processing continues
- **Parallelism**: Handlers run concurrently

## Why Arc Wrapping

Events are wrapped in `Arc<EventBook>` during distribution. This enforces:

- **Immutability**: Handlers can't modify events (mutations would affect other handlers)
- **Zero-copy**: Multiple handlers share the same memory, no serialization between threads
- **Lifetime safety**: The event lives until all handlers finish

## Handler Dispatch Utilities

The `dispatch` module provides common patterns for handler invocation, eliminating duplication across bus implementations:

```rust
use angzarr::bus::{dispatch_to_handlers, process_message, DispatchResult};

// Dispatch an already-decoded EventBook to handlers
let success = dispatch_to_handlers(&handlers, &book).await;
if success {
    message.ack().await;
}

// Or use process_message for the full decode → dispatch → result cycle
match process_message(&payload, &handlers).await {
    DispatchResult::Success => message.ack().await,
    DispatchResult::HandlerFailed => message.nack().await,
    DispatchResult::DecodeError => message.ack().await, // Don't retry malformed messages
}
```

### DispatchResult

The `DispatchResult` enum captures the three possible outcomes:

| Variant | Meaning | Recommended Action |
|---------|---------|-------------------|
| `Success` | All handlers completed without error | Acknowledge message |
| `HandlerFailed` | At least one handler returned an error | Nack for retry or send to DLQ |
| `DecodeError` | Payload couldn't be decoded as EventBook | Acknowledge (retry won't help) |

The `should_ack()` method returns `true` for `Success` and `DecodeError`—malformed messages should not be retried indefinitely.

## Choosing a Transport

| Transport | Durability | Latency | Use Case |
|-----------|------------|---------|----------|
| IPC | None | Sub-millisecond | Multi-process local dev. Named pipes, Unix only. |
| AMQP (RabbitMQ) | Configurable | Milliseconds | Production default. Mature, widely deployed. |
| Kafka | Strong | Milliseconds | High-throughput, log retention for replay. |
| GCP Pub/Sub | Strong | Milliseconds | GCP deployments. Managed, scalable. |
| AWS SNS/SQS | Strong | Milliseconds | AWS deployments. Managed, integrated with IAM. |
| Outbox | Database-backed | Higher | Guaranteed delivery via transactional outbox pattern. |

### Cloud Transports

**GCP Pub/Sub**: Google Cloud native pub/sub. Uses topics per domain: `{prefix}-events-{domain}`. Subscriptions are named: `{prefix}-{subscriber_id}-{domain}`. Authentication via Application Default Credentials (ADC).

**AWS SNS/SQS**: SNS topics for publishing, SQS queues for subscribing. Topics: `{prefix}-events-{domain}`. Queues: `{prefix}-{subscriber_id}-{domain}`. Supports LocalStack for local development.

### Future: AWS Kinesis

Kinesis support is planned but not yet implemented. Design notes:

- **Streams per domain**: `angzarr-events-{domain}`
- **Partition key**: Aggregate root ID (ordering within aggregate)
- **Consumer**: Kinesis Consumer Library (KCL) pattern
- **DLQ**: `angzarr-dlq-{domain}` stream

Kinesis is appropriate when you need:
- Replay from any point in time (configurable retention)
- Multiple independent consumers reading at their own pace
- Integration with AWS Lambda, Firehose, Analytics

### When to Use Each

**IPC**: Local-dev deployments with multiple processes (aggregate + projectors). Low latency, zero network overhead.

**AMQP**: Distributed deployments. Durable queues, topic-based routing, dead-letter handling.

**Kafka**: When you need event log retention beyond immediate consumption. Analytics, replay capabilities, cross-team event sharing.

**Pub/Sub**: GCP deployments. Fully managed, integrates with GCP IAM and monitoring.

**SNS/SQS**: AWS deployments. Fully managed, integrates with AWS IAM and CloudWatch.

**Outbox**: When publish-after-commit is insufficient. Guarantees events are published even if the application crashes between commit and publish. **Rarely needed** - AMQP, Kafka, Pub/Sub, and SNS/SQS all guarantee at-least-once delivery. Outbox adds database round trips, polling overhead, and operational complexity. Only use it when your application cannot tolerate the narrow window between database commit and broker acknowledgment (e.g., regulatory requirements, financial transactions).

## Dead Letter Queue (DLQ)

When event processing fails, the message needs somewhere to go. The DLQ captures failed messages for manual review and replay.

### Architecture

DLQ is **separate from the EventBus**. The bus's job is event delivery; DLQ is a failure-handling concern.

```
┌──────────────┐    publish    ┌──────────┐    deliver    ┌──────────┐
│  Aggregate   │──────────────▶│ EventBus │──────────────▶│ Handler  │
└──────────────┘               └──────────┘               └────┬─────┘
                                                               │
                                                        (on failure)
                                                               │
                                                               ▼
                                                    ┌────────────────────┐
                                                    │ DeadLetterPublisher│
                                                    └────────────────────┘
                                                               │
                                                               ▼
                                                    ┌────────────────────┐
                                                    │  angzarr.dlq.{dom} │
                                                    └────────────────────┘
```

Handlers call `DeadLetterPublisher::publish()` when they determine a message is unprocessable. This is explicit—the bus doesn't automatically DLQ failed messages.

### DLQ Topic Naming

Per-domain isolation: `angzarr.dlq.{domain}`

This enables:
- Domain-specific retention policies
- Targeted replay (just orders, not everything)
- Access control by domain

### Dead Letter Payload

The `AngzarrDeadLetter` message captures:
- **cover**: Original routing info (domain, root, correlation_id)
- **payload**: The failed message (CommandBook or EventBook)
- **rejection_reason**: Human-readable explanation
- **rejection_details**: Structured details (sequence mismatch, handler error, etc.)
- **source_component**: Which component failed
- **occurred_at**: When the failure happened
- **metadata**: Additional context

### Rejection Types

| Type | Cause | Action |
|------|-------|--------|
| SequenceMismatch | Command expects sequence N, aggregate at M | Manual merge or retry |
| EventProcessingFailed | Handler threw an error | Fix handler, replay |
| PayloadRetrievalFailed | Claim check fetch failed | Restore payload, replay |

### DLQ Backends

Each transport has a corresponding DLQ publisher in `src/dlq/mod.rs`:

| Backend | DLQ Publisher | Topic Format |
|---------|---------------|--------------|
| Channel | `ChannelDeadLetterPublisher` | In-memory (in-process/test) |
| AMQP | `AmqpDeadLetterPublisher` | Exchange: `angzarr.dlq`, routing key: `{domain}` |
| Kafka | `KafkaDeadLetterPublisher` | Topic: `angzarr-dlq-{domain}` |
| Pub/Sub | `PubSubDeadLetterPublisher` | Topic: `angzarr-dlq-{domain}` |
| SNS/SQS | `SnsSqsDeadLetterPublisher` | Topic: `angzarr-dlq-{domain}` |

### Usage

```rust
use angzarr::dlq::{create_publisher_async, DlqConfig, AngzarrDeadLetter};

// Create publisher for your backend
let config = DlqConfig::amqp("amqp://localhost:5672");
let dlq_publisher = create_publisher_async(&config).await?;

// In your handler, on failure:
let dead_letter = AngzarrDeadLetter::from_event_processing_failure(
    &events,
    "Handler failed: invalid state",
    3,     // retry_count
    false, // not transient
    "my-projector",
    "projector",
);
dlq_publisher.publish(dead_letter).await?;
```

## Offloading Large Payloads

Some events contain large payloads (images, documents). Message brokers have size limits:

- SNS/SQS: 256 KB
- Pub/Sub: 10 MB
- Kafka: Broker-configurable, typically 1 MB

The `OffloadingEventBus` wrapper transparently handles this:

1. Payloads exceeding threshold are stored externally (S3, GCS, filesystem)
2. Event carries a reference (hash) instead of the payload
3. Consumers fetch the payload on demand

This is the "claim check" pattern. The bus remains fast; storage handles bulk.

## Feature Specifications

See the embedded contract above. Full specifications cover:

- Publish/subscribe semantics
- Domain filtering
- Fan-out to multiple subscribers
- Payload integrity

## Running Interface Tests

```bash
# Test against a specific transport
BUS_BACKEND=amqp cargo test --test interfaces
BUS_BACKEND=kafka cargo test --test interfaces
```

Tests verify every transport implements the same contract.
