# DELETED

What has been removed from angzarr core, what it did, and where to get it back.
"Last hash" is the last commit in which the item existed: `git show <hash>:<path>`
or `git checkout <hash> -- <path>` recovers it.

Most of this went in 9193c819 ("Port the coordinators to the latest angzarr
proto"), which moved core onto angzarr-project 541cadc (`io.angzarr.v1`,
passes 1-13 plus the owner's decisions 14.x) and kept only what the stage-1
MVP runs: the aggregate and projector coordinators, SQLite storage and the
AMQP bus. Its parent, **16db7169**, is the last commit with everything below.

There are two kinds of entry. **Deleted** items are gone for good: the spec
removed them, or the owner approved deleting them. **Parked** items are still
wanted. They weren't ported to the latest proto yet, and they come back from
16db7169 as they are ported.

## Deleted: removed by the spec (angzarr-project 541cadc)

| Item | What it did | Last hash |
|---|---|---|
| Cascade / 2PC: `src/orchestration/aggregate/two_phase.rs`, `src/cascade/` (`CascadeReaper`), the `cascade_id` columns (migrations `0004_cascade_columns` sqlite, `0005_cascade_columns` postgres) | Synchronous CASCADE mode. The aggregate called the subscribed sagas and PMs in-line, ran a two-phase commit across the chain, and a reaper rolled back stuck cascades. The spec deletes cascade sync modes (24920c8; RM-1, RM-6). | 16db7169 |
| Speculative execution (`client_traits.rs`, the speculative paths in `orchestration/aggregate/{pipeline,types}.rs`, `orchestration/projector`, `services/aggregate.rs`) | Ran handlers without persisting, to preview their output. The spec deletes it. | 16db7169 |
| Compensation and the rejection-notification channel (`src/utils/saga_compensation/`, `RejectionNotification` handling in the saga/PM orchestration, `features/acceptance/compensation_{emit,handle}.feature`) | When a saga-issued command was rejected, it sent a notification back so the saga could compensate. Under the spec, a target refuses by recording a domain event, and there is no compensation input. | 16db7169 |
| `angzarr.saga.compensation.total` counter (`src/advice/metrics.rs`) | Counted saga compensations; nothing emitted it once compensation went. | c10b16b3 |
| The `"angzarr"` edition alias (`DEFAULT_EDITION` and `is_main_timeline` in `src/proto_ext/{constants,edition}.rs`, postgres migration `0010_delete_edition_events_reject_angzarr`) | Treated the edition name "angzarr" as the main timeline. Under the spec, an unset edition means main. | 16db7169 |
| u32 sequences | Event sequences were u32; the spec makes them u64. Changed, not removed: every store and type on this line is u64. | 16db7169 (u32 form) |
| `SagaResponse.events` and PM facts (`orchestration/saga`, `orchestration/process_manager`, `orchestration/fact`) | Sagas could return events, and PMs emitted facts, alongside commands. Both are absent from the spec at 541cadc. The saga and PM coordinators themselves are parked (see below). | 16db7169 |

## Deleted: approved by the owner (B6, 2026-10-26)

| Item | What it did | Last hash |
|---|---|---|
| Bigtable stores (`src/storage/bigtable/`) | Event, snapshot and position stores on GCP Bigtable. No tests, no config, no deployment used them. | 16db7169 |
| DynamoDB stores (`src/storage/dynamo/`) | Event, snapshot and position stores on AWS DynamoDB. No tests, no config, no deployment used them. | 16db7169 |
| Payload offloading: `src/bus/offloading.rs` (`OffloadingEventBus`), `src/payload_store/` (filesystem, GCS, S3 and its TTL `reaper`) | Claim-check for oversized messages. A payload over the bus's size limit was stored externally and replaced by a reference, and the reaper expired stored payloads. Zero callers. | 16db7169 |
| `EventBus::max_message_size` and AMQP's `MAX_MESSAGE_SIZE`, `MockEventBus::with_max_message_size` | Told `OffloadingEventBus` each transport's size wall. Nothing else read it. | 651132ed |
| StreamService (`src/handlers/projectors/stream/`, `src/handlers/projectors/`) | Broadcast events to registered gRPC stream subscribers. It had zero constructors. | 16db7169 |
| Instrumented PM/projector/saga wrappers (`src/advice/instrumented_handlers.rs`) | Metrics wrappers around the handler traits; never wired up. The storage and bus `Instrumented` wrappers are used and stay. | 16db7169 |
| `LossyBus` (`src/advice/lossy.rs`, the `lossy` feature, the optional `rand` dependency) | A test bus that dropped messages at random. Unused. | ed3b96e3 |
| `angzarr_upcaster` bin (`src/bin/angzarr_upcaster.rs`) | A passthrough upcaster process that changed nothing. Upcasting now happens on every read and publish path inside the coordinators (3c62e164), with the upcaster part of its component. | 16db7169 |
| Stale configs | **Not done.** The deploy tree (`deploy/k8s/helm`, `deploy/tofu`) still names the status console, the stream service, the upcaster image and a compensation dashboard. Bringing it up to the bins that exist is taskloom row penniless-reprise, and none of it can be verified without a cluster. | n/a |

## Parked: not yet ported to the latest proto (restore from 16db7169)

These were removed on the port line (9193c819) because they were not yet
ported. They are not deletions; each comes back from **16db7169** as it is
ported.

| Item | Paths at 16db7169 |
|---|---|
| Saga and PM coordinators and bins | `src/bin/angzarr_saga.rs`, `src/bin/angzarr_process_manager.rs`, `src/services/{saga,pm}_coord.rs`, `src/orchestration/{saga,process_manager,command,destination,fact}/`, `src/handlers/core/saga.rs` |
| Postgres storage (the backend full-sail deploys) | `src/storage/postgres/`, `src/storage/sql/`, `migrations/postgres/` |
| Redis and immudb storage | `src/storage/redis/`, `src/storage/immudb/` |
| Kafka, Pub/Sub and SNS/SQS buses | `src/bus/{kafka,pubsub,sns_sqs}/` |
| The status console (`angzarr_status`) and DLQ admin, replay and audit | `src/bin/angzarr_status.rs`, `src/status/`, `src/dlq/{audit,filter,reader,replay}.rs`, `proto/io/angzarr/status/v1/dlq_admin.proto`, `migrations/status/` |
| DLQ publishers other than AMQP, channel, filesystem, logging and noop (database, kafka, pubsub, sns-sqs, the offload stores) | `src/dlq/publishers/` |
| Gap fill, event query and the position store | `src/services/gap_fill/`, `src/services/event_query/`, `src/storage/position_store.rs` |
| Mock storage and the shared storage test suites | `src/storage/mock/`, `tests/storage/` |
| The SQLite migration history (replaced by one baseline, 9eb9f360) | `migrations/sqlite/0001..0010` (last at 0a4cc813) |

The `DlqConfig` still accepts the offload DLQ types
(`offload_filesystem`, `offload_gcs`, `offload_s3`), although nothing on this
line builds them. They belong to the parked DLQ publishers, not to payload
offloading.
