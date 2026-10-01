//! The event-store outbox log: records, attempts and closes are appended to
//! a framework-owned stream per obligation, and open obligations are read
//! back with their progress.

use super::*;
use crate::orchestration::outbox::OutboxEntry;
use crate::proto::{
    command_page, page_header::SequenceType, AngzarrDeferredSequence, CommandPage, Cover,
    Uuid as ProtoUuid,
};
use crate::storage::mock::MockEventStore;

fn reserve_stock(command_index: u32) -> CommandBook {
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
                    command_index,
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

fn log() -> (EventStoreOutboxLog, Arc<MockEventStore>) {
    let store = Arc::new(MockEventStore::new());
    (
        EventStoreOutboxLog::new(store.clone(), "Fulfillment"),
        store,
    )
}

#[tokio::test]
async fn records_are_read_back_open_with_their_attempts() {
    let (log, _) = log();
    let entry = OutboxEntry::command(reserve_stock(0));

    log.append_record(&entry).await.unwrap();
    log.append_attempt(&entry.key, "first").await.unwrap();
    log.append_attempt(&entry.key, "second").await.unwrap();

    let open = log.open_entries().await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].key, entry.key);
    assert_eq!(open[0].book, entry.book);
    assert_eq!(open[0].attempts, 2);
    assert_eq!(open[0].last_error, "second");
}

#[tokio::test]
async fn closed_records_are_not_open() {
    let (log, _) = log();
    let first = OutboxEntry::command(reserve_stock(0));
    let second = OutboxEntry::command(reserve_stock(1));

    log.append_record(&first).await.unwrap();
    log.append_record(&second).await.unwrap();
    log.append_close(&first.key).await.unwrap();

    let open = log.open_entries().await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].key, second.key);
}

/// Recording an existing obligation again is a no-op, not an error.
#[tokio::test]
async fn re_recording_is_a_no_op() {
    let (log, store) = log();
    let entry = OutboxEntry::command(reserve_stock(0));

    log.append_record(&entry).await.unwrap();
    log.append_record(&entry).await.unwrap();

    let root = EventStoreOutboxLog::root(&entry.key);
    assert_eq!(store.get(log.domain(), "", root).await.unwrap().len(), 1);
}

/// The log lives in its own framework-owned domain, keyed by the entry.
#[tokio::test]
async fn streams_live_in_the_outbox_domain() {
    let (log, store) = log();
    let entry = OutboxEntry::command(reserve_stock(0));
    log.append_record(&entry).await.unwrap();

    assert_eq!(log.domain(), "_angzarr_outbox.Fulfillment");
    let roots = store.list_roots(log.domain(), "").await.unwrap();
    assert_eq!(roots, vec![EventStoreOutboxLog::root(&entry.key)]);
    assert_ne!(
        EventStoreOutboxLog::root(&entry.key),
        EventStoreOutboxLog::root(&OutboxEntry::command(reserve_stock(1)).key)
    );
}

/// Pages that are not outbox pages are ignored; a stream without a record
/// is not an obligation.
#[test]
fn unrecognized_pages_are_skipped() {
    let stray = EventStoreOutboxLog::page(0, "/x.Unknown", vec![]);
    assert!(EventStoreOutboxLog::entry_from_pages(std::slice::from_ref(&stray)).is_none());
    let attempt_only =
        EventStoreOutboxLog::page(0, ATTEMPT_TYPE_URL, "e".to_string().encode_to_vec());
    assert!(EventStoreOutboxLog::entry_from_pages(&[attempt_only]).is_none());

    let entry = OutboxEntry::command(reserve_stock(0));
    let record = EventStoreOutboxLog::page(0, type_url::COMMAND_BOOK, entry.book.encode_to_vec());
    let rebuilt = EventStoreOutboxLog::entry_from_pages(&[record, stray]).unwrap();
    assert_eq!(rebuilt, entry);
}

/// The memory log persists nothing.
#[tokio::test]
async fn memory_log_keeps_nothing() {
    let log = MemoryOutboxLog;
    let entry = OutboxEntry::command(reserve_stock(0));
    log.append_record(&entry).await.unwrap();
    log.append_attempt(&entry.key, "e").await.unwrap();
    log.append_close(&entry.key).await.unwrap();
    assert!(log.open_entries().await.unwrap().is_empty());
}
