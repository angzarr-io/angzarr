//! The storage contract, run against every backend.
//!
//! Why this matters: the coordinator's correctness (optimistic appends,
//! deduplication on command ids, time-bounded prefixes, the newest-only
//! snapshot) rests on every backend behaving identically. Each backend's
//! test module invokes [`event_store_contract!`] and
//! [`snapshot_store_contract!`] with a constructor.

use prost_types::Timestamp;
use uuid::Uuid;

use crate::proto::{event_meta::Causation, EventMeta, EventPage, Snapshot};

use super::StreamRef;

pub(crate) const DOMAIN: &str = "order";

pub(crate) fn root(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

pub(crate) fn ts(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}

/// A stamped page as the coordinator would append it.
pub(crate) fn page(sequence: u64, command: Uuid, correlation: Uuid, at: i64) -> EventPage {
    EventPage {
        payload: Some(prost_types::Any {
            type_url: "/orders.v1.ItemAdded".to_string(),
            value: vec![sequence as u8],
        }),
        meta: Some(EventMeta {
            causation: Some(Causation::CommandId(command.into())),
            correlation_id: Some(correlation.into()),
            recorded_at: Some(ts(at)),
            rules_version: "1.4.2+build.7".to_string(),
            fact: None,
        }),
        sequence,
    }
}

pub(crate) fn snapshot(sequence: u64) -> Snapshot {
    Snapshot {
        sequence,
        state: Some(prost_types::Any {
            type_url: "/orders.v1.OrderState".to_string(),
            value: vec![1],
        }),
        rules_version: "1.4.2+build.7".to_string(),
    }
}

pub(crate) fn main(n: u128) -> StreamRef<'static> {
    StreamRef::main(DOMAIN, root(n))
}

macro_rules! event_store_contract {
    ($make:expr) => {
        use $crate::storage::contract::*;
        use $crate::storage::{EventStore, StorageError, StreamRef};

        #[tokio::test]
        async fn contract_append_then_read_in_order() {
            let store = $make;
            let (c, k) = (root(100), root(200));
            store
                .append(main(1), &[page(0, c, k, 10), page(1, c, k, 10)])
                .await
                .unwrap();
            let pages = store.read(main(1), 0, None).await.unwrap();
            assert_eq!(pages.iter().map(|p| p.sequence).collect::<Vec<_>>(), [0, 1]);
            assert_eq!(pages[0], page(0, c, k, 10));
        }

        #[tokio::test]
        async fn contract_append_refuses_a_stale_first_sequence() {
            let store = $make;
            let (c, k) = (root(100), root(200));
            store.append(main(1), &[page(0, c, k, 10)]).await.unwrap();
            let err = store
                .append(main(1), &[page(0, c, k, 11)])
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    StorageError::SequenceConflict {
                        expected: 0,
                        actual: 1
                    }
                ),
                "{err}"
            );
            let gap = store
                .append(main(1), &[page(5, c, k, 11)])
                .await
                .unwrap_err();
            assert!(matches!(gap, StorageError::SequenceConflict { .. }));
            assert_eq!(store.read(main(1), 0, None).await.unwrap().len(), 1);
        }

        #[tokio::test]
        async fn contract_read_bounds_are_half_open() {
            let store = $make;
            let (c, k) = (root(100), root(200));
            let pages: Vec<_> = (0..5).map(|s| page(s, c, k, 10)).collect();
            store.append(main(1), &pages).await.unwrap();
            let mid = store.read(main(1), 1, Some(3)).await.unwrap();
            assert_eq!(mid.iter().map(|p| p.sequence).collect::<Vec<_>>(), [1, 2]);
            let tail = store.read(main(1), 3, None).await.unwrap();
            assert_eq!(tail.iter().map(|p| p.sequence).collect::<Vec<_>>(), [3, 4]);
        }

        #[tokio::test]
        async fn contract_head_reports_next_sequence_and_last_time() {
            let store = $make;
            assert_eq!(store.head(main(1)).await.unwrap().next_sequence, 0);
            let (c, k) = (root(100), root(200));
            store
                .append(main(1), &[page(0, c, k, 10), page(1, c, k, 12)])
                .await
                .unwrap();
            let head = store.head(main(1)).await.unwrap();
            assert_eq!(head.next_sequence, 2);
            assert_eq!(head.last_recorded_at, Some(ts(12)));
        }

        #[tokio::test]
        async fn contract_streams_are_isolated_by_root_and_edition() {
            let store = $make;
            let (c, k) = (root(100), root(200));
            store.append(main(1), &[page(0, c, k, 10)]).await.unwrap();
            store.append(main(2), &[page(0, c, k, 10)]).await.unwrap();
            let edition = StreamRef {
                domain: DOMAIN,
                edition: Some("what-if"),
                root: root(1),
            };
            assert!(store.read(edition, 0, None).await.unwrap().is_empty());
            store.append(edition, &[page(0, c, k, 10)]).await.unwrap();
            assert_eq!(store.read(main(1), 0, None).await.unwrap().len(), 1);
            assert_eq!(store.list_streams().await.unwrap().len(), 3);
        }

        #[tokio::test]
        async fn contract_read_until_time_is_a_prefix() {
            let store = $make;
            let (c, k) = (root(100), root(200));
            store
                .append(
                    main(1),
                    &[page(0, c, k, 10), page(1, c, k, 20), page(2, c, k, 30)],
                )
                .await
                .unwrap();
            let prefix = store.read_until_time(main(1), &ts(20)).await.unwrap();
            assert_eq!(
                prefix.iter().map(|p| p.sequence).collect::<Vec<_>>(),
                [0, 1]
            );
        }

        #[tokio::test]
        async fn contract_find_by_command_id_returns_that_writes_pages() {
            let store = $make;
            let (first, second, k) = (root(100), root(101), root(200));
            store
                .append(main(1), &[page(0, first, k, 10), page(1, first, k, 10)])
                .await
                .unwrap();
            store
                .append(main(1), &[page(2, second, k, 11)])
                .await
                .unwrap();
            let hit = store.find_by_command_id(main(1), &first).await.unwrap();
            assert_eq!(hit.iter().map(|p| p.sequence).collect::<Vec<_>>(), [0, 1]);
            assert!(store
                .find_by_command_id(main(2), &first)
                .await
                .unwrap()
                .is_empty());
            assert!(store
                .find_by_command_id(main(1), &root(999))
                .await
                .unwrap()
                .is_empty());
        }

        #[tokio::test]
        async fn contract_read_by_correlation_groups_by_stream() {
            let store = $make;
            let (c, k, other) = (root(100), root(200), root(201));
            store
                .append(main(1), &[page(0, c, k, 10), page(1, c, other, 10)])
                .await
                .unwrap();
            store.append(main(2), &[page(0, c, k, 10)]).await.unwrap();
            let books = store
                .read_by_correlation(Some(DOMAIN), None, &k)
                .await
                .unwrap();
            assert_eq!(books.len(), 2);
            assert!(books.iter().all(|b| b.pages.len() == 1));
            assert!(store
                .read_by_correlation(Some("other"), None, &k)
                .await
                .unwrap()
                .is_empty());
            assert_eq!(
                store
                    .read_by_correlation(None, None, &k)
                    .await
                    .unwrap()
                    .len(),
                2
            );
        }
    };
}

macro_rules! snapshot_store_contract {
    ($make:expr) => {
        use $crate::storage::SnapshotStore as _;

        #[tokio::test]
        async fn contract_snapshot_newest_only() {
            let store = $make;
            let stream = $crate::storage::contract::main(1);
            assert!(store.get(stream).await.unwrap().is_none());
            store
                .put(stream, &$crate::storage::contract::snapshot(5))
                .await
                .unwrap();
            store
                .put(stream, &$crate::storage::contract::snapshot(9))
                .await
                .unwrap();
            let got = store.get(stream).await.unwrap().unwrap();
            assert_eq!(got, $crate::storage::contract::snapshot(9));
            assert!(store
                .get($crate::storage::contract::main(2))
                .await
                .unwrap()
                .is_none());
        }
    };
}

pub(crate) use event_store_contract;
pub(crate) use snapshot_store_contract;
