//! Tests for the `DestinationFetcher` trait's default `fetch_by_root`.
//!
//! WHY: `fetch_by_root` is the trait's one bridge from a (domain, root,
//! edition) key onto the `fetch(cover)` contract. Every implementor that
//! doesn't override it inherits this default, so the delegation itself IS
//! the contract: it must construct a cover carrying exactly the caller's
//! key and pass the inner fetch's answer through untouched. If the default
//! quietly returned `None` (or a fabricated default book) instead of
//! delegating, a PM looking up its state by root would treat a live
//! workflow as brand new — the same failure family as O9.

use tokio::sync::Mutex;

use super::*;
use crate::proto::EventPage;

/// Minimal implementor: only the two required methods, so `fetch_by_root`
/// resolves to the trait's default body. Records every cover handed to
/// `fetch` and answers with a fixed, non-default book.
struct RecordingFetcher {
    seen_covers: Mutex<Vec<Cover>>,
    book: EventBook,
}

#[async_trait]
impl DestinationFetcher for RecordingFetcher {
    async fn fetch(&self, cover: &Cover) -> Result<Option<EventBook>, Status> {
        self.seen_covers.lock().await.push(cover.clone());
        Ok(Some(self.book.clone()))
    }

    async fn fetch_by_correlation(
        &self,
        _domain: &str,
        _correlation_id: &str,
    ) -> Result<Option<EventBook>, Status> {
        panic!("fetch_by_root must delegate to fetch(), never fetch_by_correlation()");
    }
}

/// The default `fetch_by_root` delegates to `fetch` with a cover built
/// from the caller's (domain, root, edition) and returns the inner
/// fetch's book unchanged. Pins both halves of the delegation: a mutant
/// returning `Ok(None)` loses the book; a mutant returning
/// `Ok(Some(Default::default()))` fabricates state with no cover/pages
/// and never consults the source of truth.
#[tokio::test]
async fn test_fetch_by_root_default_delegates_to_fetch_with_constructed_cover() {
    let root = ProtoUuid {
        value: uuid::Uuid::new_v4().as_bytes().to_vec(),
    };
    // Deliberately non-default so `Some(Default::default())` is distinguishable.
    let inner_book = EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: Some(root.clone()),
            ..Default::default()
        }),
        pages: vec![EventPage::default()],
        ..Default::default()
    };
    let fetcher = RecordingFetcher {
        seen_covers: Mutex::new(Vec::new()),
        book: inner_book.clone(),
    };

    let got = fetcher
        .fetch_by_root("orders", &root, "audit")
        .await
        .expect("delegated fetch succeeds")
        .expect("inner fetch returned a book — the default must pass it through, not invent None");

    assert_eq!(
        got, inner_book,
        "the inner fetch's book must come back unchanged (not a fabricated default)"
    );

    let seen = fetcher.seen_covers.lock().await;
    assert_eq!(seen.len(), 1, "exactly one delegated fetch call");
    let cover = &seen[0];
    assert_eq!(
        cover.domain, "orders",
        "cover must carry the caller's domain"
    );
    assert_eq!(
        cover.root.as_ref().map(|r| r.value.clone()),
        Some(root.value.clone()),
        "cover must carry the caller's root"
    );
    assert_eq!(
        cover.edition.as_ref().map(|e| e.name.as_str()),
        Some("audit"),
        "cover must carry the caller's edition"
    );
    assert!(
        cover.correlation_id.is_empty(),
        "a root-keyed lookup carries no correlation_id — the root IS the key"
    );
}
