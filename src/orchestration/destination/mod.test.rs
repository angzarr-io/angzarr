//! Tests for the `DestinationFetcher` trait's default `fetch_pm_state`.
//!
//! WHY: fetchers without local PM storage (the remote gRPC fetcher) inherit
//! this default, which must answer from `fetch_by_correlation` and pass its
//! result — including "no state" and errors — through untouched. Returning
//! a fabricated book or swallowing an error would make a PM restart a live
//! workflow (the O9 failure family).

use tokio::sync::Mutex;

use super::*;
use crate::proto::EventPage;

/// Minimal implementor: only the required methods, so `fetch_pm_state`
/// resolves to the trait default. Records each correlation lookup.
struct RecordingFetcher {
    seen: Mutex<Vec<(String, String)>>,
    answer: Result<Option<EventBook>, Status>,
}

#[async_trait]
impl DestinationFetcher for RecordingFetcher {
    async fn fetch(&self, _cover: &Cover) -> Result<Option<EventBook>, Status> {
        panic!("fetch_pm_state must use fetch_by_correlation");
    }

    async fn fetch_by_correlation(
        &self,
        domain: &str,
        correlation_id: &str,
    ) -> Result<Option<EventBook>, Status> {
        self.seen
            .lock()
            .await
            .push((domain.to_string(), correlation_id.to_string()));
        self.answer.clone()
    }
}

#[tokio::test]
async fn test_fetch_pm_state_default_delegates_to_correlation_lookup() {
    let book = EventBook {
        pages: vec![EventPage::default()],
        ..Default::default()
    };
    let fetcher = RecordingFetcher {
        seen: Mutex::new(Vec::new()),
        answer: Ok(Some(book.clone())),
    };
    let got = fetcher
        .fetch_pm_state("pm-flow", "branch", "corr-1")
        .await
        .unwrap();
    assert_eq!(got, Some(book));
    assert_eq!(
        *fetcher.seen.lock().await,
        vec![("pm-flow".to_string(), "corr-1".to_string())]
    );
}

#[tokio::test]
async fn test_fetch_pm_state_default_passes_through_absence_and_errors() {
    let absent = RecordingFetcher {
        seen: Mutex::new(Vec::new()),
        answer: Ok(None),
    };
    assert_eq!(absent.fetch_pm_state("pm", "", "c").await.unwrap(), None);

    let failing = RecordingFetcher {
        seen: Mutex::new(Vec::new()),
        answer: Err(Status::unavailable("down")),
    };
    assert_eq!(
        failing
            .fetch_pm_state("pm", "", "c")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
}
