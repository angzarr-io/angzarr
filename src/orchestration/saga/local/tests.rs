//! Tests for `LocalSagaContext::handle` — specifically the
//! coordinator's edition propagation contract per
//! `coordinator-contract/edition_propagation.feature` (audit #86).
//!
//! Why test here, not at the SagaRetryContext mock layer?
//! `super::tests::*` use mock `SagaRetryContext` impls (AlwaysSucceeds /
//! RetryingSagaContext) to exercise retry logic. The edition-
//! propagation contract is implemented inside the *real*
//! `LocalSagaContext::handle` body — not in the trait. Mocking
//! SagaRetryContext bypasses the propagation code entirely. These
//! tests drive the concrete LocalSagaContext with a stub SagaHandler,
//! the only way to verify the propagation actually fires.
//!
//! Maps to:
//! - C-0138 saga propagates source edition to outgoing commands
//! - C-0139 saga propagates source edition to outgoing events
//! - C-0140 always-override: handler-set "beta" → coordinator stamps source's "alpha"
//! - C-0141 main-timeline (no edition) → outgoing has no edition
//! - C-0142 source edition with divergences → preserved verbatim

use super::*;
use crate::proto::{Cover, DomainDivergence, Edition, EventBook, EventPage};

/// Stub `SagaHandler` that returns a pre-configured `SagaResponse`.
///
/// Captures the `source` it received so tests can verify the saga
/// observed the right input — though the propagation contract is
/// independent of what the handler does, the contract sits in the
/// coordinator wrapper.
struct StubSagaHandler {
    response: std::sync::Mutex<Option<SagaResponse>>,
}

impl StubSagaHandler {
    fn new(response: SagaResponse) -> Arc<Self> {
        Arc::new(Self {
            response: std::sync::Mutex::new(Some(response)),
        })
    }
}

#[async_trait]
impl SagaHandler for StubSagaHandler {
    async fn handle(
        &self,
        _source: &EventBook,
        _destination_sequences: &HashMap<String, u32>,
    ) -> Result<SagaResponse, tonic::Status> {
        let response = self
            .response
            .lock()
            .unwrap()
            .take()
            .expect("StubSagaHandler.handle called twice");
        Ok(response)
    }
}

fn cover_with_edition(domain: &str, edition_name: Option<&str>) -> Cover {
    Cover {
        domain: domain.to_string(),
        edition: edition_name.map(|name| Edition {
            name: name.to_string(),
            divergences: vec![],
        }),
        ..Default::default()
    }
}

fn source_event_book(source_cover: Cover) -> Arc<EventBook> {
    Arc::new(EventBook {
        cover: Some(source_cover),
        pages: vec![EventPage::default()],
        ..Default::default()
    })
}

// ---------------------------------------------------------------
// C-0138 — saga propagates source edition to outgoing commands
// ---------------------------------------------------------------

#[tokio::test]
async fn c0138_source_edition_stamps_outgoing_commands() {
    let handler = StubSagaHandler::new(SagaResponse {
        commands: vec![CommandBook {
            cover: Some(cover_with_edition("inventory", None)),
            ..Default::default()
        }],
        events: vec![],
    });
    let source = source_event_book(cover_with_edition("order", Some("speculative")));
    let ctx = LocalSagaContext::new(handler, source);

    let response = ctx
        .handle(HashMap::new(), crate::proto::SyncMode::Async)
        .await
        .expect("handle ok");

    let cmd_cover = response.commands[0].cover.as_ref().expect("cmd cover");
    let edition = cmd_cover.edition.as_ref().expect("edition stamped");
    assert_eq!(edition.name, "speculative");
}

// ---------------------------------------------------------------
// C-0139 — saga propagates source edition to outgoing events (facts)
// ---------------------------------------------------------------

#[tokio::test]
async fn c0139_source_edition_stamps_outgoing_events() {
    let handler = StubSagaHandler::new(SagaResponse {
        commands: vec![],
        events: vec![EventBook {
            cover: Some(cover_with_edition("audit", None)),
            ..Default::default()
        }],
    });
    let source = source_event_book(cover_with_edition("order", Some("speculative")));
    let ctx = LocalSagaContext::new(handler, source);

    let response = ctx
        .handle(HashMap::new(), crate::proto::SyncMode::Async)
        .await
        .expect("handle ok");

    let event_cover = response.events[0].cover.as_ref().expect("event cover");
    let edition = event_cover.edition.as_ref().expect("edition stamped");
    assert_eq!(edition.name, "speculative");
}

// ---------------------------------------------------------------
// C-0140 — always-override: handler-set edition gets overwritten
// ---------------------------------------------------------------

#[tokio::test]
async fn c0140_handler_set_edition_overwritten_with_source() {
    // Handler sets "beta" on outgoing; coordinator must stamp source's
    // "alpha" over it. The framework guarantees timeline consistency
    // — handlers cannot escape into a different edition.
    let handler = StubSagaHandler::new(SagaResponse {
        commands: vec![CommandBook {
            cover: Some(cover_with_edition("inventory", Some("beta"))),
            ..Default::default()
        }],
        events: vec![EventBook {
            cover: Some(cover_with_edition("audit", Some("beta"))),
            ..Default::default()
        }],
    });
    let source = source_event_book(cover_with_edition("order", Some("alpha")));
    let ctx = LocalSagaContext::new(handler, source);

    let response = ctx
        .handle(HashMap::new(), crate::proto::SyncMode::Async)
        .await
        .expect("handle ok");

    let cmd_edition = response.commands[0]
        .cover
        .as_ref()
        .unwrap()
        .edition
        .as_ref()
        .unwrap();
    assert_eq!(cmd_edition.name, "alpha");
    let event_edition = response.events[0]
        .cover
        .as_ref()
        .unwrap()
        .edition
        .as_ref()
        .unwrap();
    assert_eq!(event_edition.name, "alpha");
}

// ---------------------------------------------------------------
// C-0141 — main-timeline (no edition) propagates as no edition
// ---------------------------------------------------------------

#[tokio::test]
async fn c0141_no_source_edition_clears_handler_set_edition() {
    // Source has no edition; the coordinator must clear any edition
    // the handler set, so the outgoing book matches main-timeline.
    let handler = StubSagaHandler::new(SagaResponse {
        commands: vec![CommandBook {
            cover: Some(cover_with_edition("inventory", Some("leftover"))),
            ..Default::default()
        }],
        events: vec![],
    });
    let source = source_event_book(cover_with_edition("order", None));
    let ctx = LocalSagaContext::new(handler, source);

    let response = ctx
        .handle(HashMap::new(), crate::proto::SyncMode::Async)
        .await
        .expect("handle ok");

    assert!(
        response.commands[0]
            .cover
            .as_ref()
            .unwrap()
            .edition
            .is_none(),
        "main-timeline source must clear handler-set edition"
    );
}

// ---------------------------------------------------------------
// C-0142 — source edition with divergences propagates verbatim
// ---------------------------------------------------------------

#[tokio::test]
async fn c0142_source_edition_divergences_propagate_verbatim() {
    // Source edition has a divergence at order=5; outgoing must carry
    // the same divergence. Full Edition struct copy semantics.
    let source_cover = Cover {
        domain: "order".to_string(),
        edition: Some(Edition {
            name: "speculative".to_string(),
            divergences: vec![DomainDivergence {
                domain: "order".to_string(),
                sequence: 5,
            }],
        }),
        ..Default::default()
    };
    let handler = StubSagaHandler::new(SagaResponse {
        commands: vec![CommandBook {
            cover: Some(cover_with_edition("inventory", None)),
            ..Default::default()
        }],
        events: vec![],
    });
    let source = source_event_book(source_cover);
    let ctx = LocalSagaContext::new(handler, source);

    let response = ctx
        .handle(HashMap::new(), crate::proto::SyncMode::Async)
        .await
        .expect("handle ok");

    let edition = response.commands[0]
        .cover
        .as_ref()
        .unwrap()
        .edition
        .as_ref()
        .expect("edition stamped");
    assert_eq!(edition.name, "speculative");
    assert_eq!(edition.divergences.len(), 1);
    assert_eq!(edition.divergences[0].domain, "order");
    assert_eq!(edition.divergences[0].sequence, 5);
}
