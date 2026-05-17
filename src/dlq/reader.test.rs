//! Tests for the `DeadLetterReader` trait surface.
//!
//! WHY: the trait, filter, and pagination types are the contract every
//! backend impl + the status-binary handler layer keys off. A subtle
//! drift here (e.g., default page size silently becoming 1, AIP-158
//! cap regressing, NoopDeadLetterReader accidentally claiming
//! `is_configured = true`) would cripple operator-facing queries
//! across the whole console.

use super::*;

#[test]
fn default_page_size_when_zero() {
    // AIP-158: page_size = 0 means "server picks". Our pick is the
    // documented DEFAULT_PAGE_SIZE.
    let f = ListFilter::default();
    assert_eq!(f.page_size, 0);
    assert_eq!(f.effective_page_size(), DEFAULT_PAGE_SIZE);
}

#[test]
fn effective_page_size_clamps_to_max() {
    // Hostile / buggy clients asking for huge pages get bounded
    // memory, not an error (AIP-158 server-defined max behavior).
    let f = ListFilter {
        page_size: 1_000_000,
        ..Default::default()
    };
    assert_eq!(f.effective_page_size(), MAX_PAGE_SIZE);
}

#[test]
fn effective_page_size_passes_through_in_range() {
    // The common case — a sensible page_size from the UI's
    // pagination control rides through unchanged.
    let f = ListFilter {
        page_size: 25,
        ..Default::default()
    };
    assert_eq!(f.effective_page_size(), 25);
}

#[test]
fn effective_page_size_max_passes_through() {
    // Boundary: requesting exactly MAX is allowed; only ABOVE the cap
    // clamps. Catches the easy off-by-one mutation `>= MAX`.
    let f = ListFilter {
        page_size: MAX_PAGE_SIZE,
        ..Default::default()
    };
    assert_eq!(f.effective_page_size(), MAX_PAGE_SIZE);
}

#[test]
fn default_filter_has_no_constraints() {
    // Plan tolerance contract: a list call with no filter must
    // return everything (page-by-page). Catches the failure mode
    // where Default::default() accidentally sets an exclusive
    // upper bound.
    let f = ListFilter::default();
    assert!(f.domain.is_none());
    assert!(f.correlation_id.is_none());
    assert!(f.rejection_type.is_none());
    assert!(f.source_component.is_none());
    assert!(f.occurred_after.is_none());
    assert!(f.occurred_before.is_none());
    assert!(f.page_token.is_none());
}

#[tokio::test]
async fn noop_reader_is_not_configured() {
    // The status handler keys "DLQ unavailable" off this flag.
    // If a future copy-paste flipped the default to `true`, every
    // gRPC List call would silently swallow the NotConfigured error
    // and report empty results — looking like "no dead letters" when
    // it's really "no DB to query".
    let r = NoopDeadLetterReader;
    assert!(!r.is_configured());
}

#[tokio::test]
async fn noop_reader_list_returns_not_configured() {
    // Caller behavior contract: NotConfigured is the discriminator
    // the status handler maps to a degraded Health<T> response.
    let r = NoopDeadLetterReader;
    let err = r.list(ListFilter::default()).await.unwrap_err();
    assert!(matches!(err, DlqError::NotConfigured));
}

#[tokio::test]
async fn noop_reader_get_returns_not_configured() {
    let r = NoopDeadLetterReader;
    let err = r.get(1).await.unwrap_err();
    assert!(matches!(err, DlqError::NotConfigured));
}

#[tokio::test]
async fn noop_reader_delete_returns_not_configured() {
    let r = NoopDeadLetterReader;
    let err = r.delete(1).await.unwrap_err();
    assert!(matches!(err, DlqError::NotConfigured));
}

/// Reader impl that uses the trait's default `is_configured`
/// (i.e., does NOT override). Used solely to pin the default's
/// return value so a future copy-paste flipping `true` → `false`
/// on the default body fails this test — without it, no live test
/// reaches the default implementation.
struct DefaultsReader;

#[async_trait::async_trait]
impl DeadLetterReader for DefaultsReader {
    async fn list(&self, _filter: ListFilter) -> Result<DeadLetterPage> {
        Ok(DeadLetterPage {
            entries: vec![],
            next_page_token: None,
        })
    }
    async fn get(&self, _id: i64) -> Result<Option<StoredDeadLetter>> {
        Ok(None)
    }
    async fn delete(&self, _id: i64) -> Result<bool> {
        Ok(false)
    }
}

#[test]
fn trait_default_is_configured_returns_true() {
    // A backend that doesn't explicitly override is_configured
    // (e.g., a real DB-backed reader that's always live) must
    // report `true`. Catches the mutation that flips the default
    // body to `false` — which would silently make every real
    // reader look unconfigured to the status console.
    let r = DefaultsReader;
    assert!(r.is_configured());
}

#[test]
fn noop_source_id_is_noop() {
    // The Health<T> envelope's `source` field is how the UI
    // distinguishes "this answer came from a real Postgres" from
    // "we have no backend, please ignore". Pinning the string
    // catches a silent rename.
    let r = NoopDeadLetterReader;
    assert_eq!(r.source_id(), "noop");
}

#[test]
fn trait_default_source_id_is_unknown() {
    // Real backends must override; default is a placeholder.
    let r = DefaultsReader;
    assert_eq!(r.source_id(), "unknown");
}
