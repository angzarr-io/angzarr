//! Tests for event store value objects.
//!
//! These are pure data structures with simple methods - no async, no I/O.

use super::{AddOutcome, CascadeParticipant, EventStore, SourceInfo};
use crate::proto::{EventBook, EventPage};
use crate::storage::{Result, StorageError};
use async_trait::async_trait;
use uuid::Uuid;

// ============================================================================
// SourceInfo Tests
// ============================================================================

/// SourceInfo::new creates with all fields.
#[test]
fn source_info_new_sets_all_fields() {
    let root = Uuid::new_v4();
    let info = SourceInfo::new("angzarr", "orders", root, 42);

    assert_eq!(info.edition, "angzarr");
    assert_eq!(info.domain, "orders");
    assert_eq!(info.root, root);
    assert_eq!(info.seq, 42);
}

/// SourceInfo::new accepts Into<String> for edition and domain.
#[test]
fn source_info_new_accepts_into_string() {
    let root = Uuid::new_v4();
    let info = SourceInfo::new(String::from("v2"), String::from("inventory"), root, 1);

    assert_eq!(info.edition, "v2");
    assert_eq!(info.domain, "inventory");
}

/// SourceInfo::is_empty returns true when edition and domain are empty.
#[test]
fn source_info_is_empty_when_both_empty() {
    let info = SourceInfo::default();
    assert!(info.is_empty());
}

/// SourceInfo::is_empty returns false when edition is set.
#[test]
fn source_info_is_not_empty_when_edition_set() {
    let info = SourceInfo {
        edition: "angzarr".to_string(),
        ..Default::default()
    };
    assert!(!info.is_empty());
}

/// SourceInfo::is_empty returns false when domain is set.
#[test]
fn source_info_is_not_empty_when_domain_set() {
    let info = SourceInfo {
        domain: "orders".to_string(),
        ..Default::default()
    };
    assert!(!info.is_empty());
}

/// SourceInfo::is_empty returns false when both are set.
#[test]
fn source_info_is_not_empty_when_both_set() {
    let info = SourceInfo::new("angzarr", "orders", Uuid::new_v4(), 1);
    assert!(!info.is_empty());
}

// ============================================================================
// AddOutcome Tests
// ============================================================================

/// AddOutcome::Added is_added returns true.
#[test]
fn add_outcome_added_is_added() {
    let outcome = AddOutcome::Added {
        first_sequence: 1,
        last_sequence: 5,
    };
    assert!(outcome.is_added());
    assert!(!outcome.is_duplicate());
}

/// AddOutcome::Duplicate is_duplicate returns true.
#[test]
fn add_outcome_duplicate_is_duplicate() {
    let outcome = AddOutcome::Duplicate {
        first_sequence: 1,
        last_sequence: 5,
    };
    assert!(outcome.is_duplicate());
    assert!(!outcome.is_added());
}

/// AddOutcome::first_sequence returns first for Added.
#[test]
fn add_outcome_first_sequence_added() {
    let outcome = AddOutcome::Added {
        first_sequence: 10,
        last_sequence: 15,
    };
    assert_eq!(outcome.first_sequence(), 10);
}

/// AddOutcome::first_sequence returns first for Duplicate.
#[test]
fn add_outcome_first_sequence_duplicate() {
    let outcome = AddOutcome::Duplicate {
        first_sequence: 20,
        last_sequence: 25,
    };
    assert_eq!(outcome.first_sequence(), 20);
}

/// AddOutcome::last_sequence returns last for Added.
#[test]
fn add_outcome_last_sequence_added() {
    let outcome = AddOutcome::Added {
        first_sequence: 10,
        last_sequence: 15,
    };
    assert_eq!(outcome.last_sequence(), 15);
}

/// AddOutcome::last_sequence returns last for Duplicate.
#[test]
fn add_outcome_last_sequence_duplicate() {
    let outcome = AddOutcome::Duplicate {
        first_sequence: 20,
        last_sequence: 25,
    };
    assert_eq!(outcome.last_sequence(), 25);
}

/// AddOutcome equality works correctly.
#[test]
fn add_outcome_equality() {
    let a = AddOutcome::Added {
        first_sequence: 1,
        last_sequence: 5,
    };
    let b = AddOutcome::Added {
        first_sequence: 1,
        last_sequence: 5,
    };
    let c = AddOutcome::Duplicate {
        first_sequence: 1,
        last_sequence: 5,
    };

    assert_eq!(a, b);
    assert_ne!(a, c);
}

// ============================================================================
// H-20: default `get_with_divergence` MUST NOT silently call `get()`
// ============================================================================
//
// Pre-fix bug: the default trait impl ignored `explicit_divergence` and
// delegated to `get(domain, edition, root)`. Any backend that did NOT
// override (Mock/Dynamo/Bigtable/NATS/ImmuDB) silently returned the wrong
// events for new-branch reads at a non-implicit divergence point.
// Post-fix contract: the default returns `Err(NotImplemented)` so missing
// per-backend implementations are loud, not silent.

/// Minimal EventStore stub that exercises ONLY the default trait impls.
///
/// Every required trait method `unimplemented!()`s — we only call
/// `get_with_divergence` on it, which is the one method with a default body.
struct DefaultImplStub;

#[async_trait]
impl EventStore for DefaultImplStub {
    async fn add(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _events: Vec<EventPage>,
        _correlation_id: &str,
        _external_id: Option<&str>,
        _source_info: Option<&SourceInfo>,
    ) -> Result<AddOutcome> {
        unimplemented!("not exercised in this test")
    }
    async fn get(&self, _domain: &str, _edition: &str, _root: Uuid) -> Result<Vec<EventPage>> {
        // Sentinel: if the default `get_with_divergence` falls through to
        // `get()` (pre-fix behavior), it would return an empty Vec here and
        // the test would see `Ok(vec![])`. The post-fix default returns
        // `Err(NotImplemented)` WITHOUT ever calling `get()`.
        Ok(vec![])
    }
    async fn get_from(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _from: u32,
    ) -> Result<Vec<EventPage>> {
        unimplemented!()
    }
    async fn get_from_to(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _from: u32,
        _to: u32,
    ) -> Result<Vec<EventPage>> {
        unimplemented!()
    }
    async fn list_roots(&self, _domain: &str, _edition: &str) -> Result<Vec<Uuid>> {
        unimplemented!()
    }
    async fn list_domains(&self) -> Result<Vec<String>> {
        unimplemented!()
    }
    async fn get_next_sequence(&self, _domain: &str, _edition: &str, _root: Uuid) -> Result<u32> {
        unimplemented!()
    }
    async fn get_until_timestamp(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _until: &str,
    ) -> Result<Vec<EventPage>> {
        unimplemented!()
    }
    async fn get_by_correlation(&self, _correlation_id: &str) -> Result<Vec<EventBook>> {
        unimplemented!()
    }
    async fn find_by_source(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _source_info: &SourceInfo,
    ) -> Result<Option<Vec<EventPage>>> {
        unimplemented!()
    }
    async fn find_by_external_id(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _external_id: &str,
    ) -> Result<Option<Vec<EventPage>>> {
        unimplemented!()
    }
    async fn delete_edition_events(&self, _domain: &str, _edition: &str) -> Result<u32> {
        unimplemented!()
    }
    async fn query_stale_cascades(&self, _threshold: &str) -> Result<Vec<String>> {
        unimplemented!()
    }
    async fn query_cascade_participants(
        &self,
        _cascade_id: &str,
    ) -> Result<Vec<CascadeParticipant>> {
        unimplemented!()
    }
}

/// H-20: a backend that does NOT override `get_with_divergence` must
/// return `NotImplemented` for any caller that supplies an explicit
/// divergence point — NOT a silent empty result via `get()`.
#[tokio::test]
async fn default_get_with_divergence_returns_not_implemented_for_explicit_branch() {
    let store = DefaultImplStub;
    let root = Uuid::new_v4();

    let result = store
        .get_with_divergence("orders", "new-explicit-branch", root, Some(3))
        .await;

    match result {
        Err(StorageError::NotImplemented(_)) => {}
        Err(other) => panic!("expected NotImplemented, got different error: {:?}", other),
        Ok(events) => panic!(
            "expected NotImplemented, got Ok({} events) — default impl is silently \
             falling back to get() and producing wrong events for a new branch",
            events.len()
        ),
    }
}

/// H-20: same contract for `explicit_divergence = None` — the trait
/// default should not silently substitute `get()` even when the caller
/// asks for implicit-divergence semantics. Backends that genuinely
/// support divergence must implement it explicitly.
#[tokio::test]
async fn default_get_with_divergence_returns_not_implemented_for_implicit_branch() {
    let store = DefaultImplStub;
    let root = Uuid::new_v4();

    let result = store.get_with_divergence("orders", "v2", root, None).await;

    assert!(
        matches!(result, Err(StorageError::NotImplemented(_))),
        "default impl must not silently delegate to get(); got {:?}",
        result
    );
}
