//! Tests for input validation functions.
//!
//! All data crossing trust boundaries (gRPC inputs, config values) must
//! be validated. These functions enforce consistent rules across the system.
//!
//! Why this matters: Invalid input causes hard-to-debug failures deep in the
//! system. Validating at the boundary catches issues early with clear errors.
//!
//! Validation categories:
//! - Domain names: lowercase, alphanumeric + underscore/hyphen
//! - Correlation IDs: alphanumeric + underscore/hyphen (allows uppercase)
//! - Component names: lowercase, alphanumeric + underscore/hyphen
//! - Edition names: lowercase, alphanumeric + underscore/hyphen
//! - Resource limits: pages per book, payload size

use super::*;

// ============================================================================
// Domain Validation Tests
// ============================================================================

mod domain_validation {
    //! Domain names identify bounded contexts (e.g., "order", "inventory").
    //! Must start with lowercase letter (or underscore for internal domains).
    //!
    //! Why these rules:
    //! - Lowercase: K8s service naming compatibility
    //! - Underscore prefix: Reserved for internal domains like "_angzarr"
    //! - 64 char limit: DNS label compatibility

    use super::*;

    /// Valid domain names are accepted.
    #[test]
    fn test_valid_domains() {
        assert!(validate_domain("order").is_ok());
        assert!(validate_domain("inventory").is_ok());
        assert!(validate_domain("order-fulfillment").is_ok());
        assert!(validate_domain("order_fulfillment").is_ok());
        assert!(validate_domain("order123").is_ok());
        assert!(validate_domain("a").is_ok());
        assert!(validate_domain("_angzarr").is_ok()); // internal domain
    }

    /// Empty domain is rejected.
    ///
    /// Domain identifies the aggregate; empty means unknown target.
    #[test]
    fn test_empty_domain() {
        let result = validate_domain("");
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains(errmsg::DOMAIN_EMPTY));
    }

    /// Domain exceeding 64 chars is rejected.
    ///
    /// DNS label limit is 63 chars; we use 64 for slight flexibility.
    #[test]
    fn test_domain_too_long() {
        let long_domain = "a".repeat(65);
        let result = validate_domain(&long_domain);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains(errmsg::DOMAIN_TOO_LONG));
    }

    /// Domain at exactly 64 chars is accepted.
    #[test]
    fn test_domain_max_length() {
        let max_domain = "a".repeat(64);
        assert!(validate_domain(&max_domain).is_ok());
    }

    /// Domain not starting with lowercase letter is rejected.
    ///
    /// Exception: underscore prefix for internal domains.
    #[test]
    fn test_domain_invalid_start() {
        assert!(validate_domain("1order").is_err());
        assert!(validate_domain("-order").is_err());
        assert!(validate_domain("Order").is_err());
    }

    /// Domain with invalid characters is rejected.
    ///
    /// Allowed: a-z, 0-9, underscore, hyphen.
    #[test]
    fn test_domain_invalid_chars() {
        assert!(validate_domain("order.fulfillment").is_err());
        assert!(validate_domain("order/fulfillment").is_err());
        assert!(validate_domain("order fulfillment").is_err());
        assert!(validate_domain("Order").is_err()); // uppercase
    }
}

// ============================================================================
// Correlation ID Validation Tests
// ============================================================================

mod correlation_id_validation {
    //! Correlation IDs link events across domains in a workflow.
    //! More permissive than domains: allows uppercase (for UUIDs, etc.).
    //!
    //! Why allow uppercase: UUIDs are commonly used and include uppercase hex.
    //! Why 128 char limit: UUIDs are 36 chars; allow for prefixes/suffixes.

    use super::*;

    /// Valid correlation IDs are accepted (including empty).
    ///
    /// Empty is allowed because correlation IDs are optional.
    /// Single-domain operations don't need correlation.
    #[test]
    fn test_valid_correlation_ids() {
        assert!(validate_correlation_id("").is_ok()); // empty is allowed
        assert!(validate_correlation_id("abc123").is_ok());
        assert!(validate_correlation_id("ABC123").is_ok());
        assert!(validate_correlation_id("order-123-abc").is_ok());
        assert!(validate_correlation_id("order_123_abc").is_ok());
        assert!(validate_correlation_id("OrderFulfillment123").is_ok());
    }

    /// Correlation ID exceeding 128 chars is rejected.
    #[test]
    fn test_correlation_id_too_long() {
        let long_id = "a".repeat(129);
        let result = validate_correlation_id(&long_id);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains(errmsg::CORRELATION_ID_TOO_LONG));
    }

    /// Correlation ID at exactly 128 chars is accepted.
    #[test]
    fn test_correlation_id_max_length() {
        let max_id = "a".repeat(128);
        assert!(validate_correlation_id(&max_id).is_ok());
    }

    /// Correlation ID with invalid characters is rejected.
    ///
    /// Allowed: a-zA-Z, 0-9, underscore, hyphen.
    #[test]
    fn test_correlation_id_invalid_chars() {
        assert!(validate_correlation_id("order.123").is_err());
        assert!(validate_correlation_id("order/123").is_err());
        assert!(validate_correlation_id("order 123").is_err());
        assert!(validate_correlation_id("order@123").is_err());
    }
}

// ============================================================================
// Component Name Validation Tests
// ============================================================================

mod component_name_validation {
    //! Component names identify sagas, projectors, and process managers.
    //! Same rules as domains except no underscore prefix allowed.
    //!
    //! Why no underscore prefix: Underscore is reserved for internal domains.
    //! Components are user-defined and shouldn't appear "internal".

    use super::*;

    /// Valid component names are accepted.
    #[test]
    fn test_valid_component_names() {
        assert!(validate_component_name("inventory").is_ok());
        assert!(validate_component_name("saga-order-fulfillment").is_ok());
        assert!(validate_component_name("projector-inventory-stock").is_ok());
        assert!(validate_component_name("agg123").is_ok());
    }

    /// Empty component name is rejected.
    #[test]
    fn test_empty_component_name() {
        let result = validate_component_name("");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains(errmsg::COMPONENT_NAME_EMPTY));
    }

    /// Component name exceeding 128 chars is rejected.
    #[test]
    fn test_component_name_too_long() {
        let long_name = "a".repeat(129);
        let result = validate_component_name(&long_name);
        assert!(result.is_err());
    }

    /// Component name at exactly 128 chars is accepted.
    #[test]
    fn test_component_name_max_length() {
        let max_name = "a".repeat(128);
        assert!(validate_component_name(&max_name).is_ok());
    }

    /// Component name not starting with lowercase letter is rejected.
    ///
    /// Unlike domains, underscore prefix is NOT allowed.
    #[test]
    fn test_component_name_invalid_start() {
        assert!(validate_component_name("1saga").is_err());
        assert!(validate_component_name("-saga").is_err());
        assert!(validate_component_name("Saga").is_err());
        assert!(validate_component_name("_saga").is_err()); // unlike domain, no underscore prefix
    }

    /// Component name with invalid characters is rejected.
    #[test]
    fn test_component_name_invalid_chars() {
        assert!(validate_component_name("saga.order").is_err());
        assert!(validate_component_name("saga/order").is_err());
        assert!(validate_component_name("Saga-Order").is_err()); // uppercase
    }
}

// ============================================================================
// Edition Validation Tests
// ============================================================================

mod edition_validation {
    //! Edition names identify diverged timelines (branched histories).
    //! Empty = main timeline ("angzarr").
    //!
    //! Why editions: Enables time-travel debugging and what-if analysis.
    //! Branched timelines can replay events with different handlers.

    use super::*;

    /// Valid edition names are accepted (including empty).
    ///
    /// Empty means main timeline; most operations use empty.
    #[test]
    fn test_valid_editions() {
        assert!(validate_edition("").is_ok()); // empty is allowed (defaults to "angzarr")
        assert!(validate_edition("angzarr").is_ok());
        assert!(validate_edition("v2").is_ok());
        assert!(validate_edition("edition-123").is_ok());
        assert!(validate_edition("edition_123").is_ok());
    }

    /// Edition exceeding 64 chars is rejected.
    #[test]
    fn test_edition_too_long() {
        let long_edition = "a".repeat(65);
        let result = validate_edition(&long_edition);
        assert!(result.is_err());
    }

    /// Edition at exactly 64 chars is accepted.
    #[test]
    fn test_edition_max_length() {
        let max_edition = "a".repeat(64);
        assert!(validate_edition(&max_edition).is_ok());
    }

    /// Edition not starting with lowercase letter is rejected.
    #[test]
    fn test_edition_invalid_start() {
        assert!(validate_edition("1edition").is_err());
        assert!(validate_edition("-edition").is_err());
        assert!(validate_edition("Edition").is_err());
    }

    /// Edition with invalid characters is rejected.
    #[test]
    fn test_edition_invalid_chars() {
        assert!(validate_edition("edition.v2").is_err());
        assert!(validate_edition("Edition").is_err()); // uppercase
    }
}

// ============================================================================
// Resource Limits Validation Tests
// ============================================================================

mod resource_limits_validation {
    //! Resource limits prevent unbounded memory usage and ensure messages
    //! fit within bus constraints. Tests verify validation against limits.
    //!
    //! Why limits matter:
    //! - Bus backends have hard limits (SQS: 256KB, Pub/Sub: 10MB)
    //! - Unbounded pages/payloads cause OOM
    //! - Explicit limits catch oversized messages early

    use super::*;
    use crate::proto::{command_page, page_header, CommandPage, Cover, MergeStrategy, PageHeader};
    use prost_types::Any;

    fn make_command_book(pages: Vec<CommandPage>) -> CommandBook {
        CommandBook {
            cover: Some(Cover {
                domain: "test".to_string(),
                root: None,
                correlation_id: String::new(),
                edition: None,
            }),
            pages,
        }
    }

    fn make_page_with_payload(size: usize) -> CommandPage {
        CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(0)),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "test/Command".to_string(),
                value: vec![0u8; size],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }
    }

    /// Empty command book is valid.
    #[test]
    fn test_empty_command_book() {
        let book = make_command_book(vec![]);
        let limits = ResourceLimits::default();
        assert!(validate_command_book(&book, &limits).is_ok());
    }

    /// Command book within limits is valid.
    #[test]
    fn test_command_book_within_limits() {
        let pages: Vec<_> = (0..10).map(|_| make_page_with_payload(1024)).collect();
        let book = make_command_book(pages);
        let limits = ResourceLimits::default();
        assert!(validate_command_book(&book, &limits).is_ok());
    }

    /// Command book at exactly max pages (100) is valid.
    #[test]
    fn test_command_book_at_max_pages() {
        let pages: Vec<_> = (0..100).map(|_| make_page_with_payload(64)).collect();
        let book = make_command_book(pages);
        let limits = ResourceLimits::default();
        assert!(validate_command_book(&book, &limits).is_ok());
    }

    /// Command book exceeding max pages is rejected.
    ///
    /// Default limit: 100 pages. Prevents unbounded batch sizes.
    #[test]
    fn test_command_book_too_many_pages() {
        let pages: Vec<_> = (0..101).map(|_| make_page_with_payload(64)).collect();
        let book = make_command_book(pages);
        let limits = ResourceLimits::default();
        let result = validate_command_book(&book, &limits);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains(errmsg::TOO_MANY_PAGES));
    }

    /// Command book at exactly max payload (256 KB) is valid.
    #[test]
    fn test_command_book_at_max_payload() {
        let pages = vec![make_page_with_payload(256 * 1024)]; // exactly 256 KB
        let book = make_command_book(pages);
        let limits = ResourceLimits::default();
        assert!(validate_command_book(&book, &limits).is_ok());
    }

    /// Command book with payload exceeding 256 KB is rejected.
    ///
    /// Default limit matches SQS/SNS (256KB). Prevents bus rejects.
    #[test]
    fn test_command_book_payload_too_large() {
        let pages = vec![make_page_with_payload(256 * 1024 + 1)]; // 256 KB + 1
        let book = make_command_book(pages);
        let limits = ResourceLimits::default();
        let result = validate_command_book(&book, &limits);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains(errmsg::PAYLOAD_TOO_LARGE));
    }

    /// Custom limits (IPC: 10 MB) allow larger payloads.
    ///
    /// IPC has no bus constraints; larger payloads acceptable.
    #[test]
    fn test_command_book_with_custom_limits() {
        // Test with IPC limits (10 MB)
        let pages = vec![make_page_with_payload(5 * 1024 * 1024)]; // 5 MB
        let book = make_command_book(pages);
        let limits = ResourceLimits::for_ipc();
        assert!(validate_command_book(&book, &limits).is_ok());
    }
}

// ============================================================================
// Error Message Constant Tests
// ============================================================================
//
// Error messages are exported as constants in the errmsg module.
// Tests verify constants are non-empty and properly defined.

/// All validation error message constants are non-empty.
#[test]
fn test_errmsg_constants_non_empty() {
    assert!(!errmsg::DOMAIN_EMPTY.is_empty());
    assert!(!errmsg::DOMAIN_TOO_LONG.is_empty());
    assert!(!errmsg::DOMAIN_INVALID_START.is_empty());
    assert!(!errmsg::DOMAIN_INVALID_CHARS.is_empty());
    assert!(!errmsg::CORRELATION_ID_TOO_LONG.is_empty());
    assert!(!errmsg::CORRELATION_ID_INVALID_CHARS.is_empty());
    assert!(!errmsg::COMPONENT_NAME_EMPTY.is_empty());
    assert!(!errmsg::COMPONENT_NAME_TOO_LONG.is_empty());
    assert!(!errmsg::COMPONENT_NAME_INVALID_START.is_empty());
    assert!(!errmsg::COMPONENT_NAME_INVALID_CHARS.is_empty());
    assert!(!errmsg::EDITION_TOO_LONG.is_empty());
    assert!(!errmsg::EDITION_INVALID_START.is_empty());
    assert!(!errmsg::EDITION_INVALID_CHARS.is_empty());
    assert!(!errmsg::TOO_MANY_PAGES.is_empty());
    assert!(!errmsg::PAYLOAD_TOO_LARGE.is_empty());
}
