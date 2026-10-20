use super::*;
use crate::descriptor::Target;
use crate::proto::{Cover, EventPage};
use prost_types::Any;

fn make_event_book(domain: &str, event_types: &[&str]) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(uuid::Uuid::new_v4().into()),
            edition: None,
        }),
        pages: event_types
            .iter()
            .enumerate()
            .map(|(i, et)| EventPage {
                payload: Some(Any {
                    type_url: format!("type.googleapis.com/example.{}", et),
                    value: vec![],
                }),
                sequence: i as u64,
                meta: None,
            })
            .collect(),
        snapshot: None,
        ..Default::default()
    }
}

#[test]
fn test_messaging_config_default() {
    let config = MessagingConfig::default();
    assert_eq!(config.messaging_type, "channel");
    assert_eq!(config.amqp.url, "amqp://localhost:5672");
}

#[test]
fn test_target_matches_domain_only() {
    let book = make_event_book("order", &["OrderCreated"]);
    let target = Target {
        domain: "order".to_string(),
        types: vec![],
    };
    assert!(target_matches(&book, &target));
}

#[test]
fn test_target_matches_wrong_domain() {
    let book = make_event_book("order", &["OrderCreated"]);
    let target = Target {
        domain: "inventory".to_string(),
        types: vec![],
    };
    assert!(!target_matches(&book, &target));
}

#[test]
fn test_target_matches_specific_event_type() {
    let book = make_event_book("order", &["OrderCreated", "OrderShipped"]);
    let target = Target {
        domain: "order".to_string(),
        types: vec!["OrderCreated".to_string()],
    };
    assert!(target_matches(&book, &target));
}

#[test]
fn test_target_matches_event_type_not_present() {
    let book = make_event_book("order", &["OrderCreated"]);
    let target = Target {
        domain: "order".to_string(),
        types: vec!["OrderShipped".to_string()],
    };
    assert!(!target_matches(&book, &target));
}

/// R2-01: short subscription names must not widen to events that
/// merely end with the substring (e.g., `Created` must not match
/// `OrderCreated`). Pins `target_matches` to the token-boundary rule
/// owned by `Target::matches_type` so this delegation cannot regress
/// to a raw `ends_with`.
#[test]
fn target_matches_short_name_does_not_widen() {
    let book = make_event_book("order", &["OrderCreated"]);
    let target = Target {
        domain: "order".to_string(),
        types: vec!["Created".to_string()],
    };
    assert!(
        !target_matches(&book, &target),
        "subscription to short name \"Created\" must NOT match an OrderCreated event"
    );
}

#[test]
fn test_any_target_matches_first() {
    let book = make_event_book("order", &["OrderCreated"]);
    let targets = vec![
        Target {
            domain: "order".to_string(),
            types: vec!["OrderCreated".to_string()],
        },
        Target {
            domain: "inventory".to_string(),
            types: vec![],
        },
    ];
    assert!(any_target_matches(&book, &targets));
}

#[test]
fn test_any_target_matches_second() {
    let book = make_event_book("inventory", &["StockReserved"]);
    let targets = vec![
        Target {
            domain: "order".to_string(),
            types: vec![],
        },
        Target {
            domain: "inventory".to_string(),
            types: vec![],
        },
    ];
    assert!(any_target_matches(&book, &targets));
}

#[test]
fn test_any_target_matches_none() {
    let book = make_event_book("customer", &["CustomerCreated"]);
    let targets = vec![
        Target {
            domain: "order".to_string(),
            types: vec![],
        },
        Target {
            domain: "inventory".to_string(),
            types: vec![],
        },
    ];
    assert!(!any_target_matches(&book, &targets));
}

// ============================================================================
// domain_matches_any tests
// ============================================================================

#[test]
fn test_domain_matches_any_empty_patterns_matches_all() {
    assert!(domain_matches_any("anything", &[]));
    assert!(domain_matches_any("order", &[]));
    assert!(domain_matches_any("deep.nested.domain", &[]));
}

#[test]
fn test_domain_matches_any_wildcard_matches_all() {
    let patterns = vec!["*".to_string()];
    assert!(domain_matches_any("order", &patterns));
    assert!(domain_matches_any("inventory", &patterns));
    assert!(domain_matches_any("any.domain", &patterns));
}

#[test]
fn test_domain_matches_any_exact_match() {
    let patterns = vec!["order".to_string()];
    assert!(domain_matches_any("order", &patterns));
    assert!(!domain_matches_any("orders", &patterns));
    assert!(!domain_matches_any("order.sub", &patterns));
}

#[test]
fn test_domain_matches_any_hierarchical_prefix() {
    let patterns = vec!["game.*".to_string()];

    // Should match subdomains (domain.subdomain format)
    assert!(domain_matches_any("game.player", &patterns));
    assert!(domain_matches_any("game.table", &patterns));
    assert!(domain_matches_any("game.x", &patterns));

    // Should not match exact domain (needs subdomain)
    assert!(!domain_matches_any("game", &patterns));

    // Should not match unrelated domains
    assert!(!domain_matches_any("other", &patterns));

    // Should not match domains that just start with prefix (no dot separator)
    assert!(!domain_matches_any("gameplay", &patterns));
    assert!(!domain_matches_any("gamer", &patterns));
}

#[test]
fn test_domain_matches_any_multiple_patterns() {
    let patterns = vec!["order".to_string(), "inventory.*".to_string()];

    assert!(domain_matches_any("order", &patterns));
    assert!(domain_matches_any("inventory.stock", &patterns));
    assert!(!domain_matches_any("shipping", &patterns));
}

#[test]
fn test_domain_matches_any_no_match() {
    let patterns = vec!["order".to_string(), "inventory".to_string()];
    assert!(!domain_matches_any("shipping", &patterns));
    assert!(!domain_matches_any("customer", &patterns));
}
