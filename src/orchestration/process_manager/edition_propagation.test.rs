//! Tests for the PM coordinator's edition propagation contract per
//! `coordinator-contract/edition_propagation.feature` (audit #86).
//!
//! Tests at the helper level because the helper is a pure
//! transformation that the gRPC pipeline (`grpc/mod.rs`) delegates to.
//!
//! Maps to:
//! - C-0143 PM propagates trigger edition to outgoing commands
//! - C-0144 PM propagates trigger edition to every emitted process_events book
//! - C-0145 always-override: handler-set "beta" → coordinator stamps trigger's "alpha"
//! - (analogue of C-0141) main-timeline trigger → outgoing has no edition
//! - (analogue of C-0142) trigger edition with divergences → preserved verbatim
//! - facts coverage: same propagation runs on PM-emitted facts

use super::propagate_trigger_edition;
use crate::proto::{CommandBook, Cover, DomainDivergence, Edition, EventBook};

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

fn book_with_cover(cover: Cover) -> EventBook {
    EventBook {
        cover: Some(cover),
        ..Default::default()
    }
}

fn cmd_book_with_cover(cover: Cover) -> CommandBook {
    CommandBook {
        cover: Some(cover),
        ..Default::default()
    }
}

// ---------------------------------------------------------------
// C-0143 — PM propagates trigger edition to outgoing commands
// ---------------------------------------------------------------

#[test]
fn c0143_trigger_edition_stamps_outgoing_commands() {
    let trigger_cover = cover_with_edition("order", Some("speculative"));
    let mut commands = vec![cmd_book_with_cover(cover_with_edition("shipping", None))];
    let mut process_events: Vec<EventBook> = vec![];
    let mut facts: Vec<EventBook> = vec![];

    propagate_trigger_edition(
        Some(&trigger_cover),
        &mut commands,
        &mut process_events,
        &mut facts,
    );

    let edition = commands[0]
        .cover
        .as_ref()
        .unwrap()
        .edition
        .as_ref()
        .expect("stamped");
    assert_eq!(edition.name, "speculative");
}

// ---------------------------------------------------------------
// C-0144 — PM propagates trigger edition to EVERY process_events book
// ---------------------------------------------------------------

#[test]
fn c0144_trigger_edition_stamps_every_process_events_book() {
    // PM emits two distinct process_events books (audit #92: list,
    // not single book). Coordinator stamps each independently.
    let trigger_cover = cover_with_edition("order", Some("speculative"));
    let mut commands: Vec<CommandBook> = vec![];
    let mut process_events = vec![
        book_with_cover(cover_with_edition("fulfillment", None)),
        book_with_cover(cover_with_edition("audit-trail", None)),
    ];
    let mut facts: Vec<EventBook> = vec![];

    propagate_trigger_edition(
        Some(&trigger_cover),
        &mut commands,
        &mut process_events,
        &mut facts,
    );

    for (i, book) in process_events.iter().enumerate() {
        let edition = book
            .cover
            .as_ref()
            .unwrap()
            .edition
            .as_ref()
            .unwrap_or_else(|| panic!("book {} edition not stamped", i));
        assert_eq!(edition.name, "speculative", "book {} edition", i);
    }
}

#[test]
fn c0144_supplement_trigger_edition_stamps_facts() {
    // Same contract for `facts` — PM emits cross-aggregate fact
    // injections, coordinator stamps each.
    let trigger_cover = cover_with_edition("order", Some("speculative"));
    let mut commands: Vec<CommandBook> = vec![];
    let mut process_events: Vec<EventBook> = vec![];
    let mut facts = vec![
        book_with_cover(cover_with_edition("inventory", None)),
        book_with_cover(cover_with_edition("ledger", None)),
    ];

    propagate_trigger_edition(
        Some(&trigger_cover),
        &mut commands,
        &mut process_events,
        &mut facts,
    );

    for book in &facts {
        let edition = book.cover.as_ref().unwrap().edition.as_ref().unwrap();
        assert_eq!(edition.name, "speculative");
    }
}

// ---------------------------------------------------------------
// C-0145 — always-override: handler-set "beta" → trigger's "alpha"
// ---------------------------------------------------------------

#[test]
fn c0145_handler_set_edition_overwritten_with_trigger() {
    let trigger_cover = cover_with_edition("order", Some("alpha"));
    let mut commands = vec![cmd_book_with_cover(cover_with_edition(
        "shipping",
        Some("beta"),
    ))];
    let mut process_events = vec![book_with_cover(cover_with_edition(
        "fulfillment",
        Some("beta"),
    ))];
    let mut facts = vec![book_with_cover(cover_with_edition("ledger", Some("beta")))];

    propagate_trigger_edition(
        Some(&trigger_cover),
        &mut commands,
        &mut process_events,
        &mut facts,
    );

    assert_eq!(
        commands[0]
            .cover
            .as_ref()
            .unwrap()
            .edition
            .as_ref()
            .unwrap()
            .name,
        "alpha",
        "command edition must be overwritten with trigger's"
    );
    assert_eq!(
        process_events[0]
            .cover
            .as_ref()
            .unwrap()
            .edition
            .as_ref()
            .unwrap()
            .name,
        "alpha",
        "process_events edition must be overwritten",
    );
    assert_eq!(
        facts[0]
            .cover
            .as_ref()
            .unwrap()
            .edition
            .as_ref()
            .unwrap()
            .name,
        "alpha",
        "facts edition must be overwritten",
    );
}

// ---------------------------------------------------------------
// Main-timeline trigger → outgoing has no edition
// ---------------------------------------------------------------

#[test]
fn no_trigger_edition_clears_handler_set_edition() {
    // Trigger has no edition; coordinator must clear any handler-set
    // edition on outgoing books — outgoing matches main-timeline.
    let trigger_cover = cover_with_edition("order", None);
    let mut commands = vec![cmd_book_with_cover(cover_with_edition(
        "shipping",
        Some("leftover"),
    ))];
    let mut process_events = vec![book_with_cover(cover_with_edition(
        "fulfillment",
        Some("stale"),
    ))];
    let mut facts = vec![book_with_cover(cover_with_edition("ledger", Some("stale")))];

    propagate_trigger_edition(
        Some(&trigger_cover),
        &mut commands,
        &mut process_events,
        &mut facts,
    );

    assert!(commands[0].cover.as_ref().unwrap().edition.is_none());
    assert!(process_events[0].cover.as_ref().unwrap().edition.is_none());
    assert!(facts[0].cover.as_ref().unwrap().edition.is_none());
}

// ---------------------------------------------------------------
// Trigger edition with divergences propagates verbatim
// ---------------------------------------------------------------

#[test]
fn trigger_edition_divergences_propagate_verbatim() {
    let trigger_cover = Cover {
        domain: "order".to_string(),
        edition: Some(Edition {
            name: "speculative".to_string(),
            divergences: vec![DomainDivergence {
                domain: "order".to_string(),
                sequence: 7,
            }],
        }),
        ..Default::default()
    };
    let mut commands = vec![cmd_book_with_cover(cover_with_edition("shipping", None))];
    let mut process_events = vec![book_with_cover(cover_with_edition("fulfillment", None))];
    let mut facts: Vec<EventBook> = vec![];

    propagate_trigger_edition(
        Some(&trigger_cover),
        &mut commands,
        &mut process_events,
        &mut facts,
    );

    for cover in [commands[0].cover.as_ref(), process_events[0].cover.as_ref()] {
        let edition = cover.unwrap().edition.as_ref().expect("edition stamped");
        assert_eq!(edition.name, "speculative");
        assert_eq!(edition.divergences.len(), 1);
        assert_eq!(edition.divergences[0].domain, "order");
        assert_eq!(edition.divergences[0].sequence, 7);
    }
}

// ---------------------------------------------------------------
// No trigger cover → no propagation (defensive)
// ---------------------------------------------------------------

#[test]
fn no_trigger_cover_skips_propagation() {
    // Defensive: if the trigger has no cover at all (malformed
    // input), the helper must not panic and must leave outgoing
    // covers untouched.
    let mut commands = vec![cmd_book_with_cover(cover_with_edition(
        "shipping",
        Some("untouched"),
    ))];
    let mut process_events: Vec<EventBook> = vec![];
    let mut facts: Vec<EventBook> = vec![];

    propagate_trigger_edition(None, &mut commands, &mut process_events, &mut facts);

    assert_eq!(
        commands[0]
            .cover
            .as_ref()
            .unwrap()
            .edition
            .as_ref()
            .unwrap()
            .name,
        "untouched",
        "no trigger cover → outgoing edition must remain as handler set",
    );
}
