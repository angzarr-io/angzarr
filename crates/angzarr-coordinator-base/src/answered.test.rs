use angzarr_proto::v1::{Cover, DeliveryOutcome, EventBook};

use super::*;

fn book() -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: "player".to_string(),
            ..Cover::default()
        }),
        next_sequence: 3,
        ..EventBook::default()
    }
}

/// An answer is the events and what the coordinator did with the delivery,
/// the two fields of the distributed transport's `CommandResult`.
#[test]
fn answered_carries_events_and_delivery_outcome() {
    let applied = Answered::applied(book());
    assert_eq!(applied.events, Some(book()));
    assert_eq!(applied.delivery, DeliveryOutcome::Applied);
    assert!(!applied.is_duplicate());

    let duplicate = Answered::duplicate(book());
    assert_eq!(duplicate.events, Some(book()));
    assert_eq!(duplicate.delivery, DeliveryOutcome::Duplicate);
    assert!(duplicate.is_duplicate());
}

/// Nothing reported is no events and UNSPECIFIED, and not a duplicate.
#[test]
fn an_unreported_answer_is_unspecified() {
    let none = Answered::default();
    assert_eq!(none.events, None);
    assert_eq!(none.delivery, DeliveryOutcome::Unspecified);
    assert!(!none.is_duplicate());
    // The wire's names, as the spec writes them.
    assert_eq!(
        DeliveryOutcome::Applied.as_str_name(),
        "DELIVERY_OUTCOME_APPLIED"
    );
    assert_eq!(
        DeliveryOutcome::Duplicate.as_str_name(),
        "DELIVERY_OUTCOME_DUPLICATE"
    );
}
