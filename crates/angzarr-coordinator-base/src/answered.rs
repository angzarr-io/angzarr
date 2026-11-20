//! [`Answered`]: what a coordinator answers for one command.

use angzarr_proto::v1::{DeliveryOutcome, EventBook};

/// What a coordinator answers for one command, in core's messages: the
/// addressed stream's events and what it did with the delivery, as the
/// distributed transport's `CommandResult` carries them (`events` and
/// `delivery`; decision 14.105), without the transport. Never persisted.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Answered {
    /// The addressed stream's events: those persisted, or, for a duplicate,
    /// those the stream already held; its cover unset means the command's.
    pub events: Option<EventBook>,
    /// What the coordinator did with the delivery: APPLIED or DUPLICATE
    /// (UNSPECIFIED: not reported).
    pub delivery: DeliveryOutcome,
}

impl Answered {
    /// Business code ran for this delivery: `events` are new.
    pub fn applied(events: EventBook) -> Answered {
        Answered {
            events: Some(events),
            delivery: DeliveryOutcome::Applied,
        }
    }

    /// The stream already held this command's events: `events` are re-read.
    pub fn duplicate(events: EventBook) -> Answered {
        Answered {
            events: Some(events),
            delivery: DeliveryOutcome::Duplicate,
        }
    }

    /// Whether the delivery was a duplicate.
    pub fn is_duplicate(&self) -> bool {
        self.delivery == DeliveryOutcome::Duplicate
    }
}

#[cfg(test)]
#[path = "answered.test.rs"]
mod tests;
