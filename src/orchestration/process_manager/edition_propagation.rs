//! Audit #86: PM coordinator's trigger-edition propagation contract.
//!
//! Free function so unit tests can drive it without standing up the
//! gRPC pipeline. Stamps the trigger cover's edition (full struct
//! including divergences) onto every outgoing book — commands,
//! process_events, and facts. Always-override: handler-set editions
//! get overwritten so the coordinator guarantees timeline consistency
//! across cross-domain emissions. When the trigger has no cover, no
//! propagation runs (outgoing books keep whatever the handler set).

use crate::proto::{CommandBook, EventBook};

pub(crate) fn propagate_trigger_edition(
    trigger_cover: Option<&crate::proto::Cover>,
    commands: &mut [CommandBook],
    process_events: &mut [EventBook],
    facts: &mut [EventBook],
) {
    let Some(trigger_cover) = trigger_cover else {
        return;
    };
    for cmd in commands.iter_mut() {
        if let Some(c) = &mut cmd.cover {
            c.propagate_edition_from(trigger_cover);
        }
    }
    for book in process_events.iter_mut() {
        if let Some(c) = &mut book.cover {
            c.propagate_edition_from(trigger_cover);
        }
    }
    for book in facts.iter_mut() {
        if let Some(c) = &mut book.cover {
            c.propagate_edition_from(trigger_cover);
        }
    }
}

#[cfg(test)]
#[path = "edition_propagation.test.rs"]
mod tests;
