//! The kinds fixture on angzarr-embedded: its wire types, domain layer,
//! adapters and register() generated, its components the scaffold's
//! interfaces and implementations, bodies written, and one projector
//! written by hand.

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/kinds.v1.rs"));
}
pub mod domain {
    include!(concat!(env!("OUT_DIR"), "/kinds.v1.domain.rs"));
}
include!(concat!(env!("OUT_DIR"), "/angzarr-adapters.rs"));
include!(concat!(env!("OUT_DIR"), "/angzarr-register.rs"));
/// Each component's name in the application, as the generator gives it
/// (`angzarr_build::component_names`).
pub mod names {
    include!(concat!(env!("OUT_DIR"), "/names.rs"));
}

pub mod audit_state_impl;
pub mod hand_written;
pub mod ledger_audit_policy_impl;
pub mod ledger_state_impl;
pub mod ledger_view_impl;

/// The application's registration: the generated components, then the
/// projector written by hand, through angzarr-embedded's public API only.
pub fn register_all(
    registry: &mut angzarr_embedded::Registry,
) -> Result<(), angzarr_embedded::Misconfigured> {
    register(registry)?;
    registry.projector_handler(&["ledger"], hand_written::Tally)?;
    Ok(())
}

/// The same application with the generated policy swapped for the one
/// written by hand under the generated one's name: the generated
/// components registered one by one, then the hand-written ones.
pub fn register_swapped(
    registry: &mut angzarr_embedded::Registry,
) -> Result<(), angzarr_embedded::Misconfigured> {
    registry
        .aggregate::<adapters::LedgerState>(adapters::LedgerState::default)?
        .aggregate::<adapters::AuditState>(adapters::AuditState::default)?
        .projector(adapters::LedgerView::default())?
        .policy(hand_written::LedgerAudit)?
        .projector_handler(&["ledger"], hand_written::Tally)?;
    Ok(())
}
