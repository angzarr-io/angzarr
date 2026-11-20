//! Components written by hand beside the generated ones, naming nothing of
//! the generator's but the domain layer's types and the names
//! (`crate::names`): a projector, angzarr-router's `ProjectorHandler`
//! registered with `Registry::projector_handler`, and a policy in the
//! markup standing in for the generated one under its name.

use angzarr_embedded::markup::{Commands, PolicyContext};
use angzarr_embedded_macros::{on_event, policy};
use angzarr_router::error::CodedError;
use angzarr_router::hpb;
use angzarr_router::router::ProjectorHandler;

/// Counts the ledger's events, a committed book at a time.
pub struct Tally;

impl ProjectorHandler for Tally {
    fn name(&self) -> &str {
        "projector-tally"
    }

    async fn dispatch(&self, req: &hpb::ProjectRequest) -> Result<hpb::ProjectResponse, CodedError> {
        let events = req.events.clone().unwrap_or_default();
        let domain = events
            .cover
            .as_ref()
            .map_or("", |cover| cover.domain.as_str());
        println!("tally: {} events from {domain}", events.pages.len());
        Ok(hpb::ProjectResponse::default())
    }
}

/// The generated LedgerAuditPolicy's reaction, written by hand: each Noted
/// audits the ledger's root.
#[derive(Clone, Debug, Default)]
pub struct LedgerAudit;

#[policy(name = crate::names::LEDGERAUDITPOLICY, source = crate::adapters::LedgerState, target = crate::adapters::AuditState)]
impl LedgerAudit {
    #[on_event(crate::domain::Noted)]
    fn noted(
        &self,
        _event: &crate::domain::Noted,
        ctx: &PolicyContext,
    ) -> Result<Commands<crate::adapters::AuditState>, crate::domain::Bug> {
        Ok(Commands::none().to(ctx.source_root().clone(), crate::domain::Audit {}))
    }
}
