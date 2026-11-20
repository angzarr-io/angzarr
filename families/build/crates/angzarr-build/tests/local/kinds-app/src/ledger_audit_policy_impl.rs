//! LedgerAuditPolicyImpl: the implementation of `LedgerAuditPolicyInterface`, the generated domain layer's
//! interface, written once by `angzarr scaffold rust` and yours from then on:
//! `--missing` adds the required methods it lacks, each `todo!()`.

use crate::domain::*;

#[derive(Clone, Debug, Default)]
pub struct LedgerAuditPolicyImpl;

impl LedgerAuditPolicyInterface for LedgerAuditPolicyImpl {
    /// Audits the ledger's root.
    fn noted(
        &self,
        source: &[u8],
        _event: &Noted,
        _ctx: &PolicyContext,
    ) -> Result<Vec<(Vec<u8>, AuditStateOutput)>, Bug> {
        Ok(vec![(source.to_vec(), Audit {}.into())])
    }

    /// A refused Audit is the ledger's Arrived; the refusal, the choice of
    /// Audit's two types, is printed as it is handed over.
    fn refused(
        &self,
        _command: AuditStateCommand,
        refusal: AuditStateRefusal,
        _ctx: &Refused,
    ) -> Result<LedgerStateFact, Bug> {
        println!("audit refused: {refusal:?}");
        Ok(Arrived {}.into())
    }
}
