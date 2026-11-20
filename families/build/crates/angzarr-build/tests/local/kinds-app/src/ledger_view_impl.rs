//! LedgerViewImpl: the implementation of `LedgerViewInterface`, the generated domain layer's
//! interface, written once by `angzarr scaffold rust` and yours from then on:
//! `--missing` adds the required methods it lacks, each `todo!()`.

use crate::domain::*;

/// The projection the methods update.
#[derive(Clone, Debug, Default)]
pub struct LedgerViewImpl {
    pub state: crate::wire::LedgerView,
}

impl LedgerViewInterface for LedgerViewImpl {
    async fn posted(&mut self, _event: &Posted, ctx: &ProjectionContext) -> Result<(), Bug> {
        self.state.seen += 1;
        println!("view: Posted from {} ({})", ctx.stream().domain, self.state.seen);
        Ok(())
    }

    async fn audited(&mut self, _event: &Audited, ctx: &ProjectionContext) -> Result<(), Bug> {
        self.state.seen += 1;
        println!("view: Audited from {} ({})", ctx.stream().domain, self.state.seen);
        Ok(())
    }
}
