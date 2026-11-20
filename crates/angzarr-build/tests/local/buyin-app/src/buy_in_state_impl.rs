//! BuyInStateImpl: the scaffold of `BuyInState`'s implementation, bodies written.

use crate::domain::*;

/// The state the `apply_` methods fold.
#[derive(Clone, Debug, Default)]
pub struct BuyInStateImpl {
    pub state: crate::wire::BuyInState,
}

impl BuyInStateInterface for BuyInStateImpl {
    fn start_buy_in(
        &self,
        cmd: &StartBuyIn,
        _ctx: &Context,
    ) -> Result<Vec<BuyInStateEvent>, Failed<BuyInRefused>> {
        Ok(vec![
            BuyInStarted {
                wallet: cmd.wallet.clone(),
                cents: cmd.cents,
            }
            .into(),
        ])
    }

    fn record_held(
        &self,
        cmd: &RecordHeld,
        _ctx: &Context,
    ) -> Result<Vec<BuyInStateEvent>, Failed<BuyInRefused>> {
        Ok(vec![BuyInHeld { cents: cmd.cents }.into()])
    }

    fn hold_lost(&self, fact: &HoldLost, _ctx: &Context) -> Result<Vec<BuyInStateEvent>, Bug> {
        Ok(vec![BuyInFailed { short: fact.short }.into()])
    }

    fn apply_buy_in_started(&mut self, _event: &BuyInStarted) {
        self.state.started = true;
    }

    fn apply_buy_in_held(&mut self, event: &BuyInHeld) {
        self.state.held += event.cents;
    }

    fn apply_buy_in_failed(&mut self, _event: &BuyInFailed) {
        self.state.failed = true;
    }
}

impl BuyInStateImpl {
    pub fn held(&self) -> i64 {
        self.state.held
    }

    pub fn failed(&self) -> bool {
        self.state.failed
    }
}
