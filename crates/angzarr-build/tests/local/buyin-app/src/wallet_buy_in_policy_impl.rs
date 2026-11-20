//! WalletBuyInPolicyImpl: the scaffold of `WalletBuyInPolicy`'s implementation,
//! bodies written. The buy-in's root is the one the Hold carried; a refused
//! RecordHeld is answered with RecordBuyInRefused.

use crate::domain::*;

#[derive(Clone, Debug, Default)]
pub struct WalletBuyInPolicyImpl;

impl WalletBuyInPolicyInterface for WalletBuyInPolicyImpl {
    fn held(
        &self,
        _source: &[u8],
        event: &Held,
        _ctx: &PolicyContext,
    ) -> Result<Vec<(Vec<u8>, BuyInStateOutput)>, Bug> {
        Ok(vec![(
            event.buy_in.clone(),
            RecordHeld { cents: event.cents }.into(),
        )])
    }

    fn refused(
        &self,
        command: BuyInStateCommand,
        refusal: BuyInStateRefusal,
        _ctx: &Refused,
    ) -> Result<WalletStateFact, Bug> {
        match (command, refusal) {
            (BuyInStateCommand::RecordHeld(_), BuyInStateRefusal::BuyInRefused(refusal)) => {
                Ok(RecordBuyInRefused {
                    reason: refusal.reason,
                }
                .into())
            }
            // Never sent: the wallet starts no buy-in.
            _ => Err(Bug::new("wallet-buyin-unexpected-refusal")),
        }
    }
}
