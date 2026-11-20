//! BuyInWalletPolicyImpl: the scaffold of `BuyInWalletPolicy`'s implementation,
//! its one `refused` callback over the wallet's commands and refusals, bodies
//! written: a refused Hold is answered with HoldLost.

use crate::domain::*;

#[derive(Clone, Debug, Default)]
pub struct BuyInWalletPolicyImpl;

impl BuyInWalletPolicyInterface for BuyInWalletPolicyImpl {
    fn buy_in_started(
        &self,
        source: &[u8],
        event: &BuyInStarted,
        _ctx: &PolicyContext,
    ) -> Result<Vec<(Vec<u8>, WalletStateOutput)>, Bug> {
        Ok(vec![(
            event.wallet.clone(),
            Hold {
                cents: event.cents,
                buy_in: source.to_vec(),
            }
            .into(),
        )])
    }

    fn refused(
        &self,
        command: WalletStateCommand,
        refusal: WalletStateRefusal,
        _ctx: &Refused,
    ) -> Result<BuyInStateFact, Bug> {
        match (command, refusal) {
            (WalletStateCommand::Hold(_), WalletStateRefusal::InsufficientFunds(refusal)) => {
                Ok(HoldLost {
                    short: refusal.short,
                }
                .into())
            }
            // Never sent: the buy-in deposits nothing.
            _ => Err(Bug::new("buyin-wallet-unexpected-refusal")),
        }
    }
}
