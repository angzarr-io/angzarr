//! The credit gate: what a transport whose broker cannot bound in-flight
//! per stream counts its unsettled deliveries with: at most its instances
//! in all, and one of a stream.

use std::collections::BTreeMap;

use super::transport::OrderingKey;
use super::Credit;

/// Unsettled deliveries, in all and per ordering key, against a credit.
#[derive(Clone, Debug)]
pub struct CreditGate {
    credit: Credit,
    total: usize,
    of: BTreeMap<OrderingKey, usize>,
}

impl CreditGate {
    /// Nothing unsettled, against `credit`.
    pub fn new(credit: Credit) -> CreditGate {
        CreditGate {
            credit,
            total: 0,
            of: BTreeMap::new(),
        }
    }

    /// Whether one of `key` is within the credit: none of it unsettled, and
    /// room left.
    pub fn admits(&self, key: &OrderingKey) -> bool {
        !self.full() && !self.of.contains_key(key)
    }

    /// Whether nothing more is within the credit: a broker adapter pauses
    /// its fetch.
    pub fn full(&self) -> bool {
        self.total >= self.credit.instances().get()
    }

    /// One more of `key` unsettled.
    pub fn take(&mut self, key: &OrderingKey) {
        self.total += 1;
        *self.of.entry(key.clone()).or_default() += 1;
    }

    /// One of `key` settled.
    pub fn release(&mut self, key: &OrderingKey) {
        self.total = self.total.saturating_sub(1);
        if let Some(of) = self.of.get_mut(key) {
            *of = of.saturating_sub(1);
            if *of == 0 {
                self.of.remove(key);
            }
        }
    }

    /// How many are unsettled.
    pub fn unsettled(&self) -> usize {
        self.total
    }
}

#[cfg(test)]
#[path = "credit.test.rs"]
mod tests;
