//! Routing and reach: where a request arrived from, how far a domain is
//! served, and why a domain was answered as an unknown one.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};

/// Where a request arrived from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arrival {
    /// This machine: a loopback address, or a socket only this machine can
    /// open.
    Loopback,
    /// The network: any other address.
    Ip,
}

impl Arrival {
    /// As an arg: `"loopback"` or `"ip"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Arrival::Loopback => "loopback",
            Arrival::Ip => "ip",
        }
    }

    /// Whether a domain reached as far as `reach` is served to a request
    /// from here: loopback is admitted by `Loopback` and `Ip`, the network
    /// by `Ip` alone, and `None` admits nothing.
    pub fn admitted_by(&self, reach: Reach) -> bool {
        match self {
            Arrival::Loopback => reach >= Reach::Loopback,
            Arrival::Ip => reach == Reach::Ip,
        }
    }
}

/// How far a domain is served: the ceiling a frontend reports per domain.
/// Ordered by reach, `None` the least.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Reach {
    /// Served to no request.
    #[default]
    None,
    /// Served to requests from this machine.
    Loopback,
    /// Served to requests from the network too.
    Ip,
}

impl Reach {
    /// As an arg: `"none"`, `"loopback"` or `"ip"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Reach::None => "none",
            Reach::Loopback => "loopback",
            Reach::Ip => "ip",
        }
    }

    /// Whether this reach admits `arrival` ([`Arrival::admitted_by`]).
    pub fn admits(&self, arrival: Arrival) -> bool {
        arrival.admitted_by(*self)
    }
}

/// What says, per domain, how far it is served: the embedded host's expose
/// map, a distributed coordinator's single domain, a read container's
/// configured table. A domain it does not name is not served.
pub trait Served {
    /// How far `domain` is served.
    fn reach(&self, domain: &str) -> Reach;

    /// Whether a request for `domain` that arrived from `arrival` may be
    /// served, or why not: the [`Hidden`] a frontend answers as an unknown
    /// domain and logs.
    fn route(&self, domain: &str, arrival: Arrival) -> Result<(), Hidden> {
        let reach = self.reach(domain);
        if arrival.admitted_by(reach) {
            Ok(())
        } else {
            Err(Hidden {
                domain: domain.to_string(),
                arrival,
                reach,
            })
        }
    }
}

impl Served for BTreeMap<String, Reach> {
    fn reach(&self, domain: &str) -> Reach {
        self.get(domain).copied().unwrap_or_default()
    }
}

impl Served for HashMap<String, Reach> {
    fn reach(&self, domain: &str) -> Reach {
        self.get(domain).copied().unwrap_or_default()
    }
}

/// Why a request for `domain` was answered as an unknown domain: it is
/// served as far as `reach`, which does not admit `arrival`. The
/// frontend's local log line, never the caller's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hidden {
    pub domain: String,
    pub arrival: Arrival,
    pub reach: Reach,
}

impl fmt::Display for Hidden {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for Hidden {
    fn code(&self) -> &str {
        code!("hidden")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![
            ("domain", self.domain.as_str().into()),
            ("arrival", self.arrival.as_str().into()),
            ("reach", self.reach.as_str().into()),
        ]
    }
}

impl std::error::Error for Hidden {}

/// One value of every error this module defines, for the catalog parity
/// test.
pub(crate) fn samples() -> Vec<Box<dyn Diagnostic>> {
    vec![Box::new(Hidden {
        domain: "player".to_string(),
        arrival: Arrival::Ip,
        reach: Reach::Loopback,
    })]
}

#[cfg(test)]
#[path = "routing.test.rs"]
mod tests;
