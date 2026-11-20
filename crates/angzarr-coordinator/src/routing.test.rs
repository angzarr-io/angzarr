use std::collections::{BTreeMap, HashMap};

use super::*;

const REACHES: [Reach; 3] = [Reach::None, Reach::Loopback, Reach::Ip];

fn admitting(arrival: Arrival) -> Vec<Reach> {
    REACHES
        .into_iter()
        .filter(|reach| arrival.admitted_by(*reach))
        .collect()
}

/// This machine reaches what is served to this machine or to the network,
/// and nothing served to none.
#[test]
fn a_loopback_arrival_is_admitted_by_loopback_and_ip_reach_and_not_by_none() {
    assert_eq!(admitting(Arrival::Loopback), [Reach::Loopback, Reach::Ip]);
    assert!(!Arrival::Loopback.admitted_by(Reach::None));
}

/// The network reaches only what is served to the network.
#[test]
fn an_ip_arrival_is_admitted_by_ip_reach_alone() {
    assert_eq!(admitting(Arrival::Ip), [Reach::Ip]);
}

/// `Reach::admits` is the same rule read from the other side.
#[test]
fn a_reach_admits_the_arrivals_at_or_inside_it() {
    for arrival in [Arrival::Loopback, Arrival::Ip] {
        for reach in REACHES {
            assert_eq!(reach.admits(arrival), arrival.admitted_by(reach));
        }
    }
}

/// Reach is ordered by how far it goes, and a domain nothing names is not
/// served.
#[test]
fn reach_is_ordered_and_defaults_to_none() {
    assert!(Reach::None < Reach::Loopback && Reach::Loopback < Reach::Ip);
    assert_eq!(Reach::default(), Reach::None);
}

#[test]
fn arrival_and_reach_are_their_args() {
    assert_eq!(Arrival::Loopback.as_str(), "loopback");
    assert_eq!(Arrival::Ip.as_str(), "ip");
    assert_eq!(Reach::None.as_str(), "none");
    assert_eq!(Reach::Loopback.as_str(), "loopback");
    assert_eq!(Reach::Ip.as_str(), "ip");
}

/// The log line names the domain, where the request arrived from and how
/// far the domain is served, as code and args.
#[test]
fn hidden_carries_domain_arrival_and_reach_for_the_log() {
    let hidden = Hidden {
        domain: "table".to_string(),
        arrival: Arrival::Ip,
        reach: Reach::None,
    };
    assert_eq!(hidden.code(), "angzarr-coordinator-hidden");
    assert_eq!(
        hidden.to_string(),
        "angzarr-coordinator-hidden domain=table arrival=ip reach=none"
    );
    let args: Vec<_> = hidden.args().into_iter().map(|(name, _)| name).collect();
    assert_eq!(args, ["domain", "arrival", "reach"]);
}

/// A map serves the domains it names to the reach it gives, and every
/// other domain to none.
#[test]
fn a_map_serves_the_domains_it_names_and_no_other() {
    let btree: BTreeMap<String, Reach> = [
        ("player".to_string(), Reach::Ip),
        ("table".to_string(), Reach::Loopback),
    ]
    .into();
    let hash: HashMap<String, Reach> = btree.clone().into_iter().collect();
    assert_eq!(btree.reach("player"), Reach::Ip);
    assert_eq!(btree.reach("table"), Reach::Loopback);
    assert_eq!(btree.reach("hand"), Reach::None);
    assert_eq!(hash.reach("player"), Reach::Ip);
    assert_eq!(hash.reach("table"), Reach::Loopback);
    assert_eq!(hash.reach("hand"), Reach::None);
}

/// A route is served when the reach admits the arrival, and is the
/// `Hidden` with the real reason when it does not (a domain nothing names
/// is hidden at reach none).
#[test]
fn a_route_is_served_or_the_hidden_reason() {
    let map: BTreeMap<String, Reach> = [("table".to_string(), Reach::Loopback)].into();
    assert_eq!(map.route("table", Arrival::Loopback), Ok(()));
    assert_eq!(
        map.route("table", Arrival::Ip),
        Err(Hidden {
            domain: "table".to_string(),
            arrival: Arrival::Ip,
            reach: Reach::Loopback,
        })
    );
    assert_eq!(
        map.route("hand", Arrival::Loopback),
        Err(Hidden {
            domain: "hand".to_string(),
            arrival: Arrival::Loopback,
            reach: Reach::None,
        })
    );
}
