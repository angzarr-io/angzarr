use std::net::SocketAddr;

use super::*;

/// A loopback arrival reaches loopback and ip domains; an ip arrival only
/// ip domains; a domain of no reach is reached by none.
#[test]
fn an_arrival_is_admitted_by_the_reach_that_covers_it() {
    let table = [
        (Arrival::Loopback, Reach::None, false),
        (Arrival::Loopback, Reach::Loopback, true),
        (Arrival::Loopback, Reach::Ip, true),
        (Arrival::Ip, Reach::None, false),
        (Arrival::Ip, Reach::Loopback, false),
        (Arrival::Ip, Reach::Ip, true),
    ];
    for (arrival, reach, admitted) in table {
        assert_eq!(
            arrival.admitted_by(reach),
            admitted,
            "{arrival:?} {reach:?}"
        );
    }
}

#[test]
fn an_arrival_is_named_as_it_is_logged() {
    assert_eq!(Arrival::Loopback.as_str(), "loopback");
    assert_eq!(Arrival::Ip.as_str(), "ip");
}

/// Loopback addresses (v4 and v6) default to loopback, every other address,
/// the unspecified ones included, to ip.
#[test]
fn a_bind_address_declares_loopback_only_when_it_is_one() {
    let table = [
        ("127.0.0.1:1", Arrival::Loopback),
        ("127.9.9.9:1", Arrival::Loopback),
        ("[::1]:1", Arrival::Loopback),
        ("0.0.0.0:1", Arrival::Ip),
        ("[::]:1", Arrival::Ip),
        ("10.0.0.5:1", Arrival::Ip),
        ("192.168.1.2:1", Arrival::Ip),
    ];
    for (addr, arrival) in table {
        let addr: SocketAddr = addr.parse().unwrap();
        assert_eq!(Arrival::for_bind(addr), arrival, "{addr}");
    }
}

/// What a listener inserted is what a handler reads; a request that has
/// none reads as the arrival that reaches least.
#[test]
fn a_request_without_a_declared_arrival_reads_as_ip() {
    let mut request = tonic::Request::new(());
    assert_eq!(arrival_of(&request), Arrival::Ip);
    request.extensions_mut().insert(Arrival::Loopback);
    assert_eq!(arrival_of(&request), Arrival::Loopback);
}
