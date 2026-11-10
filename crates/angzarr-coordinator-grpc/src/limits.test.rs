//! The defaults, as documented: a listener nobody tuned behaves as close
//! to an unlimited one as a limit can, and every setter sets its own limit
//! alone.

use std::num::NonZeroU32;
use std::time::Duration;

use super::Limits;
use crate::Listener;

#[test]
fn the_defaults_are_the_documented_values() {
    #[cfg(feature = "tls")]
    assert_eq!(
        Listener::DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        Duration::from_secs(10)
    );
    assert_eq!(
        Listener::DEFAULT_KEEPALIVE_INTERVAL,
        Duration::from_secs(60)
    );
    assert_eq!(Listener::DEFAULT_KEEPALIVE_TIMEOUT, Duration::from_secs(20));
    assert_eq!(Listener::DEFAULT_MAX_CONCURRENT_STREAMS, 200);
}

/// Every kind of listener starts at the defaults, and no request timeout
/// (the client's own deadline is the only one).
#[test]
fn a_listener_starts_at_the_defaults() {
    let expected = Limits {
        #[cfg(feature = "tls")]
        tls_handshake_timeout: Duration::from_secs(10),
        keepalive_interval: Some(Duration::from_secs(60)),
        keepalive_timeout: Duration::from_secs(20),
        request_timeout: None,
        max_concurrent_streams: NonZeroU32::new(200).unwrap(),
    };
    assert_eq!(Limits::default(), expected);
    assert_eq!(
        Listener::tcp("127.0.0.1:0".parse().unwrap()).limits,
        expected
    );
    assert_eq!(Listener::uds("/run/a.sock").limits, expected);
}

/// Each setter changes its own limit and leaves the others as they were.
#[test]
fn each_setter_sets_its_limit_alone() {
    let base = || Listener::tcp("127.0.0.1:0".parse().unwrap());
    let defaults = Limits::default();

    #[cfg(feature = "tls")]
    {
        let set = base()
            .tls_handshake_timeout(Duration::from_millis(7))
            .limits;
        assert_eq!(
            set,
            Limits {
                tls_handshake_timeout: Duration::from_millis(7),
                ..defaults.clone()
            }
        );
    }

    let set = base()
        .keepalive(Some(Duration::from_millis(5)), Duration::from_millis(6))
        .limits;
    assert_eq!(
        set,
        Limits {
            keepalive_interval: Some(Duration::from_millis(5)),
            keepalive_timeout: Duration::from_millis(6),
            ..defaults.clone()
        }
    );
    let set = base().keepalive(None, Duration::from_millis(6)).limits;
    assert_eq!(set.keepalive_interval, None);

    let set = base().request_timeout(Duration::from_millis(9)).limits;
    assert_eq!(
        set,
        Limits {
            request_timeout: Some(Duration::from_millis(9)),
            ..defaults.clone()
        }
    );

    let set = base()
        .max_concurrent_streams(NonZeroU32::new(4).unwrap())
        .limits;
    assert_eq!(
        set,
        Limits {
            max_concurrent_streams: NonZeroU32::new(4).unwrap(),
            ..defaults
        }
    );
}
