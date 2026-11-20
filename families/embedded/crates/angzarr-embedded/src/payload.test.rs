//! Payloads are `Any`, packed from prost messages by their `prost::Name`.
//! This runs: packing and decoding are implemented.

use crate::{pack, type_name, Any, Payload, PayloadError};
use prost::Message;

use crate::toys::{account, AccountEvent};
use angzarr_diagnostic::code;

#[test]
fn payloads_are_any_packed_by_their_prost_name() {
    let deposited = account::Deposited { cents: 250 };
    let any = pack(&deposited);
    assert_eq!(any.type_url, "/toy.account.v1.Deposited", "angzarr's '/'");
    assert_eq!(any.value, deposited.encode_to_vec());

    assert_eq!(
        type_name("/toy.account.v1.Deposited"),
        "toy.account.v1.Deposited"
    );
    assert_eq!(
        type_name("type.googleapis.com/toy.account.v1.Deposited"),
        "toy.account.v1.Deposited",
        "any prefix names the same type"
    );
    assert_eq!(
        type_name("toy.account.v1.Deposited"),
        "toy.account.v1.Deposited"
    );

    let event = AccountEvent::Deposited(250);
    assert_eq!(event.to_any(), any);
    assert_eq!(AccountEvent::from_any(&any), Ok(event));
    assert_eq!(
        AccountEvent::type_urls(),
        [
            "/toy.account.v1.Deposited",
            "/toy.account.v1.Opened",
            "/toy.account.v1.Received",
            "/toy.account.v1.Withdrew",
        ],
        "the current types, in name order"
    );

    let old = pack(&account::v0::Credited { euros: 2 });
    assert_eq!(
        AccountEvent::from_any(&old),
        Err(PayloadError::UnknownType {
            type_url: "/toy.account.v0.Credited".to_string()
        }),
        "an old type is upcast first, never decoded as if current"
    );
    let garbled = Any {
        type_url: "/toy.account.v1.Deposited".to_string(),
        value: vec![0xff],
    };
    assert!(matches!(
        AccountEvent::from_any(&garbled),
        Err(PayloadError::Decode { type_url, .. }) if type_url == "/toy.account.v1.Deposited"
    ));
}

#[test]
fn a_domain_that_takes_no_facts_decodes_none() {
    use crate::Payload;
    use std::convert::Infallible;
    assert!(<Infallible as Payload>::type_urls().is_empty());
    assert_eq!(
        <Infallible as Payload>::from_any(&crate::Any {
            type_url: "/a.B".to_string(),
            value: Vec::new(),
        })
        .err(),
        Some(crate::PayloadError::UnknownType {
            type_url: "/a.B".to_string()
        })
    );
}

/// A wire message and the domain type it converts to, as angzarr's generator
/// emits them: the domain type requires `at`, which the wire leaves optional.
mod wire_and_domain {
    use crate::{Timestamp, WireError};

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Stamped {
        #[prost(message, optional, tag = "1")]
        pub at: Option<Timestamp>,
    }

    impl prost::Name for Stamped {
        const NAME: &'static str = "Stamped";
        const PACKAGE: &'static str = "toy.wire.v1";
    }

    #[derive(Clone, PartialEq, Debug)]
    pub struct StampedDomain {
        pub at: Timestamp,
    }

    impl TryFrom<Stamped> for StampedDomain {
        type Error = WireError;

        fn try_from(wire: Stamped) -> Result<Self, WireError> {
            Ok(StampedDomain {
                at: wire.at.ok_or(WireError::Missing {
                    message: "toy.wire.v1.Stamped",
                    field: "at",
                })?,
            })
        }
    }
}

#[test]
fn decode_as_converts_a_valid_wire_message_to_its_domain_type() {
    use crate::{decode_as, Timestamp};
    use wire_and_domain::{Stamped, StampedDomain};

    let at = Timestamp {
        seconds: 7,
        nanos: 0,
    };
    let decoded = decode_as::<Stamped, StampedDomain>(&pack(&Stamped { at: Some(at) }));
    assert_eq!(decoded, Ok(StampedDomain { at }));
}

#[test]
fn decode_as_refuses_a_wire_message_its_domain_type_cannot_hold() {
    use crate::decode_as;
    use angzarr_diagnostic::Diagnostic;
    use wire_and_domain::{Stamped, StampedDomain};

    let error = decode_as::<Stamped, StampedDomain>(&pack(&Stamped { at: None }))
        .expect_err("at is required");
    let PayloadError::Invalid { type_url, reason } = &error else {
        panic!("expected an invalid payload, got {error:?}");
    };
    assert_eq!(type_url, "/toy.wire.v1.Stamped");
    assert_eq!(reason.code(), code!("wire-missing"));
}

#[test]
fn decode_as_refuses_bytes_that_do_not_decode() {
    use crate::decode_as;
    use wire_and_domain::{Stamped, StampedDomain};

    let any = Any {
        type_url: "/toy.wire.v1.Stamped".to_string(),
        value: vec![0x0a],
    };
    assert!(matches!(
        decode_as::<Stamped, StampedDomain>(&any),
        Err(PayloadError::Decode { .. })
    ));
}
