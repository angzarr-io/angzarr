//! Errors carry a key and an argument map, never rendered English
//! (decision 14.37): `Diagnostic { code, args }`, with its numbers. A
//! dead letter's why (a typed refusal, a reason or a transport's why) is
//! tested in `roundtrip.rs`; `PayloadRetrievalFailed` is the distributed
//! transport's (decision 14.64), tested in
//! angzarr-proto-transport-distributed.

use angzarr_proto::v1::Diagnostic;
use prost::{Message, Name};

fn round_trip<M: Message + Default>(m: &M) -> M {
    M::decode(&*m.encode_to_vec()).expect("decodes")
}

/// The first byte of an encoding: the first field's tag, as
/// `(number << 3) | wire type`.
fn first_tag<M: Message>(m: &M) -> u8 {
    m.encode_to_vec()[0]
}

fn no_root() -> Diagnostic {
    Diagnostic {
        code: "ANGZARR_REASON_POLICY_COMMAND_NO_ROOT".into(),
        args: [
            ("domain".to_string(), "inventory".to_string()),
            ("command_type".to_string(), "ReserveStock".to_string()),
        ]
        .into(),
    }
}

#[test]
fn a_diagnostic_is_a_code_and_its_args_in_fields_1_and_2() {
    assert_eq!(Diagnostic::full_name(), "io.angzarr.v1.Diagnostic");
    let d = no_root();
    assert_eq!(round_trip(&d), d);
    assert_eq!(
        first_tag(&Diagnostic {
            code: "X".into(),
            ..Default::default()
        }),
        (1 << 3) | 2
    );
    assert_eq!(
        first_tag(&Diagnostic {
            args: [("k".to_string(), "v".to_string())].into(),
            ..Default::default()
        }),
        (2 << 3) | 2
    );
}

#[test]
fn a_wrapped_cause_is_flattened_into_the_args() {
    // The cause's code under the arg's name, its args under "<name>.<arg>".
    let d = Diagnostic {
        code: "angzarr-ports-store-unavailable".into(),
        args: [
            ("cause".to_string(), "annals-store-sql-io".to_string()),
            ("cause.errno".to_string(), "10".to_string()),
        ]
        .into(),
    };
    assert_eq!(round_trip(&d), d);
}

/// One fact must have one byte form: event bytes are compared and hashed.
/// A `HashMap` iterates in a per-process random order, so a Diagnostic with
/// several args would encode differently from one encode to the next;
/// the args are a `BTreeMap`, encoded in key order whatever the insertion order.
#[test]
fn a_diagnostic_encodes_to_the_same_bytes_whatever_the_order_its_args_were_added_in() {
    let args = [("zeta", "1"), ("alpha", "2"), ("mid", "3"), ("beta", "4")];
    let build = |order: &[usize]| {
        let mut d = Diagnostic {
            code: "angzarr-test-order".into(),
            ..Default::default()
        };
        for &i in order {
            d.args.insert(args[i].0.to_string(), args[i].1.to_string());
        }
        d.encode_to_vec()
    };
    let forward = build(&[0, 1, 2, 3]);
    assert_eq!(forward, build(&[3, 2, 1, 0]));
    assert_eq!(forward, build(&[2, 0, 3, 1]));

    // The entries follow the code in key order: alpha, beta, mid, zeta.
    let mut expected = vec![(1 << 3) | 2, 18];
    expected.extend_from_slice(b"angzarr-test-order");
    for (key, value) in [("alpha", "2"), ("beta", "4"), ("mid", "3"), ("zeta", "1")] {
        expected.extend_from_slice(&[(2 << 3) | 2, (2 + key.len() + 2 + value.len()) as u8]);
        expected.extend_from_slice(&[(1 << 3) | 2, key.len() as u8]);
        expected.extend_from_slice(key.as_bytes());
        expected.extend_from_slice(&[(2 << 3) | 2, value.len() as u8]);
        expected.extend_from_slice(value.as_bytes());
    }
    assert_eq!(forward, expected);
}
