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
