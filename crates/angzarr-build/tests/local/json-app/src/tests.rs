//! The generated JSON: golden files for the canonical mapping's edges and
//! for each codegen option; round trips, wire to JSON to wire, over
//! generated values; and prost-reflect's canonical mapping reading what it
//! writes, and the other way round.

use prost::{Message, Name};
use proptest::prelude::*;
use prost_reflect::{DescriptorPool, DynamicMessage};

use crate::json::{Json, JsonError, JsonMessage, Registered, Registry};
use crate::wire::json::v1::{
    Choice, Finished, Known, Output, Plain, Site, Started, Status, Tone, choice, output,
};

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/golden");
const SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/angzarr.descriptors"));

/// `text` is the golden file `name` (rewritten under `ANGZARR_BLESS`).
fn golden(name: &str, text: &str) {
    let path = format!("{GOLDEN}/{name}");
    if std::env::var_os("ANGZARR_BLESS").is_some() {
        std::fs::write(&path, format!("{text}\n")).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("no {path}"));
    assert_eq!(format!("{text}\n"), expected, "{path}");
}

fn registry() -> Registry {
    Registry::new().with::<Plain>().with::<Started>()
}

/// `message` as JSON text, through the registry.
fn text<T: JsonMessage>(message: &T) -> String {
    serde_json::to_string(&Registered(message, &registry())).expect("written")
}

/// `text` read back as a `T`, through the registry.
fn read<T: JsonMessage>(text: &str) -> Result<T, JsonError> {
    let json: Json = serde_json::from_str(text).expect("JSON text");
    T::from_json(&json, &registry())
}

fn uuid(byte: u8) -> angzarr_proto::v1::Uuid {
    angzarr_proto::v1::Uuid::from_bytes([byte; 16])
}

fn edges() -> Plain {
    Plain {
        f64: f64::NAN,
        f32: f32::INFINITY,
        i32: i32::MIN,
        i64: i64::MIN,
        u32: u32::MAX,
        u64: u64::MAX,
        s32: -7,
        s64: (1 << 53) + 1,
        x32: 9,
        x64: 10,
        sx32: -11,
        sx64: -12,
        flag: true,
        text: "quote \" backslash \\ line\nfeed \u{1}\u{2028}".to_string(),
        data: vec![0, 1, 254, 255],
        tone: 7,
        maybe: Some(0),
        note: None,
        counts: vec![-1, 0, i64::MAX],
        ratios: vec![f64::NEG_INFINITY, -0.0, 0.1, 1e300],
        tones: vec![Tone::Soft as i32, 99],
        blobs: vec![vec![], vec![255; 4]],
        totals: [("b".to_string(), 2), ("a".to_string(), i64::MIN)].into(),
        moods: [("z".to_string(), Tone::Loud as i32), ("y".to_string(), 5)].into(),
        names: [(10, "ten".to_string()), (-1, "minus".to_string()), (2, "two".to_string())]
            .into(),
        nested: [(
            3,
            Plain {
                text: "inner".to_string(),
                ..Plain::default()
            },
        )]
        .into(),
        switches: [(true, 1.5), (false, -2.0)].into(),
        child: Some(Box::new(Plain {
            i64: 1,
            ..Plain::default()
        })),
        children: vec![Plain::default()],
        snake_case_field: "s".to_string(),
        renamed: "r".to_string(),
    }
}

fn known() -> Known {
    Known {
        at: Some(prost_types::Timestamp {
            seconds: 1_795_428_000,
            nanos: 500_000_000,
        }),
        took: Some(prost_types::Duration {
            seconds: -1,
            nanos: -500_000,
        }),
        id: Some(uuid(0xab)),
        payload: Some(prost_types::Any::from_msg(&Plain {
            text: "packed".to_string(),
            ..Plain::default()
        })
        .unwrap()),
        payloads: vec![
            prost_types::Any::from_msg(&prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            })
            .unwrap(),
            prost_types::Any::from_msg(&Started {
                root: "/r".to_string(),
                jobs: 2,
                site: None,
            })
            .unwrap(),
        ],
        ids: vec![uuid(1), uuid(2)],
    }
}

fn finished() -> Finished {
    Finished {
        counts: [("survived".to_string(), 1), ("killed".to_string(), 3)].into(),
        took_ms: 1234,
        exit: 0,
        status: Status::NotCalled as i32,
        why: Some("no tests".to_string()),
        site: Some(Site {
            file: "src/lib.rs".to_string(),
            end_line: 9,
        }),
        tests: None,
    }
}

// region golden

/// The canonical mapping's edges: 64-bit integers as strings, NaN and the
/// infinities as their names, an enum number without a value as its
/// number, bytes as base64, map keys sorted as text, defaults left out.
#[test]
fn the_canonical_edges_are_their_golden_json() {
    golden("plain.json", &text(&edges()));
    golden("plain-default.json", &text(&Plain::default()));
}

/// Timestamp in RFC 3339 with `Z`, Duration in seconds, UUID as its
/// hyphenated text, and Any expanded through the registry (a well-known
/// type's under `value`).
#[test]
fn the_well_known_types_are_their_golden_json() {
    golden("known.json", &text(&known()));
}

/// Each arm of a oneof, under its own key.
#[test]
fn a_oneofs_arms_are_their_golden_json() {
    let arms = [
        choice::Pick::Word("w".to_string()),
        choice::Pick::Big(-5),
        choice::Pick::Tone(Tone::Loud as i32),
        choice::Pick::Plain(Plain::default()),
        choice::Pick::When(prost_types::Timestamp::default()),
    ];
    let lines: Vec<String> = arms
        .into_iter()
        .map(|pick| text(&Choice { pick: Some(pick) }))
        .collect();
    golden("choice.jsonl", &lines.join("\n"));
}

/// The options: a tag first, snake_case keys, every default written, an
/// unset optional as null, a renamed key, a flattened field, kebab-case
/// and renamed enum values, and a oneof tagged under `event` with its arm
/// beside the tag.
#[test]
fn the_options_are_their_golden_json() {
    let documents = [
        text(&finished()),
        text(&Finished {
            status: Status::Failed as i32,
            why: None,
            site: None,
            ..finished()
        }),
        text(&Started {
            root: "/r".to_string(),
            jobs: 0,
            site: None,
        }),
        text(&Output {
            run: Some(uuid(7)),
            event: Some(output::Event::Started(Started {
                root: "/r".to_string(),
                jobs: 4,
                site: Some(Site::default()),
            })),
        }),
        text(&Output {
            run: Some(uuid(7)),
            event: Some(output::Event::Finished(finished())),
        }),
        text(&Output {
            run: None,
            event: Some(output::Event::Note("n".to_string())),
        }),
    ];
    golden("options.jsonl", &documents.join("\n"));
}

/// The same message is the same bytes every time: no map or object order
/// comes from a hash.
#[test]
fn the_json_is_the_same_bytes_every_time() {
    let first = text(&edges());
    for _ in 0..8 {
        assert_eq!(text(&edges()), first);
    }
}

// endregion golden

// region reading

/// What the options write reads back to the same message.
#[test]
fn the_options_read_back() {
    let messages = [
        finished(),
        Finished {
            why: None,
            site: None,
            ..finished()
        },
    ];
    for message in messages {
        assert_eq!(read::<Finished>(&text(&message)), Ok(message));
    }
    let output = Output {
        run: Some(uuid(7)),
        event: Some(output::Event::Finished(finished())),
    };
    assert_eq!(read::<Output>(&text(&output)), Ok(output));
}

/// A key the message does not have, a value of the wrong kind, and an
/// Any of a type the registry lacks are refused.
#[test]
fn what_is_not_the_message_is_refused() {
    assert!(matches!(
        read::<Plain>(r#"{"nope": 1}"#),
        Err(JsonError::UnknownField { .. })
    ));
    assert!(matches!(
        read::<Plain>(r#"{"i32": "x"}"#),
        Err(JsonError::Invalid { .. })
    ));
    assert!(matches!(
        read::<Known>(r#"{"payload": {"@type": "type.googleapis.com/other.v1.Thing"}}"#),
        Err(JsonError::UnknownType { .. })
    ));
}

/// The canonical mapping's alternatives read too: a proto field name, a
/// 64-bit integer as a number, an enum by number, null for a default.
#[test]
fn the_canonical_alternatives_read() {
    let read = read::<Plain>(
        r#"{"snake_case_field": "s", "i64": 5, "tone": 2, "text": null, "u64": "7"}"#,
    )
    .unwrap();
    assert_eq!(
        read,
        Plain {
            snake_case_field: "s".to_string(),
            i64: 5,
            tone: Tone::Loud as i32,
            u64: 7,
            ..Plain::default()
        }
    );
}

/// Without a registry an Any cannot be written: the plain `Serialize`
/// refuses it rather than writing something no reader expands.
#[test]
fn serialize_without_a_registry_refuses_an_any() {
    assert!(serde_json::to_string(&known()).is_err());
    assert!(serde_json::to_string(&edges()).is_ok());
    let read: Plain = serde_json::from_str(&text(&edges())).unwrap();
    assert_eq!(read.text, edges().text);
}

// endregion reading

// region prost-reflect

fn pool() -> DescriptorPool {
    DescriptorPool::decode(SET).expect("the descriptor set")
}

/// `message`, written by the generated code, read by prost-reflect: the
/// same message.
fn reflect_reads<T: JsonMessage + Message + Name + Default + PartialEq + std::fmt::Debug>(
    message: &T,
) {
    let descriptor = pool().get_message_by_name(&T::full_name()).unwrap();
    let written = text(message);
    let mut de = serde_json::Deserializer::from_str(&written);
    let dynamic = DynamicMessage::deserialize(descriptor, &mut de).expect("prost-reflect reads it");
    let back = T::decode(dynamic.encode_to_vec().as_slice()).unwrap();
    assert_eq!(&back, message);
}

/// `message`, written by prost-reflect, read by the generated code.
fn reads_reflect<T: JsonMessage + Message + Name + Default + PartialEq + std::fmt::Debug>(
    message: &T,
) {
    let descriptor = pool().get_message_by_name(&T::full_name()).unwrap();
    let dynamic = DynamicMessage::decode(descriptor, message.encode_to_vec().as_slice()).unwrap();
    let written = serde_json::to_string(&dynamic).unwrap();
    assert_eq!(read::<T>(&written).as_ref(), Ok(message), "{written}");
}

fn finite(mut plain: Plain) -> Plain {
    // prost-reflect refuses f32::MAX's shortest text, and NaN never equals.
    plain.f64 = 0.25;
    plain.f32 = 0.5;
    plain.ratios.retain(|r| r.is_finite());
    plain
}

#[test]
fn prost_reflect_reads_the_canonical_json_and_the_generated_code_reads_its() {
    let plain = finite(edges());
    reflect_reads(&plain);
    reads_reflect(&plain);
    // angzarr's UUID is its hyphenated text here, its own JSON form
    // (identity-and-streams.md); prost-reflect knows it as a message of
    // bytes. Every other field is canonical.
    let known = Known {
        payloads: Vec::new(),
        id: None,
        ids: Vec::new(),
        ..known()
    };
    reflect_reads(&known);
    reads_reflect(&known);
    for pick in [
        choice::Pick::Word("w".to_string()),
        choice::Pick::Plain(Plain::default()),
    ] {
        let choice = Choice { pick: Some(pick) };
        reflect_reads(&choice);
        reads_reflect(&choice);
    }
}

// endregion prost-reflect

// region round trips

fn leaf() -> impl Strategy<Value = Plain> {
    (
        (any::<f64>(), any::<f32>(), any::<i32>(), any::<i64>()),
        (any::<u32>(), any::<u64>(), any::<bool>(), ".*"),
        (proptest::collection::vec(any::<u8>(), 0..8), any::<i32>()),
        (
            proptest::option::of(any::<i64>()),
            proptest::option::of(".*"),
        ),
        (
            proptest::collection::vec(any::<i64>(), 0..4),
            proptest::collection::vec(any::<f64>(), 0..4),
        ),
        (
            proptest::collection::btree_map(".*", any::<i64>(), 0..4),
            proptest::collection::btree_map(".*", any::<i32>(), 0..4),
            proptest::collection::btree_map(any::<i64>(), ".*", 0..4),
            proptest::collection::btree_map(any::<bool>(), any::<f64>(), 0..2),
        ),
    )
        .prop_map(
            |(
                (f64, f32, i32, i64),
                (u32, u64, flag, text),
                (data, tone),
                (maybe, note),
                (counts, ratios),
                (totals, moods, names, switches),
            )| Plain {
                f64,
                f32,
                i32,
                i64,
                u32,
                u64,
                s32: i32,
                s64: i64,
                x32: u32,
                x64: u64,
                sx32: i32,
                sx64: i64,
                flag,
                text: text.clone(),
                data: data.clone(),
                tone,
                maybe,
                note,
                counts,
                ratios,
                tones: vec![tone],
                blobs: vec![data],
                totals,
                moods,
                names,
                switches,
                snake_case_field: text,
                ..Plain::default()
            },
        )
}

fn plain() -> impl Strategy<Value = Plain> {
    (
        leaf(),
        proptest::option::of(leaf()),
        proptest::collection::vec(leaf(), 0..2),
    )
        .prop_map(|(mut plain, child, children)| {
            plain.child = child.map(Box::new);
            plain.children = children;
            plain
        })
}

fn timestamp() -> impl Strategy<Value = prost_types::Timestamp> {
    // The proto3 range: 0001-01-01T00:00:00Z to 9999-12-31T23:59:59Z.
    (-62_135_596_800_i64..=253_402_300_799, 0..1_000_000_000_i32)
        .prop_map(|(seconds, nanos)| prost_types::Timestamp { seconds, nanos })
}

fn duration() -> impl Strategy<Value = prost_types::Duration> {
    (-315_576_000_000_i64..=315_576_000_000, 0..1_000_000_000_i32).prop_map(|(seconds, nanos)| {
        // A negative duration's nanos are not positive.
        let nanos = if seconds < 0 { -nanos } else { nanos };
        prost_types::Duration { seconds, nanos }
    })
}

/// A NaN compares unequal to itself: compared through its bits.
fn same(a: &Plain, b: &Plain) -> bool {
    a.encode_to_vec() == b.encode_to_vec()
}

proptest! {
    #[test]
    fn plain_round_trips(message in plain()) {
        let back = read::<Plain>(&text(&message)).unwrap();
        prop_assert!(same(&back, &message), "{:?}\n{:?}", back, message);
    }

    #[test]
    fn known_round_trips(
        at in proptest::option::of(timestamp()),
        took in proptest::option::of(duration()),
        id in proptest::option::of(any::<[u8; 16]>()),
        payload in proptest::option::of(leaf()),
    ) {
        let message = Known {
            at,
            took,
            id: id.map(angzarr_proto::v1::Uuid::from_bytes),
            payload: payload.map(|p| prost_types::Any::from_msg(&p).unwrap()),
            payloads: Vec::new(),
            ids: Vec::new(),
        };
        let back = read::<Known>(&text(&message)).unwrap();
        prop_assert_eq!(back.encode_to_vec(), message.encode_to_vec());
    }

    #[test]
    fn finished_round_trips(
        counts in proptest::collection::btree_map(".*", any::<u32>(), 0..4),
        took_ms in any::<u64>(),
        exit in any::<i32>(),
        status in 0..5_i32,
        why in proptest::option::of(".*"),
        site in proptest::option::of((".*", any::<u32>())),
        tests in proptest::option::of(any::<u32>()),
    ) {
        let message = Finished {
            counts,
            took_ms,
            exit,
            status,
            why,
            // A flattened site of defaults writes as no site.
            site: site
                .map(|(file, end_line)| Site { file, end_line })
                .filter(|s| s != &Site::default()),
            tests,
        };
        prop_assert_eq!(read::<Finished>(&text(&message)), Ok(message));
    }
}

// endregion round trips
