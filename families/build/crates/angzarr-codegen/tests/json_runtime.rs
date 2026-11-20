//! The runtime every generated JSON file opens with (`src/json/runtime.rs`),
//! compiled here as a wire crate compiles it: the well-known types' text at
//! the proto3 range's edges, base64, integers, enum names, and an object's
//! keys taken once.

#[path = "../src/json/runtime.rs"]
mod runtime;

use runtime::*;

fn ts(seconds: i64, nanos: i32) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos }
}

fn text(json: Result<Json, JsonError>) -> String {
    match json.unwrap() {
        Json::String(s) => s,
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_a_timestamp_is_rfc_3339_in_utc_with_z_and_reads_back() {
    for (seconds, nanos, expected) in [
        (0, 0, "1970-01-01T00:00:00Z"),
        (-62_135_596_800, 0, "0001-01-01T00:00:00Z"),
        (
            253_402_300_799,
            999_999_999,
            "9999-12-31T23:59:59.999999999Z",
        ),
        (951_782_400, 10_000_000, "2000-02-29T00:00:00.010Z"),
        (-1, 1_000, "1969-12-31T23:59:59.000001Z"),
    ] {
        assert_eq!(text(timestamp(&ts(seconds, nanos), "t")), expected);
        assert_eq!(
            read_timestamp(&Json::String(expected.to_string()), "t"),
            Ok(ts(seconds, nanos))
        );
    }
}

#[test]
fn test_a_timestamp_reads_an_offset_and_refuses_what_is_not_one() {
    assert_eq!(
        read_timestamp(&Json::String("1970-01-01T01:30:00+01:30".to_string()), "t"),
        Ok(ts(0, 0))
    );
    assert_eq!(
        read_timestamp(&Json::String("1969-12-31T23:00:00-01:00".to_string()), "t"),
        Ok(ts(0, 0))
    );
    for bad in [
        "1970-01-01 00:00:00Z",
        "1970-02-30T00:00:00Z",
        "1970-01-01T24:00:00Z",
        "1970-01-01T00:00:00",
        "1970-01-01T00:00:00.1234567890Z",
        "10000-01-01T00:00:00Z",
    ] {
        assert!(
            read_timestamp(&Json::String(bad.to_string()), "t").is_err(),
            "{bad}"
        );
    }
    assert!(timestamp(&ts(253_402_300_800, 0), "t").is_err());
    assert!(timestamp(&ts(0, -1), "t").is_err());
}

#[test]
fn test_a_duration_is_its_seconds_and_reads_back() {
    for (seconds, nanos, expected) in [
        (0, 0, "0s"),
        (1, 500_000_000, "1.500s"),
        (-1, -500_000, "-1.000500s"),
        (0, -1, "-0.000000001s"),
        (315_576_000_000, 0, "315576000000s"),
    ] {
        let d = prost_types::Duration { seconds, nanos };
        assert_eq!(text(duration(&d, "d")), expected);
        assert_eq!(
            read_duration(&Json::String(expected.to_string()), "d"),
            Ok(d)
        );
    }
    for bad in ["1", "s", "1.s", "--1s", "315576000001s"] {
        assert!(
            read_duration(&Json::String(bad.to_string()), "d").is_err(),
            "{bad}"
        );
    }
    assert!(
        duration(
            &prost_types::Duration {
                seconds: 1,
                nanos: -1
            },
            "d"
        )
        .is_err()
    );
}

#[test]
fn test_bytes_are_padded_base64_and_read_either_alphabet() {
    for (data, expected) in [
        (&b""[..], ""),
        (b"f", "Zg=="),
        (b"fo", "Zm8="),
        (b"foo", "Zm9v"),
        (&[0xfb, 0xff][..], "+/8="),
    ] {
        assert_eq!(text(Ok(bytes(data))), expected);
        assert_eq!(
            read_bytes(&Json::String(expected.to_string()), "b"),
            Ok(data.to_vec())
        );
    }
    assert_eq!(
        read_bytes(&Json::String("-_8".to_string()), "b"),
        Ok(vec![0xfb, 0xff])
    );
    for bad in ["Zg===", "Z", "Zh==", "!!!!"] {
        assert!(
            read_bytes(&Json::String(bad.to_string()), "b").is_err(),
            "{bad}"
        );
    }
}

#[test]
fn test_an_integer_reads_from_a_number_or_its_text_in_range() {
    assert_eq!(read_int::<i64>(&long(i64::MIN), "i"), Ok(i64::MIN));
    assert_eq!(read_int::<u64>(&long(u64::MAX), "i"), Ok(u64::MAX));
    assert_eq!(
        read_int::<i32>(&Json::Number(Number::F64(1e2)), "i"),
        Ok(100)
    );
    assert_eq!(
        read_int::<i32>(&Json::String("1e2".to_string()), "i"),
        Ok(100)
    );
    assert!(read_int::<i32>(&Json::Number(Number::F64(1.5)), "i").is_err());
    assert!(read_int::<u32>(&int(-1), "i").is_err());
    assert!(read_int::<i32>(&long(i64::MAX), "i").is_err());
}

#[test]
fn test_a_double_is_a_number_or_the_name_of_nan_or_an_infinity() {
    assert_eq!(double(f64::INFINITY), Json::String("Infinity".to_string()));
    assert_eq!(
        double(f64::NEG_INFINITY),
        Json::String("-Infinity".to_string())
    );
    assert_eq!(double(f64::NAN), Json::String("NaN".to_string()));
    assert_eq!(float(f32::NAN), Json::String("NaN".to_string()));
    assert!(
        read_double(&Json::String("NaN".to_string()), "d")
            .unwrap()
            .is_nan()
    );
    assert_eq!(
        read_double(&Json::String("-Infinity".to_string()), "d"),
        Ok(f64::NEG_INFINITY)
    );
    assert_eq!(read_float(&Json::Number(Number::F64(0.5)), "f"), Ok(0.5));
}

#[test]
fn test_an_enum_writes_its_name_or_an_unknown_number_and_reads_either() {
    let names = EnumNames {
        values: &[(1, "soft", &["soft", "TONE_SOFT"])],
    };
    assert_eq!(names.write(1), Json::String("soft".to_string()));
    assert_eq!(names.write(9), int(9));
    assert_eq!(
        names.read(&Json::String("TONE_SOFT".to_string()), "e"),
        Ok(1)
    );
    assert_eq!(names.read(&int(9), "e"), Ok(9));
    assert!(matches!(
        names.read(&Json::String("loud".to_string()), "e"),
        Err(JsonError::UnknownEnum { .. })
    ));
}

#[test]
fn test_a_map_writes_its_keys_sorted_as_text() {
    let entries: std::collections::BTreeMap<i64, i64> = [(10, 1), (-1, 2), (2, 3)].into();
    let Json::Object(written) = map(&entries, |v| Ok(long(*v))).unwrap() else {
        panic!("an object")
    };
    let keys: Vec<&str> = written.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["-1", "10", "2"]);
}

#[test]
fn test_an_objects_keys_are_taken_once_and_leftovers_refused() {
    let object = Json::Object(vec![
        ("a".to_string(), int(1)),
        ("b".to_string(), Json::Null),
        ("c".to_string(), int(3)),
    ]);
    let mut fields = Fields::of(&object, "m").unwrap();
    assert_eq!(fields.take(&["a", "alias"]), Ok(Some(&int(1))));
    assert_eq!(fields.take(&["a"]), Ok(None));
    assert_eq!(fields.take(&["b"]), Ok(None), "null reads as absent");
    assert_eq!(fields.taken(), 2);
    assert!(matches!(
        fields.finish(),
        Err(JsonError::UnknownField { key, .. }) if key == "c"
    ));
    let both = Json::Object(vec![
        ("a".to_string(), int(1)),
        ("alias".to_string(), int(2)),
    ]);
    assert!(matches!(
        Fields::of(&both, "m").unwrap().take(&["a", "alias"]),
        Err(JsonError::Duplicate { .. })
    ));
    let twice = Json::Object(vec![("a".to_string(), int(1)), ("a".to_string(), int(2))]);
    assert!(matches!(
        Fields::of(&twice, "m"),
        Err(JsonError::Duplicate { .. })
    ));
}

#[test]
fn test_a_uuid_is_its_hyphenated_text_and_a_malformed_one_is_unwritable() {
    let id = angzarr_proto::v1::Uuid::from_bytes([0xab; 16]);
    assert_eq!(text(uuid(&id, "u")), "abababab-abab-abab-abab-abababababab");
    assert_eq!(
        read_uuid(
            &Json::String("ABABABAB-ABAB-ABAB-ABAB-ABABABABABAB".to_string()),
            "u"
        ),
        Ok(id)
    );
    let short = angzarr_proto::v1::Uuid {
        value: vec![1, 2].into(),
    };
    assert!(matches!(
        uuid(&short, "u"),
        Err(JsonError::Unwritable { .. })
    ));
}

/// An Any of a well-known type holds its JSON under `value`, and one of a
/// type the registry lacks is refused both ways.
#[test]
fn test_an_any_is_expanded_through_the_registry() {
    use prost::Message;
    let registry = Registry::new();
    let at = ts(1, 0);
    let any = prost_types::Any {
        type_url: "type.googleapis.com/google.protobuf.Timestamp".to_string(),
        value: at.encode_to_vec(),
    };
    let json = any.to_json(&registry).unwrap();
    assert_eq!(
        json,
        Json::Object(vec![
            (
                "@type".to_string(),
                string("type.googleapis.com/google.protobuf.Timestamp")
            ),
            ("value".to_string(), string("1970-01-01T00:00:01Z")),
        ])
    );
    assert_eq!(prost_types::Any::from_json(&json, &registry), Ok(any));
    let other = prost_types::Any {
        type_url: "type.googleapis.com/other.v1.Thing".to_string(),
        value: Vec::new(),
    };
    assert!(matches!(
        other.to_json(&registry),
        Err(JsonError::UnknownType { .. })
    ));
    let unknown = Json::Object(vec![("@type".to_string(), string("x/other.v1.Thing"))]);
    assert!(matches!(
        prost_types::Any::from_json(&unknown, &registry),
        Err(JsonError::UnknownType { .. })
    ));
}

/// The value helpers generated code calls: lists and maps of each value,
/// booleans, strings, unsigned integers, a field's default and a oneof's
/// local.
#[test]
fn test_the_value_helpers_read_what_they_write() {
    let values = [1_u32, 2];
    let written = list(&values, |v| Ok(uint(*v))).unwrap();
    assert_eq!(
        read_list(&written, "l", |v| read_int::<u32>(v, "l")),
        Ok(values.to_vec())
    );
    let entries: std::collections::BTreeMap<bool, bool> = [(true, false)].into();
    let written = map(&entries, |v| Ok(boolean(*v))).unwrap();
    assert_eq!(read_map(&written, "m", |v| read_bool(v, "m")), Ok(entries));
    assert_eq!(read_string(&string("s"), "s"), Ok("s".to_string()));
    assert!(read_bool(&string("s"), "b").is_err());
    let boxed: Box<i32> = own(3);
    assert_eq!(*boxed, 3);
    assert!(is_default(&0_i64) && !is_default(&"x".to_string()));
    assert!(is_set(&Some(1)) && !is_set(&None::<i32>));
    let object = Json::Object(Vec::new());
    assert_eq!(Fields::of(&object, "m").unwrap().at("k"), "m.k");
}

/// The serde entry points: a message through a registry, and through the
/// well-known types alone, written and read with serde_json.
#[test]
fn test_serde_writes_and_reads_through_the_registry() {
    let at = ts(0, 0);
    let written = serde_json::to_string(&Registered(&at, &Registry::new())).unwrap();
    assert_eq!(written, r#""1970-01-01T00:00:00Z""#);
    let mut out = serde_json::Serializer::new(Vec::new());
    serialize(&at, &mut out).unwrap();
    assert_eq!(out.into_inner(), written.as_bytes());
    let mut input = serde_json::Deserializer::from_str(&written);
    assert_eq!(
        deserialize::<prost_types::Timestamp, _>(&mut input).unwrap(),
        at
    );
    let json: Json = serde_json::from_str(r#"{"a":[1,-2,0.5,"s",true,null]}"#).unwrap();
    assert_eq!(
        serde_json::to_string(&json).unwrap(),
        r#"{"a":[1,-2,0.5,"s",true,null]}"#
    );
}
