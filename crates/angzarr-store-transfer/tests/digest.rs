//! The re-fold digest's lines (the store export/import plan, section
//! 5.3.2, and D1t's half that writes them): each build folds its own
//! store and writes one line per stream, `{cover, head, state}`, the state
//! in proto3 JSON, so two builds' digests compare byte for byte.

use angzarr_ports::angzarr_proto::v1::{Cover, FactOrigin};
use angzarr_ports::{StreamRef, Uuid};
use angzarr_store_transfer::{descriptor_pool, write_digest, Digested, ExportError};
use prost::Message;
use prost_types::Any;

fn cover(n: u8) -> Cover {
    StreamRef::main("tasks", Uuid::from_bytes([n; 16])).to_cover()
}

/// A state the pool resolves: angzarr's `FactOrigin`, standing in for an
/// app's state.
fn state(id: &str) -> Any {
    Any {
        type_url: "/io.angzarr.v1.FactOrigin".to_string(),
        value: FactOrigin {
            external_id: id.to_string(),
            description: String::new(),
        }
        .encode_to_vec(),
    }
}

fn digest(streams: &[Digested]) -> Result<Vec<u8>, ExportError> {
    let pool = descriptor_pool(&[]).unwrap();
    let mut out = Vec::new();
    write_digest(streams, &pool, &mut out)?;
    Ok(out)
}

#[test]
fn a_digest_is_one_line_per_stream_its_state_expanded_and_deterministic() {
    let streams = [
        Digested {
            cover: cover(1),
            head: 3,
            state: state("one"),
        },
        Digested {
            cover: cover(2),
            head: 0,
            state: state("two"),
        },
    ];
    let first = digest(&streams).unwrap();
    assert_eq!(digest(&streams).unwrap(), first, "the same bytes again");
    let lines: Vec<serde_json::Value> = std::str::from_utf8(&first)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2, "one line per stream");
    assert_eq!(lines[0]["cover"]["domain"], "tasks");
    assert_eq!(lines[0]["head"], "3");
    assert_eq!(lines[1]["head"], "0");
    assert_eq!(
        lines[0]["state"],
        serde_json::json!({"@type": "/io.angzarr.v1.FactOrigin", "externalId": "one"})
    );
    let keys: Vec<&String> = lines[1].as_object().unwrap().keys().collect();
    assert_eq!(keys, ["cover", "head", "state"]);
}

/// A state of a type the descriptors do not name is refused, naming the
/// stream: a digest that cannot show a state compares nothing.
#[test]
fn a_digest_refuses_a_state_the_pool_cannot_resolve() {
    let streams = [Digested {
        cover: cover(1),
        head: 1,
        state: Any {
            type_url: "/nowhere.v1.State".to_string(),
            value: Vec::new(),
        },
    }];
    match digest(&streams) {
        Err(ExportError::UnresolvedType { row, type_url }) => {
            assert_eq!(row.kind, "state");
            assert_eq!(row.stream, format!("tasks::{}", Uuid::from_bytes([1; 16])));
            assert_eq!(type_url, "/nowhere.v1.State");
        }
        other => panic!("an unresolvable state refused: {other:?}"),
    }
}
