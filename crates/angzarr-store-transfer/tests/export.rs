//! The export half's red tests (the store export/import plan, section 7,
//! E1 to E8), on angzarr-ports' MemoryStore and wrappers around it.

use std::io::Write as _;

use angzarr_ports::angzarr_proto::v1::event_meta::Causation;
use angzarr_ports::angzarr_proto::v1::{
    AngzarrDeadLetter, Diagnostic, EventMeta, EventPage, FactOrigin, Snapshot,
};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{
    AppliedMigration, DeadLetter, DeadLetterAt, EventStore, Export, ExportRow, Migration,
    Migrations, OutboxEntry, PositionKey, SnapshotStore, StoreError, StreamRef, Uuid, Watch, Write,
};
use angzarr_store_transfer::{descriptor_pool, export, ExportError, GENERATION};
use prost::Message;
use prost_types::{Any, Timestamp};
use serde_json::Value;

fn uuid(n: u8) -> Uuid {
    Uuid::from_bytes([n; 16])
}

fn stream() -> StreamRef<'static> {
    StreamRef::main("tasks", uuid(1))
}

/// A payload the pool resolves: angzarr's own `FactOrigin`, standing in
/// for an app's event.
fn origin(id: &str) -> Any {
    Any {
        type_url: "/io.angzarr.v1.FactOrigin".to_string(),
        value: FactOrigin {
            external_id: id.to_string(),
            description: "test".to_string(),
        }
        .encode_to_vec(),
    }
}

fn page_of(sequence: u64, payload: Any) -> EventPage {
    EventPage {
        payload: Some(payload),
        meta: Some(EventMeta {
            causation: Some(Causation::CommandId(uuid(0xC0 + sequence as u8).into())),
            correlation_id: Some(uuid(0xCC).into()),
            recorded_at: Some(Timestamp {
                seconds: 1_761_930_950 + sequence as i64,
                nanos: 139_429_316,
            }),
            rules_version: "markup-1".to_string(),
            ..EventMeta::default()
        }),
        sequence,
    }
}

fn page(sequence: u64) -> EventPage {
    page_of(sequence, origin(&format!("event-{sequence}")))
}

fn letter() -> DeadLetter {
    DeadLetter {
        stream: stream().to_cover(),
        at: DeadLetterAt::Sequence(1),
        letter: AngzarrDeadLetter {
            reason: Some(Diagnostic {
                code: "ROUTER_REASON_UNHANDLED_HANDLER_ERROR".to_string(),
                args: [("why".to_string(), "test".to_string())].into(),
            }),
            source_component: "projector".to_string(),
            occurred_at: Some(Timestamp {
                seconds: 7,
                nanos: 0,
            }),
            ..AngzarrDeadLetter::default()
        },
    }
}

/// A store of every kind of row an export writes: two pages, a snapshot,
/// a position, a dead letter.
async fn store() -> MemoryStore {
    let store = MemoryStore::new();
    let pages = [page(0), page(1)];
    let positions = [(
        PositionKey {
            handler: "projector",
            stream: stream(),
        },
        2,
    )];
    let letters = [letter()];
    store
        .write(Write {
            positions: &positions,
            dead_letters: &letters,
            ..Write::append(stream(), 0, &pages)
        })
        .await
        .unwrap();
    let snapshot = Snapshot {
        sequence: 1,
        state: Some(origin("state")),
        rules_version: "markup-1".to_string(),
    };
    store.write_snapshot(stream(), &snapshot).await.unwrap();
    store
}

/// Export `store` through angzarr's descriptors: the file, or the refusal.
async fn exported<S: Export + Migrations>(store: &S) -> Result<Vec<u8>, ExportError> {
    let pool = descriptor_pool(&[]).unwrap();
    let mut out = Vec::new();
    let result = export(store, &pool, &mut out).await;
    out.flush().unwrap();
    match result {
        Ok(_) => Ok(out),
        Err(e) => {
            assert!(out.is_empty(), "a refused export writes nothing: {e}");
            Err(e)
        }
    }
}

fn lines(file: &[u8]) -> Vec<Value> {
    std::str::from_utf8(file)
        .expect("UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("each line is JSON"))
        .collect()
}

/// The one key of a record: its kind.
fn kind(record: &Value) -> &str {
    let object = record.as_object().expect("a record is an object");
    assert_eq!(object.len(), 1, "a record has one key: {record}");
    object.keys().next().unwrap()
}

/// E1: a header naming the build's generation, the records, the end.
#[tokio::test]
async fn export_writes_header_records_then_end() {
    let file = exported(&store().await).await.unwrap();
    let records = lines(&file);
    let kinds: Vec<&str> = records.iter().map(kind).collect();
    assert_eq!(
        kinds,
        [
            "header",
            "page",
            "page",
            "snapshot",
            "position",
            "deadLetter",
            "end"
        ]
    );
    let header = &records[0]["header"];
    assert_eq!(header["format"], "angzarr-store-jsonl");
    assert_eq!(header["formatVersion"], 1);
    assert_eq!(header["generation"], GENERATION);
    assert_eq!(
        GENERATION, "20261019",
        "this build's protos: the draft before the rewrite"
    );
    assert_eq!(header["store"], "angzarr-ports", "MemoryStore's package");
    assert_eq!(header["ledger"], serde_json::json!([]));

    let first = &records[1]["page"];
    assert_eq!(first["cover"]["domain"], "tasks");
    assert_eq!(
        first["page"]["payload"]["@type"], "/io.angzarr.v1.FactOrigin",
        "the payload expanded"
    );
    assert_eq!(first["page"]["payload"]["externalId"], "event-0");
    assert_eq!(first["page"]["meta"]["rulesVersion"], "markup-1");
    assert_eq!(
        first["page"]["meta"]["recordedAt"], "2025-10-31T17:15:50.139429316Z",
        "a Timestamp in RFC 3339"
    );
    assert!(
        first["page"].get("sequence").is_none(),
        "sequence 0, the default, omitted"
    );
    assert_eq!(
        records[2]["page"]["page"]["sequence"], "1",
        "a uint64 as a string"
    );
    assert_eq!(
        records[3]["snapshot"]["snapshot"]["state"]["externalId"],
        "state"
    );
    assert_eq!(records[4]["position"]["handler"], "projector");
    assert_eq!(records[4]["position"]["next"], "2");
    assert_eq!(
        records[5]["deadLetter"]["at"],
        serde_json::json!({"sequence": "1"})
    );
    assert_eq!(
        records[5]["deadLetter"]["letter"]["sourceComponent"],
        "projector"
    );

    let end = &records[6]["end"];
    assert_eq!(end["records"], "5");
    assert_eq!(
        end["streams"],
        serde_json::json!([{
            "cover": records[1]["page"]["cover"],
            "pages": "2",
            "head": "2",
            "snapshot": "1",
            "positions": "1",
            "deadLetters": "1",
        }])
    );
}

/// E2: one store exports to the same bytes every time.
#[tokio::test]
async fn two_exports_of_one_store_are_byte_identical() {
    let store = store().await;
    let first = exported(&store).await.unwrap();
    let second = exported(&store).await.unwrap();
    assert_eq!(first, second);
}

/// E3: an export writes nothing to the store: its change token stands,
/// and it exports the same after.
#[tokio::test]
async fn export_never_writes_the_source() {
    let store = store().await;
    let token = store.change_token().await.unwrap();
    let rows = store.export().await.unwrap();
    exported(&store).await.unwrap();
    assert_eq!(
        store.change_token().await.unwrap(),
        token,
        "nothing committed"
    );
    assert_eq!(
        store.export().await.unwrap(),
        rows,
        "and every row as it was"
    );
}

/// A store whose export plants a field this build does not know, number
/// 99, in its first page's stored bytes, or in the first page's payload.
#[derive(Clone)]
struct Planted {
    inner: MemoryStore,
    in_payload: bool,
}

/// Field 99, a varint 1: a field no message here declares.
const UNKNOWN_FIELD: [u8; 3] = [0x98, 0x06, 0x01];

impl Export for Planted {
    fn backend(&self) -> &str {
        "planted"
    }

    async fn export(&self) -> Result<Vec<ExportRow>, StoreError> {
        let mut rows = self.inner.export().await?;
        if let Some(ExportRow::Page { stored, .. }) = rows.first_mut() {
            if self.in_payload {
                let mut page = EventPage::decode(stored.as_slice()).unwrap();
                let payload = page.payload.as_mut().unwrap();
                payload.value.extend_from_slice(&UNKNOWN_FIELD);
                *stored = page.encode_to_vec();
            } else {
                stored.extend_from_slice(&UNKNOWN_FIELD);
            }
        }
        Ok(rows)
    }
}

/// A store's ledger: none for a store in memory, as `MemoryStore` has.
macro_rules! no_ledger {
    ($store:ty) => {
        impl Migrations for $store {
            fn migrations(&self) -> Vec<Migration> {
                Vec::new()
            }
            async fn applied(&self) -> Result<Vec<AppliedMigration>, StoreError> {
                Ok(Vec::new())
            }
            async fn run_to(&self, target: u64) -> Result<Vec<AppliedMigration>, StoreError> {
                self.inner.run_to(target).await
            }
            async fn undo_to(&self, target: u64) -> Result<Vec<AppliedMigration>, StoreError> {
                self.inner.undo_to(target).await
            }
        }
    };
}

no_ledger!(Planted);

/// E4: a page carrying a field this build does not know is refused,
/// naming its stream and sequence: re-encoded, it would lose the field.
#[tokio::test]
async fn export_refuses_a_page_whose_reencoding_differs() {
    let planted = Planted {
        inner: store().await,
        in_payload: false,
    };
    match exported(&planted).await {
        Err(ExportError::ReencodingDiffers { row, message }) => {
            assert_eq!(row.kind, "page");
            assert_eq!(row.stream, format!("tasks::{}", uuid(1)));
            assert_eq!(row.place, "0");
            assert_eq!(message, "io.angzarr.v1.EventPage");
        }
        other => panic!("a planted field refused: {other:?}"),
    }
}

/// E5: a payload of a type the descriptors do not name is refused.
#[tokio::test]
async fn export_refuses_a_payload_type_the_pool_cannot_resolve() {
    let store = MemoryStore::new();
    let unknown = Any {
        type_url: "/nowhere.v1.Unknown".to_string(),
        value: vec![0x08, 0x01],
    };
    store
        .write(Write::append(stream(), 0, &[page_of(0, unknown)]))
        .await
        .unwrap();
    match exported(&store).await {
        Err(ExportError::UnresolvedType { row, type_url }) => {
            assert_eq!(row.kind, "page");
            assert_eq!(row.place, "0");
            assert_eq!(type_url, "/nowhere.v1.Unknown");
        }
        other => panic!("an unresolvable type refused: {other:?}"),
    }
}

/// E6: a payload holding a field its type does not declare is refused,
/// though the page around it re-encodes to its bytes.
#[tokio::test]
async fn export_refuses_unknown_fields_in_a_payload() {
    let planted = Planted {
        inner: store().await,
        in_payload: true,
    };
    match exported(&planted).await {
        Err(ExportError::UnknownFields { row, message, path }) => {
            assert_eq!(row.kind, "page");
            assert_eq!(row.place, "0");
            assert_eq!(message, "io.angzarr.v1.FactOrigin");
            assert_eq!(path, "payload");
        }
        other => panic!("an unknown field in a payload refused: {other:?}"),
    }
}

/// A store whose build and ledger a test chooses.
struct Ledgered {
    inner: MemoryStore,
    build: Vec<u64>,
    ledger: Vec<u64>,
}

fn migration(version: u64) -> Migration {
    Migration {
        version,
        description: format!("m{version}"),
        checksum: vec![version as u8],
        reversible: false,
    }
}

impl Export for Ledgered {
    fn backend(&self) -> &str {
        self.inner.backend()
    }

    async fn export(&self) -> Result<Vec<ExportRow>, StoreError> {
        self.inner.export().await
    }
}

impl Migrations for Ledgered {
    fn migrations(&self) -> Vec<Migration> {
        self.build.iter().copied().map(migration).collect()
    }

    async fn applied(&self) -> Result<Vec<AppliedMigration>, StoreError> {
        Ok(self
            .ledger
            .iter()
            .copied()
            .map(|version| AppliedMigration {
                version,
                description: format!("m{version}"),
                checksum: vec![version as u8],
                success: true,
                installed_on: Timestamp::default(),
                execution_time: std::time::Duration::ZERO,
            })
            .collect())
    }

    async fn run_to(&self, _: u64) -> Result<Vec<AppliedMigration>, StoreError> {
        Ok(Vec::new())
    }

    async fn undo_to(&self, _: u64) -> Result<Vec<AppliedMigration>, StoreError> {
        Ok(Vec::new())
    }
}

/// E7: a store whose ledger is older or newer than the build's set is
/// refused; one exactly the build's exports, its versions in the header.
#[tokio::test]
async fn export_refuses_a_store_whose_ledger_is_not_the_builds() {
    for (ledger, why) in [(vec![1, 2], "older"), (vec![1, 2, 3, 4], "newer")] {
        let store = Ledgered {
            inner: store().await,
            build: vec![1, 2, 3],
            ledger,
        };
        match exported(&store).await {
            Err(ExportError::LedgerNotTheBuilds { applied, build }) => {
                assert_eq!(build, "1,2,3", "{why}");
                assert!(applied != build, "{why}");
            }
            other => panic!("a store {why} than the build refused: {other:?}"),
        }
    }
    let current = Ledgered {
        inner: store().await,
        build: vec![1, 2, 3],
        ledger: vec![1, 2, 3],
    };
    let file = exported(&current).await.unwrap();
    assert_eq!(
        lines(&file)[0]["header"]["ledger"],
        serde_json::json!([1, 2, 3])
    );
}

/// E8: a store with pending outbox entries is refused (D5 (a)): deliver
/// them first.
#[tokio::test]
async fn export_refuses_a_pending_outbox() {
    let store = store().await;
    let entry = OutboxEntry {
        id: uuid(9),
        origin: "policy".to_string(),
        message: origin("command"),
        edition: None,
    };
    store
        .write(Write {
            outbox: &[entry],
            ..Write::default()
        })
        .await
        .unwrap();
    assert_eq!(
        exported(&store).await,
        Err(ExportError::PendingOutbox { entries: 1 })
    );
}

/// A ledger row that is not the build's own, though its version is: a
/// migration changed since it was applied, or one that did not finish.
#[test]
fn check_ledger_refuses_a_changed_or_failed_migration() {
    use angzarr_store_transfer::check_ledger;
    let build: Vec<Migration> = [1, 2].into_iter().map(migration).collect();
    let row = |version: u64| AppliedMigration {
        version,
        description: format!("m{version}"),
        checksum: vec![version as u8],
        success: true,
        installed_on: Timestamp::default(),
        execution_time: std::time::Duration::ZERO,
    };
    assert_eq!(check_ledger(&build, &[row(1), row(2)]), Ok(vec![1, 2]));
    let changed = AppliedMigration {
        checksum: vec![0xFF],
        ..row(2)
    };
    let failed = AppliedMigration {
        success: false,
        ..row(2)
    };
    for (ledger, why) in [(changed, "changed"), (failed, "failed")] {
        assert_eq!(
            check_ledger(&build, &[row(1), ledger]),
            Err(ExportError::LedgerNotTheBuilds {
                applied: "1,2".to_string(),
                build: "1,2".to_string(),
            }),
            "a migration {why}"
        );
    }
    let renumbered = AppliedMigration {
        version: 3,
        ..row(2)
    };
    assert!(check_ledger(&build, &[row(1), renumbered]).is_err());
}

/// Every line reads back as the record it is, the format's own types
/// refusing nothing they wrote; the last is the end the export answered.
#[tokio::test]
async fn every_line_reads_back_as_its_record() {
    use angzarr_store_transfer::record::{Count, Record};
    let store = store().await;
    let pool = descriptor_pool(&[]).unwrap();
    let mut out = Vec::new();
    let end = export(&store, &pool, &mut out).await.unwrap();
    let records: Vec<Record> = std::str::from_utf8(&out)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.last(), Some(&Record::End(end.clone())));
    assert_eq!(end.records, Count(5));
    assert_eq!(end.streams[0].head, Count(2));
    for line in std::str::from_utf8(&out).unwrap().lines() {
        let record: Record = serde_json::from_str(line).unwrap();
        assert_eq!(
            serde_json::to_string(&record).unwrap(),
            line,
            "written again, the same line"
        );
    }
    assert!(
        serde_json::from_str::<Count>("\"x\"").is_err(),
        "a count is a decimal string"
    );
}

/// A dead letter refused names where on its stream it is: a command's id.
#[tokio::test]
async fn a_refused_dead_letter_is_named_by_its_place() {
    let store = MemoryStore::new();
    let mut letter = letter();
    letter.at = DeadLetterAt::Command(uuid(7));
    letter.letter.reason = None;
    letter.letter.rejection_details = None;
    letter.letter.payload = Some(
        angzarr_ports::angzarr_proto::v1::angzarr_dead_letter::Payload::RejectedEvents(
            angzarr_ports::angzarr_proto::v1::EventBook {
                cover: Some(stream().to_cover()),
                pages: vec![page_of(
                    0,
                    Any {
                        type_url: "/nowhere.v1.Gone".to_string(),
                        value: Vec::new(),
                    },
                )],
                ..Default::default()
            },
        ),
    );
    store
        .write(Write {
            dead_letters: &[letter],
            ..Write::default()
        })
        .await
        .unwrap();
    match exported(&store).await {
        Err(ExportError::UnresolvedType { row, type_url }) => {
            assert_eq!(row.kind, "dead-letter");
            assert_eq!(row.place, uuid(7).to_string());
            assert_eq!(type_url, "/nowhere.v1.Gone", "a page in a list, checked");
        }
        other => panic!("a dead letter's unresolvable page refused: {other:?}"),
    }
}

/// A length-delimited field `number` holding `bytes` (both short).
fn field(number: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![(number << 3) | 2, bytes.len() as u8];
    out.extend_from_slice(bytes);
    out
}

/// An app's descriptor set: `test.v1.Holder`, a map of `Item`s and a list
/// of them, and `test.v1.Item`, a name.
fn holder_set() -> Vec<u8> {
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        MessageOptions,
    };
    let field = |name: &str, number, label: Label, kind: Type, type_name: Option<&str>| {
        FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            label: Some(label as i32),
            r#type: Some(kind as i32),
            type_name: type_name.map(str::to_string),
            json_name: Some(name.to_string()),
            ..FieldDescriptorProto::default()
        }
    };
    let item = DescriptorProto {
        name: Some("Item".to_string()),
        field: vec![field("name", 1, Label::Optional, Type::String, None)],
        ..DescriptorProto::default()
    };
    let entry = DescriptorProto {
        name: Some("ItemsEntry".to_string()),
        field: vec![
            field("key", 1, Label::Optional, Type::String, None),
            field(
                "value",
                2,
                Label::Optional,
                Type::Message,
                Some(".test.v1.Item"),
            ),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..MessageOptions::default()
        }),
        ..DescriptorProto::default()
    };
    let holder = DescriptorProto {
        name: Some("Holder".to_string()),
        field: vec![
            field(
                "items",
                1,
                Label::Repeated,
                Type::Message,
                Some(".test.v1.Holder.ItemsEntry"),
            ),
            field(
                "list",
                2,
                Label::Repeated,
                Type::Message,
                Some(".test.v1.Item"),
            ),
        ],
        nested_type: vec![entry],
        ..DescriptorProto::default()
    };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test/v1/holder.proto".to_string()),
            package: Some("test.v1".to_string()),
            message_type: vec![holder, item],
            syntax: Some("proto3".to_string()),
            ..FileDescriptorProto::default()
        }],
    }
    .encode_to_vec()
}

/// A `Holder` payload whose one `Item`, in its map or in its list, holds
/// a field `Item` does not declare.
fn holder(in_map: bool) -> Any {
    let mut item = field(1, b"one");
    item.extend_from_slice(&UNKNOWN_FIELD);
    let value = if in_map {
        let mut entry = field(1, b"k");
        entry.extend(field(2, &item));
        field(1, &entry)
    } else {
        field(2, &item)
    };
    Any {
        type_url: "/test.v1.Holder".to_string(),
        value,
    }
}

/// An app's payload is resolved through the app's descriptor set, and an
/// unknown field in an item of its map or its list is refused at its path.
#[tokio::test]
async fn an_unknown_field_in_a_maps_or_lists_item_is_refused() {
    let set = holder_set();
    let pool = descriptor_pool(&[&set]).unwrap();
    for (in_map, path) in [(true, "payload.items"), (false, "payload.list")] {
        let store = MemoryStore::new();
        store
            .write(Write::append(stream(), 0, &[page_of(0, holder(in_map))]))
            .await
            .unwrap();
        let mut out = Vec::new();
        match export(&store, &pool, &mut out).await {
            Err(ExportError::UnknownFields {
                message, path: at, ..
            }) => {
                assert_eq!(message, "test.v1.Item");
                assert_eq!(at, path);
            }
            other => panic!("an unknown field at {path} refused: {other:?}"),
        }
        assert!(out.is_empty());
    }
}

/// A descriptor set that does not decode is refused by its index: the
/// app's first is 1, angzarr's own 0.
#[test]
fn descriptor_pool_refuses_a_set_that_does_not_decode_by_its_index() {
    let set = holder_set();
    assert!(descriptor_pool(&[&set]).is_ok());
    assert_eq!(
        descriptor_pool(&[&set, b"\xff\xff"]).err(),
        Some(ExportError::DescriptorSet { index: 2 })
    );
}
