//! Each operation's entry, and replaying entries.

use angzarr_ports::contract::{command, dead_letter, entry, main_stream, page, snapshot};
use angzarr_ports::{Append, DeadLetterAt, EventStore, PositionKey, PositionStore, Write};

use super::*;
use crate::schema::entry::Op;

fn stream() -> StreamRef<'static> {
    main_stream("orders", 1)
}

fn op(plan: Plan) -> Op {
    match plan {
        Plan::Line(entry) => entry.op.expect("an operation"),
        Plan::Skip => panic!("skipped"),
        Plan::Unencodable => panic!("unencodable"),
    }
}

#[test]
fn test_a_write_is_one_entry_with_everything_it_changes() {
    let pages = [page(0, 1), page(1, 2)];
    let positions = [(
        PositionKey {
            handler: "h",
            stream: stream(),
        },
        2,
    )];
    let outbox = [entry(1, "h", 1)];
    let letters = [dead_letter(stream(), DeadLetterAt::Sequence(0), "h", 1)];
    let cleared = [command(7), command(8)];
    let recorded = op(write(&Write {
        events: Some(Append {
            stream: stream(),
            expected: 0,
            pages: &pages,
        }),
        positions: &positions,
        outbox: &outbox,
        dead_letters: &letters,
        cleared: &cleared,
    }));
    let Op::Write(w) = recorded else {
        panic!("{recorded:?}")
    };
    let events = w.events.expect("events");
    assert_eq!((events.expected, events.pages.len()), (0, 2));
    assert_eq!(events.pages[1], pages[1].encode_to_vec());
    assert_eq!(w.positions.len(), 1);
    assert_eq!(
        (w.positions[0].handler.as_str(), w.positions[0].next),
        ("h", 2)
    );
    assert_eq!(w.outbox.len(), 1);
    assert_eq!(w.outbox[0].origin, "h");
    assert_eq!(w.dead_letters.len(), 1);
    assert_eq!(
        w.cleared,
        vec![command(7).to_string(), command(8).to_string()]
    );
}

#[test]
fn test_a_write_that_changes_nothing_is_skipped() {
    assert!(matches!(write(&Write::default()), Plan::Skip));
    let checked = Write::append(stream(), 3, &[]);
    assert!(matches!(write(&checked), Plan::Skip));
    // Each part alone is a change.
    let pages = [page(0, 1)];
    assert!(matches!(
        write(&Write::append(stream(), 0, &pages)),
        Plan::Line(_)
    ));
    let positions = [(
        PositionKey {
            handler: "h",
            stream: stream(),
        },
        1,
    )];
    assert!(matches!(
        write(&Write::positions(&positions)),
        Plan::Line(_)
    ));
    let outbox = [entry(1, "h", 1)];
    let only_outbox = Write {
        outbox: &outbox,
        ..Write::default()
    };
    assert!(matches!(write(&only_outbox), Plan::Line(_)));
    let letters = [dead_letter(stream(), DeadLetterAt::Sequence(0), "h", 1)];
    let only_letters = Write {
        dead_letters: &letters,
        ..Write::default()
    };
    assert!(matches!(write(&only_letters), Plan::Line(_)));
    let ids = [command(1)];
    let only_cleared = Write {
        cleared: &ids,
        ..Write::default()
    };
    assert!(matches!(write(&only_cleared), Plan::Line(_)));
}

#[test]
fn test_a_dead_letter_about_no_stream_cannot_be_an_entry() {
    let mut letter = dead_letter(stream(), DeadLetterAt::Sequence(0), "h", 1);
    letter.stream.root = None;
    let letters = [letter];
    let w = Write {
        dead_letters: &letters,
        ..Write::default()
    };
    assert!(matches!(write(&w), Plan::Unencodable));
}

#[test]
fn test_the_other_operations_record_their_arguments() {
    let Op::SnapshotWritten(s) = op(super::snapshot(stream(), &snapshot(4, "v"))) else {
        panic!()
    };
    assert_eq!(s.snapshot, snapshot(4, "v").encode_to_vec());
    assert_eq!(s.stream.unwrap().domain, "orders");

    let Op::Purged(p) = op(purge(stream(), 9)) else {
        panic!()
    };
    assert_eq!(p.expected, 9);
    assert!(matches!(
        op(remove_edition_stream(stream())),
        Op::EditionStreamRemoved(_)
    ));
    assert!(matches!(op(drain(stream())), Op::DeadLettersDrained(_)));

    let id = Uuid::from_bytes([3; 16]);
    let Op::OutboxCleared(c) = op(clear(&[id, id])) else {
        panic!()
    };
    assert_eq!(c.ids, vec![id.to_string(), id.to_string()]);

    let letter = dead_letter(stream(), DeadLetterAt::Command(id), "policy", 1);
    let key = letter.key().unwrap();
    let Op::DeadLettersRemoved(r) = op(remove_dead_letters(&[key])) else {
        panic!()
    };
    assert_eq!(r.keys[0].component, "policy");
    assert_eq!(
        r.keys[0].at,
        Some(schema::Place {
            at: Some(schema::place::At::Command(id.to_string()))
        })
    );
}

fn replayed(recorded: Op) -> Result<(), Replay> {
    replay(&MemoryStore::new(), schema::Entry { op: Some(recorded) })
}

#[test]
fn test_an_entry_with_no_operation_is_malformed() {
    let result = replay(&MemoryStore::new(), schema::Entry { op: None });
    assert!(matches!(result, Err(Replay::Malformed("op"))), "{result:?}");
    let result = replayed(Op::Header(schema::Header { format: 1 }));
    assert!(
        matches!(result, Err(Replay::Malformed("header"))),
        "{result:?}"
    );
}

fn good_stream() -> schema::Stream {
    schema::Stream {
        domain: "d".to_string(),
        edition: None,
        root: Uuid::from_bytes([1; 16]).to_string(),
    }
}

/// Entries of the other operations each missing or garbling a field.
fn other_cases() -> Vec<(Op, &'static str)> {
    let good = good_stream();
    let bad_root = schema::Stream {
        root: "x".to_string(),
        ..good.clone()
    };
    let empty_edition = schema::Stream {
        edition: Some(String::new()),
        ..good.clone()
    };
    vec![
        (
            Op::Purged(schema::Purged {
                stream: None,
                expected: 0,
            }),
            "purged.stream",
        ),
        (
            Op::Purged(schema::Purged {
                stream: Some(bad_root),
                expected: 0,
            }),
            "purged.stream",
        ),
        (
            Op::EditionStreamRemoved(schema::EditionStreamRemoved {
                stream: Some(empty_edition),
            }),
            "edition_stream_removed.stream",
        ),
        (
            Op::SnapshotWritten(schema::SnapshotWritten {
                stream: Some(good.clone()),
                snapshot: vec![0xff],
            }),
            "snapshot_written.snapshot",
        ),
        (
            Op::DeadLettersDrained(schema::DeadLettersDrained { stream: None }),
            "dead_letters_drained.stream",
        ),
        (
            Op::OutboxCleared(schema::OutboxCleared {
                ids: vec!["x".to_string()],
            }),
            "outbox_cleared.ids",
        ),
        (
            Op::DeadLettersRemoved(schema::DeadLettersRemoved {
                keys: vec![schema::DeadLetterKey {
                    stream: Some(good.clone()),
                    at: None,
                    component: String::new(),
                }],
            }),
            "dead_letters_removed.keys.at",
        ),
        (
            Op::DeadLettersRemoved(schema::DeadLettersRemoved {
                keys: vec![schema::DeadLetterKey {
                    stream: None,
                    at: None,
                    component: String::new(),
                }],
            }),
            "dead_letters_removed.keys.stream",
        ),
    ]
}

/// Entries of a write each missing or garbling a field.
fn write_cases() -> Vec<(Op, &'static str)> {
    let good = good_stream();
    vec![
        (
            Op::Write(schema::Write {
                events: Some(schema::Append {
                    stream: Some(good.clone()),
                    expected: 0,
                    pages: vec![vec![0xff, 0xff]],
                }),
                ..schema::Write::default()
            }),
            "write.events.pages",
        ),
        (
            Op::Write(schema::Write {
                positions: vec![schema::Position {
                    handler: "h".to_string(),
                    stream: None,
                    next: 1,
                }],
                ..schema::Write::default()
            }),
            "write.positions.stream",
        ),
        (
            Op::Write(schema::Write {
                outbox: vec![schema::OutboxEntry {
                    id: "x".to_string(),
                    ..schema::OutboxEntry::default()
                }],
                ..schema::Write::default()
            }),
            "write.outbox.id",
        ),
        (
            Op::Write(schema::Write {
                outbox: vec![schema::OutboxEntry {
                    id: Uuid::from_bytes([1; 16]).to_string(),
                    message: vec![0xff],
                    ..schema::OutboxEntry::default()
                }],
                ..schema::Write::default()
            }),
            "write.outbox.message",
        ),
        (
            Op::Write(schema::Write {
                cleared: vec!["x".to_string()],
                ..schema::Write::default()
            }),
            "write.cleared",
        ),
        (
            Op::Write(schema::Write {
                dead_letters: vec![schema::DeadLetter {
                    stream: Some(good.clone()),
                    at: Some(schema::Place {
                        at: Some(schema::place::At::Command("x".to_string())),
                    }),
                    letter: Vec::new(),
                }],
                ..schema::Write::default()
            }),
            "write.dead_letters.at",
        ),
        (
            Op::Write(schema::Write {
                dead_letters: vec![schema::DeadLetter {
                    stream: Some(good),
                    at: Some(schema::Place {
                        at: Some(schema::place::At::Sequence(1)),
                    }),
                    letter: vec![0xff],
                }],
                ..schema::Write::default()
            }),
            "write.dead_letters.letter",
        ),
    ]
}

#[test]
fn test_replay_names_the_field_it_cannot_read() {
    for (recorded, field) in other_cases().into_iter().chain(write_cases()) {
        let result = replayed(recorded);
        assert!(
            matches!(&result, Err(Replay::Malformed(f)) if *f == field),
            "{field}: {result:?}"
        );
    }
}

#[test]
fn test_replay_of_what_the_store_refuses_says_how() {
    let at = schema::Stream {
        domain: "d".to_string(),
        edition: None,
        root: Uuid::from_bytes([1; 16]).to_string(),
    };
    let result = replayed(Op::Purged(schema::Purged {
        stream: Some(at),
        expected: 4,
    }));
    assert!(
        matches!(&result, Err(Replay::Refused(StoreError::Conflict { .. }))),
        "{result:?}"
    );
}

#[tokio::test]
async fn test_replaying_what_was_recorded_makes_the_same_store() {
    let live = MemoryStore::new();
    let pages = [page(0, 1), page(1, 2)];
    let positions = [(
        PositionKey {
            handler: "h",
            stream: stream(),
        },
        2,
    )];
    let outbox = [entry(1, "h", 1)];
    let letters = [dead_letter(stream(), DeadLetterAt::Sequence(0), "h", 1)];
    let recorded = Write {
        events: Some(Append {
            stream: stream(),
            expected: 0,
            pages: &pages,
        }),
        positions: &positions,
        outbox: &outbox,
        dead_letters: &letters,
        cleared: &[],
    };
    live.write(recorded).await.unwrap();
    let copy = MemoryStore::new();
    let Plan::Line(line) = write(&recorded) else {
        panic!()
    };
    replay(&copy, line).unwrap();
    assert_eq!(
        live.read(stream(), 0, None).await.unwrap(),
        copy.read(stream(), 0, None).await.unwrap()
    );
    assert_eq!(
        live.pending(9).await.unwrap(),
        copy.pending(9).await.unwrap()
    );
    assert_eq!(
        live.dead_letters(stream()).await.unwrap(),
        copy.dead_letters(stream()).await.unwrap()
    );
    assert_eq!(
        live.position(positions[0].0).await.unwrap(),
        copy.position(positions[0].0).await.unwrap()
    );
}

#[tokio::test]
async fn test_replaying_a_write_clears_what_it_cleared_before_adding_its_own() {
    let first = [entry(1, "h", 1), entry(2, "h", 2)];
    let own = [entry(1, "h", 9)];
    let cleared = [command(1)];
    let seed = Write {
        outbox: &first,
        ..Write::default()
    };
    let step = Write {
        outbox: &own,
        cleared: &cleared,
        ..Write::default()
    };
    let live = MemoryStore::new();
    live.write(seed).await.unwrap();
    live.write(step).await.unwrap();
    let copy = MemoryStore::new();
    for recorded in [&seed, &step] {
        let Plan::Line(line) = write(recorded) else {
            panic!()
        };
        replay(&copy, line).unwrap();
    }
    assert_eq!(
        copy.pending(9).await.unwrap(),
        vec![first[1].clone(), own[0].clone()]
    );
    assert_eq!(
        live.pending(9).await.unwrap(),
        copy.pending(9).await.unwrap()
    );
}
