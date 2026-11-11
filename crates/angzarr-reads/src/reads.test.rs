use std::sync::atomic::Ordering;
use std::sync::Arc;

use angzarr_coordinator_base::{Code, Refusal};
use angzarr_ports::contract::{self, correlated, main_stream, page};
use angzarr_ports::StoreError;
use angzarr_proto::v1::event_page::Body;
use angzarr_proto::v1::{AngzarrReason, Cover, EventBook, EventPage, Uuid};
use prost::Message;

use super::*;
use crate::support::{
    block_on, cover, cover_in, reads_over, seed, sequences, unavailable, uuid, Clock, Handle,
    Upcaster,
};
use crate::Invalid;

fn query(cover: Cover, select: Select) -> Query {
    Query {
        cover,
        select,
        correlation: None,
    }
}

/// A handle with `player`'s stream 1 holding `pages` pages.
fn player(pages: u64) -> Handle {
    let handle = Handle::new();
    block_on(seed(&handle.store, main_stream("player", 1), pages));
    handle
}

fn pages_per_book(max_pages: usize) -> Limits {
    Limits {
        max_pages,
        max_bytes: usize::MAX,
    }
}

fn invalid(error: ReadError) -> Invalid {
    match error {
        ReadError::Invalid(invalid) => invalid,
        other => panic!("not an Invalid: {other:?}"),
    }
}

// ----------------------------------------------------------- events: stream

/// A read from a sequence answers the pages from it, in order, cut into
/// books of the limit's size; every book carries the stream's cover and the
/// head as read (not the last page of the book: a client's resume point is
/// its own last sequence plus one).
#[test]
fn events_from_a_sequence_return_pages_in_order_and_chunk_by_size() {
    let handle = player(5);
    let reads = reads_over(&handle).with_limits(pages_per_book(2));
    let books = block_on(reads.events(&query(
        cover("player", 1),
        Select::Range { from: 1, to: None },
    )))
    .unwrap();
    assert_eq!(
        books.iter().map(sequences).collect::<Vec<_>>(),
        [vec![1, 2], vec![3, 4]]
    );
    for book in &books {
        assert_eq!(book.cover, Some(cover("player", 1)));
        assert_eq!(book.next_sequence, 5, "the head as read");
        assert!(book.snapshot.is_none(), "no snapshots");
    }
}

/// Size is bytes too: a limit that fits two encoded pages cuts at two.
#[test]
fn books_are_cut_by_encoded_size_as_well() {
    let handle = player(5);
    // Page 0 is smaller (proto3 leaves its sequence out): two pages of the
    // later size fit [0, 1] and [2, 3] exactly.
    let two = 2 * page(1, 1).encoded_len();
    let reads = reads_over(&handle).with_limits(Limits {
        max_pages: usize::MAX,
        max_bytes: two,
    });
    let books = block_on(reads.events(&query(cover("player", 1), Select::All))).unwrap();
    assert_eq!(
        books.iter().map(sequences).collect::<Vec<_>>(),
        [vec![0, 1], vec![2, 3], vec![4]]
    );
}

/// A read from the head or past it is ONE empty book that knows the head,
/// not no book and not an error.
#[test]
fn reading_past_the_head_is_one_empty_book_at_the_head() {
    let handle = player(5);
    let reads = reads_over(&handle);
    for from in [5, 9] {
        let books =
            block_on(reads.events(&query(cover("player", 1), Select::Range { from, to: None })))
                .unwrap();
        assert_eq!(
            books,
            [EventBook {
                cover: Some(cover("player", 1)),
                pages: Vec::new(),
                next_sequence: 5,
                ..EventBook::default()
            }],
            "from {from}"
        );
    }
}

/// A stream that was never written is not an error: one empty book at 0.
#[test]
fn an_absent_stream_reads_as_empty_at_zero() {
    let handle = player(2);
    let books =
        block_on(reads_over(&handle).events(&query(cover("player", 9), Select::All))).unwrap();
    assert_eq!(
        books,
        [EventBook {
            cover: Some(cover("player", 9)),
            pages: Vec::new(),
            next_sequence: 0,
            ..EventBook::default()
        }]
    );
}

/// Each selection answers its own pages: a range, a set, as of a sequence,
/// all; a selection that keeps nothing of a stream that has pages is the
/// empty book at the head.
#[test]
fn each_selection_answers_its_pages_of_a_stream() {
    let handle = player(6);
    let reads = reads_over(&handle);
    let read = |select| {
        let books = block_on(reads.events(&query(cover("player", 1), select))).unwrap();
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].next_sequence, 6);
        sequences(&books[0])
    };
    assert_eq!(read(Select::All), [0, 1, 2, 3, 4, 5]);
    assert_eq!(
        read(Select::Range {
            from: 1,
            to: Some(3)
        }),
        [1, 2]
    );
    assert_eq!(read(Select::Sequences([4, 1, 77].into())), [1, 4]);
    assert_eq!(read(Select::AsOfSequence(2)), [0, 1, 2]);
    assert_eq!(read(Select::AsOfSequence(u64::MAX)), [0, 1, 2, 3, 4, 5]);
    assert_eq!(read(Select::Sequences([77].into())), Vec::<u64>::new());
}

/// As of a time reads the stream and cuts it at the time, or, with a time
/// port, asks the port and does not read the stream at all.
#[test]
fn as_of_a_time_is_cut_from_a_read_or_asked_of_the_time_port() {
    let handle = player(6);
    let at = Select::AsOfTime(contract::at(2));
    let books =
        block_on(reads_over(&handle).events(&query(cover("player", 1), at.clone()))).unwrap();
    assert_eq!(sequences(&books[0]), [0, 1, 2]);
    assert_eq!(books[0].next_sequence, 6);
    assert_eq!(handle.calls(), 1);

    let clock = Arc::new(Clock {
        store: handle.store.clone(),
        asked: 0.into(),
    });
    let before = handle.calls();
    let books = block_on(
        reads_over(&handle)
            .with_time(clock.clone())
            .events(&query(cover("player", 1), at)),
    )
    .unwrap();
    assert_eq!(sequences(&books[0]), [0, 1, 2]);
    assert_eq!(books[0].next_sequence, 6);
    assert_eq!(clock.asked.load(Ordering::SeqCst), 1);
    assert_eq!(handle.calls(), before, "the port answered, no stream read");
}

/// The correlation selector on one stream: only the pages of that
/// correlation, within the selection; none matching is the empty book at the
/// head (that stream only).
#[test]
fn a_correlation_on_one_stream_keeps_only_its_pages() {
    let handle = Handle::new();
    let pages: Vec<EventPage> = [(0, 0xA1), (1, 0xB1), (2, 0xA1), (3, 0xA1)]
        .into_iter()
        .map(|(s, c)| correlated(s, 1, c))
        .collect();
    block_on(contract::append(
        &handle.store,
        main_stream("player", 1),
        0,
        &pages,
    ))
    .unwrap();
    let reads = reads_over(&handle);
    let read = |correlation: Option<Uuid>, select| {
        let books = block_on(reads.events(&Query {
            cover: cover("player", 1),
            select,
            correlation,
        }))
        .unwrap();
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].next_sequence, 4);
        sequences(&books[0])
    };
    assert_eq!(read(Some(uuid(0xA1)), Select::All), [0, 2, 3]);
    assert_eq!(read(Some(uuid(0xB1)), Select::All), [1]);
    assert_eq!(
        read(Some(uuid(0xA1)), Select::Range { from: 1, to: None }),
        [2, 3]
    );
    assert_eq!(read(Some(uuid(0xDD)), Select::All), Vec::<u64>::new());
}

/// One read of the store per request: a page another process appends while
/// the books are being cut is in none of them, and every book names the
/// same head, the one read. The write lands right after the read answers.
#[test]
fn a_write_during_the_read_does_not_tear_the_books() {
    let handle = player(3);
    handle.after_next_read(|store| {
        block_on(contract::append(
            store,
            main_stream("player", 1),
            3,
            &[page(3, 1)],
        ))
        .unwrap();
    });
    let reads = reads_over(&handle).with_limits(pages_per_book(2));
    let books = block_on(reads.events(&query(cover("player", 1), Select::All))).unwrap();
    assert_eq!(
        books.iter().map(sequences).collect::<Vec<_>>(),
        [vec![0, 1], vec![2]]
    );
    assert!(books.iter().all(|book| book.next_sequence == 3));
    assert_eq!(handle.calls(), 1, "one store read for the request");

    let books = block_on(reads.events(&query(cover("player", 1), Select::All))).unwrap();
    assert_eq!(
        books.iter().map(sequences).collect::<Vec<_>>(),
        [vec![0, 1], vec![2, 3]]
    );
}

/// A store that cannot answer fails the read under its key.
#[test]
fn a_store_failure_fails_the_read_with_its_key() {
    let handle = player(3);
    handle.fail_next_read(unavailable());
    let error =
        block_on(reads_over(&handle).events(&query(cover("player", 1), Select::All))).unwrap_err();
    let coded = error.coded();
    assert_eq!(coded.code, Code::Unavailable);
    assert_eq!(coded.key, AngzarrReason::StoreUnavailable.as_str_name());
}

// --------------------------------------------------------------- upcasting

/// Every page of every book is upcast when a port is given, through the
/// port of the book's domain, after the read and before the books are cut
/// (so the size judged is the answered pages'); with none, pages are as
/// stored.
#[test]
fn every_page_read_is_upcast_when_a_port_is_given() {
    let handle = player(5);
    let port = Upcaster::new(false);
    let reads = reads_over(&handle)
        .with_upcast(port.clone())
        .with_limits(pages_per_book(2));
    let books = block_on(reads.events(&query(cover("player", 1), Select::All))).unwrap();
    assert_eq!(books.len(), 3);
    for page in books.iter().flat_map(|book| &book.pages) {
        let Some(Body::Event(any)) = &page.body else {
            panic!("an event")
        };
        assert_eq!(any.type_url, "/up.player");
        assert_eq!(any.value, [page.sequence as u8]);
    }
    assert_eq!(*port.asked.lock().unwrap(), ["player"], "one request");

    let plain =
        block_on(reads_over(&handle).events(&query(cover("player", 1), Select::All))).unwrap();
    let Some(Body::Event(any)) = &plain[0].pages[0].body else {
        panic!("an event")
    };
    assert_eq!(any.type_url, "/angzarr.contract.v1.Page");
}

/// A correlated read upcasts each book through its own domain's port call.
#[test]
fn a_correlated_read_is_upcast_per_domain() {
    let handle = Handle::new();
    block_on(seed(&handle.store, main_stream("player", 1), 1));
    block_on(seed(&handle.store, main_stream("order", 2), 1));
    let port = Upcaster::new(false);
    let books = block_on(
        reads_over(&handle)
            .with_upcast(port.clone())
            .correlated(Some(&uuid(0xC0)), None),
    )
    .unwrap();
    let urls: Vec<String> = books
        .iter()
        .map(|book| match &book.pages[0].body {
            Some(Body::Event(any)) => any.type_url.clone(),
            _ => panic!("an event"),
        })
        .collect();
    assert_eq!(urls, ["/up.order", "/up.player"]);
    assert_eq!(*port.asked.lock().unwrap(), ["order", "player"]);
}

/// A down upcaster fails the read UNAVAILABLE under its key, with the domain.
#[test]
fn a_down_upcaster_fails_the_read_unavailable_with_its_key() {
    let handle = player(2);
    let reads = reads_over(&handle).with_upcast(Upcaster::new(true));
    let coded = block_on(reads.events(&query(cover("player", 1), Select::All)))
        .unwrap_err()
        .coded();
    assert_eq!(coded.code, Code::Unavailable);
    assert_eq!(coded.key, AngzarrReason::UpcasterUnavailable.as_str_name());
    assert_eq!(coded.args["domain"], "player");
}

/// No page, no request: an absent stream asks the upcaster nothing, so a
/// down one does not fail it.
#[test]
fn an_empty_book_asks_the_upcaster_nothing() {
    let handle = player(2);
    let reads = reads_over(&handle).with_upcast(Upcaster::new(true));
    let books = block_on(reads.events(&query(cover("player", 9), Select::All))).unwrap();
    assert_eq!(books.len(), 1);
    assert!(books[0].pages.is_empty());
}

// ---------------------------------------------------------------- editions

/// An edition no fork of exists is NOT_FOUND, asked before any read, for a
/// stream query and a correlated one alike.
#[test]
fn an_unknown_edition_is_not_found() {
    let handle = player(2);
    let reads = reads_over(&handle);
    let errors = [
        block_on(reads.events(&query(cover_in("player", 1, "nope"), Select::All))).unwrap_err(),
        block_on(reads.correlated(Some(&uuid(0xC0)), Some("nope"))).unwrap_err(),
        block_on(reads.event_book(&query(cover_in("player", 1, "nope"), Select::All))).unwrap_err(),
    ];
    for error in errors {
        let coded = error.coded();
        assert_eq!(coded.code, Code::NotFound);
        assert_eq!(coded.key, "angzarr-ports-store-unknown-edition");
    }
    assert_eq!(handle.calls(), 0, "no store read");
    assert_eq!(handle.editions_asked(), ["nope", "nope", "nope"]);
}

/// A known edition reads as the store reads it: main below the fork, then
/// its own events, under the edition's cover. Main (no edition) asks nothing.
#[test]
fn a_known_edition_reads_by_its_fork() {
    let handle = player(2);
    let edition = contract::edition_stream("player", 1, "x");
    block_on(contract::fork(&handle.store, edition));
    let reads = reads_over(&handle);
    let books = block_on(reads.events(&query(cover_in("player", 1, "x"), Select::All))).unwrap();
    assert_eq!(books.len(), 1);
    assert_eq!(books[0].cover, Some(cover_in("player", 1, "x")));
    assert_eq!(sequences(&books[0]), [0, 1, 2], "main's two, then the fork");
    assert_eq!(handle.editions_asked(), ["x"]);

    block_on(reads.events(&query(cover("player", 1), Select::All))).unwrap();
    assert_eq!(handle.editions_asked(), ["x"], "main is never asked about");
}

// -------------------------------------------------------------- validation

/// A malformed cover, edition or correlation is INVALID_ARGUMENT before any
/// store call, with its own key: a table of requests and what each is.
#[test]
fn a_malformed_cover_or_correlation_is_invalid_argument() {
    let handle = player(2);
    let reads = reads_over(&handle);
    let with_root = |bytes: usize| Cover {
        root: Some(Uuid {
            value: vec![1; bytes].into(),
        }),
        ..cover("player", 1)
    };
    let no_domain = Cover {
        domain: String::new(),
        ..cover("player", 1)
    };
    let domain_only = Cover {
        root: None,
        ..cover("player", 1)
    };
    let empty_edition = cover_in("player", 1, "");
    let rows = [
        (no_domain, None, Invalid::NoDomain),
        (with_root(15), None, Invalid::BadRoot),
        (with_root(17), None, Invalid::BadRoot),
        (domain_only, None, Invalid::NoTarget),
        (empty_edition, None, Invalid::EmptyEdition),
        (
            cover("player", 1),
            Some(Uuid {
                value: vec![7; 15].into(),
            }),
            Invalid::BadCorrelation,
        ),
    ];
    for (cover, correlation, expected) in rows {
        let query = Query {
            cover,
            select: Select::All,
            correlation,
        };
        for error in [
            block_on(reads.events(&query)).unwrap_err(),
            block_on(reads.event_book(&query)).unwrap_err(),
        ] {
            assert_eq!(error.coded().code, Code::InvalidArgument, "{expected:?}");
            assert_eq!(invalid(error), expected);
        }
    }
    assert_eq!(handle.calls(), 0, "refused before any store call");
    assert!(handle.editions_asked().is_empty());
}

/// A correlated read needs a 16-byte correlation and, when it names an
/// edition, a non-empty one.
#[test]
fn a_malformed_correlated_read_is_invalid_argument() {
    let handle = player(2);
    let reads = reads_over(&handle);
    let short = Uuid {
        value: vec![7; 15].into(),
    };
    let long = Uuid {
        value: vec![7; 17].into(),
    };
    let rows = [
        (None, None, Invalid::BadCorrelation),
        (Some(&short), None, Invalid::BadCorrelation),
        (Some(&long), None, Invalid::BadCorrelation),
        (Some(&uuid(1)), Some(""), Invalid::EmptyEdition),
    ];
    for (correlation, edition, expected) in rows {
        let error = block_on(reads.correlated(correlation, edition)).unwrap_err();
        assert_eq!(invalid(error), expected);
    }
    assert_eq!(handle.calls(), 0);
}

// -------------------------------------------------------------- visibility

fn only_player(stream: &Cover) -> bool {
    stream.domain == "player"
}

/// A stream the caller may not read answers as an absent one: one empty book
/// at 0 under the query's cover, with no store call and no upcast, and not
/// even the edition asked about (so a denied stream in an unknown edition
/// is not told apart from a denied one in a known edition).
#[test]
fn a_read_of_a_stream_not_visible_is_empty() {
    let handle = Handle::new();
    block_on(seed(&handle.store, main_stream("player", 1), 3));
    block_on(seed(&handle.store, main_stream("order", 2), 3));
    let port = Upcaster::new(false);
    let reads = reads_over(&handle)
        .with_visible(Arc::new(only_player))
        .with_upcast(port.clone());
    let denied = EventBook {
        cover: Some(cover("order", 2)),
        pages: Vec::new(),
        next_sequence: 0,
        ..EventBook::default()
    };
    let books = block_on(reads.events(&query(cover("order", 2), Select::All))).unwrap();
    assert_eq!(books, std::slice::from_ref(&denied));
    assert_eq!(
        block_on(reads.event_book(&query(cover("order", 2), Select::All))).unwrap(),
        denied
    );
    assert_eq!(handle.calls(), 0);
    assert!(port.asked.lock().unwrap().is_empty());

    let books = block_on(reads.events(&query(cover_in("order", 2, "nope"), Select::All))).unwrap();
    assert!(books[0].pages.is_empty());
    assert!(handle.editions_asked().is_empty());

    let books = block_on(reads.events(&query(cover("player", 1), Select::All))).unwrap();
    assert_eq!(sequences(&books[0]), [0, 1, 2], "what is visible reads");
}

/// A correlation's books and a list leave out every stream not visible.
#[test]
fn correlated_reads_and_list_streams_drop_what_is_not_visible() {
    let handle = Handle::new();
    block_on(seed(&handle.store, main_stream("player", 1), 1));
    block_on(seed(&handle.store, main_stream("order", 2), 1));
    block_on(seed(&handle.store, main_stream("player", 3), 1));
    let reads = reads_over(&handle).with_visible(Arc::new(only_player));

    let books = block_on(reads.correlated(Some(&uuid(0xC0)), None)).unwrap();
    let covers: Vec<Option<Cover>> = books.into_iter().map(|b| b.cover).collect();
    assert_eq!(covers, [Some(cover("player", 1)), Some(cover("player", 3))]);

    let by_domain = block_on(reads.events(&Query {
        cover: Cover {
            root: None,
            ..cover("order", 2)
        },
        select: Select::All,
        correlation: Some(uuid(0xC0)),
    }))
    .unwrap();
    assert!(by_domain.is_empty(), "a domain not visible has no books");

    let listed = block_on(reads.list_streams()).unwrap();
    assert_eq!(listed, [cover("player", 1), cover("player", 3)]);
}

/// A closure is a predicate; the engine's default sees every stream.
#[test]
fn every_stream_is_visible_by_default() {
    let handle = Handle::new();
    block_on(seed(&handle.store, main_stream("order", 2), 1));
    let listed = block_on(reads_over(&handle).list_streams()).unwrap();
    assert_eq!(listed, [cover("order", 2)]);
    assert!(Visible::visible(&only_player, &cover("player", 1)));
    assert!(!Visible::visible(&only_player, &cover("order", 1)));
}

// ------------------------------------------------------------- correlated

fn workflow() -> Handle {
    let handle = Handle::new();
    let write = |domain: &'static str, n: u8, pages: &[(u64, u8)]| {
        let pages: Vec<EventPage> = pages.iter().map(|(s, c)| correlated(*s, 1, *c)).collect();
        block_on(contract::append(
            &handle.store,
            main_stream(domain, n),
            0,
            &pages,
        ))
        .unwrap();
    };
    write("player", 1, &[(0, 0xA1), (1, 0xB1), (2, 0xA1)]);
    write("order", 2, &[(0, 0xA1)]);
    write("hand", 3, &[(0, 0xB1)]);
    handle
}

/// A correlation answers one book per stream holding it, in every domain,
/// those pages only, each book at its stream's head.
#[test]
fn a_correlation_answers_one_book_per_stream_in_every_domain() {
    let handle = workflow();
    let books = block_on(reads_over(&handle).correlated(Some(&uuid(0xA1)), None)).unwrap();
    let summary: Vec<(String, Vec<u64>, u64)> = books
        .iter()
        .map(|b| {
            (
                b.cover.as_ref().unwrap().domain.clone(),
                sequences(b),
                b.next_sequence,
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("order".to_string(), vec![0], 1),
            ("player".to_string(), vec![0, 2], 3)
        ]
    );
}

/// Books are cut within the limits too.
#[test]
fn a_correlations_books_are_cut_within_the_limits() {
    let handle = workflow();
    let reads = reads_over(&handle).with_limits(pages_per_book(1));
    let books = block_on(reads.correlated(Some(&uuid(0xA1)), None)).unwrap();
    assert_eq!(
        books.iter().map(sequences).collect::<Vec<_>>(),
        [vec![0], vec![0], vec![2]]
    );
}

/// A correlation with no events is an empty answer, not NOT_FOUND, on the
/// RPC and on a domain query alike.
#[test]
fn a_correlation_with_no_events_is_empty_not_not_found() {
    let handle = workflow();
    let reads = reads_over(&handle);
    assert!(block_on(reads.correlated(Some(&uuid(0xEE)), None))
        .unwrap()
        .is_empty());
    let none = block_on(reads.events(&Query {
        cover: Cover {
            root: None,
            ..cover("player", 1)
        },
        select: Select::All,
        correlation: Some(uuid(0xEE)),
    }))
    .unwrap();
    assert!(none.is_empty());
}

/// A query of a domain and a correlation (no root) searches every stream of
/// the domain, and the selection applies within each.
#[test]
fn a_domain_and_a_correlation_select_within_each_stream_of_the_domain() {
    let handle = workflow();
    block_on(contract::append(
        &handle.store,
        main_stream("player", 4),
        0,
        &[correlated(0, 1, 0xA1), correlated(1, 1, 0xA1)],
    ))
    .unwrap();
    let reads = reads_over(&handle);
    let read = |select| {
        block_on(reads.events(&Query {
            cover: Cover {
                root: None,
                ..cover("player", 1)
            },
            select,
            correlation: Some(uuid(0xA1)),
        }))
        .unwrap()
    };
    let books = read(Select::All);
    assert_eq!(
        books
            .iter()
            .map(|b| b.cover.clone().unwrap().root.unwrap())
            .collect::<Vec<_>>(),
        [uuid(1), uuid(4)]
    );
    assert_eq!(
        books.iter().map(sequences).collect::<Vec<_>>(),
        [vec![0, 2], vec![0, 1]]
    );
    let books = read(Select::Range { from: 1, to: None });
    assert_eq!(
        books.iter().map(sequences).collect::<Vec<_>>(),
        [vec![2], vec![1]]
    );
    let books = read(Select::AsOfTime(contract::at(1)));
    assert_eq!(
        books.iter().map(sequences).collect::<Vec<_>>(),
        [vec![0], vec![0, 1]]
    );
}

/// One stream per edition in a list, every edition and domain, each once.
#[test]
fn list_streams_answers_one_cover_per_stream_of_every_edition() {
    let handle = Handle::new();
    block_on(seed(&handle.store, main_stream("player", 1), 2));
    block_on(seed(&handle.store, main_stream("order", 2), 1));
    block_on(contract::fork(
        &handle.store,
        contract::edition_stream("player", 1, "x"),
    ));
    block_on(contract::fork(
        &handle.store,
        contract::edition_stream("order", 2, "y"),
    ));
    let mut listed = block_on(reads_over(&handle).list_streams()).unwrap();
    let mut expected = vec![
        cover("player", 1),
        cover("order", 2),
        cover_in("player", 1, "x"),
        cover_in("order", 2, "y"),
    ];
    let key = |c: &Cover| format!("{c:?}");
    listed.sort_by_key(key);
    expected.sort_by_key(key);
    assert_eq!(listed, expected);
}

// ------------------------------------------------------------- unary book

/// GetEventBook is one book: the whole read when it fits one, otherwise
/// RESOURCE_EXHAUSTED with what it would take (a unary RPC cannot chunk).
#[test]
fn get_event_book_past_the_limit_is_resource_exhausted() {
    let handle = player(5);
    let q = query(cover("player", 1), Select::All);

    let fits = reads_over(&handle).with_limits(pages_per_book(5));
    let book = block_on(fits.event_book(&q)).unwrap();
    assert_eq!(sequences(&book), [0, 1, 2, 3, 4]);
    assert_eq!(book.next_sequence, 5);

    let over = reads_over(&handle).with_limits(Limits {
        max_pages: 4,
        max_bytes: 1 << 20,
    });
    let error = block_on(over.event_book(&q)).unwrap_err();
    assert_eq!(error.coded().code, Code::ResourceExhausted);
    assert_eq!(
        invalid(error),
        Invalid::TooLarge {
            books: 2,
            pages: 5,
            max_pages: 4,
            max_bytes: 1 << 20
        }
    );
}

/// An absent stream, or past the head, is one empty book on the unary read
/// too.
#[test]
fn get_event_book_of_an_absent_stream_is_the_empty_book() {
    let handle = player(2);
    let book =
        block_on(reads_over(&handle).event_book(&query(cover("player", 9), Select::All))).unwrap();
    assert_eq!(book.cover, Some(cover("player", 9)));
    assert!(book.pages.is_empty());
    assert_eq!(book.next_sequence, 0);
}

/// A unary read of a domain and correlation answers the one stream that has
/// it; none is an empty book under the query's cover; several are refused
/// (a unary RPC answers one book).
#[test]
fn get_event_book_by_correlation_answers_one_stream_or_refuses() {
    let handle = workflow();
    let by_corr = |c: u8, domain: &str| Query {
        cover: Cover {
            domain: domain.to_string(),
            root: None,
            edition: None,
        },
        select: Select::All,
        correlation: Some(uuid(c)),
    };
    let reads = reads_over(&handle);

    let book = block_on(reads.event_book(&by_corr(0xA1, "player"))).unwrap();
    assert_eq!(book.cover, Some(cover("player", 1)));
    assert_eq!(sequences(&book), [0, 2]);

    let none = block_on(reads.event_book(&by_corr(0xEE, "player"))).unwrap();
    assert_eq!(none.cover.as_ref().unwrap().domain, "player");
    assert!(none.pages.is_empty());

    block_on(contract::append(
        &handle.store,
        main_stream("player", 4),
        0,
        &[correlated(0, 1, 0xA1)],
    ))
    .unwrap();
    let error = block_on(reads.event_book(&by_corr(0xA1, "player"))).unwrap_err();
    assert_eq!(invalid(error), Invalid::ManyStreams { streams: 2 });
}

// ----------------------------------------------------------- read-only

/// A handle that has no `write` at all serves every read: `Handle` is an
/// `EventRead` and no `EventStore`, and is what the engine is built over in
/// every test of this file; here each read is asked once, with every
/// optional port given.
#[test]
fn a_read_only_store_handle_serves_every_read() {
    let handle = workflow();
    let reads = reads_over(&handle)
        .with_upcast(Upcaster::new(false))
        .with_visible(Arc::new(|_: &Cover| true))
        .with_time(Arc::new(Clock {
            store: handle.store.clone(),
            asked: 0.into(),
        }));
    assert_eq!(
        block_on(reads.events(&query(cover("player", 1), Select::All)))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        sequences(&block_on(reads.event_book(&query(cover("hand", 3), Select::All))).unwrap()),
        [0]
    );
    assert_eq!(
        block_on(reads.correlated(Some(&uuid(0xB1)), None))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(block_on(reads.list_streams()).unwrap().len(), 3);
    let _: Option<StoreError> = None;
}

// ------------------------------------------------------------------ store

/// Why: a frontend that follows a stream asks the engine's own store for
/// what the engine does not answer (its change token, its commit push), so
/// `store` is that store, not another handle.
#[test]
fn the_engine_hands_out_the_store_it_reads() {
    let handle = player(2);
    let reads = reads_over(&handle);
    assert!(Arc::ptr_eq(&reads.store().0, &handle.0));
}
