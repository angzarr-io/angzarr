//! Conformance harness support: the generated fixture types, a descriptor
//! pool for parsing `.txtpb` fixtures, the tier's components built on the
//! router core (the CounterAggregate, OrderPolicy and CounterProjector of
//! fixture/FIXTURE.md; the order aggregate, the fulfillment process manager
//! and the order-fulfillment policy of handling.feature), and the "parse a
//! skeleton, set the scenario's data" helpers the step definitions call.
//! The requests are the handler layer's (`io.angzarr.handler.v1`, decision
//! 14.78). The cucumber step defs live in `tests/`.

use std::sync::{Arc, Mutex, OnceLock};

use angzarr_router::domain::{DomainDispatch, Emit, FactEmit};
use angzarr_router::error::{
    Bug, BugExt, BusinessStatus, CodedError, HandlerError, RetryableStatus,
};
use angzarr_router::policy::{self, PolicyDispatch};
use angzarr_router::projector::ProjectorDispatch;
use angzarr_router::rebuild::Rebuilder;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};

/// Generated `test.counter` fixture messages (Increased, IncreaseBy, …).
pub mod counter {
    include!(concat!(env!("OUT_DIR"), "/test.counter.rs"));
}

/// Generated `orders.v1` fixture messages: the handler-contract tier's
/// order vocabulary.
pub mod orders {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/orders.v1.rs"));
    }
}

/// Re-export the router's framework types under `pb` and `hpb`.
pub use angzarr_router::{hpb, pb};
pub use counter::CounterState;

const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/conformance_fds.bin"));

// The orthogonal request skeletons — structural scaffold only; the
// test-meaningful field is omitted and supplied by the step definitions.
const SKEL_INCREASE: &str = include_str!(
    "../../../angzarr-project/features/handler-contract/fixture/txtpb/command_increase.txtpb"
);
const SKEL_FAILHARD: &str = include_str!(
    "../../../angzarr-project/features/handler-contract/fixture/txtpb/command_failhard.txtpb"
);
const SKEL_UNHANDLED: &str = include_str!(
    "../../../angzarr-project/features/handler-contract/fixture/txtpb/command_unhandled.txtpb"
);
const SKEL_INCREASED_EVENT: &str = include_str!(
    "../../../angzarr-project/features/handler-contract/fixture/txtpb/event_increased.txtpb"
);

const HANDLE_REQUEST: &str = "io.angzarr.handler.v1.HandleRequest";
const EVENT_PAGE: &str = "io.angzarr.v1.EventPage";

/// What the IncreaseBy handler observed at dispatch time: the historical-state
/// evidence the framework supplies (`next_sequence`, `had_prior_events`) and
/// the rebuilt `count`. Recorded into a harness-owned sink, since host state
/// never crosses the boundary — this is how scenarios assert that evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observed {
    pub next_sequence: u64,
    pub had_prior_events: bool,
    pub count: u32,
}

/// Sink the harness passes into the fixture to capture each [`Observed`].
pub type ObservedSink = Arc<Mutex<Vec<Observed>>>;

/// The descriptor pool over the fixture and framework protos — resolves
/// `io.angzarr.v1.*` envelopes, `io.angzarr.handler.v1.*` requests and
/// `test.counter.*` payloads, so Any-expanded textproto fixtures decode.
pub fn pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("conformance descriptor set must decode")
    })
}

/// Parse a `.txtpb` fixture (textproto, Any-expansion allowed) into a typed
/// prost message. `full_name` is the fixture's root message type.
pub fn parse_txtpb<M>(full_name: &str, text: &str) -> M
where
    M: prost::Message + Default,
{
    let descriptor = pool()
        .get_message_by_name(full_name)
        .unwrap_or_else(|| panic!("{full_name} not in conformance pool"));
    let dynamic = DynamicMessage::parse_text_format(descriptor, text)
        .unwrap_or_else(|e| panic!("parse {full_name} txtpb: {e}"));
    dynamic
        .transcode_to::<M>()
        .unwrap_or_else(|e| panic!("transcode {full_name}: {e}"))
}

/// `msg` packed under the canonical bare prefix.
pub fn any_of<M: prost::Message + prost::Name>(msg: &M) -> prost_types::Any {
    prost_types::Any {
        type_url: angzarr_router::type_url(&M::full_name()),
        value: msg.encode_to_vec(),
    }
}

/// A page whose body is the domain event `event`.
pub fn event_page(sequence: u64, event: prost_types::Any) -> pb::EventPage {
    pb::EventPage {
        body: Some(pb::event_page::Body::Event(event)),
        meta: None,
        sequence,
    }
}

/// A command page whose Command arm carries `payload`, with no Handling.
pub fn command_page(payload: prost_types::Any) -> pb::CommandPage {
    pb::CommandPage {
        body: Some(pb::command_page::Body::Command(pb::Command {
            payload: Some(payload),
            handling: None,
        })),
        ..Default::default()
    }
}

/// A command page whose Fact arm carries `payload`, from `external_id`.
pub fn fact_page(payload: prost_types::Any, external_id: &str) -> pb::CommandPage {
    pb::CommandPage {
        body: Some(pb::command_page::Body::Fact(pb::Fact {
            payload: Some(payload),
            origin: Some(pb::FactOrigin {
                external_id: external_id.to_string(),
                description: "conformance".to_string(),
            }),
        })),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// The fixture: CounterAggregate on the router (the tier's fixture/FIXTURE.md).
// ---------------------------------------------------------------------------

/// Build the CounterAggregate dispatch table (appliers and handlers).
pub fn counter_aggregate(observed: ObservedSink) -> DomainDispatch<CounterState> {
    let rebuilder = Rebuilder::of_message().apply(
        "test.counter.Increased",
        |state: &mut CounterState, event| {
            // Decode the payload so a corrupt persisted event fails the fold
            // (ANGZARR_REASON_FOLD_FAILED: a failing fold is a handler bug).
            // Increased is empty, so every well-formed event decodes and
            // simply increments.
            counter::Increased::decode(event.value.as_slice())?;
            state.count += 1;
            Ok(())
        },
    );

    DomainDispatch::new("counter", "counter", rebuilder)
        // n > 0 emits n Increased; n == 0 is refused with its typed refusal.
        .on_command("test.counter.IncreaseBy", move |any, state, ctx| {
            // Record the historical-state evidence (host state never crosses).
            observed.lock().unwrap().push(Observed {
                next_sequence: ctx.next_sequence,
                had_prior_events: ctx.had_prior_events,
                count: state.count,
            });
            let cmd = counter::IncreaseBy::decode(any.value.as_slice())
                .map_err(|e| HandlerError::Other(format!("decode IncreaseBy: {e}")))?;
            if cmd.n == 0 {
                return Err(value_not_positive(cmd.n).into());
            }
            Ok(Emit::of((0..cmd.n).map(|_| increased_any())))
        })
        // Unclassified failure → ANGZARR_REASON_UNHANDLED_HANDLER_ERROR.
        .on_command("test.counter.FailHard", |_any, _state, _ctx| {
            Err(HandlerError::Other("hard failure".to_string()))
        })
}

/// IncreaseBy's typed refusal, under the status its message declares
/// (INVALID_ARGUMENT; fixture/counter.proto).
pub fn value_not_positive(n: u32) -> CodedError {
    CodedError::refused(
        BusinessStatus::InvalidArgument,
        any_of(&counter::ValueNotPositive { n }),
    )
}

fn increased_any() -> prost_types::Any {
    any_of(&counter::Increased {})
}

// ---------------------------------------------------------------------------
// The fixture: CounterProjector on the router core (read-side dispatch).
// ---------------------------------------------------------------------------

/// The projection the CounterProjector folds events into. Host state — it
/// never crosses the boundary; the harness reads the fold count back out of
/// the answered projection.
#[derive(Default)]
pub struct ProjectorState {
    pub count: u32,
}

/// Build the CounterProjector dispatch table: over `domains` it folds each
/// Increased event into a running count, then finishes into a projection
/// payload, the CounterProjectorState carrying that count. A run from any
/// other domain folds nothing (C-0713).
pub fn counter_projector(domains: &[&str]) -> ProjectorDispatch<ProjectorState> {
    ProjectorDispatch::new("counter-projector", ProjectorState::default)
        .for_domains(domains.iter().copied())
        .on_event(
            "test.counter.Increased",
            |state: &mut ProjectorState, event, _ctx| {
                // Decode so a corrupt event fails the fold, exactly as the
                // aggregate applier does. Increased is empty — every
                // well-formed event decodes and increments.
                counter::Increased::decode(event.value.as_slice())
                    .map_err(|e| HandlerError::Other(format!("decode Increased: {e}")))?;
                state.count += 1;
                Ok(())
            },
        )
        .finish(|state: &mut ProjectorState, _events| {
            Ok(any_of(&counter::CounterProjectorState {
                count: state.count,
            }))
        })
}

/// A run of `n` Increased events whose cover carries `domain` — the
/// projector's delivery.
pub fn delivery(domain: &str, n: u32) -> hpb::ProjectRequest {
    hpb::ProjectRequest {
        events: Some(pb::EventBook {
            cover: Some(pb::Cover {
                domain: domain.to_string(),
                ..Default::default()
            }),
            pages: (0..u64::from(n))
                .map(|seq| event_page(seq, increased_any()))
                .collect(),
            ..Default::default()
        }),
    }
}

/// The count a CounterProjector answer carries.
pub fn projected_count(answer: &hpb::ProjectResponse) -> u32 {
    let payload = answer.projection.as_ref().expect("a projection");
    counter::CounterProjectorState::decode(payload.value.as_slice())
        .expect("a CounterProjectorState")
        .count
}

// ---------------------------------------------------------------------------
// The fixture: OrderPolicy on the router core (translation-side dispatch).
// ---------------------------------------------------------------------------

/// The root business code names for the OrderPolicy's Reserve.
pub const RESERVE_ROOT: &str = "inv-7";

/// Build the OrderPolicy dispatch table: it answers each Increased trigger
/// with one Reserve command for "inventory", rooted at [`RESERVE_ROOT`]
/// (passed through unstamped), recording each trigger's sequence in `seen`.
/// Undeclared events are skipped.
pub fn order_policy(seen: SequenceSink) -> PolicyDispatch {
    PolicyDispatch::new("order-policy", "counter", "inventory").on_event(
        "test.counter.Increased",
        move |_any, source| {
            seen.lock().unwrap().push(source.sequence);
            Ok(vec![reserve_output()])
        },
    )
}

fn reserve_output() -> hpb::Output {
    policy::command(
        Some(pb::Uuid {
            value: root_of(RESERVE_ROOT).into(),
        }),
        any_of(&counter::Reserve {}),
    )
}

/// React's request: a trigger book holding one event of `event_fq` in the
/// "counter" domain at sequence `seq`.
pub fn policy_event_source(event_fq: &str, seq: u64) -> hpb::ReactRequest {
    hpb::ReactRequest {
        trigger: Some(pb::EventBook {
            cover: Some(pb::Cover {
                domain: "counter".to_string(),
                ..Default::default()
            }),
            pages: vec![event_page(
                seq,
                prost_types::Any {
                    type_url: angzarr_router::type_url(event_fq),
                    value: Vec::new(),
                },
            )],
            ..Default::default()
        }),
    }
}

// ---------------------------------------------------------------------------
// Skeleton → request helpers: parse the orthogonal request, then SET the
// scenario's data by field (never string-templating the textproto).
// ---------------------------------------------------------------------------

/// An IncreaseBy request with the scenario's `n` set on the inner message.
pub fn increase_command(n: u32) -> hpb::HandleRequest {
    let mut req: hpb::HandleRequest = parse_txtpb(HANDLE_REQUEST, SKEL_INCREASE);
    let any = inner_command_any(&mut req);
    let mut inner =
        counter::IncreaseBy::decode(any.value.as_slice()).expect("decode IncreaseBy skeleton");
    inner.n = n;
    any.value = inner.encode_to_vec();
    req
}

/// The FailHard request (no scenario data).
pub fn failhard_command() -> hpb::HandleRequest {
    parse_txtpb(HANDLE_REQUEST, SKEL_FAILHARD)
}

/// A request whose command type has no registered handler (drives
/// ANGZARR_REASON_UNKNOWN_COMMAND_TYPE).
pub fn unhandled_command() -> hpb::HandleRequest {
    parse_txtpb(HANDLE_REQUEST, SKEL_UNHANDLED)
}

/// Prior history of `n` confirmed increases: the Increased skeleton replayed
/// at consecutive sequences, with `next_sequence` continuing past them.
pub fn prior_history(n: u32) -> Option<pb::EventBook> {
    if n == 0 {
        return None;
    }
    let page: pb::EventPage = parse_txtpb(EVENT_PAGE, SKEL_INCREASED_EVENT);
    let pages = (0..u64::from(n))
        .map(|seq| pb::EventPage {
            sequence: seq,
            ..page.clone()
        })
        .collect();
    Some(pb::EventBook {
        pages,
        next_sequence: u64::from(n),
        ..Default::default()
    })
}

/// Prior history whose single Increased event carries undecodable payload
/// bytes (a truncated varint): the applier fails the fold, surfacing
/// ANGZARR_REASON_FOLD_FAILED (a handler bug) when a known command rebuilds
/// over it.
pub fn corrupt_prior_history() -> Option<pb::EventBook> {
    let mut page: pb::EventPage = parse_txtpb(EVENT_PAGE, SKEL_INCREASED_EVENT);
    if let Some(pb::event_page::Body::Event(any)) = page.body.as_mut() {
        any.value = vec![0xff, 0xff, 0xff];
    }
    Some(pb::EventBook {
        pages: vec![page],
        next_sequence: 1,
        ..Default::default()
    })
}

/// A counter delivered a starting state of `count` at `sequence` and one
/// `Increased` event at `event_at`: the pages after the snapshot only, as
/// the coordinator delivers them (decision 14.40).
pub fn starting_state_history(count: u32, sequence: u64, event_at: u64) -> Option<pb::EventBook> {
    Some(pb::EventBook {
        snapshot: Some(pb::Snapshot {
            state: Some(any_of(&CounterState { count })),
            sequence,
            ..Default::default()
        }),
        pages: vec![pb::EventPage {
            sequence: event_at,
            ..parse_txtpb(EVENT_PAGE, SKEL_INCREASED_EVENT)
        }],
        next_sequence: event_at + 1,
        ..Default::default()
    })
}

fn inner_command_any(req: &mut hpb::HandleRequest) -> &mut prost_types::Any {
    let book = req.input.as_mut().expect("an input book");
    match book.page.as_mut().and_then(|p| p.body.as_mut()) {
        Some(pb::command_page::Body::Command(pb::Command {
            payload: Some(any), ..
        })) => any,
        other => panic!("conformance fixtures carry an inline command, got {other:?}"),
    }
}

/// Rewrites every Any type URL in `req` (the command) and its history (the
/// events) to `prefix` + the fully-qualified name.
pub fn with_type_url_prefix(mut req: hpb::HandleRequest, prefix: &str) -> hpb::HandleRequest {
    let rewrite = |any: &mut prost_types::Any| {
        any.type_url = format!(
            "{prefix}{}",
            angzarr_router::type_name_from_url(&any.type_url)
        );
    };
    if let Some(Some(pb::command_page::Body::Command(pb::Command {
        payload: Some(any), ..
    }))) = req
        .input
        .as_mut()
        .and_then(|book| book.page.as_mut())
        .map(|page| page.body.as_mut())
    {
        rewrite(any);
    }
    if let Some(book) = req.history.as_mut() {
        for page in &mut book.pages {
            if let Some(pb::event_page::Body::Event(any)) = page.body.as_mut() {
                rewrite(any);
            }
        }
    }
    req
}

// ---------------------------------------------------------------------------
// Context fixtures: facts, cover access, projector page context
// (context.feature).
// ---------------------------------------------------------------------------

/// The root bytes for a label: UUID v5 in the OID namespace.
pub fn root_of(label: &str) -> Vec<u8> {
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, label.as_bytes())
        .as_bytes()
        .to_vec()
}

/// The wire UUID for a label ([`root_of`]).
pub fn uuid_of(label: &str) -> pb::Uuid {
    pb::Uuid {
        value: root_of(label).into(),
    }
}

/// A cover in `domain` with the root for `label`.
pub fn cover_of(domain: &str, label: &str) -> pb::Cover {
    pb::Cover {
        domain: domain.to_string(),
        root: Some(uuid_of(label)),
        ..Default::default()
    }
}

/// Covers a handler observed.
pub type CoverSink = Arc<Mutex<Vec<Option<pb::Cover>>>>;

/// Page sequences an applier observed.
pub type SequenceSink = Arc<Mutex<Vec<u64>>>;

fn increased_page(seq: u64) -> pb::EventPage {
    event_page(seq, increased_any())
}

/// What a policy handler or a projector fold read of one event: its book's
/// root, its sequence and its whole metadata (context.feature, C-0757 and
/// C-0758).
pub type MetaSink = Arc<Mutex<Vec<(Vec<u8>, u64, Option<pb::EventMeta>)>>>;

/// An event's metadata, every field set: produced by the fact whose command
/// id is the label `command`'s UUID v5 and whose external id is `fact`
/// (the causation's fact arm, decision 14.102), in correlation
/// `correlation`, decided under `rules_version`, recorded at `second`.
pub fn full_meta(
    command: &str,
    correlation: &str,
    fact: &str,
    rules_version: &str,
    second: i64,
) -> pb::EventMeta {
    pb::EventMeta {
        causation: Some(pb::event_meta::Causation::Fact(pb::FactCause {
            command_id: Some(uuid_of(command)),
            origin: Some(pb::FactOrigin {
                external_id: fact.to_string(),
                ..Default::default()
            }),
        })),
        correlation_id: Some(uuid_of(correlation)),
        recorded_at: Some(prost_types::Timestamp {
            seconds: second,
            nanos: 0,
        }),
        rules_version: rules_version.to_string(),
    }
}

/// One Increased event of "counter" root `label` at `sequence`, delivered
/// with `meta`.
pub fn metadata_book(label: &str, sequence: u64, meta: pb::EventMeta) -> pb::EventBook {
    pb::EventBook {
        cover: Some(cover_of("counter", label)),
        pages: vec![pb::EventPage {
            meta: Some(meta),
            ..increased_page(sequence)
        }],
        ..Default::default()
    }
}

/// What a handler's page context holds, as a [`MetaSink`] keeps it.
fn read_back(ctx: &angzarr_router::PageContext<'_>) -> (Vec<u8>, u64, Option<pb::EventMeta>) {
    let root = ctx
        .cover
        .and_then(|c| c.root.as_ref())
        .map(|r| r.value.to_vec())
        .unwrap_or_default();
    (root, ctx.sequence, ctx.meta.cloned())
}

/// The order policy reading back each Increased event's whole context.
pub fn metadata_policy(seen: MetaSink) -> PolicyDispatch {
    PolicyDispatch::new("order-policy", "counter", "inventory").on_event(
        "test.counter.Increased",
        move |_any, ctx| {
            seen.lock().unwrap().push(read_back(ctx));
            Ok(vec![reserve_output()])
        },
    )
}

/// The tracking projector reading back each Increased event's whole
/// context.
pub fn metadata_projector(seen: MetaSink) -> ProjectorDispatch<()> {
    ProjectorDispatch::new("tracker", || ()).on_event(
        "test.counter.Increased",
        move |_p, _e, ctx| {
            seen.lock().unwrap().push(read_back(ctx));
            Ok(())
        },
    )
}

/// The ledger aggregate (domain "ledger") over CounterState: Increased folds
/// count += 1 and records the page sequence it applied; a starting state
/// decodes as CounterState; IncreaseBy records the handled cover and emits one
/// Increased; the only fact, Increased, is recorded as received and flagged
/// by a CounterState carrying the count it brings the ledger to.
pub fn ledger_aggregate(seen: CoverSink, applied: SequenceSink) -> DomainDispatch<CounterState> {
    let rebuilder = Rebuilder::of_message().apply_with_context(
        "test.counter.Increased",
        move |state: &mut CounterState, _, ctx| {
            state.count += 1;
            applied.lock().unwrap().push(ctx.sequence);
            Ok(())
        },
    );
    DomainDispatch::new("ledger", "ledger", rebuilder)
        .on_command("test.counter.IncreaseBy", move |_cmd, _state, cctx| {
            seen.lock().unwrap().push(cctx.cover.clone());
            Ok(Emit::of([increased_any()]))
        })
        .on_fact(
            "test.counter.Increased",
            |fact, state: &CounterState, _ctx| {
                Ok(FactEmit::of([
                    fact.clone(),
                    any_of(&CounterState {
                        count: state.count + 1,
                    }),
                ]))
            },
        )
}

/// An IncreaseBy command for the ledger root `label`.
pub fn ledger_command(label: &str) -> hpb::HandleRequest {
    hpb::HandleRequest {
        input: Some(pb::CommandBook {
            cover: Some(cover_of("ledger", label)),
            page: Some(command_page(any_of(&counter::IncreaseBy { n: 1 }))),
        }),
        ..Default::default()
    }
}

/// [`ledger_command`] over prior Increased events at `sequences`.
pub fn ledger_command_over(label: &str, sequences: &[u64]) -> hpb::HandleRequest {
    let mut req = ledger_command(label);
    req.history = Some(pb::EventBook {
        pages: sequences.iter().map(|s| increased_page(*s)).collect(),
        next_sequence: sequences.last().map_or(0, |s| s + 1),
        ..Default::default()
    });
    req
}

/// A `test.counter.<fact>` fact (empty payload) for the ledger, delivered in
/// the Fact arm over `prior` Increased events.
pub fn ledger_fact(fact: &str, prior: u64) -> hpb::HandleRequest {
    let mut req = ledger_command("ledger-1");
    req.input.as_mut().expect("an input book").page = Some(fact_page(
        prost_types::Any {
            type_url: angzarr_router::type_url(&format!("test.counter.{fact}")),
            value: Vec::new(),
        },
        "fact-1",
    ));
    if prior > 0 {
        req.history = Some(pb::EventBook {
            pages: (0..prior).map(increased_page).collect(),
            next_sequence: prior,
            ..Default::default()
        });
    }
    req
}

/// (root bytes, sequence) pairs a projector fold observed.
pub type PageSink = Arc<Mutex<Vec<(Vec<u8>, u64)>>>;

/// The tracking projector: every Increased fold records its book's root and
/// the page's sequence.
pub fn tracking_projector(seen: PageSink) -> ProjectorDispatch<()> {
    ProjectorDispatch::new("tracker", || ()).on_event(
        "test.counter.Increased",
        move |_p, _e, ctx| {
            let root = ctx
                .cover
                .and_then(|c| c.root.as_ref())
                .map(|r| r.value.to_vec())
                .unwrap_or_default();
            seen.lock().unwrap().push((root, ctx.sequence));
            Ok(())
        },
    )
}

/// A run of Increased events of "counter" root `label` at `sequences`.
pub fn tracked_book(label: &str, sequences: &[u64]) -> hpb::ProjectRequest {
    hpb::ProjectRequest {
        events: Some(pb::EventBook {
            cover: Some(cover_of("counter", label)),
            pages: sequences.iter().map(|s| increased_page(*s)).collect(),
            ..Default::default()
        }),
    }
}

// ---------------------------------------------------------------------------
// The order aggregate and the fulfillment process manager: the
// handler-contract tier (angzarr-project
// features/handler-contract/handling.feature).
// ---------------------------------------------------------------------------

/// What business code saw when it ran.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderObserved {
    /// The fully-qualified type business code received.
    pub handled: String,
    pub item: String,
    /// The item keys of the state it decided on, sorted.
    pub items: Vec<String>,
    /// When its input was accepted, as its context said
    /// (CommandContext.recorded_at; decision 14.37).
    pub recorded_at: Option<prost_types::Timestamp>,
}

/// How the order aggregate's AddItem handler (or a fact handler) fails,
/// when it does (decision 14.37). The text each "logs for itself" is for
/// the handler's own log: it is handed to nothing that crosses a boundary.
#[derive(Debug, Clone)]
pub enum AddItemFails {
    /// A typed refusal: the refusal object ItemOutOfStock, under the
    /// FAILED_PRECONDITION its message declares.
    Refuses { item: String, on_hand: u32 },
    /// An unclassified error whose text is `text`.
    Errs { text: String },
    /// A panic with `text`.
    Panics { text: String },
    /// A bug with its own `code` and `args`: what a key of business code's
    /// own is, a rejection that is not a typed refusal included (decision
    /// 14.95).
    Bug {
        code: String,
        args: Vec<(String, String)>,
        logs: String,
    },
    /// An immediate-return business error ("re-read and re-decide",
    /// ABORTED) under the handler's own `code` and `args`.
    Immediate {
        code: String,
        args: Vec<(String, String)>,
        logs: String,
    },
    /// A bug the handler marks retryable, under the `status` it picks,
    /// with its own `code` and `args`.
    BugRetryable {
        status: RetryableStatus,
        code: String,
        args: Vec<(String, String)>,
        logs: String,
    },
}

impl AddItemFails {
    /// The failure as business code answers it.
    pub fn fail(&self) -> HandlerError {
        match self {
            AddItemFails::Refuses { item, on_hand } => CodedError::refused(
                BusinessStatus::FailedPrecondition,
                any_of(&orders::v1::ItemOutOfStock {
                    item: item.clone(),
                    on_hand: *on_hand,
                }),
            )
            .into(),
            AddItemFails::Errs { text } => HandlerError::Other(text.clone()),
            AddItemFails::Panics { text } => panic!("{text}"),
            AddItemFails::Bug { code, args, logs } => {
                let _ = logs; // the handler's own log
                CodedError::bug(code, args.clone()).into()
            }
            AddItemFails::BugRetryable {
                status,
                code,
                args,
                logs,
            } => {
                let _ = logs; // the handler's own log
                CodedError::retryable_bug(*status, code, args.clone()).into()
            }
            AddItemFails::Immediate { code, args, logs } => {
                let _ = logs; // the handler's own log
                CodedError::immediate(code, args.clone()).into()
            }
        }
    }
}

impl AddItemFails {
    /// The failure as a FACT handler answers it: only a bug (decisions
    /// 14.72 and 14.98), so a refusal or a re-run is the bug it becomes
    /// through the deliberate [`Bug::from_error`].
    pub fn fail_as_bug(&self) -> Bug {
        match self {
            AddItemFails::Errs { text } => Bug::new(text),
            AddItemFails::Bug { code, args, .. } => Bug::coded(code, args.clone()),
            AddItemFails::BugRetryable {
                status, code, args, ..
            } => Bug::retryable(*status, code, args.clone()),
            AddItemFails::Panics { .. }
            | AddItemFails::Refuses { .. }
            | AddItemFails::Immediate { .. } => match self.fail() {
                HandlerError::Coded(coded) => Bug::from_error(coded),
                HandlerError::Other(text) => Bug::new(text),
            },
        }
    }
}

/// What the order aggregate's AddItem returns: its ItemAdded, and the
/// continuation its Emit carries (decision 14.104; C-0848 to C-0852).
#[derive(Debug, Clone, Default)]
pub enum AddItemReturns {
    /// Emit of ItemAdded alone.
    #[default]
    ItemAdded,
    /// Then a step that records OrderNoteAdded "restock <item>" when the
    /// item's quantity reaches 1, and continues nowhere.
    ThenRestockNote,
    /// Then a step that records what it saw in its state into the sink and
    /// emits nothing.
    ThenObserve(OrderSink),
    /// Then a step that fails as given.
    ThenFails(AddItemFails),
    /// Then `n` steps, each recording one OrderNoteAdded and continuing to
    /// the next.
    ThenChain(u32),
}

/// What the order aggregate's ItemDelivered fact handler returns, or how it
/// fails (C-0845, C-0851).
#[derive(Debug, Clone, Default)]
pub enum FactReturns {
    /// Emit of the fact's own event alone.
    #[default]
    Recorded,
    /// Then a step that records OrderNoteAdded "delivered <item>".
    ThenDeliveredNote,
    /// It fails as given.
    Fails(AddItemFails),
}

/// How the order aggregate is built for a scenario.
#[derive(Debug, Clone, Default)]
pub struct OrderBehaviour {
    pub rules_version: String,
    /// How AddItem fails, when it does.
    pub fails: Option<AddItemFails>,
    pub returns: AddItemReturns,
    pub fact: FactReturns,
}

/// Sink the harness passes into the order fixtures.
pub type OrderSink = Arc<Mutex<Vec<OrderObserved>>>;

fn sorted_items(state: &orders::v1::OrderState) -> Vec<String> {
    let mut items: Vec<String> = state.items.keys().cloned().collect();
    items.sort();
    items
}

/// The item whose ItemAdded the order aggregate's fold refuses to fold
/// (C-0670's failing folds).
pub const ROTTEN_ITEM: &str = "rotten";

/// The order state's rebuilder: ItemAdded bumps an item (and fails on
/// [`ROTTEN_ITEM`]), ItemDelivered records a delivery, OrderNoteAdded sets
/// the notes; a starting state decodes as OrderState.
pub fn order_rebuilder() -> Rebuilder<orders::v1::OrderState> {
    Rebuilder::of_message()
        .apply(
            "orders.v1.ItemAdded",
            |state: &mut orders::v1::OrderState, event| {
                let item = orders::v1::ItemAdded::decode(event.value.as_slice())?.item;
                if item == ROTTEN_ITEM {
                    return Err(format!("cannot fold the {ROTTEN_ITEM} item").into());
                }
                *state.items.entry(item).or_default() += 1;
                Ok(())
            },
        )
        .apply(
            "orders.v1.ItemDelivered",
            |state: &mut orders::v1::OrderState, event| {
                let item = orders::v1::ItemDelivered::decode(event.value.as_slice())?.item;
                state.delivered.push(item);
                Ok(())
            },
        )
        .apply(
            "orders.v1.OrderNoteAdded",
            |state: &mut orders::v1::OrderState, event| {
                state.notes = orders::v1::OrderNoteAdded::decode(event.value.as_slice())?.note;
                Ok(())
            },
        )
}

/// An OrderNoteAdded of `note`.
pub fn note(note: &str) -> prost_types::Any {
    any_of(&orders::v1::OrderNoteAdded {
        note: note.to_string(),
    })
}

/// The restock step of C-0848 and C-0853: OrderNoteAdded "restock <item>"
/// while the item's quantity is 1 and no such note is in the state.
pub fn restock_step(
    item: String,
) -> impl Fn(
    &orders::v1::OrderState,
    &angzarr_router::context::CommandContext,
) -> Result<Emit<orders::v1::OrderState>, HandlerError>
       + Clone
       + 'static {
    move |state, _ctx| {
        let wanted = format!("restock {item}");
        if state.items.get(&item) == Some(&1) && state.notes != wanted {
            return Ok(Emit::of([note(&wanted)]));
        }
        Ok(Emit::none())
    }
}

/// `n` chained steps, each recording one OrderNoteAdded and continuing to
/// the next.
fn chain(n: u32) -> Emit<orders::v1::OrderState> {
    let mut emit = Emit::none();
    for k in 1..=n {
        emit = emit.then(move |_state, _ctx| Ok(Emit::of([note(&format!("note {k}"))])));
    }
    emit
}

/// AddItem's answer for `item` under `returns`.
fn add_item_emit(item: String, returns: &AddItemReturns) -> Emit<orders::v1::OrderState> {
    let added = Emit::of([any_of(&orders::v1::ItemAdded { item: item.clone() })]);
    match returns.clone() {
        AddItemReturns::ItemAdded => added,
        AddItemReturns::ThenRestockNote => added.then(restock_step(item)),
        AddItemReturns::ThenObserve(sink) => added.then(move |state, ctx| {
            sink.lock().unwrap().push(OrderObserved {
                handled: "step".to_string(),
                item: String::new(),
                items: sorted_items(state),
                recorded_at: ctx.recorded_at,
            });
            Ok(Emit::none())
        }),
        AddItemReturns::ThenFails(fails) => added.then(move |_state, _ctx| Err(fails.fail())),
        AddItemReturns::ThenChain(n) => added.then(move |_state, _ctx| Ok(chain(n))),
    }
}

/// The order aggregate (domain "order") behaving as `behaviour`: AddItem
/// records what business code saw in `seen`, then fails or answers its
/// Emit; Acknowledge answers no events; the fact ItemDelivered is recorded
/// as received (or as `behaviour.fact` says).
pub fn order_aggregate(
    behaviour: OrderBehaviour,
    seen: OrderSink,
) -> DomainDispatch<orders::v1::OrderState> {
    let facts = seen.clone();
    let OrderBehaviour {
        rules_version,
        fails,
        returns,
        fact,
    } = behaviour;
    DomainDispatch::new("order", "order", order_rebuilder())
        .with_rules_version(rules_version)
        .on_command("orders.v1.AddItem", move |cmd, state, cctx| {
            let item = angzarr_router::decode_message::<orders::v1::AddItem>(&cmd.value)?.item;
            seen.lock().unwrap().push(OrderObserved {
                handled: angzarr_router::type_name_from_url(&cmd.type_url).to_string(),
                item: item.clone(),
                items: sorted_items(state),
                recorded_at: cctx.recorded_at,
            });
            if let Some(fails) = &fails {
                return Err(fails.fail());
            }
            Ok(add_item_emit(item, &returns))
        })
        // Acknowledge: business code answers with no events.
        .on_command("orders.v1.Acknowledge", |_cmd, _state, _cctx| {
            Ok(Emit::none())
        })
        .on_fact("orders.v1.ItemDelivered", move |payload, state, cctx| {
            // A payload that does not decode is ANY_DECODE_FAILED, naming
            // its message type (C-0670).
            let item = angzarr_router::decode_message::<orders::v1::ItemDelivered>(&payload.value)
                .bug()?
                .item;
            facts.lock().unwrap().push(OrderObserved {
                handled: angzarr_router::type_name_from_url(&payload.type_url).to_string(),
                item: item.clone(),
                items: sorted_items(state),
                recorded_at: cctx.recorded_at,
            });
            let recorded = FactEmit::of([payload.clone()]);
            match &fact {
                FactReturns::Recorded => Ok(recorded),
                FactReturns::ThenDeliveredNote => {
                    let delivered = format!("delivered {item}");
                    Ok(recorded.then(move |_state, _ctx| Ok(FactEmit::of([note(&delivered)]))))
                }
                FactReturns::Fails(fails) => Err(fails.fail_as_bug()),
            }
        })
}

fn order_event(kind: &str, value: &str) -> prost_types::Any {
    match kind {
        "ItemAdded" => any_of(&orders::v1::ItemAdded {
            item: value.to_string(),
        }),
        "OrderNoteAdded" => note(value),
        other => panic!("no order event {other}"),
    }
}

/// The order stream's history of `history` ((event kind, value) at
/// sequences 0..) and an optional delivered snapshot.
pub fn order_history(
    history: &[(String, String)],
    snapshot: Option<pb::Snapshot>,
) -> pb::EventBook {
    pb::EventBook {
        cover: Some(cover_of("order", "order-1")),
        snapshot,
        pages: history
            .iter()
            .zip(0u64..)
            .map(|((kind, value), sequence)| event_page(sequence, order_event(kind, value)))
            .collect(),
        next_sequence: history.len() as u64,
    }
}

/// An `orders.v1.<command>` command for `item` to the order aggregate, over
/// `history` and an optional delivered snapshot.
pub fn order_command(
    command: &str,
    item: &str,
    history: &[(String, String)],
    snapshot: Option<pb::Snapshot>,
) -> hpb::HandleRequest {
    hpb::HandleRequest {
        input: Some(pb::CommandBook {
            cover: Some(cover_of("order", "order-1")),
            page: Some(command_page(prost_types::Any {
                type_url: angzarr_router::type_url(&format!("orders.v1.{command}")),
                // Every orders.v1 command carries its item in field 1.
                value: orders::v1::AddItem {
                    item: item.to_string(),
                }
                .encode_to_vec(),
            })),
        }),
        history: Some(order_history(history, snapshot)),
        recorded_at: None,
        causation: None,
    }
}

/// An `orders.v1.<fact>` fact for `item`, delivered in the Fact arm over
/// `history`.
pub fn order_fact(fact: &str, item: &str, history: &[(String, String)]) -> hpb::HandleRequest {
    let mut req = order_command("AddItem", item, history, None);
    req.input.as_mut().expect("an input book").page = Some(fact_page(
        prost_types::Any {
            type_url: angzarr_router::type_url(&format!("orders.v1.{fact}")),
            value: orders::v1::ItemDelivered {
                item: item.to_string(),
            }
            .encode_to_vec(),
        },
        "delivery-1",
    ));
    req
}

/// A delivered snapshot of OrderState holding `items`, recorded under
/// `rules_version` (which the router never judges: decision 14.1).
pub fn order_snapshot(items: &[&str], rules_version: &str) -> pb::Snapshot {
    pb::Snapshot {
        sequence: 0,
        state: Some(any_of(&orders::v1::OrderState {
            items: items.iter().map(|i| (i.to_string(), 1)).collect(),
            ..Default::default()
        })),
        rules_version: rules_version.to_string(),
    }
}

/// The fulfillment process manager at `rules_version`: a domain (decision
/// 14.31) whose StartFulfillment command, its starting input, records one
/// FulfillmentStarted event when `writes` says so (it counts into its
/// state's `placed`), or nothing.
pub fn fulfillment_pm(
    rules_version: &str,
    writes: Writes,
) -> DomainDispatch<orders::v1::FulfillmentState> {
    let rebuilder = Rebuilder::of_message().apply(
        "orders.v1.FulfillmentStarted",
        |state: &mut orders::v1::FulfillmentState, _event| {
            state.placed += 1;
            Ok(())
        },
    );
    DomainDispatch::process_manager("fulfillment", "fulfillment", rebuilder)
        .with_rules_version(rules_version)
        .on_command("orders.v1.StartFulfillment", move |_c, _s, _ctx| {
            Ok(match writes {
                Writes::FulfillmentStarted => {
                    Emit::of([any_of(&orders::v1::FulfillmentStarted {})])
                }
                Writes::Nothing => Emit::none(),
            })
        })
}

/// Whether the fulfillment process manager writes its event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Writes {
    FulfillmentStarted,
    Nothing,
}

/// A StartFulfillment command for the fulfillment instance of correlation
/// "order-1" (its root; decision 14.30), over no prior events.
pub fn start_fulfillment() -> hpb::HandleRequest {
    hpb::HandleRequest {
        input: Some(pb::CommandBook {
            cover: Some(cover_of("fulfillment", "order-1")),
            page: Some(command_page(any_of(&orders::v1::StartFulfillment {}))),
        }),
        ..Default::default()
    }
}

/// The fulfillment process manager's state with `placed`, as its process
/// state image.
pub fn fulfillment_state(placed: u32) -> prost_types::Any {
    any_of(&orders::v1::FulfillmentState { placed })
}

/// An order state image read back: its type URL, its items (sorted) with
/// their quantities, its delivered items in order, and its notes. (Map
/// encoding order is unspecified, so images compare decoded.)
pub fn read_order_state(
    image: &prost_types::Any,
) -> (String, Vec<(String, u32)>, Vec<String>, String) {
    let state = orders::v1::OrderState::decode(image.value.as_slice()).expect("OrderState");
    let mut items: Vec<(String, u32)> = state.items.into_iter().collect();
    items.sort();
    (image.type_url.clone(), items, state.delivered, state.notes)
}

/// An "order" trigger book: one OrderPlaced event of the order stream
/// "order-1" at `sequence`.
pub fn order_placed_trigger(sequence: u64) -> pb::EventBook {
    pb::EventBook {
        cover: Some(cover_of("order", "order-1")),
        pages: vec![event_page(sequence, any_of(&orders::v1::OrderPlaced {}))],
        ..Default::default()
    }
}

/// The router's status of a gRPC code's name (`FAILED_PRECONDITION`), as
/// the features spell it.
pub fn status_named(name: &str) -> angzarr_router::error::GrpcCode {
    use angzarr_router::error::GrpcCode;
    match name {
        "UNKNOWN" => GrpcCode::Unknown,
        "INVALID_ARGUMENT" => GrpcCode::InvalidArgument,
        "DEADLINE_EXCEEDED" => GrpcCode::DeadlineExceeded,
        "NOT_FOUND" => GrpcCode::NotFound,
        "ALREADY_EXISTS" => GrpcCode::AlreadyExists,
        "PERMISSION_DENIED" => GrpcCode::PermissionDenied,
        "RESOURCE_EXHAUSTED" => GrpcCode::ResourceExhausted,
        "FAILED_PRECONDITION" => GrpcCode::FailedPrecondition,
        "ABORTED" => GrpcCode::Aborted,
        "OUT_OF_RANGE" => GrpcCode::OutOfRange,
        "UNIMPLEMENTED" => GrpcCode::Unimplemented,
        "INTERNAL" => GrpcCode::Internal,
        "UNAVAILABLE" => GrpcCode::Unavailable,
        "DATA_LOSS" => GrpcCode::DataLoss,
        other => panic!("no status {other}"),
    }
}

#[cfg(test)]
mod smoke {
    use super::*;

    /// The whole pipeline works: compile the fixtures, extern the framework
    /// types to the router crate, build the pool, and parse an Any-expanded
    /// request skeleton into the router's own HandleRequest.
    #[test]
    fn parses_the_increase_request_skeleton() {
        let req: hpb::HandleRequest = parse_txtpb(HANDLE_REQUEST, SKEL_INCREASE);
        let book = req.input.expect("an input book");
        assert_eq!(book.cover.as_ref().expect("cover").domain, "counter");
        let any = angzarr_router::command_payload(book.page.as_ref().expect("page"))
            .expect("command any");
        assert!(any.type_url.ends_with("test.counter.IncreaseBy"));
    }
}
