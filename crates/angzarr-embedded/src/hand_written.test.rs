//! A **counter** aggregate and a **notes** policy written by hand, with no
//! macro: every impl the markup would generate (`Payload`, `State`,
//! `Domain`, `Routed`, `Decides`, `Policy`) is written here through
//! `markup`'s public functions, the ones the expansion calls. On
//! angzarr-embedded as a dependent sees it, `::angzarr_embedded`, so
//! nothing private is reached: what this compiles against is the API a
//! hand-written component has.
//!
//! What is pinned: a hand-written aggregate registered with
//! `Registry::aggregate` decides its commands as one decision (its event,
//! or its typed refusal with nothing committed), and a hand-written policy
//! registered with `Registry::policy` answers its source's event with a
//! command to another root of its own domain (never the trigger's own
//! stream, decision 14.71), which the kernel runs as the next step. A policy's target known by its name
//! and commands alone (`CommandTarget`), a domain a coordinator elsewhere
//! owns (`Registry::remote`), needs no stand-in aggregate: its step goes
//! to that coordinator. A policy's source known by its name and events
//! alone (`EventSource`), a domain a child process serves
//! (`Registry::command_handler`), needs none either: its events set the
//! policy off as a typed domain's do.

use std::convert::Infallible;
use std::fmt;
use std::sync::{Arc, Mutex};

use ::angzarr_embedded::angzarr_proto::v1::{
    component_descriptor, AggregateDescriptor, ComponentDescriptor,
};
use ::angzarr_embedded::angzarr_router::domain::DomainDispatch;
use ::angzarr_embedded::angzarr_router::error::CodedError;
use ::angzarr_embedded::angzarr_router::hpb;
use ::angzarr_embedded::angzarr_router::router::CommandHandler;
use ::angzarr_embedded::markup::{
    self, CommandTarget, Commands, Context, Decides, DomainMessage, EventSource, Failed, Ingests,
    Policy, PolicyContext, Refusing, Routed, TypeName, TypedRefusal, Unrefusable,
};
use ::angzarr_embedded::v1::{
    command_page, reaction_delivery, Command, CommandBook, CommandPage, Cover, DeliveryOutcome,
    EventBook, EventPage, ReactionDelivery, StateDescriptor, Uuid,
};
use ::angzarr_embedded::{
    type_name, Answered, Any, Coordinated, CoordinatorPort, Domain, Failure, Handled, Kernel,
    MemoryDeadLetters, OnRetryable, Payload, PayloadError, Registry, Sampled, State, Timestamp,
    WireError,
};
use coded_diagnostic::{Arg, Diagnostic, Report};

// ---------------------------------------------------------------- the wire

/// The prost messages the counter's types travel as.
mod wire {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Bump {
        #[prost(int64, tag = "1")]
        pub by: i64,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Bumped {
        #[prost(int64, tag = "1")]
        pub by: i64,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Note {}

    /// A typed refusal object: a bump by zero or less.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct NotPositive {
        #[prost(int64, tag = "1")]
        pub by: i64,
    }

    impl ::prost::Name for NotPositive {
        const NAME: &'static str = "NotPositive";
        const PACKAGE: &'static str = "toy.counter.v1";
    }

    /// A typed refusal object: a bump past the counter's limit.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TooBig {
        #[prost(int64, tag = "1")]
        pub by: i64,
    }

    impl ::prost::Name for TooBig {
        const NAME: &'static str = "TooBig";
        const PACKAGE: &'static str = "toy.counter.v1";
    }

    /// A command of the remote bank's, which nothing here decides.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Deposit {
        #[prost(int64, tag = "1")]
        pub cents: i64,
    }

    impl ::prost::Name for Deposit {
        const NAME: &'static str = "Deposit";
        const PACKAGE: &'static str = "toy.bank.v1";
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Noted {}

    /// The counter's anchor: the state it registers, and never snapshots.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CounterState {
        #[prost(int64, tag = "1")]
        pub total: i64,
    }

    impl ::prost::Name for CounterState {
        const NAME: &'static str = "CounterState";
        const PACKAGE: &'static str = "toy.counter.v1";
    }

    impl ::prost::Name for Bump {
        const NAME: &'static str = "Bump";
        const PACKAGE: &'static str = "toy.counter.v1";
    }

    impl ::prost::Name for Bumped {
        const NAME: &'static str = "Bumped";
        const PACKAGE: &'static str = "toy.counter.v1";
    }

    impl ::prost::Name for Note {
        const NAME: &'static str = "Note";
        const PACKAGE: &'static str = "toy.counter.v1";
    }

    impl ::prost::Name for Noted {
        const NAME: &'static str = "Noted";
        const PACKAGE: &'static str = "toy.counter.v1";
    }
}

// ------------------------------------------------------- the domain types

#[derive(Clone, Debug, PartialEq)]
pub struct Bump {
    by: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Bumped {
    by: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Note;

#[derive(Clone, Debug, PartialEq)]
pub struct Noted;

#[derive(Clone, Debug, PartialEq)]
pub struct Deposit {
    cents: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TooBig {
    by: i64,
}

impl DomainMessage for TooBig {
    type Wire = wire::TooBig;

    fn to_wire(&self) -> wire::TooBig {
        wire::TooBig { by: self.by }
    }

    fn from_wire(wire: wire::TooBig) -> Result<Self, WireError> {
        Ok(TooBig { by: wire.by })
    }
}

impl fmt::Display for TooBig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        coded_diagnostic::display(self, f)
    }
}

impl Diagnostic for TooBig {
    fn code(&self) -> &str {
        "toy-counter-too-big"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("by", self.by.into())]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NotPositive {
    by: i64,
}

impl DomainMessage for NotPositive {
    type Wire = wire::NotPositive;

    fn to_wire(&self) -> wire::NotPositive {
        wire::NotPositive { by: self.by }
    }

    fn from_wire(wire: wire::NotPositive) -> Result<Self, WireError> {
        Ok(NotPositive { by: wire.by })
    }
}

impl DomainMessage for Deposit {
    type Wire = wire::Deposit;

    fn to_wire(&self) -> wire::Deposit {
        wire::Deposit { cents: self.cents }
    }

    fn from_wire(wire: wire::Deposit) -> Result<Self, WireError> {
        Ok(Deposit { cents: wire.cents })
    }
}

impl DomainMessage for Bump {
    type Wire = wire::Bump;

    fn to_wire(&self) -> wire::Bump {
        wire::Bump { by: self.by }
    }

    fn from_wire(wire: wire::Bump) -> Result<Self, WireError> {
        Ok(Bump { by: wire.by })
    }
}

impl DomainMessage for Bumped {
    type Wire = wire::Bumped;

    fn to_wire(&self) -> wire::Bumped {
        wire::Bumped { by: self.by }
    }

    fn from_wire(wire: wire::Bumped) -> Result<Self, WireError> {
        Ok(Bumped { by: wire.by })
    }
}

impl DomainMessage for Note {
    type Wire = wire::Note;

    fn to_wire(&self) -> wire::Note {
        wire::Note {}
    }

    fn from_wire(_wire: wire::Note) -> Result<Self, WireError> {
        Ok(Note)
    }
}

impl DomainMessage for Noted {
    type Wire = wire::Noted;

    fn to_wire(&self) -> wire::Noted {
        wire::Noted {}
    }

    fn from_wire(_wire: wire::Noted) -> Result<Self, WireError> {
        Ok(Noted)
    }
}

// ------------------------------------------------------- the payload sets

/// The counter's commands, as one payload.
#[derive(Clone, Debug)]
pub enum CounterCommand {
    Bump(Bump),
    Note(Note),
}

impl Payload for CounterCommand {
    fn to_any(&self) -> Any {
        match self {
            CounterCommand::Bump(command) => markup::pack_message(command),
            CounterCommand::Note(command) => markup::pack_message(command),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        if let Some(command) = markup::decode_message::<Bump>(any)? {
            return Ok(CounterCommand::Bump(command));
        }
        if let Some(command) = markup::decode_message::<Note>(any)? {
            return Ok(CounterCommand::Note(command));
        }
        Err(PayloadError::UnknownType {
            type_url: any.type_url.clone(),
        })
    }

    fn type_urls() -> Vec<String> {
        vec![markup::type_url_of::<Bump>(), markup::type_url_of::<Note>()]
    }
}

/// The counter's events, as one payload.
#[derive(Clone, Debug)]
pub enum CounterEvent {
    Bumped(Bumped),
    Noted(Noted),
}

impl From<Bumped> for CounterEvent {
    fn from(event: Bumped) -> Self {
        CounterEvent::Bumped(event)
    }
}

impl From<Noted> for CounterEvent {
    fn from(event: Noted) -> Self {
        CounterEvent::Noted(event)
    }
}

impl Payload for CounterEvent {
    fn to_any(&self) -> Any {
        match self {
            CounterEvent::Bumped(event) => markup::pack_message(event),
            CounterEvent::Noted(event) => markup::pack_message(event),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        if let Some(event) = markup::decode_message::<Bumped>(any)? {
            return Ok(CounterEvent::Bumped(event));
        }
        if let Some(event) = markup::decode_message::<Noted>(any)? {
            return Ok(CounterEvent::Noted(event));
        }
        Err(PayloadError::UnknownType {
            type_url: any.type_url.clone(),
        })
    }

    fn type_urls() -> Vec<String> {
        vec![
            markup::type_url_of::<Bumped>(),
            markup::type_url_of::<Noted>(),
        ]
    }
}

// --------------------------------------------------------- the aggregate

/// The typed refusals a bump lists (`refuses(NotPositive)`): the choice
/// the markup would build.
#[derive(Clone, Debug, PartialEq)]
pub enum BumpRefuses {
    NotPositive(NotPositive),
}

impl From<BumpRefuses> for TypedRefusal {
    fn from(choice: BumpRefuses) -> TypedRefusal {
        match choice {
            BumpRefuses::NotPositive(object) => TypedRefusal::of(&object),
        }
    }
}

impl markup::Fails<Counter> for BumpRefuses {
    fn into_failed(self) -> Failed<TypedRefusal> {
        Failed::rejected(self)
    }
}

impl Refusing<Bump> for Counter {
    type Refuses = BumpRefuses;
    const REFUSES: &'static [TypeName] = &[TypeName::of::<wire::NotPositive>()];
}

/// A running total, written by hand.
#[derive(Clone, Debug, Default)]
pub struct Counter {
    total: i64,
}

impl Counter {
    fn bump(&self, command: Bump, _ctx: &Context) -> Result<Bumped, BumpRefuses> {
        match command.by > 0 {
            true => Ok(Bumped { by: command.by }),
            false => Err(BumpRefuses::NotPositive(NotPositive { by: command.by })),
        }
    }

    /// A note lists no refusal: it cannot refuse.
    fn note(&self, _command: Note, _ctx: &Context) -> Result<Noted, Failed<Unrefusable>> {
        Ok(Noted)
    }
}

/// Registered as its anchor, and keeping both defaults: it offers no
/// snapshot, so every load replays.
impl State for Counter {
    type Anchor = wire::CounterState;
}

impl Domain for Counter {
    type State = Self;
    type Command = CounterCommand;
    type Fact = Infallible;
    type Event = CounterEvent;
    type Rejection = TypedRefusal;
    const RULES_VERSION: &'static str = "v1";

    const NAME: &'static str = "counter";

    fn apply(state: &mut Self, event: &CounterEvent, _: &::angzarr_embedded::PageContext<'_>) {
        if let CounterEvent::Bumped(bumped) = event {
            state.total += bumped.by;
        }
    }
}

impl Routed for Counter {
    fn route(table: DomainDispatch<Counter>) -> DomainDispatch<Counter> {
        table
            .on_command(
                &TypeName::of::<wire::Bump>().full_name(),
                |any, state, cctx| {
                    markup::run_command::<Counter, Bump, _, _>(any, state, cctx, Counter::bump)
                },
            )
            .on_command(
                &TypeName::of::<wire::Note>().full_name(),
                |any, state, cctx| {
                    markup::run_command::<Counter, Note, _, _>(any, state, cctx, Counter::note)
                },
            )
    }
}

impl Decides<Bump> for Counter {}
impl Decides<Note> for Counter {}

// ------------------------------------------------------------- the policy

/// Notes every bump on the notes counter, [`notes`]: its own domain at
/// another root, as a policy may address it (never its trigger's own
/// stream, decision 14.71).
pub struct NoteBumps;

impl Policy for NoteBumps {
    const NAME: &'static str = "counter-notes";
    type Source = Counter;
    type Target = Counter;
    const EVENTS: &'static [TypeName] = &[TypeName::of::<wire::Bumped>()];

    fn on_event(
        &self,
        event: &Any,
        _: &PolicyContext,
    ) -> Result<Option<Commands<Counter>>, Report> {
        let Some(_bumped) = markup::decode_message::<Bumped>(event).map_err(|e| Report::of(&e))?
        else {
            return Ok(None);
        };
        markup::policy_answer::<Counter, Infallible>(Ok(Commands::none().to(notes(), Note)))
    }
}

// ------------------------------------------------- a remote domain's target

/// The bank's commands: a domain a coordinator elsewhere owns, known here
/// by its name and the commands it decides alone. No aggregate stands in
/// for it.
pub struct Bank;

impl CommandTarget for Bank {
    const DOMAIN: &'static str = "bank";

    fn command_types() -> Vec<String> {
        vec![TypeName::of::<wire::Deposit>().full_name()]
    }
}

impl Decides<Deposit> for Bank {}

/// Banks every bump: a deposit to the bank account of the counter's own
/// root.
pub struct BankBumps;

impl Policy for BankBumps {
    const NAME: &'static str = "counter-bank";
    type Source = Counter;
    type Target = Bank;
    const EVENTS: &'static [TypeName] = &[TypeName::of::<wire::Bumped>()];

    fn on_event(&self, event: &Any, ctx: &PolicyContext) -> Result<Option<Commands<Bank>>, Report> {
        let Some(bumped) = markup::decode_message::<Bumped>(event).map_err(|e| Report::of(&e))?
        else {
            return Ok(None);
        };
        markup::policy_answer::<Bank, Infallible>(Ok(Commands::none().to(
            ctx.source_root().clone(),
            Deposit {
                cents: bumped.by * 100,
            },
        )))
    }
}

/// The bank's coordinator, elsewhere: every reaction it is delivered,
/// each answered with one `Deposited`. A client's book is handled alike.
#[derive(Clone, Default)]
struct BankCoordinator {
    delivered: Arc<Mutex<Vec<ReactionDelivery>>>,
}

impl BankCoordinator {
    /// The bank's answer for `cover`: one `Deposited`, applied.
    fn deposited(cover: Option<Cover>) -> Coordinated<'static> {
        Box::pin(async move {
            Ok(Answered {
                events: Some(EventBook {
                    cover,
                    pages: vec![EventPage {
                        body: Some(::angzarr_embedded::v1::event_page::Body::Event(Any {
                            type_url: "/toy.bank.v1.Deposited".to_string(),
                            value: Vec::new(),
                        })),
                        ..EventPage::default()
                    }],
                    ..EventBook::default()
                }),
                delivery: DeliveryOutcome::Applied,
            })
        })
    }
}

impl CoordinatorPort for BankCoordinator {
    fn handle(&self, book: CommandBook) -> Coordinated<'_> {
        BankCoordinator::deposited(book.cover)
    }

    fn deliver(&self, delivery: ReactionDelivery) -> Coordinated<'_> {
        let cover = delivery.cover.clone();
        self.delivered.lock().unwrap().push(delivery);
        BankCoordinator::deposited(cover)
    }
}

// ---------------------------------------------------------------- running

type Mem = ::angzarr_embedded::angzarr_ports::memory::MemoryStore;

fn root() -> Uuid {
    Uuid::from_bytes([0x3c; 16])
}

/// The counter [`NoteBumps`] notes every bump on.
fn notes() -> Uuid {
    Uuid::from_bytes([0x3d; 16])
}

/// A bump by `by` of the counter at [`root`], as a client sends it.
fn bump(by: i64) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: Counter::NAME.to_string(),
            root: Some(root()),
            edition: None,
        }),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(markup::pack_message(&Bump { by })),
                handling: None,
            })),
            ..CommandPage::default()
        }),
    }
}

/// `book` through the kernel and a fresh store, every policy step it
/// sets off included: each committed step's stream root and event types.
fn settle(registry: &Registry, book: CommandBook) -> Result<Vec<(Uuid, Vec<String>)>, String> {
    let store = Mem::default();
    let log = MemoryDeadLetters::new();
    let sampled = Sampled {
        now: Timestamp {
            seconds: 1,
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x71; 16]),
    };
    let kernel = Kernel::new(registry)
        .log_with(&log)
        .on_retryable(OnRetryable {
            max_retries: 0,
            ..OnRetryable::default()
        });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let handled = runtime.block_on(kernel.handle::<Counter, _>(&store, book, &sampled));
    match handled {
        Ok(Handled::Applied(recorded)) => Ok(recorded
            .steps()
            .iter()
            .map(|book| {
                let root = book.cover.as_ref().and_then(|c| c.root.clone());
                let types = book
                    .pages
                    .iter()
                    .map(|p| {
                        let event = ::angzarr_embedded::angzarr_router::page_event(p).unwrap();
                        type_name(&event.type_url).to_string()
                    })
                    .collect();
                (root.unwrap_or_default(), types)
            })
            .collect()),
        Ok(_) => Err("not applied".to_string()),
        Err(Failure::Rejected(rejected)) => Err(rejected.code().to_string()),
        Err(other) => Err(format!("{other:?}")),
    }
}

/// The counter and its policy, registered as the markup's are.
fn registry() -> Registry {
    let mut registry = Registry::new();
    registry
        .aggregate::<Counter>(Counter::default)
        .unwrap()
        .policy(NoteBumps)
        .unwrap();
    registry
}

/// A hand-written aggregate decides its command, and a hand-written
/// policy answers the event with a command to the trigger's own root,
/// which the counter decides as the next step.
#[test]
fn a_hand_written_aggregate_and_policy_run_as_marked_up_ones_do() {
    assert_eq!(
        settle(&registry(), bump(2)),
        Ok(vec![
            (root(), vec!["toy.counter.v1.Bumped".to_string()]),
            (notes(), vec!["toy.counter.v1.Noted".to_string()]),
        ])
    );
}

/// A hand-written aggregate's typed refusal refuses the command under
/// `ANGZARR_REASON_COMMAND_REFUSED`, and nothing runs after it.
#[test]
fn a_hand_written_aggregates_refusal_is_the_callers() {
    assert_eq!(
        settle(&registry(), bump(0)),
        Err("ANGZARR_REASON_COMMAND_REFUSED".to_string())
    );
}

/// A hand-written aggregate's refusal comes back typed, as the domain's
/// rejection: the typed refusal its handler made, its object matched as a
/// value, never by its code.
#[test]
fn a_hand_written_aggregates_refusal_comes_back_typed_as_its_rejection() {
    let registry = registry();
    let store = Mem::default();
    let sampled = Sampled {
        now: Timestamp {
            seconds: 1,
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x71; 16]),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let failure = runtime
        .block_on(Kernel::new(&registry).handle::<Counter, _>(&store, bump(0), &sampled))
        .err()
        .expect("refused");
    let rejection = failure
        .rejection::<<Counter as Domain>::Rejection>()
        .expect("typed");
    assert_eq!(
        rejection.object::<NotPositive>(),
        Some(NotPositive { by: 0 })
    );
    let rejected = match failure {
        Failure::Rejected(rejected) => rejected,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        rejected.refusal::<NotPositive>(),
        Some(NotPositive { by: 0 })
    );
}

/// The hand-written payload sets round-trip through the public packing
/// functions, and a payload of no type of theirs is unknown, by its type.
#[test]
fn hand_written_payloads_round_trip() {
    let packed = CounterCommand::Bump(Bump { by: 4 }).to_any();
    assert_eq!(packed.type_url, "/toy.counter.v1.Bump");
    assert!(matches!(
        CounterCommand::from_any(&packed),
        Ok(CounterCommand::Bump(Bump { by: 4 }))
    ));
    let other = markup::pack_message(&Noted);
    assert!(matches!(
        CounterCommand::from_any(&other),
        Err(PayloadError::UnknownType { type_url }) if type_url == "/toy.counter.v1.Noted"
    ));
}

/// A policy whose target is a remote domain's command set: the counter's
/// event sets off a step the kernel hands the bank's coordinator, its
/// command for the bank account of the trigger's root, with no aggregate
/// of the bank's here.
#[test]
fn a_policy_targets_a_remote_domain_by_its_command_set_alone() {
    let bank = BankCoordinator::default();
    let mut registry = Registry::new();
    registry.aggregate::<Counter>(Counter::default).unwrap();
    registry.remote(Bank::DOMAIN, bank.clone()).unwrap();
    registry.policy(BankBumps).unwrap();
    // Only the counter's step is committed here: the bank's events are its
    // coordinator's.
    assert_eq!(
        settle(&registry, bump(3)),
        Ok(vec![(root(), vec!["toy.counter.v1.Bumped".to_string()])])
    );
    let delivered = bank.delivered.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    let cover = delivered[0].cover.clone().unwrap_or_default();
    assert_eq!((cover.domain.as_str(), cover.root), ("bank", Some(root())));
    assert_eq!(
        delivered[0].body,
        Some(reaction_delivery::Body::Command(markup::pack_message(
            &Deposit { cents: 300 }
        )))
    );
    assert_eq!(delivered[0].component, "counter-bank");
}

/// A domain's command set, as a policy's target reads it, is the
/// domain's: its name and the full names of its commands.
#[test]
fn a_domain_is_its_own_command_target() {
    assert_eq!(<Counter as CommandTarget>::DOMAIN, "counter");
    assert_eq!(
        <Counter as CommandTarget>::command_types(),
        ["toy.counter.v1.Bump", "toy.counter.v1.Note"]
    );
}

/// A typed refusal object reads back as its own type, and as no other.
#[test]
fn a_typed_refusal_reads_back_as_its_type() {
    let refusal = markup::TypedRefusal::of(&TooBig { by: 9 });
    assert_eq!(refusal.object::<TooBig>(), Some(TooBig { by: 9 }));
    assert_eq!(refusal.object::<Bump>(), None);
}

// ------------------------------------------------ a child-hosted source

/// The player's events: a domain a child process serves (not Rust), known
/// here by its name and the events it records. No aggregate stands in for
/// it.
pub struct Player;

const MOVE_DECIDED: &str = "toy.player.v1.MoveDecided";

impl EventSource for Player {
    const DOMAIN: &'static str = "player";

    fn event_types() -> Vec<String> {
        vec![MOVE_DECIDED.to_string()]
    }
}

/// The fact a Translate callback of the player's policy would answer: one
/// the player ingests, written beside its event set as `Decides` is beside
/// a command set.
impl Ingests<Noted> for Player {}

/// The child, as the host registers it: every command is answered with
/// one `MoveDecided`.
struct PlayerChild;

impl CommandHandler for PlayerChild {
    fn name(&self) -> &str {
        "Player"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.player.v1.Decide".to_string()]
    }

    async fn dispatch(&self, _: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        Ok(hpb::HandleResponse {
            events: vec![Any {
                type_url: format!("/{MOVE_DECIDED}"),
                value: Vec::new(),
            }],
            state: None,
            rules_version: "player-1".to_string(),
        })
    }
}

/// The child's descriptor, as it answers `Describe`.
fn player_descriptor() -> ComponentDescriptor {
    ComponentDescriptor {
        name: "Player".to_string(),
        rules_version: "player-1".to_string(),
        kind: Some(component_descriptor::Kind::Aggregate(AggregateDescriptor {
            domain: Player::DOMAIN.to_string(),
            state: Some(StateDescriptor {
                type_name: "toy.player.v1.State".to_string(),
            }),
        })),
    }
}

/// Bumps the counter of the player's own root for every move the player
/// decides. It names no `EVENTS`, so it reads every event its source
/// records: the source's `event_types`.
pub struct PlayerBumps;

impl Policy for PlayerBumps {
    const NAME: &'static str = "player-bumps";
    type Source = Player;
    type Target = Counter;

    fn on_event(
        &self,
        event: &Any,
        ctx: &PolicyContext,
    ) -> Result<Option<Commands<Counter>>, Report> {
        if type_name(&event.type_url) != MOVE_DECIDED {
            return Ok(None);
        }
        markup::policy_answer::<Counter, Infallible>(Ok(
            Commands::none().to(ctx.source_root().clone(), Bump { by: 1 })
        ))
    }
}

/// A decision of the player at [`root`], as a client sends it.
fn decide() -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: Player::DOMAIN.to_string(),
            root: Some(root()),
            edition: None,
        }),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(Any {
                    type_url: "/toy.player.v1.Decide".to_string(),
                    value: Vec::new(),
                }),
                handling: None,
            })),
            ..CommandPage::default()
        }),
    }
}

/// A policy whose source is a child-hosted domain's event set: the
/// player's event, recorded here through its child, sets off the
/// counter's step, with no aggregate of the player's here.
#[test]
fn a_policy_is_sourced_from_a_child_hosted_domain_by_its_event_set_alone() {
    let mut registry = Registry::new();
    registry.aggregate::<Counter>(Counter::default).unwrap();
    registry
        .command_handler(Player::DOMAIN, PlayerChild, player_descriptor())
        .unwrap();
    registry.policy(PlayerBumps).unwrap();
    let store = Mem::default();
    let log = MemoryDeadLetters::new();
    let sampled = Sampled {
        now: Timestamp {
            seconds: 1,
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x72; 16]),
    };
    let kernel = Kernel::new(&registry).log_with(&log);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let handled = runtime.block_on(kernel.handle_any(&store, decide(), &sampled));
    let recorded = match handled {
        Ok(Handled::Applied(recorded)) => recorded,
        Ok(_) => panic!("not applied"),
        Err(failure) => panic!("{failure:?}"),
    };
    let steps: Vec<(String, Vec<String>)> = recorded
        .steps()
        .iter()
        .map(|book| {
            let domain = book.cover.as_ref().map(|c| c.domain.clone());
            let types = book
                .pages
                .iter()
                .map(|p| {
                    let event = ::angzarr_embedded::angzarr_router::page_event(p).unwrap();
                    type_name(&event.type_url).to_string()
                })
                .collect();
            (domain.unwrap_or_default(), types)
        })
        .collect();
    assert_eq!(
        steps,
        [
            ("player".to_string(), vec![MOVE_DECIDED.to_string()]),
            (
                "counter".to_string(),
                vec!["toy.counter.v1.Bumped".to_string()]
            ),
        ]
    );
    assert!(log.letters().is_empty(), "{:?}", log.letters());
}

/// A domain's event set, as a policy's source reads it, is the domain's:
/// its name and the full names of its events.
#[test]
fn a_domain_is_its_own_event_source() {
    assert_eq!(<Counter as EventSource>::DOMAIN, "counter");
    assert_eq!(
        <Counter as EventSource>::event_types(),
        ["toy.counter.v1.Bumped", "toy.counter.v1.Noted"]
    );
}

/// A Translate callback's fact for a child-hosted source is checked
/// against what the source's event set says it ingests, and packs as any
/// fact does.
#[test]
fn a_fact_for_a_child_hosted_source_packs_as_any_fact() {
    let fact = markup::translated_fact::<Player, Noted, Infallible>(Ok(Noted));
    assert_eq!(
        fact.ok().map(|any| any.type_url),
        Some("/toy.counter.v1.Noted".to_string())
    );
}

/// The re-fold digest of a domain that takes no snapshot (the counter,
/// whose state offers none) lists its streams with their heads, and no state: it
/// has no form of one to show, and a stand-in would compare nothing
/// (store-export-import.plan.md, section 5.3.2).
#[test]
fn a_digest_of_a_domain_with_no_snapshot_form_gives_no_state() {
    let registry = registry();
    let store = Mem::default();
    let sampled = Sampled {
        now: Timestamp {
            seconds: 1,
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x72; 16]),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let kernel = Kernel::new(&registry);
    runtime
        .block_on(kernel.handle::<Counter, _>(&store, bump(2), &sampled))
        .unwrap_or_else(|failure| panic!("{failure:?}"));
    let folded = runtime
        .block_on(kernel.fold_digest::<Counter, _>(&store))
        .expect("it folds");
    assert_eq!(
        folded.len(),
        2,
        "the counter and the counter its policy notes on"
    );
    assert!(
        folded.iter().all(|f| f.state.is_none() && f.head == 1),
        "{folded:?}"
    );
}

/// A hand-written state that keeps `State`'s defaults offers no snapshot:
/// its writes store none, though the kernel stores one at every write by
/// default, and it is registered as its anchor. Its restore refuses one,
/// naming the anchor: no build of it offered one.
#[test]
fn a_state_keeping_the_defaults_writes_no_snapshot_and_restores_none() {
    use ::angzarr_embedded::angzarr_ports::SnapshotStore;
    use ::angzarr_embedded::StreamRef;

    let registry = registry();
    let store = Mem::default();
    let sampled = Sampled {
        now: Timestamp {
            seconds: 1,
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x73; 16]),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let kernel = Kernel::new(&registry);
    let book = bump(3);
    let cover = book.cover.clone().expect("a cover");
    runtime
        .block_on(kernel.handle::<Counter, _>(&store, book, &sampled))
        .unwrap_or_else(|failure| panic!("{failure:?}"));
    let stream = StreamRef::try_from(&cover).expect("a stream");
    assert_eq!(runtime.block_on(store.read_snapshot(stream)), Ok(None));
    assert_eq!(
        registry.catalog().state(Counter::NAME).map(|s| s.type_name),
        Some("toy.counter.v1.CounterState".to_string())
    );

    let refused = State::restore(&mut Counter::default(), wire::CounterState { total: 3 });
    let refusal = refused.expect_err("the default restore refuses");
    assert_eq!(refusal.code(), "angzarr-embedded-not-snapshotted");
    assert_eq!(
        refusal,
        Report::of(&::angzarr_embedded::NotSnapshotted {
            anchor: "toy.counter.v1.CounterState".to_string()
        })
    );
}
