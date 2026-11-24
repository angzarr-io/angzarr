//! Toy domains for the kernel's tests, their payloads prost messages under
//! versioned packages, as hatpick's will be:
//!
//! - an **account**: a fee on every withdrawal, facts from a payment
//!   provider, an old command type (`toy.account.cmd.v0.Credit`, whole
//!   euros) and an old event type (`toy.account.v0.Credited`) that upcast
//!   to the current ones;
//! - a **shelf**: shaped like hatpick's `ListState`, a prost state that is
//!   its own snapshot form;
//! - a **ledger** a policy commands from the account, which refuses large
//!   amounts, for the chain. Its state is a plain Rust type whose snapshot
//!   form is a prost message, as a generated domain state's is.

use std::convert::Infallible;
use std::fmt;

pub use angzarr_ports::memory::MemoryStore;

use crate::angzarr_router::domain::{CommandContext, DomainDispatch, Emit, InputArm};
use crate::angzarr_router::error::BusinessStatus;
use crate::markup::{Commands, PolicyContext};
use crate::v1::{
    command_page, Command, CommandBook, CommandPage, Cover, EventPage, Fact, FactOrigin, Handling,
    Uuid,
};
use crate::{
    ids, pack, Any, Decoder, Domain, Misconfigured, Payload, PayloadError, Policy, Registry,
    Sampled, State, Timestamp, Upcaster,
};
use crate::{EventStore, ListStreams, PositionKey, PositionStore, Store, StreamRef};
use coded_diagnostic::{Arg, Diagnostic, Report};

/// A message under `package`, with its `prost::Name`.
macro_rules! named {
    ($package:literal, $name:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct $name {
            $($(#[$attr])* pub $field: $ty,)*
        }

        impl ::prost::Name for $name {
            const NAME: &'static str = stringify!($name);
            const PACKAGE: &'static str = $package;
        }
    };
}

// ---------------------------------------------------------------- ids

/// A UUID whose first byte is `tag` and whose last bytes are `n`: ids of
/// different kinds never collide.
pub fn tagged(tag: u8, n: u128) -> Uuid {
    let mut bytes = n.to_be_bytes();
    bytes[0] = tag;
    Uuid::from_bytes(bytes)
}

/// Stream root `n`.
pub fn root(n: u128) -> Uuid {
    tagged(0x0a, n)
}

/// The client-supplied command id `n`.
pub fn command_id(n: u128) -> Uuid {
    tagged(0xc1, n)
}

/// The correlation `n`.
pub fn correlation(n: u128) -> Uuid {
    tagged(0x43, n)
}

/// The command id the application minted at `seconds` (a UUIDv7's
/// stand-in: the kernel never inspects a version).
pub fn minted(seconds: i64) -> Uuid {
    tagged(0x07, seconds as u128)
}

/// The instant `seconds` after the epoch.
pub fn instant(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}

/// What the application sampled at `seconds`: that instant and the command
/// id it minted then.
pub fn at(seconds: i64) -> Sampled {
    Sampled {
        now: instant(seconds),
        command_id: minted(seconds),
    }
}

// ---------------------------------------------------------------- books

/// `domain`'s stream `n`, on the main timeline.
pub fn cover(domain: &str, n: u128) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(root(n)),
        edition: None,
    }
}

/// `domain`'s stream `n` in `edition`.
pub fn cover_in(domain: &str, n: u128, edition: &str) -> Cover {
    Cover {
        edition: Some(edition.to_string()),
        ..cover(domain, n)
    }
}

/// A client's command to `cover`, with no ids and no expected sequence.
pub fn command(cover: Cover, payload: Any) -> CommandBook {
    CommandBook {
        cover: Some(cover),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(payload),
                handling: None,
            })),
            ..CommandPage::default()
        }),
    }
}

/// A fact to `cover` from `external_id`, with no ids.
pub fn fact(cover: Cover, payload: Any, external_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(cover),
        page: Some(CommandPage {
            body: Some(command_page::Body::Fact(Fact {
                payload: Some(payload),
                origin: Some(FactOrigin {
                    external_id: external_id.to_string(),
                    description: "toy payments webhook".to_string(),
                }),
            })),
            ..CommandPage::default()
        }),
    }
}

/// Edits to a book, for building cases.
pub trait Edit: Sized {
    fn page_mut(&mut self) -> &mut CommandPage;

    /// With the client-supplied command id `n`.
    fn id(mut self, n: u128) -> Self {
        self.page_mut().command_id = Some(command_id(n));
        self
    }

    /// In the correlation `n`.
    fn correlated(mut self, n: u128) -> Self {
        self.page_mut().correlation_id = Some(correlation(n));
        self
    }

    /// A command expecting the stream at `sequence` (its
    /// `Command.handling`); a fact's book unchanged.
    fn expecting(mut self, sequence: u64) -> Self {
        if let Some(command_page::Body::Command(command)) = self.page_mut().body.as_mut() {
            command.handling = Some(Handling {
                expected_sequence: Some(sequence),
            });
        }
        self
    }
}

impl Edit for CommandBook {
    fn page_mut(&mut self) -> &mut CommandPage {
        self.page.get_or_insert_with(CommandPage::default)
    }
}

// ---------------------------------------------------------------- handlers

/// What a toy's handler reads besides the state and its input: the
/// router's `CommandContext` (the stream, the input's ids, a fact's
/// origin), the instant the kernel records at, and the domain's rules
/// version. (A fixture standing in for a domain's handlers.)
pub struct ToyCtx<'a, D: Domain> {
    pub stream: Cover,
    pub command_id: Uuid,
    pub correlation_id: Uuid,
    pub source: Option<FactOrigin>,
    pub recorded_at: Timestamp,
    pub rules_version: &'a str,
    pub domain: std::marker::PhantomData<fn() -> D>,
    /// The stream's next sequence.
    pub head: u64,
}

/// A toy domain's handlers, written against the state: its command
/// handler, which may refuse (its rejection travels as the typed
/// [`ToyRefusal`], FAILED_PRECONDITION, and is kept for the caller), and
/// its fact handler, which may not. Registered on the router's table by
/// [`register`].
pub trait Toy: Domain {
    fn decide(
        cmd: &Self::Command,
        state: &Self::State,
        ctx: &ToyCtx<'_, Self>,
    ) -> Result<Vec<Self::Event>, Self::Rejection>;

    fn ingest(state: &Self::State, fact: &Self::Fact, ctx: &ToyCtx<'_, Self>) -> Vec<Self::Event> {
        let _ = (state, fact, ctx);
        unreachable!("{} takes no facts", Self::NAME)
    }
}

/// The router's context, with the kernel's instant and the domain's rules
/// version, as a toy reads it.
fn toy_ctx<'a, D: Domain>(cctx: &CommandContext) -> ToyCtx<'a, D> {
    ToyCtx {
        stream: cctx.cover.clone().unwrap_or_default(),
        command_id: cctx.command_id.clone().unwrap_or_default(),
        correlation_id: cctx.correlation_id.clone().unwrap_or_default(),
        source: match &cctx.arm {
            InputArm::Fact { origin } => origin.clone(),
            InputArm::Command { .. } => None,
        },
        recorded_at: cctx.recorded_at.unwrap_or_default(),
        rules_version: D::RULES_VERSION,
        domain: std::marker::PhantomData,
        head: cctx.next_sequence,
    }
}

named!(
    "toy.v1",
    ToyRefusal {
        /// The toy rejection's code.
        #[prost(string, tag = "1")]
        code: String,
    }
);

/// A toy's rejection as the typed refusal it travels as.
pub fn toy_refusal(rejection: &dyn Diagnostic) -> Any {
    pack(&ToyRefusal {
        code: rejection.code().to_string(),
    })
}

/// `D`'s handlers, on the router's table.
pub fn handlers<D: Toy>() -> impl FnOnce(DomainDispatch<D::State>) -> DomainDispatch<D::State> {
    move |mut table| {
        for url in <D::Command as Payload>::type_urls() {
            table = table.on_command(crate::type_name(&url), move |any, state, cctx| {
                let cmd = D::Command::from_any(any).expect("the router routed it by type");
                let ctx = toy_ctx::<D>(cctx);
                match D::decide(&cmd, state, &ctx) {
                    Ok(events) => Ok(Emit::of(events.iter().map(Payload::to_any))),
                    Err(rejection) => {
                        let refusal = toy_refusal(&rejection);
                        Err(crate::rejection(
                            rejection,
                            BusinessStatus::FailedPrecondition,
                            refusal,
                        ))
                    }
                }
            });
        }
        for url in <D::Fact as Payload>::type_urls() {
            table = table.on_fact(crate::type_name(&url), move |any, state, cctx| {
                let fact = D::Fact::from_any(any).expect("the router routed it by type");
                let ctx = toy_ctx::<D>(cctx);
                let events = D::ingest(state, &fact, &ctx);
                Ok(angzarr_router::domain::FactEmit::of(
                    events.iter().map(Payload::to_any),
                ))
            });
        }
        table
    }
}

/// Register toy `D` with `upcaster`, its handlers [`handlers`].
pub fn register<D: Toy>(registry: &mut Registry, upcaster: Upcaster) -> Result<(), Misconfigured> {
    registry.register::<D>(upcaster, handlers::<D>()).map(drop)
}

/// The descriptor angzarr-build generates for typed domain `D` when a
/// handler the kernel does not build serves it: of `kind`, under
/// `rules_version`, a process manager's instance started by the `starting`
/// types (full names).
pub fn descriptor_of<D: Domain>(
    kind: crate::v1::ComponentKind,
    rules_version: &str,
    starting: &[&str],
) -> crate::v1::ComponentDescriptor {
    use crate::v1::component_descriptor::Kind;
    let state = Some(crate::markup::state_descriptor::<<D::State as State>::Anchor>());
    let domain = D::NAME.to_string();
    let kind = match kind {
        crate::v1::ComponentKind::ProcessManager => {
            Kind::ProcessManager(crate::v1::ProcessManagerDescriptor {
                domain,
                state,
                starting_types: starting.iter().map(|t| t.to_string()).collect(),
            })
        }
        _ => Kind::Aggregate(crate::v1::AggregateDescriptor { domain, state }),
    };
    crate::v1::ComponentDescriptor {
        name: D::NAME.to_string(),
        rules_version: rules_version.to_string(),
        kind: Some(kind),
    }
}

/// The registry every test runs: the three toys, each with an upcaster
/// that has no rules (registering the account's v0 rules is
/// [`registry_with_upcasters`]'s).
pub fn registry() -> Registry {
    let mut registry = Registry::new();
    register::<Account>(&mut registry, Upcaster::new("account-upcaster", "account")).unwrap();
    register::<Shelf>(&mut registry, Upcaster::new("shelf-upcaster", "shelf")).unwrap();
    register::<Ledger>(&mut registry, Upcaster::new("ledger-upcaster", "ledger")).unwrap();
    registry
}

/// [`registry`], the account upcasting its v0 command and event.
pub fn registry_with_upcasters() -> Registry {
    let mut registry = Registry::new();
    register::<Account>(&mut registry, account_upcaster()).unwrap();
    register::<Shelf>(&mut registry, Upcaster::new("shelf-upcaster", "shelf")).unwrap();
    register::<Ledger>(&mut registry, Upcaster::new("ledger-upcaster", "ledger")).unwrap();
    registry
}

// ---------------------------------------------------------------- the account

pub mod account {
    use std::collections::BTreeMap;

    named!("toy.account.v1", AccountState {
        #[prost(bool, tag = "1")] open: bool,
        #[prost(int64, tag = "2")] cents: i64,
        #[prost(int64, tag = "3")] unmatched: i64,
        /// Received payments, by external id, with what ingest's context said.
        #[prost(btree_map = "string, message", tag = "4")] received: BTreeMap<String, super::account::Received>,
    });

    // Current events.
    named!("toy.account.v1", Opened {});
    named!(
        "toy.account.v1",
        Deposited {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    named!(
        "toy.account.v1",
        Withdrew {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    // A payment, ingested: what the fact said and what ingest's read-only
    // context said about it.
    named!(
        "toy.account.v1",
        Received {
            #[prost(int64, tag = "1")]
            cents: i64,
            #[prost(string, tag = "2")]
            external_id: String,
            #[prost(int64, tag = "3")]
            recorded_seconds: i64,
            #[prost(string, tag = "4")]
            rules_version: String,
            #[prost(bool, tag = "5")]
            matched: bool,
        }
    );

    /// The old deposit event: whole euros.
    pub mod v0 {
        named!(
            "toy.account.v0",
            Credited {
                #[prost(int64, tag = "1")]
                euros: i64,
            }
        );
    }

    // Commands.
    pub mod cmd {
        named!("toy.account.cmd.v1", Open {});
        named!(
            "toy.account.cmd.v1",
            Deposit {
                #[prost(int64, tag = "1")]
                cents: i64,
            }
        );
        named!(
            "toy.account.cmd.v1",
            Withdraw {
                #[prost(int64, tag = "1")]
                cents: i64,
            }
        );
        // Withdraws everything, or nothing if the balance is 0: a
        // decision that may emit no events.
        named!("toy.account.cmd.v1", Sweep {});

        /// The old command: whole euros. Upcast to `Deposit`
        /// (discouraged, here to pin that it works).
        pub mod v0 {
            named!(
                "toy.account.cmd.v0",
                Credit {
                    #[prost(int64, tag = "1")]
                    euros: i64,
                }
            );
        }
    }

    /// Facts from the payment provider.
    pub mod fact {
        named!(
            "toy.account.fact.v1",
            PaymentReceived {
                #[prost(int64, tag = "1")]
                cents: i64,
            }
        );
    }

    /// A type no account code knows: written by a later release, say.
    pub mod v9 {
        named!(
            "toy.account.v9",
            Bonus {
                #[prost(int64, tag = "1")]
                cents: i64,
            }
        );
    }
}

use account::AccountState;

/// The fee taken from every withdrawal, in cents.
pub const FEE: i64 = 1;

/// v0 → v1 for the command and the event, both in cents.
pub fn account_upcaster() -> Upcaster {
    Upcaster::new("account-upcaster", "account")
        .rule::<account::cmd::v0::Credit, account::cmd::Deposit>(|c| {
            Ok(account::cmd::Deposit {
                cents: c.euros * 100,
            })
        })
        .rule::<account::v0::Credited, account::Deposited>(|c| {
            Ok(account::Deposited {
                cents: c.euros * 100,
            })
        })
}

/// The account's state, its own snapshot form, described by its type name.
/// (A fixture standing in for generated code.)
impl State for AccountState {
    /// A prost state is its own anchor, and takes snapshots of itself.
    type Anchor = Self;

    fn snapshot(&self) -> Option<Self> {
        Some(self.clone())
    }

    fn restore(&mut self, anchor: Self) -> Result<(), Report> {
        *self = anchor;
        Ok(())
    }
}

pub struct Account;

#[derive(Clone, PartialEq, Debug)]
pub enum AccountCmd {
    Open,
    Deposit(i64),
    Withdraw(i64),
    Sweep,
}

#[derive(Clone, PartialEq, Debug)]
pub enum AccountFact {
    PaymentReceived(i64),
}

#[derive(Clone, PartialEq, Debug)]
pub enum AccountEvent {
    Opened,
    Deposited(i64),
    Withdrew(i64),
    Received(account::Received),
}

impl Payload for AccountCmd {
    fn to_any(&self) -> Any {
        match self {
            AccountCmd::Open => pack(&account::cmd::Open {}),
            AccountCmd::Deposit(cents) => pack(&account::cmd::Deposit { cents: *cents }),
            AccountCmd::Withdraw(cents) => pack(&account::cmd::Withdraw { cents: *cents }),
            AccountCmd::Sweep => pack(&account::cmd::Sweep {}),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        account_commands().decode(any)
    }

    fn type_urls() -> Vec<String> {
        account_commands().type_urls()
    }
}

fn account_commands() -> Decoder<AccountCmd> {
    Decoder::new()
        .on::<account::cmd::Open>(|_| AccountCmd::Open)
        .on::<account::cmd::Deposit>(|d| AccountCmd::Deposit(d.cents))
        .on::<account::cmd::Withdraw>(|w| AccountCmd::Withdraw(w.cents))
        .on::<account::cmd::Sweep>(|_| AccountCmd::Sweep)
}

impl Payload for AccountFact {
    fn to_any(&self) -> Any {
        match self {
            AccountFact::PaymentReceived(cents) => {
                pack(&account::fact::PaymentReceived { cents: *cents })
            }
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        account_facts().decode(any)
    }

    fn type_urls() -> Vec<String> {
        account_facts().type_urls()
    }
}

fn account_facts() -> Decoder<AccountFact> {
    Decoder::new().on::<account::fact::PaymentReceived>(|p| AccountFact::PaymentReceived(p.cents))
}

impl Payload for AccountEvent {
    fn to_any(&self) -> Any {
        match self {
            AccountEvent::Opened => pack(&account::Opened {}),
            AccountEvent::Deposited(cents) => pack(&account::Deposited { cents: *cents }),
            AccountEvent::Withdrew(cents) => pack(&account::Withdrew { cents: *cents }),
            AccountEvent::Received(r) => pack(r),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        account_events().decode(any)
    }

    fn type_urls() -> Vec<String> {
        account_events().type_urls()
    }
}

pub fn account_events() -> Decoder<AccountEvent> {
    Decoder::new()
        .on::<account::Opened>(|_| AccountEvent::Opened)
        .on::<account::Deposited>(|d| AccountEvent::Deposited(d.cents))
        .on::<account::Withdrew>(|w| AccountEvent::Withdrew(w.cents))
        .on::<account::Received>(AccountEvent::Received)
}

#[derive(Clone, PartialEq, Debug)]
pub enum Refused {
    AlreadyOpen,
    NotOpen,
    Overdrawn { short: i64 },
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refused::AlreadyOpen => f.write_str("the account is already open"),
            Refused::NotOpen => f.write_str("the account is not open"),
            Refused::Overdrawn { short } => write!(f, "{short} cents short"),
        }
    }
}

impl Diagnostic for Refused {
    fn code(&self) -> &str {
        match self {
            Refused::AlreadyOpen => "toy-account-already-open",
            Refused::NotOpen => "toy-account-not-open",
            Refused::Overdrawn { .. } => "toy-account-overdrawn",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match *self {
            Refused::Overdrawn { short } => vec![("short", short.into())],
            _ => vec![],
        }
    }
}

thread_local! {
    /// How many account events this thread has folded ([`Account`]'s
    /// `apply`): what a test of how often a step folds counts.
    pub static APPLIED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Domain for Account {
    type State = AccountState;
    type Command = AccountCmd;
    type Fact = AccountFact;
    type Event = AccountEvent;
    type Rejection = Refused;

    const NAME: &'static str = "account";
    const RULES_VERSION: &'static str = "v2";

    fn apply(s: &mut AccountState, e: &AccountEvent, _: &crate::PageContext<'_>) {
        APPLIED.with(|applied| applied.set(applied.get() + 1));
        match e {
            AccountEvent::Opened => s.open = true,
            AccountEvent::Deposited(cents) => s.cents += cents,
            AccountEvent::Withdrew(cents) => s.cents -= cents + FEE,
            AccountEvent::Received(r) => {
                if r.matched {
                    s.cents += r.cents;
                } else {
                    s.unmatched += r.cents;
                }
                s.received.insert(r.external_id.clone(), r.clone());
            }
        }
    }
}

impl Toy for Account {
    fn decide(
        cmd: &AccountCmd,
        s: &AccountState,
        _: &ToyCtx<'_, Self>,
    ) -> Result<Vec<AccountEvent>, Refused> {
        Ok(match *cmd {
            AccountCmd::Open => {
                if s.open {
                    return Err(Refused::AlreadyOpen);
                }
                vec![AccountEvent::Opened]
            }
            AccountCmd::Deposit(cents) => {
                if !s.open {
                    return Err(Refused::NotOpen);
                }
                vec![AccountEvent::Deposited(cents)]
            }
            AccountCmd::Withdraw(cents) => {
                let total = cents + FEE;
                if total > s.cents {
                    return Err(Refused::Overdrawn {
                        short: total - s.cents,
                    });
                }
                vec![AccountEvent::Withdrew(cents)]
            }
            AccountCmd::Sweep => {
                let all = s.cents - FEE;
                match all > 0 {
                    true => vec![AccountEvent::Withdrew(all)],
                    false => Vec::new(),
                }
            }
        })
    }

    /// A payment is recorded whether or not the account is open (never
    /// refused), with what the context says about it. A payment of a
    /// negative amount is a provider bug this toy does not handle: it
    /// panics, standing in for a fact handler's bug.
    fn ingest(s: &AccountState, fact: &AccountFact, ctx: &ToyCtx<'_, Self>) -> Vec<AccountEvent> {
        let AccountFact::PaymentReceived(cents) = *fact;
        assert!(cents >= 0, "toy ingest bug: a negative payment");
        let source = ctx.source.clone().expect("a fact has its origin");
        // What the rest of the context says, pinned here: a wrong value is a
        // panic, so the fact tests fail on it.
        assert_eq!(ctx.stream.domain, "account");
        // A client's fact is identified by its external id; a reaction's
        // (a policy's fact, a refusal fact) by its stamp, its origin's
        // external id the stamp's text.
        assert!(
            ctx.command_id == ids::fact_command_id(&source.external_id)
                || source.external_id == ctx.command_id.to_string(),
            "{source:?}"
        );
        assert!(ctx.correlation_id.is_well_formed());
        vec![AccountEvent::Received(account::Received {
            cents,
            external_id: source.external_id.clone(),
            recorded_seconds: ctx.recorded_at.seconds,
            rules_version: ctx.rules_version.to_string(),
            matched: s.open,
        })]
    }
}

// ---------------------------------------------------------------- a shelf

pub mod shelf {
    use std::collections::BTreeMap;

    // The state, as if generated from
    //
    // ```text
    // message ShelfState {
    //   map<string, int64> items = 1;
    //   repeated string columns = 2;
    //   Settings settings = 3;
    //   string notes = 4;
    // }
    // ```
    named!("toy.shelf.v1", ShelfState {
        #[prost(btree_map = "string, int64", tag = "1")] items: BTreeMap<String, i64>,
        #[prost(string, repeated, tag = "2")] columns: Vec<String>,
        #[prost(message, optional, tag = "3")] settings: Option<Settings>,
        #[prost(string, tag = "4")] notes: String,
    });
    named!(
        "toy.shelf.v1",
        Settings {
            #[prost(uint32, tag = "1")]
            cooldown_days: u32,
        }
    );

    // Commands.
    named!(
        "toy.shelf.cmd.v1",
        Set {
            #[prost(string, tag = "1")]
            item: String,
            #[prost(int64, tag = "2")]
            count: i64,
            /// Unread: kept so the wire shape is as before.
            #[prost(bool, tag = "3")]
            resolve: bool,
        }
    );
    named!("toy.shelf.cmd.v1", Columns {
        #[prost(string, repeated, tag = "1")] columns: Vec<String>,
    });
    named!(
        "toy.shelf.cmd.v1",
        Cooldown {
            #[prost(uint32, tag = "1")]
            days: u32,
        }
    );
    named!(
        "toy.shelf.cmd.v1",
        Note {
            #[prost(string, tag = "1")]
            note: String,
        }
    );

    // Events.
    named!("toy.shelf.v1", WasSet {
        #[prost(string, tag = "1")] item: String,
        #[prost(int64, tag = "2")] count: i64,
        /// Unset: kept so the wire shape is as before.
        #[prost(uint64, optional, tag = "3")] window: Option<u64>,
    });
    named!("toy.shelf.v1", ColumnsSet {
        #[prost(string, repeated, tag = "1")] columns: Vec<String>,
    });
    named!(
        "toy.shelf.v1",
        CooldownSet {
            #[prost(uint32, tag = "1")]
            days: u32,
        }
    );
    named!(
        "toy.shelf.v1",
        Noted {
            #[prost(string, tag = "1")]
            note: String,
        }
    );
}

pub use shelf::ShelfState;

/// **A test fixture standing in for generated code**: what angzarr's
/// generator emits for `ShelfState`. Never hand-write this for a real
/// domain.
impl State for ShelfState {
    /// A prost state is its own anchor, and takes snapshots of itself.
    type Anchor = Self;

    fn snapshot(&self) -> Option<Self> {
        Some(self.clone())
    }

    fn restore(&mut self, anchor: Self) -> Result<(), Report> {
        *self = anchor;
        Ok(())
    }
}

pub struct Shelf;

#[derive(Clone, PartialEq, Debug)]
pub enum ShelfCmd {
    Set(shelf::Set),
    Columns(Vec<String>),
    Cooldown(u32),
    Note(String),
}

#[derive(Clone, PartialEq, Debug)]
pub enum ShelfEvent {
    WasSet(shelf::WasSet),
    ColumnsSet(Vec<String>),
    CooldownSet(u32),
    Noted(String),
}

/// Set `item` to `count`.
pub fn set(item: &str, count: i64) -> Any {
    pack(&shelf::Set {
        item: item.to_string(),
        count,
        resolve: true,
    })
}

impl Payload for ShelfCmd {
    fn to_any(&self) -> Any {
        match self {
            ShelfCmd::Set(set) => pack(set),
            ShelfCmd::Columns(columns) => pack(&shelf::Columns {
                columns: columns.clone(),
            }),
            ShelfCmd::Cooldown(days) => pack(&shelf::Cooldown { days: *days }),
            ShelfCmd::Note(note) => pack(&shelf::Note { note: note.clone() }),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new()
            .on::<shelf::Set>(ShelfCmd::Set)
            .on::<shelf::Columns>(|c| ShelfCmd::Columns(c.columns))
            .on::<shelf::Cooldown>(|c| ShelfCmd::Cooldown(c.days))
            .on::<shelf::Note>(|n| ShelfCmd::Note(n.note))
            .decode(any)
    }

    fn type_urls() -> Vec<String> {
        Decoder::<ShelfCmd>::new()
            .on::<shelf::Set>(ShelfCmd::Set)
            .on::<shelf::Columns>(|c| ShelfCmd::Columns(c.columns))
            .on::<shelf::Cooldown>(|c| ShelfCmd::Cooldown(c.days))
            .on::<shelf::Note>(|n| ShelfCmd::Note(n.note))
            .type_urls()
    }
}

impl Payload for ShelfEvent {
    fn to_any(&self) -> Any {
        match self {
            ShelfEvent::WasSet(set) => pack(set),
            ShelfEvent::ColumnsSet(columns) => pack(&shelf::ColumnsSet {
                columns: columns.clone(),
            }),
            ShelfEvent::CooldownSet(days) => pack(&shelf::CooldownSet { days: *days }),
            ShelfEvent::Noted(note) => pack(&shelf::Noted { note: note.clone() }),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new()
            .on::<shelf::WasSet>(ShelfEvent::WasSet)
            .on::<shelf::ColumnsSet>(|c| ShelfEvent::ColumnsSet(c.columns))
            .on::<shelf::CooldownSet>(|c| ShelfEvent::CooldownSet(c.days))
            .on::<shelf::Noted>(|n| ShelfEvent::Noted(n.note))
            .decode(any)
    }

    fn type_urls() -> Vec<String> {
        Vec::new()
    }
}

impl Domain for Shelf {
    type State = ShelfState;
    type Command = ShelfCmd;
    type Fact = Infallible;
    type Event = ShelfEvent;
    type Rejection = Infallible;

    const NAME: &'static str = "shelf";
    const RULES_VERSION: &'static str = "v1";

    fn apply(s: &mut ShelfState, e: &ShelfEvent, _: &crate::PageContext<'_>) {
        match e {
            ShelfEvent::WasSet(set) => {
                s.items.insert(set.item.clone(), set.count);
            }
            ShelfEvent::ColumnsSet(columns) => s.columns = columns.clone(),
            ShelfEvent::CooldownSet(days) => {
                s.settings
                    .get_or_insert_with(Default::default)
                    .cooldown_days = *days
            }
            ShelfEvent::Noted(note) => s.notes = note.clone(),
        }
    }
}

impl Toy for Shelf {
    fn decide(
        cmd: &ShelfCmd,
        _: &ShelfState,
        _: &ToyCtx<'_, Self>,
    ) -> Result<Vec<ShelfEvent>, Infallible> {
        let event = match cmd {
            ShelfCmd::Set(set) => ShelfEvent::WasSet(shelf::WasSet {
                item: set.item.clone(),
                count: set.count,
                window: None,
            }),
            ShelfCmd::Columns(columns) => ShelfEvent::ColumnsSet(columns.clone()),
            ShelfCmd::Cooldown(days) => ShelfEvent::CooldownSet(*days),
            ShelfCmd::Note(note) => ShelfEvent::Noted(note.clone()),
        };
        Ok(vec![event])
    }
}

// ---------------------------------------------------------------- a ledger

pub mod ledger {
    named!(
        "toy.ledger.v1",
        LedgerSnapshot {
            /// The next account sequence the ledger is owed: its position.
            #[prost(uint64, tag = "1")]
            next_source: u64,
            #[prost(int64, tag = "2")]
            total: i64,
        }
    );
    named!(
        "toy.ledger.cmd.v1",
        Record {
            #[prost(uint64, tag = "1")]
            source_sequence: u64,
            #[prost(int64, tag = "2")]
            cents: i64,
        }
    );
    named!(
        "toy.ledger.v1",
        Recorded {
            #[prost(uint64, tag = "1")]
            source_sequence: u64,
            #[prost(int64, tag = "2")]
            cents: i64,
        }
    );
}

/// The ledger's state, a domain type: its total is never negative, so a
/// snapshot claiming one is not a ledger.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct LedgerState {
    /// The next account sequence the ledger is owed: its position.
    pub next_source: u64,
    pub total: i64,
}

/// A total below zero: the reason a ledger snapshot is refused.
#[derive(Clone, PartialEq, Debug)]
pub struct NegativeTotal {
    pub total: i64,
}

impl fmt::Display for NegativeTotal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a ledger total of {} is negative", self.total)
    }
}

impl Diagnostic for NegativeTotal {
    fn code(&self) -> &str {
        "toy-ledger-negative-total"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("total", self.total.into())]
    }
}

/// The ledger's state, anchored on its snapshot message: a state that is
/// not its anchor, so its snapshot is a conversion, and a restore may
/// refuse what the wire holds.
impl State for LedgerState {
    type Anchor = ledger::LedgerSnapshot;

    fn snapshot(&self) -> Option<ledger::LedgerSnapshot> {
        Some(ledger::LedgerSnapshot {
            next_source: self.next_source,
            total: self.total,
        })
    }

    fn restore(&mut self, anchor: ledger::LedgerSnapshot) -> Result<(), Report> {
        if anchor.total < 0 {
            return Err(Report::of(&NegativeTotal {
                total: anchor.total,
            }));
        }
        *self = LedgerState {
            next_source: anchor.next_source,
            total: anchor.total,
        };
        Ok(())
    }
}

pub struct Ledger;

/// Deposits above this are refused by the ledger: a rejection three steps
/// from nowhere, for the chain.
pub const LEDGER_LIMIT: i64 = 1_000;

#[derive(Clone, PartialEq, Debug)]
pub struct TooLarge;

impl fmt::Display for TooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("too large for the ledger")
    }
}

impl Diagnostic for TooLarge {
    fn code(&self) -> &str {
        "toy-ledger-too-large"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![]
    }
}

impl Payload for ledger::Record {
    fn to_any(&self) -> Any {
        pack(self)
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new().on::<ledger::Record>(|r| r).decode(any)
    }

    fn type_urls() -> Vec<String> {
        vec!["/toy.ledger.cmd.v1.Record".to_string()]
    }
}

impl Payload for ledger::Recorded {
    fn to_any(&self) -> Any {
        pack(self)
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new().on::<ledger::Recorded>(|r| r).decode(any)
    }

    fn type_urls() -> Vec<String> {
        vec!["/toy.ledger.v1.Recorded".to_string()]
    }
}

impl Domain for Ledger {
    type State = LedgerState;
    type Command = ledger::Record;
    type Fact = Infallible;
    type Event = ledger::Recorded;
    type Rejection = TooLarge;

    const NAME: &'static str = "ledger";
    const RULES_VERSION: &'static str = "v1";

    fn apply(s: &mut LedgerState, e: &ledger::Recorded, _: &crate::PageContext<'_>) {
        s.next_source = e.source_sequence + 1;
        s.total += e.cents;
    }
}

impl Toy for Ledger {
    fn decide(
        cmd: &ledger::Record,
        _: &LedgerState,
        _: &ToyCtx<'_, Self>,
    ) -> Result<Vec<ledger::Recorded>, TooLarge> {
        if cmd.cents > LEDGER_LIMIT {
            return Err(TooLarge);
        }
        Ok(vec![ledger::Recorded {
            source_sequence: cmd.source_sequence,
            cents: cmd.cents,
        }])
    }
}

/// The ledger every account books into.
pub fn ledger_root() -> Uuid {
    ids::compute_root("ledger", "main").unwrap()
}

/// Account deposits → the ledger.
pub struct Bookkeeping;

impl Policy for Bookkeeping {
    const NAME: &'static str = "bookkeeping";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        event: &Any,
        ctx: &PolicyContext,
    ) -> Result<Option<Commands<Ledger>>, crate::markup::Report> {
        let cents = match AccountEvent::from_any(event) {
            Ok(AccountEvent::Deposited(cents)) => cents,
            Ok(_) => 0,
            Err(_) => return Ok(None),
        };
        let record = ledger::Record {
            source_sequence: ctx.trigger().sequence,
            cents,
        };
        Ok(Some(Commands::none().send(ledger_root(), &record)))
    }
}

// ---------------------------------------------------------------- running

/// Handle `book` through the kernel over `store`, with the registry's
/// policies and no projections, logging dead letters to nowhere the test reads.
pub fn handle<D: Domain>(
    store: &impl Store,
    registry: &Registry,
    book: CommandBook,
    sampled: &Sampled,
) -> Result<crate::Handled, crate::Failure<crate::Rejected>> {
    let log = crate::MemoryDeadLetters::new();
    let kernel = crate::Kernel::new(registry)
        .log_with(&log)
        .on_retryable(no_retries());
    kernel.handle::<D, _>(store, book, sampled).ready()
}

/// Retrying turned off: what a test that is not about retrying runs with,
/// so a retryable failure comes back at once, as it did before.
pub fn no_retries() -> crate::OnRetryable {
    crate::OnRetryable {
        max_retries: 0,
        ..crate::OnRetryable::default()
    }
}

/// `stream`'s pages as stored: empty for no such stream.
pub fn pages(store: &impl EventStore, stream: &Cover) -> Vec<EventPage> {
    let stream = StreamRef::try_from(stream).expect("a well-formed cover");
    store.read(stream, 0, None).ready().unwrap().pages
}

/// `stream` as the ports key it.
pub fn sref(stream: &Cover) -> StreamRef<'_> {
    StreamRef::try_from(stream).expect("a well-formed cover")
}

/// `stream` as stored, with its head: empty at 0 for no such stream.
pub fn slice(store: &impl EventStore, stream: &Cover) -> crate::Slice {
    store.read(sref(stream), 0, None).ready().unwrap()
}

/// The fork of `stream`, an edition stream, off its main stream's head:
/// its `EditionCreated` meta-event, the page an explicit fork writes
/// (decision 14.107).
pub fn fork_page(sequence: u64) -> EventPage {
    EventPage {
        sequence,
        body: Some(crate::v1::event_page::Body::MetaEvent(
            crate::v1::MetaEvent {
                kind: Some(crate::v1::meta_event::Kind::Created(
                    crate::v1::EditionCreated {},
                )),
            },
        )),
        meta: None,
    }
}

/// Fork edition stream `stream` off its main stream's head, as an
/// application does through its store: its fork page written at that head.
pub fn fork(store: &impl Store, stream: &Cover) {
    let main = Cover {
        edition: None,
        ..stream.clone()
    };
    let head = slice(store, &main).next_sequence;
    store
        .write(crate::Write::append(sref(stream), head, &[fork_page(head)]))
        .ready()
        .expect("the fork is written");
}

/// The expected sequence and head of `rejected`, if it is the
/// coordinator's stale refusal, `ExpectedSequenceMismatch` (decision 14.94).
pub fn mismatch_of(rejected: &crate::Rejected) -> Option<(u64, u64)> {
    use prost::Name as _;
    let refusal = rejected.refusal_object();
    if refusal.type_url != crate::v1::ExpectedSequenceMismatch::type_url() {
        return None;
    }
    let mismatch: crate::v1::ExpectedSequenceMismatch =
        prost::Message::decode(&refusal.value[..]).expect("an ExpectedSequenceMismatch");
    Some((mismatch.expected, mismatch.head))
}

/// Every stream holding an event, in every domain, in the ports' order.
pub fn streams(store: &MemoryStore) -> Vec<Cover> {
    store.list_streams().ready().unwrap()
}

/// `handler`'s position on `stream`.
pub fn position(store: &impl PositionStore, handler: &str, stream: &Cover) -> u64 {
    let stream = StreamRef::try_from(stream).expect("a well-formed cover");
    store
        .position(PositionKey { handler, stream })
        .ready()
        .unwrap()
}

/// Every payload in `books`, decoded as `E`, in order.
pub fn payloads<E: Payload>(books: &[crate::v1::EventBook]) -> Vec<E> {
    books
        .iter()
        .flat_map(|b| &b.pages)
        .map(|p| {
            E::from_any(crate::angzarr_router::page_event(p).expect("a page has an event")).unwrap()
        })
        .collect()
}

/// An open account `n` holding `cents`, through the kernel: commands 1 and
/// 2 (open, then deposit) at seconds 10 and 11.
pub fn opened(store: &impl Store, registry: &Registry, n: u128, cents: i64) {
    handle::<Account>(
        store,
        registry,
        command(cover("account", n), AccountCmd::Open.to_any()).id(1),
        &at(10),
    )
    .unwrap();
    handle::<Account>(
        store,
        registry,
        command(cover("account", n), AccountCmd::Deposit(cents).to_any()).id(2),
        &at(11),
    )
    .unwrap();
}

/// Run a future that is ready at once, as `MemoryStore`'s are, and its
/// method form: the test support's.
pub use crate::testing::{block_on_ready as ready, Ready};

// ---------------------------------------------------------------- dead letters

/// angzarr's wire `Diagnostic`: a key, as the spec spells it, and its args.
pub fn wire(code: &str, args: &[(&str, &str)]) -> crate::v1::Diagnostic {
    crate::v1::Diagnostic {
        code: code.to_string(),
        args: args
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
    }
}

/// A letter's why: the reason `code` with `args` (decision 14.101).
pub fn reason(code: &str, args: &[(&str, &str)]) -> crate::v1::angzarr_dead_letter::Why {
    crate::v1::angzarr_dead_letter::Why::Reason(wire(code, args))
}

/// Account deposits → the ledger, but a deposit of 13 cents cannot happen:
/// the policy answers it with its error, a bug, not a command.
pub struct Unlucky;

impl crate::Policy for Unlucky {
    const NAME: &'static str = "unlucky";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        let Ok(crate::toys::AccountEvent::Deposited(cents)) =
            crate::toys::AccountEvent::from_any(event)
        else {
            return Ok(None);
        };
        if cents == 13 {
            return Err(crate::markup::Report::new("toy-unlucky-deposit").arg("cents", cents));
        }
        let record = ledger::Record {
            source_sequence: ctx.trigger().sequence,
            cents,
        };
        Ok(Some(
            crate::markup::Commands::none().send(ledger_root(), &record),
        ))
    }
}

/// The one letter of `letters`.
pub fn one_letter(letters: Vec<crate::v1::AngzarrDeadLetter>) -> crate::v1::AngzarrDeadLetter {
    assert_eq!(letters.len(), 1, "one dead letter: {letters:?}");
    letters.into_iter().next().unwrap()
}

/// `letter` names the kind of the component that dead-lettered it,
/// `kind`; its why is `why` (decision 14.101) and its `retry_count`
/// `retries`, and its line leads with the why's key.
pub fn assert_letter(
    letter: &crate::v1::AngzarrDeadLetter,
    kind: crate::v1::ComponentKind,
    why: &crate::v1::angzarr_dead_letter::Why,
    retries: u32,
) {
    use crate::v1::angzarr_dead_letter::Why;
    assert_eq!(letter.source_component_kind(), kind, "{letter:?}");
    assert_eq!(letter.why.as_ref(), Some(why), "{letter:?}");
    assert_eq!(letter.retry_count, retries, "{letter:?}");
    let key = match why {
        Why::Reason(reason) => reason.code.clone(),
        Why::Refusal(_) => "ANGZARR_REASON_COMMAND_REFUSED".to_string(),
        Why::Transport(transport) => crate::type_name(&transport.type_url).to_string(),
    };
    let line = crate::dead_letter_line(letter);
    assert!(
        line.contains(&format!(": {key} ")) || line.ends_with(&format!(": {key}")),
        "the line leads with the key: {line}"
    );
}

/// What every kernel test over a store uses: the store, the helpers that
/// read it, and the ports' traits, so their methods are in scope.
pub mod prelude {
    pub use super::{pages, position, slice, sref, streams, MemoryStore, Ready};
    pub use crate::{EventStore, PositionStore, Purge, SnapshotStore, Write};
}
