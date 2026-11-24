//! What angzarr-embedded-macros' expansion names: the runtime under a marked-up
//! aggregate or process manager (`#[aggregate]`, `#[process_manager]`,
//! `#[handles_cmd]`, `#[handles_fact]`, `#[on_refused]`, `#[applies]`),
//! policy (`#[policy]`, `#[on_event]`) and projector (`#[projector]`,
//! `#[projects]`); see angzarr-embedded-macros' crate docs. A hand-written
//! [`Domain`] needs none of it; a hand-written component that wants what
//! the markup does uses the same functions the expansion calls.
//!
//! - [`Context`]: a domain handler's third argument, what it may read
//!   besides itself and its input: one context, an aggregate's and a
//!   process manager's alike, its stream's root ([`Context::root`]) and
//!   whether a natural key is the stream's ([`Context::rooted_by`],
//!   [`NotItsStream`]: `#[handles_cmd(C, rooted_by = field)]`).
//! - [`PageContext`]: an applier's optional third argument, where its event
//!   sits: its stream, its sequence and its whole metadata.
//! - [`DomainMessage`]: a domain type (angzarr-codegen's domain layer) and
//!   the prost message it travels as, so the router decodes by type URL
//!   into domain types and never hands a handler a wire type.
//! - [`Emits`] and [`Emit`]: what a command handler's `Ok` may be. A shape
//!   (`E`, a tuple, `Option<E>`, `Vec<E>`) is events the router applies, in
//!   order; [`Emit::then`] adds a continuation step the router runs on the
//!   instance after them, within the same decision. It runs as
//!   angzarr-router's own `Emit` (decision 14.104), with no step cap.
//! - [`FactEmit`], [`Bug`] and [`BugExt`]: what a FACT handler returns,
//!   `Result<FactEmit<Self>, Bug>`, angzarr-router's own types. A fact is
//!   never refused, and the signature is the guarantee: its one failure is
//!   a bug. [`Emit`] is the command's.
//! - [`Refusing`], [`Refuses`] and [`TypedRefusal`]: a command's refusal,
//!   one of its declared typed refusal objects (decision 14.95: typed
//!   refusals only), and [`Unrefusable`], what a command that declares
//!   none fails with.
//! - [`Failed`], [`BusinessStatus`], [`RetryableStatus`] and [`Fails`]:
//!   what a command handler or a continuation step fails with, a business
//!   refusal (under a status of the business set), an immediate return
//!   (ABORTED), a fatal bug, or a retryable bug (under the status it
//!   picks).
//! - [`Refused`]: a refusal of a command a policy sent, as its ONE
//!   Translate callback reads it (decision 14.89).
//! - [`ProcessManager`]: what sets a process manager, a domain, apart: its
//!   starting inputs. [`Compensation`] its rollback (angzarr decision
//!   14.74): each step paired in code with its undo, the failed event and
//!   the completed steps' undos, newest first, a late reply undone at once,
//!   and an undo that becomes a command refused at registration
//!   ([`UndoTargetsCommand`]; decision 14.83).
//! - [`Routed`]: a marked-up domain's handlers, registered on
//!   angzarr-router's table.
//! - [`Policy`], [`Commands`] and [`PolicyContext`]: a policy, its output
//!   and its handler's context. [`Decides`] and [`Ingests`] say what a
//!   domain takes, for the checks on a policy.
//! - [`AsyncProjector`] and [`ProjectionContext`]: a projector.
//! - [`assert_step_idempotent`]: the test helper for a continuation step,
//!   which must be idempotent on its own result (there is no step cap).
//! - [`TypeName`]: a message's package and name, as the generated
//!   constants list them.
//! - [`Snapshots`]: a marked-up aggregate's or process manager's anchor,
//!   as its domain type, and its opt-in to snapshots: overriding
//!   [`Snapshots::snapshot`] and [`Snapshots::restore`].
//! - The functions the expansion calls, each stable and public, so
//!   anything the markup writes can be written by hand
//!   (`src/hand_written.test.rs` registers an aggregate and a policy
//!   with no macro): packing and decoding ([`pack_message`],
//!   [`type_url_of`], [`decode_message`]); running a domain's handler as
//!   one decision ([`run_command`], [`run_rooted_command`], [`run_fact`]);
//!   a policy's answers ([`policy_answer`], [`translated_fact`]); a
//!   projector's run ([`projecting`]); a state's descriptor
//!   ([`state_descriptor`]); and its snapshot as its anchor's wire type
//!   ([`snapshot_wire`], [`restore_wire`]).
//!
//! Every component here assumes nothing about distribution: the same
//! marked-up aggregate, process manager, policy or projector runs in
//! angzarr-embedded's process or as its own service, with no change to its code.
//!
//! angzarr-embedded's `src/decorated.test.rs` runs all of it.

use std::fmt;

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{Cover, EventBook, EventMeta, EventRef, FactOrigin, Uuid};
use angzarr_router::error::HandlerError;
use angzarr_router::hpb;
use coded_diagnostic::code;
use prost_types::{Any, Timestamp};

use super::domain::Domain;
use super::error::PayloadError;
use super::payload::Payload as _;
use super::wire::WireError;

/// Where an applied event sits, angzarr-router's: an `#[applies]`
/// applier's optional third argument, [`Domain::apply`]'s page.
pub use angzarr_router::PageContext;

/// The error a [`Snapshots::restore`] returns, re-exported so the
/// expansion needs no dependency of its user's.
pub use coded_diagnostic::Report;

/// angzarr-router's context of the input a handler decides: a
/// [`Context`]'s `command`, and the input's arm with what it alone
/// carries.
pub use angzarr_router::domain::{CommandContext, InputArm};

/// The statuses a business refusal may go out under, angzarr-router's
/// business set: a [`Failed::Rejected`]'s status. Any other status is a
/// fault.
pub use angzarr_router::error::BusinessStatus;

/// The statuses a handler's retryable bug may go out under,
/// angzarr-router's: a [`Failed::RetryableBug`]'s status. They are not
/// interchangeable, and the kernel's capped retry and any caller of its
/// own honour the difference:
///
/// - `Unavailable` (UNAVAILABLE): resend; the dependency is down for now.
/// - `DeadlineExceeded` (DEADLINE_EXCEEDED): resend only if the call is
///   idempotent, since the work may have happened. A command is, by its
///   `command_id` (the kernel's deduplication).
/// - `ResourceExhausted` (RESOURCE_EXHAUSTED): wait longer before
///   resending; a quota or a capacity limit was hit.
pub use angzarr_router::error::RetryableStatus;

/// What a FACT handler returns, and its continuation steps (angzarr-router's
/// types, not the markup's): `Result<FactEmit<Self>, Bug>`. A fact already
/// happened, so it is never refused; its one failure is a [`Bug`], which a
/// handler may classify ([`Bug::coded`], [`Bug::retryable`]) and a decode
/// failure keeps its code through `.bug()` ([`BugExt`]). A fact's
/// `FactEmit::then` steps take the router's [`CommandContext`] and return
/// `Result<FactEmit<Self>, Bug>` as well.
pub use angzarr_router::domain::FactEmit;

/// A fact handler's failure: see [`FactEmit`].
pub use angzarr_router::error::{Bug, BugExt};

/// What a handler, or a continuation step, reads besides the instance and
/// its input, all read-only: the ONE handler context of every domain, an
/// aggregate's and a process manager's alike.
///
/// - [`Context::command`]: angzarr-router's whole
///   [`CommandContext`](angzarr_router::domain::CommandContext), as the
///   router hands it to the handler: `cover` (the stream's domain, root and
///   edition), `next_sequence`, `had_prior_events`, `arm` (a command's
///   expected sequence as sent, or a fact's origin), the input's identity:
///   `command_id` (a client's, the one the application minted, a policy's
///   derived one, or, for a fact, the one derived from its origin; a
///   refusal fact's is the refused command's) and `correlation_id`, and,
///   for a policy's input, its `causation`, the event the policy reacted
///   to. Nothing the router knows of the input is withheld, and nothing it
///   knows is kept twice.
/// - [`Context::recorded_at`]: the instant the input was accepted and its
///   events will be recorded at, the router's `recorded_at`
///   (`HandleRequest.recorded_at`, decision 14.37), sampled by the
///   application and stamped by the kernel, so the handler stays pure:
///   there is no wall clock.
#[derive(Clone, Debug, Default)]
pub struct Context {
    /// angzarr-router's context of the input this handler decides.
    pub command: CommandContext,
}

impl Context {
    /// A context for a handler's own unit tests, recorded at `recorded_at`
    /// with the router's default context otherwise
    /// ([`Context::with_command`] sets it); the kernel builds the real one
    /// from what angzarr-router hands the handler.
    pub fn new(recorded_at: Timestamp) -> Context {
        Context {
            command: CommandContext {
                recorded_at: Some(recorded_at),
                ..CommandContext::default()
            },
        }
    }

    /// This context, with `command` as the router's context of the input,
    /// its instant included.
    pub fn with_command(self, command: CommandContext) -> Context {
        Context { command }
    }

    /// When the events will be recorded (non-decreasing within the stream):
    /// the router's `recorded_at`.
    pub fn recorded_at(&self) -> Timestamp {
        self.command.recorded_at.unwrap_or_default()
    }

    /// The root of the stream the input is for, the router's cover's.
    /// `None` only for a context built by hand without one: the kernel's
    /// check refuses a command to a stream with no root.
    pub fn root(&self) -> Option<&Uuid> {
        self.command
            .cover
            .as_ref()
            .and_then(|cover| cover.root.as_ref())
    }

    /// Whether the stream is the one `domain`'s natural key `key` names:
    /// its root is `key`'s UUIDv5 under `domain`
    /// ([`ids::compute_root`](angzarr_proto::ids::compute_root)), as
    /// [`Cover::natural`](super::CoverExt::natural) addresses it. What
    /// `#[handles_cmd(C, rooted_by = field)]` checks before its handler
    /// runs; [`NotItsStream`] otherwise.
    pub fn rooted_by(&self, domain: &str, key: &str) -> Result<(), NotItsStream> {
        let derived = angzarr_proto::ids::compute_root(domain, key).ok();
        match derived.is_some() && derived.as_ref() == self.root() {
            true => Ok(()),
            false => Err(NotItsStream {
                domain: domain.to_string(),
                key: key.to_string(),
            }),
        }
    }
}

/// A command whose natural key is not its stream's: its key's UUIDv5 under
/// its domain is not the root it was sent to ([`Context::rooted_by`]), so
/// it would record one entity's events on another's stream. Refused
/// INVALID_ARGUMENT, before its handler runs, by a `#[handles_cmd(C,
/// rooted_by = field)]` handler ([`run_rooted_command`]).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NotItsStream {
    pub domain: String,
    pub key: String,
}

impl fmt::Display for NotItsStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        coded_diagnostic::display(self, f)
    }
}

impl coded_diagnostic::Diagnostic for NotItsStream {
    fn code(&self) -> &str {
        code!("not-its-stream")
    }

    fn args(&self) -> Vec<(&str, coded_diagnostic::Arg)> {
        vec![
            ("domain", self.domain.as_str().into()),
            ("key", self.key.as_str().into()),
        ]
    }
}

/// A domain type and the wire message it travels as. angzarr-codegen's
/// domain layer emits the impl beside each domain type; a prost message
/// does not implement it, so a handler or applier taking a wire type does
/// not compile.
pub trait DomainMessage: Clone + Send + Sync + Sized + 'static {
    /// The prost message: its [`prost::Name`] is the type URL the router
    /// routes by.
    type Wire: prost::Message + prost::Name + Default;

    /// This value on the wire.
    fn to_wire(&self) -> Self::Wire;

    /// The domain value a decoded message holds, or why it holds none.
    fn from_wire(wire: Self::Wire) -> Result<Self, WireError>;
}

/// A continuation step: a pure `&self` method of the aggregate (or a
/// closure over one), run on the instance with every event so far applied.
/// It fails as a handler does ([`Fails`]): a refusal with one of its
/// command's typed refusals, as is or as a [`Failed`], or a bug.
type Step<A> = Box<dyn FnOnce(&A, &Context) -> Result<Emit<A>, Failed<TypedRefusal>>>;

/// Events for the router to apply, in order, and optionally one
/// continuation step to run on the instance after them.
///
/// ```text
/// fn deposit(&self, cmd: Deposit, ctx: &Context) -> Result<Emit<Self>, Refused> {
///     Ok(Emit::of(Deposited { .. }).then(Self::settle))
/// }
///
/// fn settle(&self, ctx: &Context) -> Result<Option<Rewarded>, Refused> { .. }
/// ```
///
/// The router applies the events, calls the step on the updated instance,
/// applies what it returns, and repeats while there is a continuation: it
/// runs as angzarr-router's `Emit` (decision 14.104). All of it is **one
/// decision**: a refusal from the handler or from any step refuses it all,
/// a bug from any of them fails it all as the handler's bug would, and what
/// is committed is every step's events, in order, in one commit. There is
/// no step cap: a step that always continues never ends, so a step must be
/// idempotent on its own result ([`assert_step_idempotent`]).
///
/// A continuation is the only way a later event is decided from the state
/// after an earlier one. Applying an event never sets off processing: an
/// applier only assigns state.
pub struct Emit<A: Domain> {
    events: Vec<A::Event>,
    next: Option<Step<A>>,
}

impl<A: Domain> Emit<A> {
    /// These events, and no continuation.
    pub fn of(events: impl Emits<A>) -> Emit<A> {
        events.into_emit()
    }

    /// Run `step` on the instance once these events are applied. The step
    /// may itself return an `Emit` with a continuation.
    pub fn then<S, O, E>(self, step: S) -> Emit<A>
    where
        S: FnOnce(&A, &Context) -> Result<O, E> + 'static,
        O: Emits<A>,
        E: Fails<A>,
    {
        self.push(Box::new(move |instance, ctx| {
            step(instance, ctx)
                .map(Emits::into_emit)
                .map_err(Fails::into_failed)
        }))
    }

    /// `step` after this emit's continuation, if it has one, else as it.
    fn push(self, step: Step<A>) -> Emit<A> {
        let next: Step<A> = match self.next {
            None => step,
            Some(first) => {
                Box::new(move |instance, ctx| first(instance, ctx).map(|emit| emit.push(step)))
            }
        };
        Emit {
            events: self.events,
            next: Some(next),
        }
    }
}

impl<A: Domain> fmt::Debug for Emit<A>
where
    A::Event: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Emit")
            .field("events", &self.events)
            .field("then", &self.next.is_some())
            .finish()
    }
}

/// What a handler's or a step's `Ok` may be, for aggregate `A`:
///
/// - a domain event `T`: one event;
/// - a tuple `(T1, T2)`, `(T1, T2, T3)`, `(T1, T2, T3, T4)`: a fixed set,
///   in order;
/// - `Option<T>`: one or none;
/// - `Vec<T>`: any number of one type;
/// - an [`Emit`]: events and a continuation.
///
/// A variable list of different types is not a shape (deferred).
pub trait Emits<A: Domain> {
    /// The events and the continuation, if any.
    fn into_emit(self) -> Emit<A>;
}

impl<A: Domain> Emits<A> for Emit<A> {
    fn into_emit(self) -> Emit<A> {
        self
    }
}

/// These events, with no continuation.
fn emitted<A: Domain>(events: Vec<A::Event>) -> Emit<A> {
    Emit { events, next: None }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for T {
    fn into_emit(self) -> Emit<A> {
        emitted(vec![self.into()])
    }
}

impl<A, T1, T2> Emits<A> for (T1, T2)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        emitted(vec![self.0.into(), self.1.into()])
    }
}

impl<A, T1, T2, T3> Emits<A> for (T1, T2, T3)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
    T3: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        emitted(vec![self.0.into(), self.1.into(), self.2.into()])
    }
}

impl<A, T1, T2, T3, T4> Emits<A> for (T1, T2, T3, T4)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
    T3: DomainMessage + Into<A::Event>,
    T4: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        emitted(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
        ])
    }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for Option<T> {
    fn into_emit(self) -> Emit<A> {
        emitted(self.into_iter().map(Into::into).collect())
    }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for Vec<T> {
    fn into_emit(self) -> Emit<A> {
        emitted(self.into_iter().map(Into::into).collect())
    }
}

/// A process manager written in the markup (`#[process_manager]`). A
/// process manager IS A DOMAIN: its inputs are commands (which policies send
/// it, and which it may refuse) and facts (timeouts, webhooks, refusal
/// facts, which it never refuses); its only output is its own events. It
/// subscribes to nothing: the policy for (another domain, the PM's domain)
/// turns that domain's events into commands to it. The macro gives it the
/// same `Domain` and `State` impls an `#[aggregate]` gets; this trait adds
/// the two things that set a PM apart. Registered with
/// [`Registry::process_manager`](super::Registry::process_manager).
///
/// - **Its root is the one its input names.** A command or fact addressed
///   to it names the instance in its cover, whatever its correlation (one
///   instance may take inputs of several correlations); a `correlation_id`
///   left unset is given the root.
/// - **The start rule**, the kernel's. Only an input type it declares
///   as starting (`#[handles_cmd(C, starts)]`, `#[handles_fact(F, starts)]`)
///   may start an instance. Any other input for a stream with no instance is
///   not delivered, decided before the handler by the type after
///   upcasting, FAILED_PRECONDITION `ANGZARR_REASON_PM_NO_INSTANCE`,
///   recording nothing: a COMMAND is answered so, and a FACT is
///   dead-lettered, its sender answered `ANGZARR_REASON_FACT_DEAD_LETTERED`
///   (C-0593). It is the framework's key, never a typed refusal, so a
///   policy's command so refused is dead-lettered, never translated.
///
/// A process manager whose `STARTS` is empty can never start an instance;
/// angzarr-embedded does not refuse one (no compile error, no check): the warning is
/// angzarr-cli's.
///
/// The kernel registers it on the router's process-manager table, and
/// applies the start rule by `STARTS`: the descriptor's starting types,
/// which the markup generates (decision 14.42).
pub trait ProcessManager: Domain<State = Self> {
    /// The input types (commands and facts) that may start an instance,
    /// in marker order: angzarr's `ComponentDescriptor.starting_types`.
    const STARTS: &'static [TypeName];
}

/// That `A` decides command `C`: `#[handles_cmd(C)]` on its impl, or, for
/// a remote domain's command set ([`CommandTarget`]), written beside it.
/// What a policy's [`Commands`] is checked against. Emitted by the macro,
/// one per command.
pub trait Decides<C: DomainMessage>: CommandTarget {}

/// That `A` ingests fact `F`: `#[handles_fact(F)]` on its impl, or, for
/// a child-hosted domain's event set ([`EventSource`]), written beside it.
/// What a policy's Translate callback is checked against: it must return a
/// fact its source ingests; and what a policy's fact for its target is
/// ([`Commands::fact`]). Emitted by the macro, one per fact.
pub trait Ingests<F: DomainMessage>: EventSource {}

// ---------------------------------------------------------------- refusals

/// The typed refusal objects `A`'s handler for command `C` may refuse
/// with: `#[handles_cmd(C, refuses(T1, T2, ..))]`, the markup's
/// `CommandOptions.refuses`. The macro emits it for each such command,
/// with [`Refusing::Refuses`] a choice of exactly those types, each
/// `From` its type (and the choice `Into` the domain's [`Rejection`]): a
/// handler returning `Err(T.into())` for a `T` not listed does not
/// compile.
pub trait Refusing<C: DomainMessage>: Domain {
    /// The choice of the listed refusal objects.
    type Refuses: Into<TypedRefusal>;
    /// The listed types, in order.
    const REFUSES: &'static [TypeName];
}

/// What a handler with `refuses(..)` returns as its `Err`:
/// `Result<Held, Refuses<Self, Hold>>`.
pub type Refuses<A, C> = <A as Refusing<C>>::Refuses;

/// A typed refusal object, packed: one of a command's `refuses` types (a
/// [`DomainMessage`] of the command's own domain). Keyed, as on the wire,
/// `ANGZARR_REASON_COMMAND_REFUSED` with the object's full type name as
/// `refusal_type` (decision 14.37): the object is the refusal's data. A
/// marked-up domain's `Domain::Rejection`: what its caller matches
/// ([`Rejected::rejection`](super::Rejected::rejection)).
#[derive(Clone, PartialEq, Debug)]
pub struct TypedRefusal {
    object: Any,
}

impl TypedRefusal {
    /// `object`, packed.
    pub fn of<T: DomainMessage>(object: &T) -> TypedRefusal {
        TypedRefusal {
            object: pack_message(object),
        }
    }

    /// The object's full type name: its `refusal_type`.
    fn refusal_type(&self) -> &str {
        super::payload::type_name(&self.object.type_url)
    }

    /// The object, packed.
    pub fn packed(&self) -> &Any {
        &self.object
    }

    /// The object, as `T`, if it is one.
    pub fn object<T: DomainMessage>(&self) -> Option<T> {
        decode_message(&self.object).ok().flatten()
    }
}

impl fmt::Display for TypedRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        coded_diagnostic::display(self, f)
    }
}

impl coded_diagnostic::Diagnostic for TypedRefusal {
    fn code(&self) -> &str {
        angzarr_proto::v1::AngzarrReason::CommandRefused.as_str_name()
    }

    fn args(&self) -> Vec<(&str, coded_diagnostic::Arg)> {
        vec![(
            angzarr_proto::v1::reason_args::REFUSAL_TYPE,
            self.refusal_type().into(),
        )]
    }
}

/// What a command that declares no refusal fails with: nothing it can
/// refuse with (decision 14.95: a command that lists no refusal cannot
/// refuse, C-0674). Its handler's `Err` is a [`Failed`] of it, so it can
/// still fail with a bug or ask for a re-run.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Unrefusable {}

impl From<Unrefusable> for TypedRefusal {
    fn from(never: Unrefusable) -> TypedRefusal {
        match never {}
    }
}

impl fmt::Display for Unrefusable {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl coded_diagnostic::Diagnostic for Unrefusable {
    fn code(&self) -> &str {
        match *self {}
    }

    fn args(&self) -> Vec<(&str, coded_diagnostic::Arg)> {
        match *self {}
    }
}

/// What a command handler, or a continuation step, fails with: one of
/// decision 14.37's four classes. `X` is what it refuses with: for a
/// handler with `refuses(..)`, its [`Refuses`] choice; for one without,
/// [`Unrefusable`].
///
/// - [`Failed::Rejected`] (business, fatal): the domain said no, with one
///   of its command's typed refusals. It goes out under `status`
///   (FAILED_PRECONDITION unless the handler says otherwise,
///   [`Failed::with_status`]: the status the refusal message declares),
///   `ANGZARR_REASON_COMMAND_REFUSED` and the object, and reaches the
///   issuing policy's Translate.
/// - [`Failed::Immediate`] (business, immediate): "re-read and
///   re-decide". It goes out as ABORTED under the report's own code and
///   args (angzarr-router's `CodedError::immediate`): back to an outside
///   caller at once, never retried or dead-lettered; a policy's step,
///   whose caller is the kernel, is reloaded and run again under the
///   capped backoff, then dead-lettered. Never a policy's refusal path.
/// - [`Failed::Bug`] (bug, fatal): a defect, never a business answer: the
///   report of the bug value (a policy's `Err` is the same), its code and
///   args. It goes out as INTERNAL ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, the code as its
///   `cause` and each arg as `cause.<arg>`, and the input is dead-lettered
///   at once.
/// - [`Failed::RetryableBug`] (bug, retryable): a defect outside the
///   handler that may clear (a dependency that is down), under the
///   [`RetryableStatus`] the handler picks. It goes out under that status
///   as ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, its cause as a fatal bug's
///   (angzarr-router's `CodedError::retryable_bug`); the kernel retries
///   it with capped backoff, then dead-letters it with its `retry_count`.
///   Never a policy's refusal path.
///
/// A handler or step that only refuses keeps returning `X` as is: it
/// converts into a `Failed` by `From`, so `?` on a `Result<_, X>` works
/// inside one returning `Failed<X>`.
///
/// ```text
/// fn gauge(&self, cmd: Gauge, ctx: &Context) -> Result<Gauged, Failed<Unrefusable>> {
///     if broken { return Err(Failed::bug(&MeterBroke { seat })); }
///     if offline { return Err(Failed::retryable_bug(RetryableStatus::Unavailable, &MeterOffline { seat })); }
///     if moved { return Err(Failed::immediate(&SeatMoved { seat })); }
///     ..
/// }
///
/// fn hold(&self, cmd: Hold, ctx: &Context) -> Result<Held, Failed<Refuses<Self, Hold>>> {
///     if broken { return Err(Failed::bug(&WalletBroke {})); }
///     Err(Failed::rejected(InsufficientFunds { short }))
/// }
/// ```
#[derive(Clone, PartialEq, Debug)]
pub enum Failed<X> {
    Rejected {
        rejection: X,
        status: BusinessStatus,
    },
    Immediate(Report),
    Bug(Report),
    RetryableBug {
        status: RetryableStatus,
        report: Report,
    },
}

impl<X> Failed<X> {
    /// A refusal with `rejection`, FAILED_PRECONDITION.
    pub fn rejected(rejection: impl Into<X>) -> Failed<X> {
        Failed::Rejected {
            rejection: rejection.into(),
            status: BusinessStatus::FailedPrecondition,
        }
    }

    /// A bug, keyed by `bug`'s code, with its args.
    pub fn bug(bug: &dyn coded_diagnostic::Diagnostic) -> Failed<X> {
        Failed::Bug(Report::of(bug))
    }

    /// A retryable bug under `status`, keyed by `bug`'s code, with its
    /// args.
    pub fn retryable_bug(
        status: RetryableStatus,
        bug: &dyn coded_diagnostic::Diagnostic,
    ) -> Failed<X> {
        Failed::RetryableBug {
            status,
            report: Report::of(bug),
        }
    }

    /// An immediate return (ABORTED), keyed by `why`'s code, with its args.
    pub fn immediate(why: &dyn coded_diagnostic::Diagnostic) -> Failed<X> {
        Failed::Immediate(Report::of(why))
    }

    /// This refusal under `status`; any other failure is unchanged.
    pub fn with_status(self, status: BusinessStatus) -> Failed<X> {
        match self {
            Failed::Rejected { rejection, .. } => Failed::Rejected { rejection, status },
            other => other,
        }
    }

    /// This failure, its refusal converted into `Y`: a typed refusal
    /// object's into its command's [`Refuses`] choice, say.
    pub fn widen<Y>(self) -> Failed<Y>
    where
        X: Into<Y>,
    {
        match self {
            Failed::Rejected { rejection, status } => Failed::Rejected {
                rejection: rejection.into(),
                status,
            },
            Failed::Immediate(report) => Failed::Immediate(report),
            Failed::Bug(report) => Failed::Bug(report),
            Failed::RetryableBug { status, report } => Failed::RetryableBug { status, report },
        }
    }
}

/// A refusal, FAILED_PRECONDITION: a handler's `Err` (or `?`) of what it
/// refuses with.
impl<X> From<X> for Failed<X> {
    fn from(rejection: X) -> Self {
        Failed::rejected(rejection)
    }
}

/// The refusal's code and args, or the report's (decision 14.37): never
/// English.
impl<X: coded_diagnostic::Diagnostic> fmt::Display for Failed<X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failed::Rejected { rejection, .. } => coded_diagnostic::display(rejection, f),
            Failed::Immediate(report)
            | Failed::Bug(report)
            | Failed::RetryableBug { report, .. } => coded_diagnostic::display(report, f),
        }
    }
}

/// What a command handler's or a continuation step's `Err` may be, for
/// domain `A`: a [`Failed`] of anything `Into` a [`TypedRefusal`] (a
/// [`Refuses`] choice, [`Unrefusable`]), or, as the macro emits for each
/// domain, a [`Refuses`] choice as is.
pub trait Fails<A: Domain> {
    /// This failure, its refusal as a typed refusal.
    fn into_failed(self) -> Failed<TypedRefusal>;
}

impl<A: Domain, X: Into<TypedRefusal>> Fails<A> for Failed<X> {
    fn into_failed(self) -> Failed<TypedRefusal> {
        self.widen()
    }
}

// ---------------------------------------------------------------- policies

/// A policy: the kernel's one [`Policy`](super::Policy) trait, which
/// `#[policy]` implements (answering refusals) and a hand-written policy
/// implements as it is. Policies should not fail for business reasons:
/// they react to events the application's own domains created, so an `Err`
/// is a bug, dead-lettered as
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed).
pub use super::policy::{no_refusal_handler, CommandTarget, EventSource, Policy, Refused};

/// A process manager's compensation (angzarr decision 14.74): its steps,
/// each paired in code with its undo, and the rollback built from them.
pub use super::compensation::{Compensation, UndoTargetsCommand};

/// What a policy's `#[on_event(E)]` handler returns as its `Ok` (its `Err`
/// is any [`Diagnostic`](coded_diagnostic::Diagnostic): a bug,
/// dead-lettered as
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed)): what it
/// sends its target `T` ([`CommandTarget`]: a domain here, or a remote
/// one's command set), each with the root of the stream it is for, in
/// order: commands of types `T` decides ([`Decides`],
/// [`Commands::to`]), and facts of types it ingests ([`Ingests`],
/// [`Commands::fact`]), which it records and never refuses (angzarr
/// decision 14.69). For a process manager the root is the trigger's
/// correlation ([`PolicyContext::correlation_id`]) by convention.
pub struct Commands<T: CommandTarget> {
    orders: Vec<(Uuid, Any)>,
    /// Whether each of `orders` is a fact, by its index.
    facts: Vec<bool>,
    target: std::marker::PhantomData<fn() -> T>,
}

impl<T: CommandTarget> Commands<T> {
    /// No commands.
    pub fn none() -> Commands<T> {
        Commands {
            orders: Vec::new(),
            facts: Vec::new(),
            target: std::marker::PhantomData,
        }
    }

    /// These outputs and `command`, for the stream of `T` at `root`.
    pub fn to<C>(self, root: Uuid, command: C) -> Commands<T>
    where
        C: DomainMessage,
        T: Decides<C>,
    {
        self.then(root, pack_message(&command), false)
    }

    /// These outputs and `fact`, a fact of a type `T` declares one, for
    /// the stream of `T` at `root`: recorded there and never refused.
    pub fn fact<F>(self, root: Uuid, fact: F) -> Commands<T>
    where
        F: DomainMessage,
        T: Ingests<F>,
    {
        self.then(root, pack_message(&fact), true)
    }

    /// These outputs and `payload` for `root`, a fact or a command.
    fn then(self, root: Uuid, payload: Any, fact: bool) -> Commands<T> {
        let (mut orders, mut facts) = (self.orders, self.facts);
        orders.push((root, payload));
        facts.push(fact);
        Commands {
            orders,
            facts,
            target: self.target,
        }
    }

    /// Each output, packed, with the root of its stream, in order: a
    /// command, or a fact where [`Commands::is_fact`] says so.
    pub fn orders(&self) -> &[(Uuid, Any)] {
        &self.orders
    }

    /// Whether the output at `index` of [`Commands::orders`] is a fact.
    pub fn is_fact(&self, index: usize) -> bool {
        self.facts.get(index).copied().unwrap_or(false)
    }

    /// Each output as the router's `Output` (decision 14.78): its stream's
    /// root and its payload in the arm of its kind, in order.
    pub(crate) fn into_outputs(self) -> Vec<hpb::Output> {
        self.orders
            .into_iter()
            .zip(self.facts)
            .map(|((root, payload), fact)| match fact {
                true => angzarr_router::policy::fact(Some(root), payload),
                false => angzarr_router::policy::command(Some(root), payload),
            })
            .collect()
    }
}

impl<T: Domain> Commands<T> {
    /// These commands and `command`, one of `T`'s, for the stream of `T` at
    /// `root`: a hand-written policy's way to send one of a domain's
    /// command payload.
    pub fn send(self, root: Uuid, command: &<T as Domain>::Command) -> Commands<T> {
        self.then(root, command.to_any(), false)
    }
}

/// What a policy's handler reads besides the event: everything the router
/// delivers with it, none withheld (decision 14.61): the triggering
/// event's `EventRef` (its stream and sequence), the root of that stream,
/// and the event's whole `EventMeta` (its cause, correlation, fact origin,
/// rules version and `recorded_at`).
#[derive(Clone, PartialEq, Debug)]
pub struct PolicyContext {
    trigger: EventRef,
    source_root: Uuid,
    correlation_id: Uuid,
    meta: EventMeta,
}

impl PolicyContext {
    /// A context for a policy's own unit tests, its metadata only a
    /// correlation and an instant ([`PolicyContext::of`] takes the whole
    /// of it).
    pub fn new(trigger: EventRef, correlation_id: Uuid, recorded_at: Timestamp) -> Self {
        let meta = EventMeta {
            correlation_id: Some(correlation_id),
            recorded_at: Some(recorded_at),
            ..EventMeta::default()
        };
        PolicyContext::of(trigger, meta)
    }

    /// The context of the event `trigger` names, delivered with `meta`:
    /// how the kernel builds it from the committed event.
    pub fn of(trigger: EventRef, meta: EventMeta) -> Self {
        let source_root = trigger
            .stream
            .as_ref()
            .and_then(|cover| cover.root.clone())
            .unwrap_or_default();
        PolicyContext {
            trigger,
            source_root,
            correlation_id: meta.correlation_id.clone().unwrap_or_default(),
            meta,
        }
    }

    /// The event reacted to.
    pub fn trigger(&self) -> &EventRef {
        &self.trigger
    }

    /// The root of the stream the event was recorded on, the source's: the
    /// trigger's `stream.root`. A policy whose target stream is the source
    /// stream's counterpart (the same root in another domain) sends its
    /// command there. The kernel's trigger always names its stream; a
    /// context built by hand whose trigger names none has the empty root.
    pub fn source_root(&self) -> &Uuid {
        &self.source_root
    }

    /// The event's correlation: the root of a process manager's instance.
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// When the event was recorded.
    pub fn recorded_at(&self) -> Timestamp {
        self.meta.recorded_at.unwrap_or_default()
    }

    /// The event's whole metadata, as delivered.
    pub fn meta(&self) -> &EventMeta {
        &self.meta
    }

    /// The id of the command (or fact) that produced the event: its cause.
    pub fn command_id(&self) -> Option<&Uuid> {
        command_id_of(&self.meta)
    }

    /// The fact the event records, for a fact's event.
    pub fn fact_origin(&self) -> Option<&FactOrigin> {
        fact_origin_of(&self.meta)
    }

    /// The full version of the build that decided the event.
    pub fn rules_version(&self) -> &str {
        &self.meta.rules_version
    }
}

/// The producing input's id `meta` names as the event's cause, either arm
/// (decision 14.102).
fn command_id_of(meta: &EventMeta) -> Option<&Uuid> {
    match &meta.causation {
        Some(Causation::Command(cause)) => cause.command_id.as_ref(),
        Some(Causation::Fact(cause)) => cause.command_id.as_ref(),
        None => None,
    }
}

/// The fact's origin `meta` names as the event's cause: `None` for a
/// command's event.
fn fact_origin_of(meta: &EventMeta) -> Option<&FactOrigin> {
    match &meta.causation {
        Some(Causation::Fact(cause)) => cause.origin.as_ref(),
        _ => None,
    }
}

/// A projector written in the markup (`#[projector]`): one or more source
/// domains, one instance per process, each event it marks handed to its
/// handler for the event's type. It reaches the kernel as any projector
/// does, on the projector port ([`Projected`]): registered with
/// [`Registry::projector`](super::Registry::projector), run after commit
/// and awaited before the call returns, a failure the caller's error,
/// [`Failure::Projection`](super::Failure::Projection), carrying what was
/// committed (the events stay committed), and a bug, dead-lettered; or
/// caught up from the store by a [`CatchUp`](super::CatchUp). Projectors run on the async
/// command path, the only one (async everywhere), so there is no guard
/// against blocking on a synchronous one. Several
/// projectors on one domain each receive every committed event of it they
/// mark; one's failure does not stop the others.
pub trait AsyncProjector: Send + 'static {
    /// Named in the error a failure raises.
    const NAME: &'static str;
    /// The domains whose events it projects, by name, in marker order:
    /// at least one. Its one instance receives the committed events of
    /// each, every stream's in its order;
    /// [`ProjectionContext::stream`] says which stream an event is from.
    const SOURCES: &'static [&'static str];
    /// The projected events, in marker order.
    const PROJECTS: &'static [TypeName];

    /// Project `event` (an event of one of its sources, packed), from its
    /// `#[projects]` handler for the event's type; `None` for a type it
    /// does not mark. Generated.
    fn project<'a>(
        &'a mut self,
        event: &'a Any,
        ctx: &'a ProjectionContext,
    ) -> Option<Projecting<'a>>;
}

/// A marked-up projector ([`AsyncProjector`]) on the projector port,
/// angzarr-router's `ProjectorHandler`: its ONE instance, handed a book,
/// runs its `#[projects]` handler for each page it marks, in order, with
/// the page's [`ProjectionContext`]. A failure is a bug (INTERNAL, its
/// code and args the handler's report's, as the cause), as a remote
/// projector's is: [`Registry::projector`](super::Registry::projector)
/// registers one, and [`CatchUp`](super::CatchUp) catches one up.
pub struct Projected<P> {
    instance: std::sync::Mutex<Option<P>>,
}

impl<P: AsyncProjector> Projected<P> {
    /// `projector` on the port.
    pub fn new(projector: P) -> Projected<P> {
        Projected {
            instance: std::sync::Mutex::new(Some(projector)),
        }
    }
}

impl<P: AsyncProjector> angzarr_router::router::ProjectorHandler for Projected<P> {
    fn name(&self) -> &str {
        P::NAME
    }

    async fn dispatch(
        &self,
        request: &hpb::ProjectRequest,
    ) -> Result<hpb::ProjectResponse, angzarr_router::error::CodedError> {
        let unsent = EventBook::default();
        let events = request.events.as_ref().unwrap_or(&unsent);
        let mut instance = self.take().await;
        let ran = project_book(&mut instance, events).await;
        *self
            .instance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(instance);
        ran.map_err(|report| {
            angzarr_router::error::CodedError::bug(
                coded_diagnostic::Diagnostic::code(&report),
                super::route::flat_args(&report),
            )
        })?;
        Ok(hpb::ProjectResponse { projection: None })
    }
}

impl<P> Projected<P> {
    /// The one instance, out of the lock while it runs: a concurrent
    /// dispatch waits for it, a turn at a time.
    async fn take(&self) -> P {
        loop {
            let taken = self
                .instance
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            match taken {
                Some(instance) => return instance,
                None => Yield(false).await,
            }
        }
    }
}

/// Every page of `book` through `projector`'s handler for its type, in
/// order, each with its context; the first failure ends it.
async fn project_book<P: AsyncProjector>(
    projector: &mut P,
    book: &EventBook,
) -> Result<(), Report> {
    let cover = book.cover.clone().unwrap_or_default();
    for page in &book.pages {
        let meta = page.meta.clone().unwrap_or_default();
        let ctx = ProjectionContext::of(cover.clone(), page.sequence, meta);
        let Some(payload) = angzarr_router::page_event(page) else {
            continue;
        };
        let run = projector.project(payload, &ctx);
        if let Some(run) = run {
            run.await?;
        }
    }
    Ok(())
}

/// A future that is pending once, waking itself: a turn for another task.
struct Yield(bool);

impl std::future::Future for Yield {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            return std::task::Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

/// A projector handler's run: awaited by the kernel after the commit.
pub type Projecting<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Report>> + Send + 'a>>;

/// What a projector's handler reads besides the event: everything the
/// router delivers with it, none withheld (decision 14.61): the stream it
/// is from, its sequence there, and its whole `EventMeta` (its cause,
/// correlation, fact origin, rules version and `recorded_at`).
#[derive(Clone, PartialEq, Debug)]
pub struct ProjectionContext {
    stream: Cover,
    sequence: u64,
    meta: EventMeta,
}

impl ProjectionContext {
    /// A context for a projector's own unit tests, its metadata only an
    /// instant ([`ProjectionContext::of`] takes the whole of it).
    pub fn new(stream: Cover, sequence: u64, recorded_at: Timestamp) -> Self {
        let meta = EventMeta {
            recorded_at: Some(recorded_at),
            ..EventMeta::default()
        };
        ProjectionContext::of(stream, sequence, meta)
    }

    /// The context of event `sequence` of `stream`, delivered with `meta`:
    /// how the kernel builds it from the committed event.
    pub fn of(stream: Cover, sequence: u64, meta: EventMeta) -> Self {
        ProjectionContext {
            stream,
            sequence,
            meta,
        }
    }

    /// The source stream.
    pub fn stream(&self) -> &Cover {
        &self.stream
    }

    /// The event's sequence in its stream.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// When the event was recorded.
    pub fn recorded_at(&self) -> Timestamp {
        self.meta.recorded_at.unwrap_or_default()
    }

    /// The event's whole metadata, as delivered.
    pub fn meta(&self) -> &EventMeta {
        &self.meta
    }

    /// The id of the command (or fact) that produced the event: its cause.
    pub fn command_id(&self) -> Option<&Uuid> {
        command_id_of(&self.meta)
    }

    /// The event's correlation.
    pub fn correlation_id(&self) -> Option<&Uuid> {
        self.meta.correlation_id.as_ref()
    }

    /// The fact the event records, for a fact's event.
    pub fn fact_origin(&self) -> Option<&FactOrigin> {
        fact_origin_of(&self.meta)
    }

    /// The full version of the build that decided the event.
    pub fn rules_version(&self) -> &str {
        &self.meta.rules_version
    }
}

/// The test helper for a continuation step: runs `step` on `instance`,
/// applies what it emits (following its continuations, as the router
/// does), then runs `step` again on the result and panics, saying the step
/// is not idempotent and naming what it emitted, unless it emits nothing
/// and continues nowhere (decision 14.104; angzarr-router's
/// `check_step_idempotent`). Returns the instance after the first run. A
/// step must be idempotent on its own result, or a chain through it never
/// settles: there is no step cap.
pub fn assert_step_idempotent<A, S, O, E>(instance: A, step: S, ctx: &Context) -> A
where
    A: Domain<State = A>,
    A::Event: fmt::Debug,
    S: Fn(&A, &Context) -> Result<O, E>,
    O: Emits<A>,
    E: Fails<A> + fmt::Debug,
{
    let mut instance = instance;
    let mut emit = match step(&instance, ctx) {
        Ok(first) => first.into_emit(),
        Err(rejection) => panic!("the step refused: {rejection:?}"),
    };
    let mut applied = 0;
    loop {
        for event in std::mem::take(&mut emit.events) {
            decided::<A>(&mut instance, &event, ctx, applied);
            applied += 1;
        }
        let Some(next) = emit.next.take() else {
            break;
        };
        emit = match next(&instance, ctx) {
            Ok(emit) => emit,
            Err(rejection) => panic!("the step's continuation refused: {rejection}"),
        };
    }
    let again = match step(&instance, ctx) {
        Ok(again) => again.into_emit(),
        Err(rejection) => panic!("the step refused on its own result: {rejection:?}"),
    };
    if !again.events.is_empty() || again.next.is_some() {
        panic!(
            "the step is not idempotent: re-run on its own result, it emitted {:?}{}",
            again.events,
            if again.next.is_some() {
                " and continued"
            } else {
                ""
            }
        );
    }
    instance
}

/// The test helper for a FACT's continuation step: the router's own
/// `DomainDispatch::check_fact_step_idempotent` over `instance`'s table.
/// Runs `step` on `instance`, folds its own events in, runs it again there,
/// and panics, naming what the second run did, unless it emitted nothing
/// and continued nowhere. A step must be idempotent on its own result, or a
/// chain through it never settles: there is no step cap.
pub fn assert_fact_step_idempotent<A, S>(instance: &A, step: S, ctx: &CommandContext)
where
    A: Domain<State = A> + Clone + Send + Sync,
    S: Fn(&A, &CommandContext) -> Result<FactEmit<A>, Bug>,
{
    let seed = instance.clone();
    let table = super::route::table::<A>(std::sync::Arc::new(move || seed.clone()), false);
    if let Err(why) = table.check_fact_step_idempotent(instance, ctx, step) {
        panic!("the fact step is not idempotent: {why:?}");
    }
}

/// Fold `event`, the decision's `index`th, into `working`: on the stream
/// `ctx` is for, at the sequence it will be given, with the metadata the
/// kernel will stamp it with ([`CommandContext::event_meta`] of `ctx`
/// under `A`'s rules version), so the state decided is the state replayed.
fn decided<A: Domain>(working: &mut A::State, event: &A::Event, ctx: &Context, index: usize) {
    let meta = ctx.command.event_meta(A::RULES_VERSION);
    let page = PageContext {
        cover: ctx.command.cover.as_ref(),
        sequence: ctx.command.next_sequence + index as u64,
        meta: Some(&meta),
    };
    A::apply(working, event, &page);
}

// ---------------------------------------------------------------- the glue
//
// What angzarr-embedded-macros' expansion calls, and a hand-written domain,
// policy or projector calls the same way: stable, public API.

/// `value` packed as its wire message: an `Any` whose type URL is its
/// wire message's (`/package.Name`). What a hand-written payload enum's
/// [`Payload::to_any`](super::Payload::to_any) does for a domain type.
pub fn pack_message<T: DomainMessage>(value: &T) -> Any {
    super::payload::pack(&value.to_wire())
}

/// `T`'s type URL, its wire message's: `/package.Name`. What a
/// hand-written payload enum's
/// [`Payload::type_urls`](super::Payload::type_urls) lists.
pub fn type_url_of<T: DomainMessage>() -> String {
    format!("/{}", <T::Wire as prost::Name>::full_name())
}

/// `any` as a `T`, if it is one: `Ok(None)` for a payload of another
/// type, so a caller tries each of its types in turn; a payload of `T`'s
/// type whose bytes do not decode is [`PayloadError::Decode`], and one
/// that decodes but is not a valid `T` ([`DomainMessage::from_wire`]
/// refused it) is [`PayloadError::Invalid`]. What a hand-written payload
/// enum's [`Payload::from_any`](super::Payload::from_any), a policy's
/// `on_event` and a projector's `project` decode with.
pub fn decode_message<T: DomainMessage>(any: &Any) -> Result<Option<T>, PayloadError> {
    if super::payload::type_name(&any.type_url) != <T::Wire as prost::Name>::full_name() {
        return Ok(None);
    }
    let wire = <T::Wire as prost::Message>::decode(&any.value[..]).map_err(|error| {
        PayloadError::Decode {
            type_url: any.type_url.clone(),
            error,
        }
    })?;
    T::from_wire(wire)
        .map(Some)
        .map_err(|error| PayloadError::Invalid {
            type_url: any.type_url.clone(),
            reason: Report::of(&error),
        })
}

/// A marked-up domain's handlers, as angzarr-router dispatches them: what
/// `#[aggregate]` and `#[process_manager]` generate, each marked handler
/// registered on the router's table for the domain (`on_command`,
/// `on_fact`).
pub trait Routed: Domain {
    /// `table`, with every marked handler registered on it.
    fn route(
        table: angzarr_router::domain::DomainDispatch<Self::State>,
    ) -> angzarr_router::domain::DomainDispatch<Self::State>;
}

/// The context a handler reads: the router's, its instant included.
fn context_of(cctx: &CommandContext) -> Context {
    Context {
        command: cctx.clone(),
    }
}

/// The input `any` as a `T`, or the router's ANGZARR_REASON_ANY_DECODE_FAILED for a
/// payload that is not one: malformed input, a bug (INTERNAL), never a
/// refusal (decision 14.37). Bytes that do not decode carry prost's text
/// as `ext_err_txt`; a payload of another type, or not a valid `T`, its
/// type alone.
fn input<T: DomainMessage>(any: &Any) -> Result<T, HandlerError> {
    input_coded(any).map_err(HandlerError::Coded)
}

/// [`input`], its failure as the router's [`CodedError`], which a fact's
/// [`Bug`] keeps (`.bug()`): the code survives to the boundary.
fn input_coded<T: DomainMessage>(any: &Any) -> Result<T, angzarr_router::error::CodedError> {
    use angzarr_proto::v1::reason_args;
    use angzarr_router::error::{AngzarrReason, CodedError};
    let message_type = || super::payload::type_name(&any.type_url).to_string();
    match decode_message::<T>(any) {
        Ok(Some(value)) => Ok(value),
        Err(PayloadError::Decode { error, .. }) => {
            Err(CodedError::undecodable(&message_type(), &error))
        }
        Ok(None) | Err(_) => Err(super::route::spec_coded(
            AngzarrReason::AnyDecodeFailed,
            [(reason_args::MESSAGE_TYPE.to_string(), message_type())],
        )),
    }
}

/// `emit`, the markup's typed decision, as angzarr-router's `Emit`
/// (decision 14.104): its events packed, and each continuation run on the
/// state the router folds them into, with the [`Context`] of the router's,
/// its failure as the router carries it ([`handler_error`]).
fn routed<A>(emit: Emit<A>) -> angzarr_router::domain::Emit<A>
where
    A: Domain<State = A>,
{
    let Emit { events, next } = emit;
    let emitted = angzarr_router::domain::Emit::of(events.iter().map(|event| event.to_any()));
    match next {
        None => emitted,
        Some(step) => emitted.then(move |state: &A, cctx: &CommandContext| {
            step(state, &context_of(cctx))
                .map(routed)
                .map_err(handler_error)
        }),
    }
}

/// A marked-up handler's failure, as the router carries it (decision
/// 14.95): a typed refusal under its status (`ANGZARR_REASON_COMMAND_REFUSED`,
/// its object in the details, and the refusal kept for an in-process
/// caller), an immediate return (ABORTED under its own code,
/// angzarr-router's `CodedError::immediate`), a fatal bug
/// (`ANGZARR_REASON_UNHANDLED_HANDLER_ERROR`, its code the cause), or a
/// retryable bug under the status it picked.
fn handler_error(failed: Failed<TypedRefusal>) -> HandlerError {
    use angzarr_router::error::CodedError;
    match failed {
        Failed::Rejected { rejection, status } => {
            let object = rejection.object.clone();
            super::route::rejection(rejection, status, object)
        }
        Failed::Immediate(report) => HandlerError::Coded(CodedError::immediate(
            coded_diagnostic::Diagnostic::code(&report),
            super::route::flat_args(&report),
        )),
        Failed::Bug(report) => super::route::bug(&report),
        Failed::RetryableBug { status, report } => HandlerError::Coded(CodedError::retryable_bug(
            status,
            coded_diagnostic::Diagnostic::code(&report),
            super::route::flat_args(&report),
        )),
    }
}

/// Runs a command handler as ONE decision, for angzarr-router's
/// `DomainDispatch::on_command` (a [`Routed`] domain's table): decodes
/// `any` as the command `C` (a payload that is not one is the router's
/// ANGZARR_REASON_ANY_DECODE_FAILED, INTERNAL), calls `handler` with the
/// instance, the command and the [`Context`] made of `cctx`, and answers
/// its decision as the router's `Emit`, which the router runs to its end:
/// each step's events applied, in order, before the next step runs on the
/// instance. A refusal, the handler's or any step's, refuses the whole
/// decision as the router carries a typed refusal, under its status; there
/// is no step cap.
///
/// What `#[handles_cmd]` registers; a hand-written [`Routed`] domain
/// registers its own handler with it, as `#[aggregate]` would.
pub fn run_command<A, C, O, E>(
    any: &Any,
    state: &A,
    cctx: &CommandContext,
    handler: impl FnOnce(&A, C, &Context) -> Result<O, E>,
) -> Result<angzarr_router::domain::Emit<A>, HandlerError>
where
    A: Domain<State = A>,
    C: DomainMessage,
    O: Emits<A>,
    E: Fails<A>,
{
    let ctx = context_of(cctx);
    let command = input::<C>(any)?;
    decide(state, command, &ctx, handler)
}

/// [`run_command`] for a command whose natural key, `key` of it, names its
/// stream: refused before `handler` runs, when the stream's root is not
/// the key's ([`Context::rooted_by`]), refused INVALID_ARGUMENT
/// ([`NotItsStream`]: a command sent to another entity's stream is the
/// sender's mistake, and no typed refusal of the command's). What
/// `#[handles_cmd(C, rooted_by = field)]` registers.
pub fn run_rooted_command<A, C, O, E>(
    any: &Any,
    state: &A,
    cctx: &CommandContext,
    key: impl FnOnce(&C) -> &str,
    handler: impl FnOnce(&A, C, &Context) -> Result<O, E>,
) -> Result<angzarr_router::domain::Emit<A>, HandlerError>
where
    A: Domain<State = A>,
    C: DomainMessage,
    O: Emits<A>,
    E: Fails<A>,
{
    let ctx = context_of(cctx);
    let command = input::<C>(any)?;
    ctx.rooted_by(A::NAME, key(&command))
        .map_err(|refusal| super::route::invalid_argument(&refusal))?;
    decide(state, command, &ctx, handler)
}

/// `handler`'s decision on `command`, as the router's `Emit`.
fn decide<A, C, O, E>(
    state: &A,
    command: C,
    ctx: &Context,
    handler: impl FnOnce(&A, C, &Context) -> Result<O, E>,
) -> Result<angzarr_router::domain::Emit<A>, HandlerError>
where
    A: Domain<State = A>,
    O: Emits<A>,
    E: Fails<A>,
{
    handler(state, command, ctx)
        .map(|emit| routed(emit.into_emit()))
        .map_err(|e| handler_error(e.into_failed()))
}

/// Runs a fact handler as ONE decision, for angzarr-router's
/// `DomainDispatch::on_fact`: decodes `any` as the fact `F` and calls
/// `handler`, whose answer is the router's `Result<FactEmit<A>, Bug>`. A
/// payload that is not an `F` is a bug that keeps the router's code,
/// ANGZARR_REASON_ANY_DECODE_FAILED (INTERNAL). A fact is never refused:
/// the handler's one failure is a [`Bug`], and what it classifies
/// ([`Bug::coded`], [`Bug::retryable`]) reaches the boundary with its code
/// and status.
///
/// What `#[handles_fact]` registers; a handler that returns a command's
/// `Emit`, or fails with anything but a `Bug`, does not compile:
///
/// ```compile_fail
/// # use angzarr_embedded::markup::{run_fact, Context, DomainMessage, Emit};
/// # use angzarr_embedded::angzarr_router::domain::CommandContext;
/// # use angzarr_embedded::{Any, Domain};
/// # fn commanding<A: Domain<State = A>, F: DomainMessage>(
/// #     any: &Any, state: &A, cctx: &CommandContext,
/// # ) {
/// let _ = run_fact::<A, F>(any, state, cctx, |_: &A, _: F, _: &Context| {
///     Ok::<Emit<A>, angzarr_embedded::angzarr_router::error::Bug>(unimplemented!())
/// });
/// # }
/// ```
pub fn run_fact<A, F>(
    any: &Any,
    state: &A,
    cctx: &CommandContext,
    handler: impl FnOnce(&A, F, &Context) -> Result<FactEmit<A>, Bug>,
) -> Result<FactEmit<A>, Bug>
where
    A: Domain<State = A>,
    F: DomainMessage,
{
    let ctx = context_of(cctx);
    let fact = input_coded::<F>(any).bug()?;
    handler(state, fact, &ctx)
}

/// What a policy's Translate callback (`#[on_refused]`) answers on
/// success, for its source `S`: a fact `F` the source ingests
/// ([`Ingests`]), the refusal translated, or a choice of such facts that
/// implements it (angzarr-cli's generated `<Domain>Fact`). There is no
/// declining and no pass-through (decision 14.89): a refusal the callback
/// does not handle is its error, a bug. A callback answering anything else
/// does not compile.
pub trait Translation<S> {
    /// The fact, packed.
    fn fact(self) -> Any;
}

impl<S: Ingests<F>, F: DomainMessage> Translation<S> for F {
    fn fact(self) -> Any {
        pack_message(&self)
    }
}

/// A Translate callback's answer as [`Policy::on_refused`] returns it: its
/// fact, one the policy's source `S` ingests, packed; or its error as a
/// [`Report`], whose code and args are the dead letter's `cause`.
pub fn translated_fact<S, T: Translation<S>, E: coded_diagnostic::Diagnostic>(
    answer: Result<T, E>,
) -> Result<Any, Report> {
    answer
        .map(Translation::fact)
        .map_err(|error| Report::of(&error))
}

/// A policy's event handler's answer as
/// [`Policy::on_event`] returns it: its commands
/// for the target `T`, as handled (`Some`); or its error as a [`Report`],
/// whose code and args are the dead letter's `cause`.
pub fn policy_answer<T: CommandTarget, E: coded_diagnostic::Diagnostic>(
    answer: Result<Commands<T>, E>,
) -> Result<Option<Commands<T>>, Report> {
    answer.map(Some).map_err(|error| Report::of(&error))
}

/// A projector handler's run, boxed as [`AsyncProjector::project`]
/// returns it for the kernel to await.
pub fn projecting<'a, F>(run: F) -> Projecting<'a>
where
    F: std::future::Future<Output = Result<(), Report>> + Send + 'a,
{
    Box::pin(run)
}

/// The state descriptor of a domain whose anchor is `S`: `S`'s full name
/// (decision 14.101), what the registry lists a typed domain's state as.
pub fn state_descriptor<S: prost::Name>() -> angzarr_proto::v1::StateDescriptor {
    angzarr_proto::v1::StateDescriptor {
        type_name: S::full_name(),
    }
}

/// A message's protobuf package and name, as constants: what `HANDLES` and
/// `APPLIES` hold, one per marked type, read off its wire message's
/// [`prost::Name`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TypeName {
    pub package: &'static str,
    pub name: &'static str,
}

impl TypeName {
    /// `M`'s package and name.
    pub const fn of<M: prost::Name>() -> TypeName {
        TypeName {
            package: M::PACKAGE,
            name: M::NAME,
        }
    }

    /// `package.Name`: what type URLs are compared by
    /// ([`type_name`](super::type_name)).
    pub fn full_name(&self) -> String {
        match self.package.is_empty() {
            true => self.name.to_string(),
            false => format!("{}.{}", self.package, self.name),
        }
    }

    /// `/package.Name`, as angzarr writes type URLs.
    pub fn type_url(&self) -> String {
        format!("/{}", self.full_name())
    }
}

/// A marked-up aggregate's or process manager's anchor, as its domain
/// type, and whether it takes snapshots: every `#[aggregate]` and
/// `#[process_manager]` implements it, and the markup writes its
/// [`State`](super::State) from it, the anchor's wire type its registered
/// state type. An implementation that keeps both defaults takes no
/// snapshots, and every load replays from the start:
///
/// ```text
/// impl Snapshots for Lamp {
///     type Anchor = LampState;
/// }
/// ```
///
/// One that overrides both opts in: it offers its state with every answer,
/// which the kernel stores as the domain's storage policy says, and
/// resumes a fresh instance from one (docs/design/snapshots.md):
///
/// ```text
/// impl Snapshots for Wallet {
///     type Anchor = WalletState;
///
///     fn snapshot(&self) -> Option<WalletState> {
///         Some(self.state.clone())
///     }
///
///     fn restore(&mut self, state: WalletState) -> Result<(), Report> {
///         self.state = state;
///         Ok(())
///     }
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` names no anchor: a marked-up aggregate or process manager implements `Snapshots`",
    note = "`impl Snapshots for {Self} {{ type Anchor = ..; }}` names it and takes no snapshots; \
            overriding `snapshot` and `restore` opts in"
)]
pub trait Snapshots {
    /// The component's anchor, as its domain type: its wire type is the
    /// state type the component registers, and what a snapshot holds.
    type Anchor: DomainMessage;

    /// The state as its anchor, offered with every answer; `None` offers
    /// none, as the default does.
    fn snapshot(&self) -> Option<Self::Anchor> {
        None
    }

    /// Take the state `anchor`, a snapshot's, holds, keeping what it does
    /// not hold (the factory's configuration). The default refuses every
    /// snapshot ([`NotSnapshotted`](super::NotSnapshotted)): the kernel
    /// then offers it none, and replays the stream from its first event, a
    /// snapshot an earlier build stored ignored.
    fn restore(&mut self, _anchor: Self::Anchor) -> Result<(), Report> {
        Err(Report::of(&super::NotSnapshotted::of::<
            <Self::Anchor as DomainMessage>::Wire,
        >()))
    }
}

/// `state`'s [`Snapshots::snapshot`], as its anchor's wire type: what a
/// marked-up domain's [`State::snapshot`](super::State::snapshot) offers.
pub fn snapshot_wire<S: Snapshots>(state: &S) -> Option<<S::Anchor as DomainMessage>::Wire> {
    state.snapshot().map(|anchor| anchor.to_wire())
}

/// `wire` restored onto `state` ([`Snapshots::restore`]), once it reads as
/// the anchor's domain type: a value it cannot hold is refused, naming
/// the field, before `restore` sees it, the state left as it was. What a
/// marked-up domain's [`State::restore`](super::State::restore) does.
pub fn restore_wire<S: Snapshots>(
    state: &mut S,
    wire: <S::Anchor as DomainMessage>::Wire,
) -> Result<(), Report> {
    let anchor = S::Anchor::from_wire(wire).map_err(|error| Report::of(&error))?;
    state.restore(anchor)
}
