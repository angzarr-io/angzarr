//! Errors, kept apart by whose they are: a domain's rejection is for the
//! person who gave the command; a [`Malformed`] book is the caller's; a
//! [`KernelError`] is a fault in delivery, storage or a log, for the
//! operator.
//!
//! Each is a [`Diagnostic`]. A condition the spec keys is coded under its
//! key, verbatim (`ANGZARR_REASON_RETRY_CAP_EXHAUSTED`), its args
//! the key's, and its English is the spec's catalog
//! (`angzarr_proto::REASONS_EN_US`; decision 14.60); every other is coded
//! under the crate's name (`code!("...")`), its messages in
//! `locales/en-US/messages.ftl` ([`EN_US`](crate::EN_US)). A wrapper of another error
//! ([`Failure::Rejected`], [`Failure::Kernel`], [`StepError`],
//! [`KernelError::Malformed`], [`KernelError::Replay`],
//! [`KernelError::Store`], [`KernelError::Router`], [`UpcastError::Payload`])
//! is that error's diagnostic. The store's errors are angzarr-ports'
//! `StoreError`, shared with angzarr: [`KernelError::Store`] forwards its
//! `angzarr-ports-*` code and args unchanged, rendered from angzarr-ports'
//! own catalog. Every error about a payload names its type URL; a stream
//! is named as `domain/root`, with `@edition` outside the main timeline.

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic, Report};
use angzarr_ports::StoreError;
use angzarr_proto::v1::{Cover, EventBook, EventRef, Uuid};

use super::check::Malformed;
use super::route::SpecKey;
use super::store::DeadLetter;

/// Why a command, or a step its chain set off, failed.
#[derive(Clone, PartialEq, Debug)]
pub enum Failure<R> {
    /// The domain refused it, deliberately: under its own code, a business
    /// status (FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND,
    /// ALREADY_EXISTS, OUT_OF_RANGE, PERMISSION_DENIED); under the
    /// router's, a FAILED_PRECONDITION. Nothing was staged, appended or
    /// applied.
    Rejected(R),
    /// A step further down the chain failed: a policy's command rejected by
    /// its target, a malformed policy book, a gap, a chain too deep, a store
    /// that failed a step's write. `path` runs from the first policy step to
    /// the one that failed. Every step before it stays committed, the
    /// originating command's included; the failed step wrote nothing but,
    /// where it was refused, its position past it. `dead_letters` are the
    /// letters the chain's steps wrote ([`Failure::dead_letters`]).
    Downstream {
        path: Vec<Step>,
        error: StepError,
        dead_letters: Vec<DeadLetter>,
    },
    /// The book was malformed, or the kernel or the store failed at the
    /// originating stream, its write included (a conflict). Nothing was
    /// committed, but for a SIMPLE call's named projector that failed
    /// ([`KernelError::SyncProjectionFailed`]): its decision committed.
    Kernel(KernelError),
    /// The chain COMMITTED, then marked-up projectors (run after the
    /// commit and awaited) failed: each `(projector, error)` in
    /// `failures`, in registration order; a failing projector does not
    /// stop the others. `committed` is what the chain appended, every
    /// stream's book: those events stay committed. Its recovery is
    /// [`Recovery::Retry`]: a retry is answered DUPLICATE. `dead_letters`
    /// are the letters the chain's steps and its projectors wrote.
    Projection {
        committed: Vec<EventBook>,
        failures: Vec<(String, Report)>,
        dead_letters: Vec<DeadLetter>,
    },
}

/// One step of a command chain: a policy's command delivered to `stream`,
/// caused by `cause`. Policies are pure, so the step's command is the
/// policy's output for that cause, recomputable from the log.
#[derive(Clone, PartialEq, Debug)]
pub struct Step {
    pub stream: Cover,
    pub cause: EventRef,
}

/// Why a step down the chain failed.
#[derive(Clone, PartialEq, Debug)]
pub enum StepError {
    /// The target's decide rejected the command: its rejection, as a
    /// report.
    Rejected(Report),
    /// A kernel fault at that step. A malformed policy book is a bug here,
    /// not the caller's: it is also written to the dead-letter log.
    Kernel(KernelError),
    /// The policy itself failed: its `on_event` or its `on_refused` (its
    /// Translate callback) answered an error, or a marked event did not decode. A defect, never a business refusal: policies react to
    /// events the application's own domains recorded, so an error is a
    /// bug. Its report (code and args) is the `cause` of the dead letter's
    /// reason, `ANGZARR_REASON_UNHANDLED_HANDLER_ERROR`, and the
    /// policy's position moved past the trigger (or stayed at it, under
    /// [`OnPolicyBug::Stay`](super::OnPolicyBug::Stay)).
    PolicyFailed(Report),
}

/// A fault outside any domain's rules.
#[derive(Clone, PartialEq, Debug)]
pub enum KernelError {
    /// The book was refused at step 1, before anything else ran.
    Malformed(Malformed),
    /// The kernel gave up on a command that lost a race at its write to
    /// another write to the same stream, every time, until the race re-run
    /// cap ([`OnRetryable::max_race_runs`](super::OnRetryable::max_race_runs);
    /// docs/design/pipeline.md §4): each run decided again on the new
    /// head, and a command with an expected sequence refused on its
    /// re-run instead. `attempts` is how many times business code ran,
    /// `cap` the limit reached. Nothing is persisted; a policy's step is
    /// dead-lettered under this error, and a command its caller waits for
    /// is the caller's (owner ruling, 2026-10-31). angzarr's INTERNAL,
    /// `ANGZARR_REASON_RETRY_CAP_EXHAUSTED`: the retries are spent, so no
    /// caller retries it. A fact at the cap is dead-lettered and answered
    /// [`KernelError::FactDeadLettered`].
    RetryCapExhausted {
        stream: Cover,
        attempts: u32,
        cap: u32,
    },
    /// The handler of `stream`'s domain answered reporting rules version
    /// `reported`, not `registered`, the version its domain is registered
    /// at (`HandleResponse.rules_version`, C-0558): a handler bug, refused
    /// with nothing persisted, and dead-lettered as every handler bug is.
    /// angzarr's INTERNAL, `ANGZARR_REASON_RULES_VERSION_MISMATCH`
    /// (decision 14.56), its `component` the stream's domain, with
    /// `registered` and `reported`.
    RulesVersionMismatch {
        stream: Cover,
        registered: String,
        reported: String,
    },
    /// The handler of `stream`'s domain answered a state of type
    /// `state_type`, not `registered`, the state type its domain is
    /// registered with (C-0818; decision 14.102): a handler bug, refused
    /// with nothing persisted, and dead-lettered. angzarr's INTERNAL,
    /// `ANGZARR_REASON_STATE_TYPE_MISMATCH`, its `component` the stream's
    /// domain.
    StateTypeMismatch {
        stream: Cover,
        registered: String,
        state_type: String,
    },
    /// A fact could not be recorded on `stream`: its handler failed, its
    /// retryable failure outlasted its retries, it lost its race at the
    /// write until the cap, or a process manager has no instance for it.
    /// It is dead-lettered, and its sender, waiting or not, is answered
    /// angzarr's INTERNAL `ANGZARR_REASON_FACT_DEAD_LETTERED`, `cause` the
    /// failure's key, non-retryable (decisions 14.100 and 14.109).
    FactDeadLettered {
        stream: Cover,
        cause: Report,
    },
    /// A command's or fact's payload type that the domain does not decode,
    /// after the upcaster: refused naming its type URL (angzarr's
    /// INVALID_ARGUMENT). Found at step 5, after deduplication.
    UnknownType {
        stream: Cover,
        type_url: String,
    },
    /// A command's or fact's payload that decodes as a type the domain
    /// reads, but is not a valid value of it (a required field absent, an
    /// undefined enum value): the sender's to fix, as `UnknownType` is.
    InvalidPayload {
        stream: Cover,
        type_url: String,
        reason: Report,
    },
    /// A command's or fact's upcast rule failed.
    Upcast {
        stream: Cover,
        error: UpcastError,
    },
    /// A book or a log for `stream` was given to an `expected` domain's
    /// code.
    WrongDomain {
        stream: Cover,
        expected: &'static str,
    },
    /// A command chain went deeper than its limit: a policy cycle, or a
    /// cascade longer than allowed.
    ChainTooDeep {
        limit: usize,
    },
    /// A dispatch from before a resumed stream's snapshot: those events are
    /// not held.
    BeforeSnapshot {
        stream: Cover,
        first: u64,
    },
    /// A stored stream of `domain` was replayed, resumed or dispatched, or
    /// its typed step run, but the registry does not hold `domain`: the
    /// application did not register it. A misconfiguration, unlike an
    /// incoming book for an unregistered domain
    /// ([`Malformed::UnknownDomain`], the caller's).
    UnregisteredDomain {
        domain: String,
    },
    /// Event `sequence` of `stream` is stored but cannot be replayed as a
    /// current event: a payload type nothing upcasts or decodes (replay is
    /// strict), or bytes that do not decode as it. `reason` names what
    /// could not be read (the type URL). A bug or a missing upcaster.
    Unreadable {
        stream: Cover,
        sequence: u64,
        reason: Report,
    },
    /// Event `sequence` of `stream`, delivered to a subscription on a bus,
    /// waits on event `missing`, which no source has, past the retry cap:
    /// its delivery is dead-lettered and the stream held, never skipped.
    PredecessorMissing {
        stream: Cover,
        sequence: u64,
        missing: u64,
    },
    Replay(ReplayError),
    /// The store's own error, angzarr-ports' (codes `angzarr-ports-*`). On
    /// the wire ([`KernelError::as_coded`]) a store that could not answer
    /// is `ANGZARR_REASON_STORE_UNAVAILABLE`, UNAVAILABLE (decision 14.51).
    Store(StoreError),
    /// angzarr-router refused the input or a policy's answer: its
    /// `CodedError`, whose `code` (SCREAMING_SNAKE, angzarr's `ErrorInfo`
    /// reason) is this error's `reason`; or one of the kernel's own spec
    /// keys answered in the router's shape (a process manager with no
    /// instance, `ANGZARR_REASON_PM_NO_INSTANCE`; an edition stream not
    /// forked, `ANGZARR_REASON_EDITION_NOT_FORKED`). A typed refusal is
    /// never this ([`Failure::Rejected`]); under a business status it is
    /// the framework's refusal, answered to a client and never a policy's
    /// Translate; under INTERNAL a bug; ABORTED a handler's re-run.
    Router(angzarr_router::error::CodedError),
    /// The domain's handler could not be reached (a remote handler whose
    /// process is not running, or whose transport failed: its
    /// `ANGZARR_REASON_HANDLER_UNAVAILABLE`, UNAVAILABLE). Nothing ran
    /// and nothing was written:
    /// run it again, as [`Kernel::resume`](super::Kernel::resume) does for
    /// a policy's step.
    HandlerUnavailable {
        stream: Cover,
        cause: String,
    },
    /// A SIMPLE call ([`Kernel::handle_sync`](super::Kernel::handle_sync))
    /// named no projector to wait for: refused before anything ran.
    /// angzarr's INVALID_ARGUMENT, `ANGZARR_REASON_SYNC_NO_PROJECTORS`
    /// (decision 14.107: a caller that waits for none calls Handle).
    SyncNoProjectors,
    /// A SIMPLE call named `projector`, which is not registered for the
    /// events of the command's `domain` (an unknown name included): refused
    /// before anything ran. angzarr's INVALID_ARGUMENT,
    /// `ANGZARR_REASON_SYNC_PROJECTOR_NOT_SUBSCRIBED`.
    SyncProjectorNotSubscribed {
        domain: String,
        projector: String,
    },
    /// A SIMPLE call's named `projector` failed to project the events of
    /// `stream` the command committed, or was still running when the
    /// call's deadline passed: `cause` its failure's key, or
    /// [`DeadlinePassed`](super::DeadlinePassed). The decision stays
    /// committed; the call is answered with no projection at all, and is
    /// never dead-lettered: its caller resends it with the same command id,
    /// a DUPLICATE that waits on the projectors again (decision 14.105).
    /// angzarr's UNAVAILABLE, `ANGZARR_REASON_SYNC_PROJECTION_FAILED`.
    SyncProjectionFailed {
        stream: Cover,
        projector: String,
        cause: Report,
    },
}

/// Why a log could not be replayed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReplayError {
    /// The page at `index` claims sequence `found`.
    OutOfSequence { index: usize, found: u64 },
    /// Event `sequence` was recorded before the event before it.
    RecordedBackwards { sequence: u64 },
    /// Event `sequence`'s `EventMeta` lacks `field` (`causation`,
    /// `correlation_id` or `recorded_at`), or holds it malformed: every
    /// recorded event has all three.
    IncompleteMeta { sequence: u64, field: &'static str },
    /// The log is `found`'s, given to an `expected` domain's code.
    WrongDomain {
        found: String,
        expected: &'static str,
    },
}

/// A payload that is not one of the types a domain decodes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PayloadError {
    /// No decoder for this type URL: not a current type of the domain's
    /// (strict: never skipped).
    UnknownType { type_url: String },
    /// The type is known, but its bytes do not decode as it.
    Decode {
        type_url: String,
        error: prost::DecodeError,
    },
    /// The bytes decode, but the message is not a valid domain value: a
    /// required field absent, an enum value the domain does not define
    /// ([`WireError`](super::WireError)), or a state's own invariant.
    Invalid { type_url: String, reason: Report },
}

/// An upcast rule that failed on one payload.
#[derive(Clone, PartialEq, Debug)]
pub enum UpcastError {
    /// The rule could not read its input.
    Payload(PayloadError),
    /// The rule from `source` failed for the domain's own reason.
    Rule { source: String, error: Report },
    /// The upcaster sidecar could not be reached, or answered under a
    /// retryable status (decision 14.39): what needs upcasting waits, and
    /// a caller is answered UNAVAILABLE, to retry. `cause` is why, as the
    /// sidecar's port reported it.
    Unavailable { cause: Report },
    /// The upcaster answered keys that are not exactly the payloads asked:
    /// for a stream's pages, each page's sequence once; for a lone payload,
    /// one payload keyed `NoSequence` (docs/design/upcasting.md §5 and §6:
    /// a rule bug, with no spec key of its own).
    AnswerKeys,
}

/// An upcaster whose rules cannot all run, found by
/// [`Registry::check`](super::Registry::check) before anything is read.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UpcastChainError {
    /// The domain, named in every error.
    pub domain: String,
    pub problem: ChainProblem,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ChainProblem {
    /// Two typed rules from the same type: the second never runs.
    Duplicate { source: String },
    /// A typed rule from `source` registered before the rule that produces
    /// `source`: rules run in registration order, so a payload reaching
    /// `source` stops there.
    OutOfOrder { source: String, produced_by: String },
    /// A chain from `source` ends at `ends_at`, which the domain does not
    /// decode: every payload of that type would be refused.
    DeadEnd { source: String, ends_at: String },
}

/// What to do about a failure: angzarr's one recovery (angzarr-diagnostic's),
/// which a store's error shares. The kernel retries a step's
/// [`Recovery::Retry`] itself, with backoff, up to a cap, then dead-letters
/// it ([`OnRetryable`](super::OnRetryable)); the others it never retries.
/// Here, `Retry` is a store conflict, a stale or unresolved command (re-read
/// and re-issue against the new head), the store unavailable, a handler that
/// could not be reached or answered under a retryable status, a gap (catch
/// the target up first); `Business` a rejection, at the origin or
/// downstream; `Invalid` a malformed book or a payload type the domain does
/// not decode (angzarr's INVALID_ARGUMENT); `Bug` a malformed policy book, a
/// handler's bug or panic (a status of the bug class: INTERNAL, UNKNOWN,
/// DATA_LOSS), the router's own error on malformed input, a discontinuous
/// append, a key of the wrong domain, a chain too deep, unknown rules, a log
/// that does not replay, an event the store cannot read back.
pub use angzarr_diagnostic::Recovery;

impl Failure<super::route::Rejected> {
    /// The domain's own refusal, typed as `R`, when this is a refusal its
    /// handler in this process made ([`Rejected::rejection`](super::Rejected::rejection)):
    /// for a typed domain `D`, `failure.rejection::<D::Rejection>()`.
    pub fn rejection<R: std::any::Any>(&self) -> Option<&R> {
        match self {
            Failure::Rejected(rejected) => rejected.rejection(),
            _ => None,
        }
    }
}

impl<R> Failure<R> {
    /// The dead letters the call wrote before it failed, whole and keyed
    /// as the store keeps them (owner ruling Q5): what its steps with no
    /// caller left gave up on. None for [`Failure::Rejected`] and
    /// [`Failure::Kernel`]: the command itself failed, and a command its
    /// caller waits for is answered, never dead-lettered here.
    pub fn dead_letters(&self) -> &[DeadLetter] {
        match self {
            Failure::Downstream { dead_letters, .. } | Failure::Projection { dead_letters, .. } => {
                dead_letters
            }
            Failure::Rejected(_) | Failure::Kernel(_) => &[],
        }
    }

    pub fn recovery(&self) -> Recovery {
        match self {
            Failure::Rejected(_) => Recovery::Business,
            Failure::Downstream { error, .. } => error.recovery(),
            Failure::Kernel(error) => error.recovery(),
            Failure::Projection { .. } => Recovery::Retry,
        }
    }
}

impl StepError {
    pub fn recovery(&self) -> Recovery {
        match self {
            StepError::Rejected(_) => Recovery::Business,
            StepError::PolicyFailed(_) => Recovery::Bug,
            StepError::Kernel(KernelError::Malformed(_)) => Recovery::Bug,
            StepError::Kernel(error) => error.recovery(),
        }
    }
}

impl KernelError {
    /// This error as the coordinator answers it on the wire, in the
    /// router's shape: its reason key, its args as metadata, and its
    /// status; `None` for an error that is not one of the coordinator's
    /// answers. A variant whose code is a spec key (decision 14.60) goes
    /// out as itself, under the status its key's option declares (decision
    /// 14.65): a command given up at the race re-run cap, a fact
    /// dead-lettered, a response of another rules version or state type
    /// and a chain too deep INTERNAL; a handler that could not be reached
    /// UNAVAILABLE (decision 14.49). The store failing to answer is
    /// `ANGZARR_REASON_STORE_UNAVAILABLE` (the store's failure its
    /// `cause`), and an upcaster sidecar that could not be reached
    /// `ANGZARR_REASON_UPCASTER_UNAVAILABLE` (`domain` and `cause`), each
    /// UNAVAILABLE, so that when the retries are spent the key is what is
    /// reported (decision 14.51).
    pub fn as_coded(&self) -> Option<angzarr_router::error::CodedError> {
        if let Some(coded) = self.forwarded() {
            return Some(coded);
        }
        let CodedAs::Spec(key) = self.coded_as() else {
            return None;
        };
        Some(super::route::spec_coded(key, super::route::flat_args(self)))
    }

    /// The store's and the upcaster sidecar's failures the coordinator
    /// answers under its own keys, UNAVAILABLE: their own codes are
    /// angzarr-ports' and the kernel's, a level below the condition the
    /// coordinator reports.
    fn forwarded(&self) -> Option<angzarr_router::error::CodedError> {
        use angzarr_proto::v1::reason_args as args;
        use angzarr_proto::v1::AngzarrReason;
        let (reason, extras) = match self {
            KernelError::Store(error @ StoreError::Unavailable(_)) => (
                AngzarrReason::StoreUnavailable,
                super::route::flat_args(&store_report(error)),
            ),
            KernelError::Upcast {
                stream,
                error: error @ UpcastError::Unavailable { .. },
            } => (
                AngzarrReason::UpcasterUnavailable,
                std::iter::once((args::DOMAIN.to_string(), stream.domain.clone()))
                    .chain(super::route::flat_args(error))
                    .collect(),
            ),
            _ => return None,
        };
        Some(super::route::spec_coded(reason, extras))
    }

    /// What this error is coded under: the spec key a condition the spec
    /// keys travels as (decision 14.60), whose status its option declares
    /// (decision 14.65); angzarr-embedded's own code; or the code of the
    /// error it wraps.
    fn coded_as(&self) -> CodedAs<'_> {
        use angzarr_proto::v1::AngzarrReason as K;
        use CodedAs::{Own, Spec, Wrapped};
        match self {
            KernelError::RetryCapExhausted { .. } => Spec(K::RetryCapExhausted),
            KernelError::RulesVersionMismatch { .. } => Spec(K::RulesVersionMismatch),
            KernelError::StateTypeMismatch { .. } => Spec(K::StateTypeMismatch),
            KernelError::FactDeadLettered { .. } => Spec(K::FactDeadLettered),
            KernelError::ChainTooDeep { .. } => Spec(K::ChainTooDeep),
            KernelError::HandlerUnavailable { .. } => Spec(K::HandlerUnavailable),
            KernelError::SyncNoProjectors => Spec(K::SyncNoProjectors),
            KernelError::SyncProjectorNotSubscribed { .. } => Spec(K::SyncProjectorNotSubscribed),
            KernelError::SyncProjectionFailed { .. } => Spec(K::SyncProjectionFailed),
            KernelError::UnknownType { .. } => Own(code!("unknown-type")),
            KernelError::InvalidPayload { .. } => Own(code!("invalid-payload")),
            KernelError::Upcast { .. } => Own(code!("upcast")),
            KernelError::WrongDomain { .. } => Own(code!("wrong-domain")),
            KernelError::BeforeSnapshot { .. } => Own(code!("before-snapshot")),
            KernelError::UnregisteredDomain { .. } => Own(code!("unregistered-domain")),
            KernelError::Unreadable { .. } => Own(code!("unreadable")),
            KernelError::PredecessorMissing { .. } => Own(code!("predecessor-missing")),
            KernelError::Malformed(m) => Wrapped(m.code()),
            KernelError::Replay(error) => Wrapped(error.code()),
            KernelError::Store(error) => Wrapped(error.code()),
            KernelError::Router(error) => Wrapped(&error.code),
        }
    }

    pub fn recovery(&self) -> Recovery {
        match self {
            KernelError::Malformed(_)
            | KernelError::UnknownType { .. }
            | KernelError::InvalidPayload { .. }
            | KernelError::SyncNoProjectors
            | KernelError::SyncProjectorNotSubscribed { .. } => Recovery::Invalid,
            KernelError::Store(error) => error.recovery(),
            KernelError::Router(error) => router_recovery(error),
            KernelError::HandlerUnavailable { .. }
            | KernelError::SyncProjectionFailed { .. }
            | KernelError::Upcast {
                error: UpcastError::Unavailable { .. },
                ..
            } => Recovery::Retry,
            KernelError::Upcast { .. }
            | KernelError::RetryCapExhausted { .. }
            | KernelError::RulesVersionMismatch { .. }
            | KernelError::StateTypeMismatch { .. }
            | KernelError::FactDeadLettered { .. }
            | KernelError::WrongDomain { .. }
            | KernelError::ChainTooDeep { .. }
            | KernelError::BeforeSnapshot { .. }
            | KernelError::UnregisteredDomain { .. }
            | KernelError::Unreadable { .. }
            | KernelError::PredecessorMissing { .. }
            | KernelError::Replay(_) => Recovery::Bug,
        }
    }
}

/// A store's error as a report: angzarr-ports' code and args, as
/// [`KernelError::Store`] forwards them.
fn store_report(error: &StoreError) -> Report {
    store_args(error)
        .into_iter()
        .fold(Report::new(error.code()), |report, (name, arg)| {
            report.arg(name, arg)
        })
}

/// A stream as an error names it: `domain/root`, then `@edition` outside
/// the main timeline.
pub(crate) fn cover_text(cover: &Cover) -> String {
    let root = cover.root.as_ref().map(Uuid::to_string).unwrap_or_default();
    match &cover.edition {
        Some(edition) => format!("{}/{root}@{edition}", cover.domain),
        None => format!("{}/{root}", cover.domain),
    }
}

fn stream(cover: &Cover) -> Arg {
    cover_text(cover).into()
}

// ---------------------------------------------------------------- Failure

impl<R: Diagnostic> fmt::Display for Failure<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl<R: Diagnostic> Diagnostic for Failure<R> {
    fn code(&self) -> &str {
        match self {
            Failure::Rejected(rejection) => rejection.code(),
            Failure::Downstream { .. } => code!("downstream"),
            Failure::Kernel(error) => error.code(),
            Failure::Projection { .. } => code!("projection-failed"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            Failure::Rejected(rejection) => rejection.args(),
            Failure::Downstream { path, error, .. } => {
                vec![("depth", path.len().into()), ("error", Arg::nested(error))]
            }
            Failure::Kernel(error) => error.args(),
            Failure::Projection { failures, .. } => {
                let mut args = vec![("failures", failures.len().into())];
                if let Some((projector, error)) = failures.first() {
                    args.push(("projector", projector.as_str().into()));
                    args.push(("error", Arg::nested(error)));
                }
                args
            }
        }
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for StepError {
    fn code(&self) -> &str {
        match self {
            StepError::Rejected(rejection) | StepError::PolicyFailed(rejection) => rejection.code(),
            StepError::Kernel(error) => error.code(),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            StepError::Rejected(rejection) | StepError::PolicyFailed(rejection) => rejection.args(),
            StepError::Kernel(error) => error.args(),
        }
    }
}

// ---------------------------------------------------------------- KernelError

/// A store's error, as the kernel forwards it.
impl From<StoreError> for KernelError {
    fn from(error: StoreError) -> KernelError {
        KernelError::Store(error)
    }
}

impl fmt::Display for KernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

/// A variant the spec keys is coded under its key, verbatim, with exactly
/// the args its `(io.angzarr.v1.reason)` option declares (decision 14.60);
/// the router's error under the key it answered, its metadata the args; a
/// store's error under angzarr-ports' code; every other under
/// angzarr-embedded's own.
impl Diagnostic for KernelError {
    fn code(&self) -> &str {
        match self.coded_as() {
            CodedAs::Spec(key) => key.key(),
            CodedAs::Own(code) | CodedAs::Wrapped(code) => code,
        }
    }

    /// The stream the error names first, then what else it names
    /// ([`KernelError::named_stream`], [`KernelError::details`]).
    fn args(&self) -> Vec<(&str, Arg)> {
        let named = self
            .named_stream()
            .map(|s| (angzarr_proto::v1::reason_args::STREAM, stream(s)));
        named.into_iter().chain(self.details()).collect()
    }
}

impl KernelError {
    /// The stream this error names first among its args: every error that
    /// concerns one stream, coded under a spec key that declares `stream`
    /// (decision 14.65) or under angzarr-embedded's own code.
    fn named_stream(&self) -> Option<&Cover> {
        match self {
            KernelError::RetryCapExhausted { stream, .. }
            | KernelError::RulesVersionMismatch { stream, .. }
            | KernelError::StateTypeMismatch { stream, .. }
            | KernelError::FactDeadLettered { stream, .. }
            | KernelError::UnknownType { stream, .. }
            | KernelError::InvalidPayload { stream, .. }
            | KernelError::Upcast { stream, .. }
            | KernelError::WrongDomain { stream, .. }
            | KernelError::BeforeSnapshot { stream, .. }
            | KernelError::Unreadable { stream, .. }
            | KernelError::PredecessorMissing { stream, .. }
            | KernelError::HandlerUnavailable { stream, .. }
            | KernelError::SyncProjectionFailed { stream, .. } => Some(stream),
            KernelError::Malformed(_)
            | KernelError::SyncNoProjectors
            | KernelError::SyncProjectorNotSubscribed { .. }
            | KernelError::ChainTooDeep { .. }
            | KernelError::UnregisteredDomain { .. }
            | KernelError::Replay(_)
            | KernelError::Store(_)
            | KernelError::Router(_) => None,
        }
    }

    /// The args after its stream: a spec key's as its option declares
    /// them, angzarr-embedded's own as its catalog does, a wrapped error's
    /// its own.
    fn details(&self) -> Vec<(&str, Arg)> {
        use angzarr_proto::v1::reason_args as args;
        match self {
            KernelError::Malformed(m) => m.args(),
            KernelError::RetryCapExhausted { attempts, cap, .. } => vec![
                (args::ATTEMPTS, (*attempts).into()),
                (args::CAP, (*cap).into()),
            ],
            KernelError::RulesVersionMismatch {
                stream: s,
                registered,
                reported,
            } => vec![
                (args::COMPONENT, s.domain.as_str().into()),
                (args::REGISTERED, registered.as_str().into()),
                (args::REPORTED, reported.as_str().into()),
            ],
            KernelError::StateTypeMismatch {
                stream: s,
                registered,
                state_type,
            } => vec![
                (args::COMPONENT, s.domain.as_str().into()),
                (args::REGISTERED, registered.as_str().into()),
                (args::STATE_TYPE, state_type.as_str().into()),
            ],
            KernelError::FactDeadLettered { cause, .. } => vec![(args::CAUSE, Arg::nested(cause))],
            KernelError::UnknownType { type_url, .. } => {
                vec![("type_url", type_url.as_str().into())]
            }
            KernelError::InvalidPayload {
                type_url, reason, ..
            } => vec![
                ("type_url", type_url.as_str().into()),
                ("reason", Arg::nested(reason)),
            ],
            KernelError::Upcast { error, .. } => vec![("error", Arg::nested(error))],
            KernelError::WrongDomain { expected, .. } => vec![("expected", (*expected).into())],
            KernelError::ChainTooDeep { limit } => vec![(args::LIMIT, (*limit).into())],
            KernelError::BeforeSnapshot { first, .. } => vec![("first", (*first).into())],
            KernelError::UnregisteredDomain { domain } => vec![("domain", domain.as_str().into())],
            KernelError::Replay(error) => error.args(),
            KernelError::Unreadable {
                sequence, reason, ..
            } => vec![
                ("sequence", (*sequence).into()),
                ("reason", Arg::nested(reason)),
            ],
            KernelError::PredecessorMissing {
                sequence, missing, ..
            } => vec![
                ("sequence", (*sequence).into()),
                ("missing", (*missing).into()),
            ],
            KernelError::Store(error) => store_args(error),
            KernelError::Router(error) => error
                .extras
                .iter()
                .map(|(name, value)| (name.as_str(), Arg::Str(value.clone())))
                .collect(),
            KernelError::HandlerUnavailable { stream: s, cause } => vec![
                (args::DOMAIN, s.domain.as_str().into()),
                (args::CAUSE, cause.as_str().into()),
            ],
            KernelError::SyncNoProjectors => Vec::new(),
            KernelError::SyncProjectorNotSubscribed { domain, projector } => vec![
                (args::DOMAIN, domain.as_str().into()),
                (args::PROJECTOR, projector.as_str().into()),
            ],
            KernelError::SyncProjectionFailed {
                projector, cause, ..
            } => vec![
                (args::PROJECTOR, projector.as_str().into()),
                (args::CAUSE, Arg::nested(cause)),
            ],
        }
    }
}

/// What a [`KernelError`] is coded under ([`KernelError::coded_as`]).
enum CodedAs<'a> {
    /// A spec key, verbatim: the condition the spec keys.
    Spec(angzarr_proto::v1::AngzarrReason),
    /// angzarr-embedded's own code.
    Own(&'static str),
    /// The code of the error the variant wraps.
    Wrapped(&'a str),
}

/// The four classes of error (decision 14.37), decided by gRPC status
/// alone, and by the kernel itself (decision 14.42): the router answers a
/// status and a key and never classifies.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Class {
    /// FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS,
    /// OUT_OF_RANGE, PERMISSION_DENIED: a refusal, final; to the caller, or
    /// the issuing policy's OnRefused; never retried.
    BusinessFatal,
    /// ABORTED: re-read and re-decide. Returned at once to an outside
    /// caller; a policy step, whose caller is the kernel, is run again.
    BusinessImmediate,
    /// INTERNAL, UNKNOWN, DATA_LOSS: dead-lettered and alerted at once.
    BugFatal,
    /// Every other status (UNAVAILABLE, DEADLINE_EXCEEDED,
    /// RESOURCE_EXHAUSTED, UNIMPLEMENTED, CANCELLED, ...): retried under the
    /// capped backoff, then dead-lettered.
    BugRetryable,
}

/// The class of the gRPC status `code`, by its wire number.
pub fn class(code: i32) -> Class {
    match code {
        3 | 5 | 6 | 7 | 9 | 11 => Class::BusinessFatal,
        10 => Class::BusinessImmediate,
        2 | 13 | 15 => Class::BugFatal,
        _ => Class::BugRetryable,
    }
}

/// What to do about the router's error: its status decides its class
/// ([`class`], decision 14.37). A business refusal is
/// the caller's to hear, a fatal bug an operator's; ABORTED (reload) and a
/// retryable bug are run again.
fn router_recovery(error: &angzarr_router::error::CodedError) -> Recovery {
    match class(error.grpc as i32) {
        Class::BusinessFatal => Recovery::Business,
        Class::BugFatal => Recovery::Bug,
        Class::BusinessImmediate | Class::BugRetryable => Recovery::Retry,
    }
}

// ---------------------------------------------------------------- ReplayError

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for ReplayError {
    fn code(&self) -> &str {
        match self {
            ReplayError::OutOfSequence { .. } => code!("replay-out-of-sequence"),
            ReplayError::RecordedBackwards { .. } => code!("replay-recorded-backwards"),
            ReplayError::IncompleteMeta { .. } => code!("replay-incomplete-meta"),
            ReplayError::WrongDomain { .. } => code!("replay-wrong-domain"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            ReplayError::OutOfSequence { index, found } => {
                vec![("index", (*index).into()), ("found", (*found).into())]
            }
            ReplayError::RecordedBackwards { sequence } => vec![("sequence", (*sequence).into())],
            ReplayError::IncompleteMeta { sequence, field } => {
                vec![("sequence", (*sequence).into()), ("field", (*field).into())]
            }
            ReplayError::WrongDomain { found, expected } => vec![
                ("found", found.as_str().into()),
                ("expected", (*expected).into()),
            ],
        }
    }
}

// ---------------------------------------------------------------- StoreError

/// A store error's args as angzarr-embedded's diagnostics carry them: angzarr-ports'
/// args by name, unchanged (`StoreError::args`), each converted by
/// angzarr-ports' own `From` (a `Uint` as a `Uint`, a `Str` as a `Str`, and
/// a wrapped `Cause`, the adapter's own code and args, as a nested
/// diagnostic under that code).
pub(crate) fn store_args(error: &StoreError) -> Vec<(&'static str, Arg)> {
    error
        .args()
        .into_iter()
        .map(|(name, arg)| (name, arg.into()))
        .collect()
}

/// A stored page the kernel cannot give back as a current event: its
/// payload upcasts or decodes as nothing the domain knows, or its bytes do
/// not decode as that type. The kernel's, never the store's (angzarr-ports'
/// `StoreError::Unreadable` is the store's: bytes it cannot return).
pub(crate) fn unreadable(stream: &Cover, sequence: u64, reason: Report) -> KernelError {
    KernelError::Unreadable {
        stream: stream.clone(),
        sequence,
        reason,
    }
}

// ---------------------------------------------------------------- payloads

impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for PayloadError {}

impl Diagnostic for PayloadError {
    fn code(&self) -> &str {
        match self {
            PayloadError::UnknownType { .. } => code!("payload-unknown-type"),
            PayloadError::Decode { .. } => code!("payload-decode"),
            PayloadError::Invalid { .. } => code!("payload-invalid"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            PayloadError::UnknownType { type_url } | PayloadError::Decode { type_url, .. } => {
                vec![("type_url", type_url.as_str().into())]
            }
            PayloadError::Invalid { type_url, reason } => vec![
                ("type_url", type_url.as_str().into()),
                ("reason", Arg::nested(reason)),
            ],
        }
    }
}

impl fmt::Display for UpcastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for UpcastError {
    fn code(&self) -> &str {
        match self {
            UpcastError::Payload(error) => error.code(),
            UpcastError::Rule { .. } => code!("upcast-rule"),
            UpcastError::Unavailable { .. } => code!("upcast-unavailable"),
            UpcastError::AnswerKeys => code!("upcast-answer-keys"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            UpcastError::Payload(error) => error.args(),
            UpcastError::Rule { source, error } => vec![
                ("source", source.as_str().into()),
                ("error", Arg::nested(error)),
            ],
            UpcastError::Unavailable { cause } => vec![("cause", Arg::nested(cause))],
            UpcastError::AnswerKeys => Vec::new(),
        }
    }
}

impl fmt::Display for UpcastChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for UpcastChainError {
    fn code(&self) -> &str {
        match self.problem {
            ChainProblem::Duplicate { .. } => code!("upcaster-duplicate"),
            ChainProblem::OutOfOrder { .. } => code!("upcaster-out-of-order"),
            ChainProblem::DeadEnd { .. } => code!("upcaster-dead-end"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let mut args = vec![("domain", self.domain.as_str().into())];
        match &self.problem {
            ChainProblem::Duplicate { source } => args.push(("source", source.as_str().into())),
            ChainProblem::OutOfOrder {
                source,
                produced_by,
            } => {
                args.push(("source", source.as_str().into()));
                args.push(("produced_by", produced_by.as_str().into()));
            }
            ChainProblem::DeadEnd { source, ends_at } => {
                args.push(("source", source.as_str().into()));
                args.push(("ends_at", ends_at.as_str().into()));
            }
        }
        args
    }
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
