//! Where the kernel meets angzarr-router: the router owns business
//! dispatch (handlers, typed refusals, a decision's continuations); this
//! module turns what the router answers into the kernel's terms.
//!
//! - The router's answers keep their codes: a spec key (an
//!   `AngzarrReason`, the router's or the kernel's own, answered in the
//!   router's shape) is the kernel's code too, verbatim, with its args, so
//!   [`KernelError::Router`](super::KernelError::Router) needs no table
//!   (decision 14.60).
//! - [`Rejected`]: a domain's typed refusal, as the caller receives it
//!   (decision 14.95: typed refusals only): its key and args, its typed
//!   refusal object and the business status it travelled as.
//! - The table a typed [`Domain`] is dispatched by: the router's
//!   `DomainDispatch` over the domain's state, folding with the domain's
//!   own `apply`, with the state's codec; the domain's handlers are
//!   registered on it.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use angzarr_diagnostic::{Arg, Diagnostic, Report};
use angzarr_router::domain::DomainDispatch;
use angzarr_router::error::{BusinessStatus, CodedError, GrpcCode, HandlerError};
use angzarr_router::hpb;
use angzarr_router::rebuild::{FoldFailure, Rebuilder};
use angzarr_router::PageContext;
use prost::Message;
use prost_types::Any;

use super::domain::{Domain, State};
use super::error::PayloadError;
use super::payload::{type_name, Payload};

/// A domain's typed refusal of a command, as its caller receives it
/// (decision 14.95): `ANGZARR_REASON_COMMAND_REFUSED` and its
/// `refusal_type`, the typed refusal object (one of the command's
/// `refuses` types, or the coordinator's `ExpectedSequenceMismatch`), and
/// the business status it went out under. The refusing domain recorded
/// nothing.
///
/// A domain's handler in this process refused with a value (a marked-up
/// domain's `Refuses` choice, a hand-written domain's own), which is kept
/// beside the report, to match as a value ([`Rejected::rejection`]); a
/// refusal from another process is its object alone. Two rejections are
/// equal when their reports, refusal objects and statuses are.
#[derive(Clone, Debug)]
pub struct Rejected {
    report: Report,
    refusal: Any,
    status: BusinessStatus,
    kept: Option<Kept>,
}

impl PartialEq for Rejected {
    fn eq(&self, other: &Rejected) -> bool {
        (&self.report, &self.refusal, self.status) == (&other.report, &other.refusal, other.status)
    }
}

/// A domain's refusal as its handler made it: any value it offered.
pub(crate) type Typed = Arc<dyn std::any::Any + Send + Sync>;

/// A [`Typed`] refusal, kept in a [`Rejected`]: shown as `..`, since its
/// report shows it.
#[derive(Clone)]
struct Kept(Typed);

impl fmt::Debug for Kept {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("..")
    }
}

impl Rejected {
    /// The router's answer `error`, if it is a typed refusal (a business
    /// status with its typed object; [`CodedError::as_refusal`]), in the
    /// kernel's terms: under the key it went out under, its metadata the
    /// args. `None` for anything else.
    pub(crate) fn of(error: &CodedError) -> Option<Rejected> {
        let refusal = error.refusal.as_deref()?.clone();
        let status = BusinessStatus::try_from(error.grpc).ok()?;
        let report = error
            .extras
            .iter()
            .fold(Report::new(error.code.clone()), |report, (name, value)| {
                report.arg(name.clone(), value.as_str())
            });
        Some(Rejected {
            report,
            refusal,
            status,
            kept: None,
        })
    }

    /// The coordinator's own typed refusal (decisions 14.93 and 14.94): a
    /// command's expected sequence that is not the head it loaded,
    /// `ExpectedSequenceMismatch { expected, head }`, FAILED_PRECONDITION.
    pub(crate) fn mismatch(expected: u64, head: u64) -> Rejected {
        let object = angzarr_proto::v1::ExpectedSequenceMismatch { expected, head };
        let error = CodedError::refused(
            BusinessStatus::FailedPrecondition,
            super::payload::pack(&object),
        );
        Rejected::of(&error).unwrap_or_else(|| unreachable!("a typed refusal is one"))
    }

    /// The same rejection, keeping `typed`, the refusal its handler made.
    pub(crate) fn kept(self, typed: Option<Typed>) -> Rejected {
        Rejected {
            kept: typed.map(Kept),
            ..self
        }
    }

    /// The domain's own refusal, as its handler in this process made it,
    /// if it is an `R`. `None` for a refusal from another process or the
    /// coordinator's (its object alone: [`Rejected::refusal_object`]), and
    /// for any other type.
    pub fn rejection<R: std::any::Any>(&self) -> Option<&R> {
        let kept: &(dyn std::any::Any + Send + Sync) = &*self.kept.as_ref()?.0;
        kept.downcast_ref::<R>()
    }

    /// The refusal as a report: its key and args.
    pub fn report(&self) -> &Report {
        &self.report
    }

    /// The business status the refusal went out under.
    pub fn status(&self) -> BusinessStatus {
        self.status
    }

    /// The refusal as a refused policy's Translate is handed it
    /// (`io.angzarr.handler.v1.Refusal`): its status and its typed object.
    pub fn as_refusal(&self) -> hpb::Refusal {
        hpb::Refusal {
            status: angzarr_proto::v1::RefusalStatus::from(self.status) as i32,
            typed: Some(self.refusal.clone()),
        }
    }

    /// The refusal as the router carries it: its status, key and object.
    pub fn as_coded(&self) -> CodedError {
        CodedError::refused(self.status, self.refusal.clone())
    }

    /// The typed refusal object, packed.
    pub fn refusal_object(&self) -> &Any {
        &self.refusal
    }

    /// The typed refusal object, as `T`, if it is one.
    pub fn refusal<T: super::markup::DomainMessage>(&self) -> Option<T> {
        super::markup::decode_message(&self.refusal).ok().flatten()
    }
}

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.report.fmt(f)
    }
}

impl Diagnostic for Rejected {
    fn code(&self) -> &str {
        self.report.code()
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        self.report.args()
    }
}

/// Whether the router's `error` is a fatal bug ([`class`](super::class):
/// INTERNAL, UNKNOWN, DATA_LOSS). A handler's bug is
/// ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, its own code and args its
/// `cause`; the router's own errors on malformed input are bugs too. The
/// kernel dead-letters it by its code and args at once, and a policy's step
/// moves the policy's position past it unless the kernel says to stay
/// ([`OnPolicyBug`](super::OnPolicyBug)). A retryable bug is retried
/// first, then dead-lettered.
pub(crate) fn is_handler_fault(error: &CodedError) -> bool {
    super::error::class(error.grpc as i32) == super::error::Class::BugFatal
}

/// A hand-written handler's typed refusal (decision 14.95): `refusal`, one
/// of the command's `refuses` types, packed, under `status`, the business
/// status its message declares, as the router carries it
/// (`ANGZARR_REASON_COMMAND_REFUSED`, the object's type as
/// `refusal_type` and the object in the details).
///
/// `rejection`, the refusal as the handler made it, is kept for the
/// kernel's caller, typed ([`Rejected::rejection`]), when the kernel
/// dispatched the handler in this process.
pub fn rejection<R: Send + Sync + 'static>(
    rejection: R,
    status: BusinessStatus,
    refusal: Any,
) -> HandlerError {
    offer(rejection);
    HandlerError::Coded(CodedError::refused(status, refusal))
}

thread_local! {
    /// The refusal kept for the dispatch being polled on this thread
    /// ([`catching`]): `None` outside one; inside, what its handler
    /// offered, if anything.
    static CATCHING: RefCell<Option<Option<Typed>>> = const { RefCell::new(None) };
}

/// Keep `rejection`, a handler's refusal as it made it, for the dispatch
/// being polled ([`catching`]); outside one, nothing is kept.
pub(crate) fn offer<R: std::any::Any + Send + Sync>(rejection: R) {
    CATCHING.with(|catching| {
        if let Some(slot) = catching.borrow_mut().as_mut() {
            *slot = Some(Arc::new(rejection));
        }
    });
}

/// The dispatch to a domain's handlers `dispatch` makes, run, and the
/// refusal its handler kept ([`offer`]) while it was made (a handler in
/// process may run as its future is made) or polled: on whatever thread
/// polls it, each poll sees only this dispatch's slot, so concurrent
/// dispatches never share one.
pub(crate) async fn catching<F: Future>(
    dispatch: impl FnOnce() -> F,
) -> (F::Output, Option<Typed>) {
    let mut caught = None;
    let work = {
        let _catch = Catch::enter(&mut caught);
        dispatch()
    };
    let mut work = std::pin::pin!(work);
    let output = std::future::poll_fn(|cx| {
        let _catch = Catch::enter(&mut caught);
        work.as_mut().poll(cx)
    })
    .await;
    (output, caught)
}

/// One poll's [`CATCHING`] slot: entered with what the dispatch caught so
/// far, and on drop (a panic included) handed back, the slot of whatever
/// was polling before restored.
struct Catch<'c> {
    caught: &'c mut Option<Typed>,
    outer: Option<Option<Typed>>,
}

impl<'c> Catch<'c> {
    fn enter(caught: &'c mut Option<Typed>) -> Catch<'c> {
        let outer = CATCHING.with(|catching| catching.replace(Some(caught.take())));
        Catch { caught, outer }
    }
}

impl Drop for Catch<'_> {
    fn drop(&mut self) {
        let ours = CATCHING.with(|catching| catching.replace(self.outer.take()));
        *self.caught = ours.flatten();
    }
}

/// A handler's bug, as the router carries it (decision 14.37): INTERNAL
/// ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, the bug's code as its `cause`
/// and its args, flattened, as `cause.<arg>`; never a refusal.
pub(crate) fn bug(bug: &dyn Diagnostic) -> HandlerError {
    HandlerError::Coded(CodedError::bug(bug.code(), flat_args(bug)))
}

/// A command the sender got wrong, refused INVALID_ARGUMENT under the
/// diagnostic's own code and args: no typed refusal of the command's, no
/// bug of its handler's.
pub(crate) fn invalid_argument(refused: &dyn Diagnostic) -> HandlerError {
    HandlerError::Coded(CodedError {
        code: refused.code().to_string(),
        extras: flat_args(refused).into_iter().collect(),
        grpc: GrpcCode::InvalidArgument,
        refusal: None,
    })
}

/// A spec key the kernel raises (decision 14.65): its full name, the code
/// it travels as, and how its `(io.angzarr.v1.reason)` option says it
/// surfaces, generated into angzarr-proto (or a transport's crate). A key
/// travels under its declared status and no other.
pub trait SpecKey: Copy {
    /// The key: its value's full name, prefix included.
    fn key(self) -> &'static str;
    /// How its option says it surfaces.
    fn surface(self) -> Option<angzarr_proto::ReasonSurface>;
    /// The gRPC code it travels with: its declared status's
    /// ([`GrpcCode::of_surface`]). A readiness key never travels as a
    /// status, so it has none.
    fn grpc(self) -> Option<GrpcCode> {
        GrpcCode::of_surface(self.surface())
    }
}

impl SpecKey for angzarr_proto::v1::AngzarrReason {
    fn key(self) -> &'static str {
        self.as_str_name()
    }
    fn surface(self) -> Option<angzarr_proto::ReasonSurface> {
        angzarr_proto::v1::AngzarrReason::surface(self)
    }
}

/// The spec key `key` with its args `extras`, in the router's shape, under
/// the status its option declares (decision 14.65). A readiness key is
/// never answered so; it is INTERNAL here, the status of the bug answering
/// it would be.
pub fn spec_coded(
    key: impl SpecKey,
    extras: impl IntoIterator<Item = (String, String)>,
) -> CodedError {
    CodedError {
        code: key.key().to_string(),
        extras: extras.into_iter().collect(),
        grpc: key.grpc().unwrap_or(GrpcCode::Internal),
        refusal: None,
    }
}

/// One of angzarr-embedded's own failures, INTERNAL under its own `code`
/// (an upcast's failure): a spec key is [`spec_coded`]'s instead. Never a
/// handler's bug, which angzarr-router's `CodedError::bug` answers as
/// ANGZARR_REASON_UNHANDLED_HANDLER_ERROR with the handler's code as its
/// cause.
pub(crate) fn internal(
    code: &str,
    extras: impl IntoIterator<Item = (String, String)>,
) -> CodedError {
    CodedError {
        code: code.to_string(),
        extras: extras.into_iter().collect(),
        grpc: GrpcCode::Internal,
        refusal: None,
    }
}

/// A diagnostic's args, flattened as angzarr carries them (decision
/// 14.37): each by its name, a wrapped cause as its code under the arg's
/// name and each of its args under `<name>.<arg>`.
pub(crate) fn flat_args(diagnostic: &dyn Diagnostic) -> Vec<(String, String)> {
    let mut flat = Vec::new();
    for (name, arg) in diagnostic.args() {
        match arg {
            Arg::Int(n) => flat.push((name.to_string(), n.to_string())),
            Arg::Uint(n) => flat.push((name.to_string(), n.to_string())),
            Arg::Str(s) => flat.push((name.to_string(), s)),
            Arg::Nested(cause) => {
                flat.push((name.to_string(), cause.code().to_string()));
                flat.extend(
                    flat_args(cause.as_ref())
                        .into_iter()
                        .map(|(inner, value)| (format!("{name}.{inner}"), value)),
                );
            }
        }
    }
    flat
}

/// `diagnostic` as angzarr's wire `Diagnostic`: its code and its args,
/// flattened ([`flat_args`]).
pub(crate) fn diagnostic(diagnostic: &dyn Diagnostic) -> angzarr_proto::v1::Diagnostic {
    angzarr_proto::v1::Diagnostic {
        code: diagnostic.code().to_string(),
        args: flat_args(diagnostic).into_iter().collect(),
    }
}

/// The router's `error` as a report: its key, its args.
pub(crate) fn report(error: &CodedError) -> Report {
    error
        .extras
        .iter()
        .fold(Report::new(error.code.clone()), |report, (name, value)| {
            report.arg(name.clone(), value.as_str())
        })
}

// ------------------------------------------------------------- the table

/// The router's table for typed domain `D`, before its handlers: named
/// `D::NAME` (a process manager's if `process_manager`), folding each
/// event, upcast by the kernel before it is handed over, through
/// `D::apply`, answering `D::RULES_VERSION`, its state's type its anchor,
/// with the state's codec: a delivered starting state restored onto a fresh
/// state from `fresh`, and the state every answer carries, as its
/// [`State::snapshot`] offers it ([`Offering`] drops the answer's state
/// when it offers none).
pub(crate) fn table<D: Domain>(
    fresh: Arc<dyn Fn() -> D::State + Send + Sync>,
    process_manager: bool,
) -> DomainDispatch<D::State> {
    let anchor = <<D::State as State>::Anchor as prost::Name>::full_name();
    let restore_fresh = Arc::clone(&fresh);
    let rebuilder = Rebuilder::new(move || fresh())
        .with_fold(move |state, pages, cover| {
            for (index, page) in pages.iter().enumerate() {
                let Some(any) = angzarr_router::page_event(page) else {
                    continue;
                };
                let failed = |cause: &dyn Diagnostic| FoldFailure {
                    page_index: Some(index),
                    cause: Some(diagnostic(cause)),
                };
                let event = D::Event::from_any(any).map_err(|e| failed(&e))?;
                let page = PageContext {
                    cover,
                    sequence: page.sequence,
                    meta: page.meta.as_ref(),
                };
                D::apply(state, &event, &page);
            }
            Ok(())
        })
        .with_state_type(&anchor)
        .with_state_codec(
            move |any| {
                if type_name(&any.type_url) != anchor {
                    return Err(Box::new(PayloadError::UnknownType {
                        type_url: any.type_url.clone(),
                    }));
                }
                let wire =
                    <D::State as State>::Anchor::decode(&any.value[..]).map_err(|error| {
                        PayloadError::Decode {
                            type_url: any.type_url.clone(),
                            error,
                        }
                    })?;
                let mut restored = restore_fresh();
                restored.restore(wire).map_err(Box::new)?;
                Ok(restored)
            },
            |state: &D::State| {
                state
                    .snapshot()
                    .map_or_else(Any::default, |anchor| super::payload::pack(&anchor))
            },
        );
    let base = match process_manager {
        true => DomainDispatch::process_manager(D::NAME, D::NAME, rebuilder),
        false => DomainDispatch::new(D::NAME, D::NAME, rebuilder),
    };
    base.with_rules_version(D::RULES_VERSION)
}

/// A typed domain's table, as the registry dispatches it: its answers as
/// the router's, but one whose state offers no snapshot carries none. The
/// router's codec answers a state with every answer, so the table's
/// encodes one that offers none as an `Any` with no type URL, which no
/// state is, and this drops it.
pub(crate) struct Offering<S>(pub(crate) DomainDispatch<S>);

impl<S> angzarr_router::router::CommandHandler for Offering<S>
where
    DomainDispatch<S>: angzarr_router::router::CommandHandler,
{
    fn name(&self) -> &str {
        self.0.name()
    }

    fn kind(&self) -> angzarr_proto::v1::ComponentKind {
        self.0.kind()
    }

    fn command_types(&self) -> Vec<String> {
        self.0.command_types()
    }

    fn dispatch(
        &self,
        req: &hpb::HandleRequest,
    ) -> impl Future<Output = Result<hpb::HandleResponse, CodedError>> + angzarr_ports::MaybeSend
    {
        std::future::ready(self.0.dispatch(req).map(|mut response| {
            response.state = response.state.filter(|state| !state.type_url.is_empty());
            response
        }))
    }
}

#[cfg(test)]
#[path = "route.test.rs"]
mod tests;
