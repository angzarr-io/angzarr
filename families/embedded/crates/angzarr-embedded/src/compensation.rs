//! A process manager's compensation (angzarr decision 14.74): each step
//! it completes paired, in code, with the undo that reverses it, and the
//! rollback built from them, so an author never writes it twice.
//!
//! The journal is the PM's own events, folded into its state: a step is
//! completed once the event recording its reply is folded, and the state
//! says so ([`Compensation::step`]'s `recorded`). An undo is an event of
//! the PM's own, built from that record, which the PM's outbound policy
//! turns into what the step's destination declares: a fact (decisions 14.69
//! and 14.83), so an undo is never refused and compensation never loops.
//!
//! ```ignore
//! let compensation = Compensation::new(
//!     |_: &ShipmentState, reason: String| ShipmentEvent::from(ShipmentFailed { reason }),
//!     |state: &ShipmentState| state.failed,
//! )
//! .step::<Inventory, ReleaseStock, _>(
//!     "reserve stock",
//!     |state| (!state.reservation_id.is_empty())
//!         .then(|| StockReserved { reservation_id: state.reservation_id.clone() }),
//!     |reserved| StockReleaseDue { reservation_id: reserved.reservation_id.clone() }.into(),
//! )?;
//! // a step refused: the failed event, then each completed step's undo,
//! // newest first
//! let events = compensation.rollback(state, "payment declined".into());
//! ```
//!
//! Which event undoes which step is known only here, so registering a step
//! checks it against what its destination declares (the owner's ruling
//! Q10): an undo whose target is one of the destination's commands, which
//! may be refused, is refused at registration ([`UndoTargetsCommand`];
//! decision 14.83), so no undo is ever refused and there is no
//! CompensationFailed.

use std::any::{Any, TypeId};
use std::fmt;
use std::sync::Arc;

use angzarr_diagnostic::{code, Arg, Diagnostic};

use super::markup::{DomainMessage, TypeName};
use super::policy::CommandTarget;

/// The failed event of a rollback, from the state and the reason.
type Failed<S, E, R> = Box<dyn Fn(&S, R) -> E + Send + Sync>;
/// Whether the state has rolled back.
type RolledBack<S> = Box<dyn Fn(&S) -> bool + Send + Sync>;
/// A step's undo, when the state records it done.
type Recorded<S, E> = Box<dyn Fn(&S) -> Option<E> + Send + Sync>;
/// A step's undo of a reply recording it done, the reply's type erased.
type Replied<E> = Box<dyn Fn(&dyn Any) -> Option<E> + Send + Sync>;

/// A process manager's steps, each with its undo, and the events that
/// roll it back: `S` its state, `E` its events, `R` a rollback's reason.
pub struct Compensation<S, E, R = String> {
    failed: Failed<S, E, R>,
    rolled_back: RolledBack<S>,
    steps: Vec<Step<S, E>>,
}

/// One registered step.
struct Step<S, E> {
    name: &'static str,
    /// The type of the event recording it done.
    forward: TypeId,
    /// Its undo, when the state records it done.
    recorded: Recorded<S, E>,
    /// Its undo of a reply recording it done.
    replied: Replied<E>,
}

impl<S, E, R> Compensation<S, E, R> {
    /// No steps yet: `failed` builds a rollback's failed event from the
    /// state and the reason, whose fold marks the state rolled back,
    /// which `rolled_back` reads.
    pub fn new(
        failed: impl Fn(&S, R) -> E + Send + Sync + 'static,
        rolled_back: impl Fn(&S) -> bool + Send + Sync + 'static,
    ) -> Self {
        Compensation {
            failed: Box::new(failed),
            rolled_back: Box::new(rolled_back),
            steps: Vec::new(),
        }
    }

    /// These steps and the step `name`, registered after them (a rollback
    /// undoes it before them): `recorded` reads from the state the event
    /// `F` that recorded it done, or `None` while it is not; `undo` builds
    /// its undo from that event. Its undo becomes `U` at `D`, the step's
    /// destination: checked here, at registration, against what `D`
    /// declares; a `U` that is one of `D`'s commands, which may be refused,
    /// is refused ([`UndoTargetsCommand`]; decision 14.83).
    pub fn step<D, U, F>(
        mut self,
        name: &'static str,
        recorded: impl Fn(&S) -> Option<F> + Send + Sync + 'static,
        undo: impl Fn(&F) -> E + Send + Sync + 'static,
    ) -> Result<Self, UndoTargetsCommand>
    where
        D: CommandTarget,
        U: DomainMessage,
        F: 'static,
    {
        let target = TypeName::of::<U::Wire>().full_name();
        if D::command_types().contains(&target) {
            return Err(UndoTargetsCommand {
                step: name,
                command: target,
            });
        }
        let undo = Arc::new(undo);
        let replied = Arc::clone(&undo);
        self.steps.push(Step {
            name,
            forward: TypeId::of::<F>(),
            recorded: Box::new(move |state| recorded(state).map(|done| undo(&done))),
            replied: Box::new(move |reply| reply.downcast_ref::<F>().map(|done| replied(done))),
        });
        Ok(self)
    }

    /// The rollback of `state` for `reason`: the failed event, then the
    /// undo of each step the state records done, newest first (the
    /// reverse of their registration). One handling's events.
    pub fn rollback(&self, state: &S, reason: R) -> Vec<E> {
        let failed = (self.failed)(state, reason);
        let undos = self.steps.iter().rev().filter_map(|s| (s.recorded)(state));
        std::iter::once(failed).chain(undos).collect()
    }

    /// The events recording `reply`, the event that records a step done:
    /// the reply alone, or, once `state` has rolled back (a reply that
    /// arrived late), the reply and at once its step's undo.
    pub fn reply<F>(&self, state: &S, reply: F) -> Vec<E>
    where
        F: Into<E> + 'static,
    {
        let undo = if (self.rolled_back)(state) {
            let forward = TypeId::of::<F>();
            self.steps
                .iter()
                .filter(|s| s.forward == forward)
                .find_map(|s| (s.replied)(&reply))
        } else {
            None
        };
        std::iter::once(reply.into()).chain(undo).collect()
    }
}

impl<S, E, R> fmt::Debug for Compensation<S, E, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let steps: Vec<&str> = self.steps.iter().map(|s| s.name).collect();
        f.debug_struct("Compensation")
            .field("steps", &steps)
            .finish()
    }
}

/// A step whose undo becomes one of its destination's commands, which the
/// destination may refuse: refused when the step is registered (decision
/// 14.83), so no undo is ever refused.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UndoTargetsCommand {
    /// The step, by the name it was registered under.
    pub step: &'static str,
    /// The command's full name.
    pub command: String,
}

impl fmt::Display for UndoTargetsCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for UndoTargetsCommand {
    fn code(&self) -> &str {
        code!("undo-targets-command")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![
            ("step", self.step.into()),
            ("command", self.command.as_str().into()),
        ]
    }
}

#[cfg(test)]
#[path = "compensation.test.rs"]
mod tests;
