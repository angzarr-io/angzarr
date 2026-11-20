//! The SIMPLE command mode: a command handled as [`Kernel::handle_any`]
//! handles it, answered only once each projector the caller names has
//! projected its events, with what each answered
//! ([`Kernel::handle_sync`]; angzarr's `HandleSync`,
//! docs/design/command-handling.md §10).
//!
//! The kernel runs every registered projector inline: after a commit, each
//! is handed the committed books of its sources and awaited before the
//! call returns ([`Registry::projector_handler`]). So the wait is the
//! projector's own future, awaited in the call, and nothing is polled. A
//! named projector differs from the others in three ways:
//!
//! - what it answers is kept, as a [`Projection`] the kernel builds
//!   (decision 14.97: the coordinator builds it, once the projector has
//!   answered), one per named projector, for the command's own stream;
//! - its failure is the CALLER's, answered at once and never retried or
//!   dead-lettered: [`KernelError::SyncProjectionFailed`], UNAVAILABLE, the
//!   decision committed (decision 14.105). The caller resends with the same
//!   command id; the resend is a DUPLICATE, and the named projectors are
//!   handed the command's events again and waited for again;
//! - its run is bounded by the kernel's deadline
//!   ([`Kernel::sync_within`]), timed on the kernel's [`Wait`]: a named
//!   projector still running when it passes fails the call as a failure of
//!   its own would, its cause [`DeadlinePassed`].
//!
//! The projectors not named run as [`Kernel::handle_any`] runs them.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use angzarr_diagnostic::{code, Arg, Diagnostic, Report};
use angzarr_ports::BoxFuture;
use angzarr_proto::v1::{Cover, EventBook, Projection};
use angzarr_router::router::DynProjectorHandler;

use super::error::KernelError;
use super::handle::{Handled, Wait};
use super::registry::Registry;

#[cfg(doc)]
use super::handle::Kernel;

/// How long a SIMPLE call waits, by default, for the projectors it names:
/// thirty seconds from the first one's run that does not answer at once.
pub const DEFAULT_SYNC_DEADLINE: Duration = Duration::from_secs(30);

/// What [`Kernel::handle_sync`] answers: the command's outcome, as
/// [`Kernel::handle_any`] answers it (its events and its delivery, APPLIED
/// or DUPLICATE), and each named projector's [`Projection`], one per name,
/// in the order first named.
pub struct HandledSync {
    pub handled: Handled,
    pub projections: Vec<Projection>,
}

/// A named projector was still running when the SIMPLE call's deadline
/// passed: the cause of its [`KernelError::SyncProjectionFailed`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeadlinePassed {
    /// The deadline that passed.
    pub deadline: Duration,
}

impl fmt::Display for DeadlinePassed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for DeadlinePassed {
    fn code(&self) -> &str {
        code!("sync-deadline-passed")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let millis = u64::try_from(self.deadline.as_millis()).unwrap_or(u64::MAX);
        vec![("deadline_ms", millis.into())]
    }
}

/// The projectors `projectors` names, each once, in the order first named,
/// for a command to `domain`: refused, before anything runs, when it names
/// none ([`KernelError::SyncNoProjectors`]) or names one `registry` does
/// not run for `domain`'s events, an unknown name included
/// ([`KernelError::SyncProjectorNotSubscribed`]).
pub(crate) fn named(
    projectors: &[String],
    domain: &str,
    registry: &Registry,
) -> Result<Vec<String>, KernelError> {
    if projectors.is_empty() {
        return Err(KernelError::SyncNoProjectors);
    }
    let mut names: Vec<String> = Vec::new();
    for name in projectors {
        let subscribed = registry
            .projectors()
            .iter()
            .any(|entry| &entry.name == name && entry.sources.iter().any(|s| s == domain));
        if !subscribed {
            return Err(KernelError::SyncProjectorNotSubscribed {
                domain: domain.to_string(),
                projector: name.clone(),
            });
        }
        if !names.contains(name) {
            names.push(name.clone());
        }
    }
    Ok(names)
}

/// A SIMPLE call's named projectors as the chain runs them: which they
/// are, the deadline and its timer, what each answered, and the first
/// failure.
pub(crate) struct Waited<'w> {
    names: Vec<String>,
    deadline: Option<Duration>,
    wait: &'w dyn Wait,
    /// Started the first time a named projector does not answer at once.
    timer: Option<Pin<Box<dyn Future<Output = ()> + Send + 'w>>>,
    answered: BTreeMap<String, Projection>,
    failed: Option<KernelError>,
}

impl<'w> Waited<'w> {
    pub(crate) fn new(names: Vec<String>, deadline: Option<Duration>, wait: &'w dyn Wait) -> Self {
        Waited {
            names,
            deadline,
            wait,
            timer: None,
            answered: BTreeMap::new(),
            failed: None,
        }
    }

    /// Whether the call waits for `projector`.
    pub(crate) fn waits_for(&self, projector: &str) -> bool {
        self.names.iter().any(|name| name == projector)
    }

    /// Hand `book` to the named projector `name`, `handler`, and wait for
    /// its answer, within the deadline. Its first answer is kept as its
    /// projection; its failure, or the deadline passing, is the call's,
    /// and once the call has failed no named projector runs again.
    pub(crate) async fn project(
        &mut self,
        name: &str,
        handler: &dyn DynProjectorHandler,
        book: &EventBook,
    ) {
        if self.failed.is_some() {
            return;
        }
        let request = angzarr_router::hpb::ProjectRequest {
            events: Some(book.clone()),
        };
        let cover = book.cover.clone().unwrap_or_default();
        let failed = |cause: Report| KernelError::SyncProjectionFailed {
            stream: cover.clone(),
            projector: name.to_string(),
            cause,
        };
        match self.within(handler.dispatch(&request)).await {
            Some(Ok(response)) => {
                let next_sequence = book.pages.last().map_or(0, |page| page.sequence + 1);
                self.answered
                    .entry(name.to_string())
                    .or_insert_with(|| Projection {
                        cover: book.cover.clone(),
                        projector: name.to_string(),
                        next_sequence,
                        projection: response.projection,
                    });
            }
            Some(Err(error)) => {
                self.failed = Some(failed(Report::of(&KernelError::Router(error))));
            }
            None => {
                let deadline = self.deadline.unwrap_or_default();
                self.failed = Some(failed(Report::of(&DeadlinePassed { deadline })));
            }
        }
    }

    /// `work`'s output, or `None` when the deadline passes first. The
    /// timer is started the first time `work` does not answer at once, and
    /// is never polled again once it has fired (the call has failed).
    async fn within<T>(&mut self, work: BoxFuture<'_, T>) -> Option<T> {
        let mut work = work;
        let deadline = self.deadline;
        let wait = self.wait;
        let timer = &mut self.timer;
        std::future::poll_fn(move |cx| {
            if let Poll::Ready(out) = work.as_mut().poll(cx) {
                return Poll::Ready(Some(out));
            }
            let Some(deadline) = deadline else {
                return Poll::Pending;
            };
            let timer = timer.get_or_insert_with(|| wait.wait(deadline));
            timer.as_mut().poll(cx).map(|()| None)
        })
        .await
    }

    /// The call's answer: its failure, else each named projector's
    /// projection in the order named; one that was handed nothing (a
    /// decision that recorded no event) names itself and `stream`, and
    /// holds no projection.
    pub(crate) fn finish(mut self, stream: &Cover) -> Result<Vec<Projection>, KernelError> {
        if let Some(failed) = self.failed.take() {
            return Err(failed);
        }
        Ok(self
            .names
            .iter()
            .map(|name| {
                self.answered.remove(name).unwrap_or_else(|| Projection {
                    cover: Some(stream.clone()),
                    projector: name.clone(),
                    next_sequence: 0,
                    projection: None,
                })
            })
            .collect())
    }
}

#[cfg(test)]
#[path = "sync.test.rs"]
mod tests;
