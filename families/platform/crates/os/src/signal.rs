//! The signals a process is asked to stop by, each heard by name
//! ([`crate::Os::signals`]), so a caller can tell a hangup from an
//! interrupt, stop on some and carry on through others, and exit with the
//! status a shell gives a process the signal ended ([`Signal::exit_status`]).
//!
//! Unix sends them by number. Windows has none of them: its console events
//! are heard as the signal of the same intent (Ctrl-C and Ctrl-Break an
//! [`Signal::Interrupt`], the console's close a [`Signal::Hangup`], the
//! user's logoff and the system's shutdown a [`Signal::Terminate`]), and a
//! caller exits with the same status there, as no other convention exists.
//!
//! [`Signals::fake`] (test support) is a source the test raises by hand, so
//! a caller's handling is tested without a signal sent to the test process.

use std::fmt;
use std::future::poll_fn;
use std::task::{Context, Poll};

/// A request, from outside the process, that it stop.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub enum Signal {
    /// SIGHUP: the terminal went away. On Windows, the console's close.
    Hangup,
    /// SIGINT: an interrupt (Ctrl-C at a terminal whose line discipline
    /// makes one). On Windows, Ctrl-C and Ctrl-Break.
    Interrupt,
    /// SIGTERM: the polite stop `kill` sends. On Windows, the user's logoff
    /// and the system's shutdown.
    Terminate,
}

impl Signal {
    /// Every signal there is to hear.
    pub const ALL: [Signal; 3] = [Signal::Hangup, Signal::Interrupt, Signal::Terminate];

    /// Its number: POSIX leaves them to the system, but every unix target
    /// of this workspace (Linux, macOS, the BSDs) gives these.
    pub const fn number(self) -> u8 {
        match self {
            Signal::Hangup => 1,
            Signal::Interrupt => 2,
            Signal::Terminate => 15,
        }
    }

    /// The status a shell reports for a process this signal ended:
    /// 128 plus its [`number`](Self::number) (129, 130, 143).
    pub const fn exit_status(self) -> u8 {
        128 + self.number()
    }

    /// Its unix name: `SIGHUP`, `SIGINT`, `SIGTERM`.
    pub const fn name(self) -> &'static str {
        match self {
            Signal::Hangup => "SIGHUP",
            Signal::Interrupt => "SIGINT",
            Signal::Terminate => "SIGTERM",
        }
    }
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What a [`Signals`] asks of its source: the next signal heard, if one is.
type Source = Box<dyn FnMut(&mut Context<'_>) -> Poll<Signal> + Send>;

/// The signals of a chosen set, each heard as it arrives, by name. Made by
/// [`crate::Os::signals`], it listens from the moment it is made: a signal
/// of its set no longer has its default disposition (it does not end the
/// process) for as long as the process lives, whether or not the
/// `Signals` is still polled. A signal outside the set keeps its default.
pub struct Signals {
    source: Source,
}

impl fmt::Debug for Signals {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Signals")
    }
}

impl Signals {
    /// Signals heard from `source`.
    pub(crate) fn from_source(
        source: impl FnMut(&mut Context<'_>) -> Poll<Signal> + Send + 'static,
    ) -> Signals {
        Signals {
            source: Box::new(source),
        }
    }

    /// The next signal, once one has arrived.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Signal> {
        (self.source)(cx)
    }

    /// Wait for the next signal.
    pub async fn recv(&mut self) -> Signal {
        poll_fn(|cx| self.poll_recv(cx)).await
    }
}

#[cfg(any(test, feature = "test-support"))]
mod fake {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex, PoisonError};
    use std::task::{Poll, Waker};

    use super::{Signal, Signals};

    #[derive(Default)]
    struct Pending {
        signals: VecDeque<Signal>,
        waker: Option<Waker>,
    }

    /// Raises signals on a [`Signals::fake`], as the system would on a
    /// real one. Clones raise on the same one. Test support.
    #[derive(Clone)]
    pub struct Raise {
        heard: Vec<Signal>,
        pending: Arc<Mutex<Pending>>,
    }

    impl std::fmt::Debug for Raise {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Raise").field("heard", &self.heard).finish()
        }
    }

    impl Raise {
        /// Raise `signal`: heard when it is of the set the fake was made
        /// for, and dropped otherwise (where a real process would get the
        /// signal's default disposition, which a test does not want).
        pub fn raise(&self, signal: Signal) {
            if !self.heard.contains(&signal) {
                return;
            }
            let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
            pending.signals.push_back(signal);
            if let Some(waker) = pending.waker.take() {
                waker.wake();
            }
        }
    }

    impl Signals {
        /// A source of `heard` that the returned [`Raise`] raises by
        /// hand, never the system: for a test of what a caller does with a
        /// signal. Signals arrive in the order raised. Test support.
        pub fn fake(heard: &[Signal]) -> (Raise, Signals) {
            let pending = Arc::new(Mutex::new(Pending::default()));
            let raise = Raise {
                heard: heard.to_vec(),
                pending: Arc::clone(&pending),
            };
            let signals = Signals::from_source(move |cx| {
                let mut pending = pending.lock().unwrap_or_else(PoisonError::into_inner);
                match pending.signals.pop_front() {
                    Some(signal) => Poll::Ready(signal),
                    None => {
                        pending.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
            });
            (raise, signals)
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub use fake::Raise;

#[cfg(test)]
#[path = "signal.test.rs"]
mod tests;
