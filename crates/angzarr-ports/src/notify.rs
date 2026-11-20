//! [`CommitNotify`]: a push when a commit lands, so a reader that follows a
//! store wakes on the commit rather than polling for it.
//!
//! A store that pushes keeps one [`CommitSignal`], shared by every handle in
//! this process whose commits are that store's, and calls
//! [`CommitSignal::notify`] after each successful commit (never before it
//! lands, never for a refused write). A follower arms a [`CommitWait`]
//! BEFORE it reads, then reads, then awaits the wait: a commit that lands
//! after the arming resolves it, whether it lands before the read (the read
//! sees it, and the wait resolves at once: one extra read) or after (the
//! wait resolves then). So nothing committed after the arming is missed.
//!
//! The signal reaches this process only. A commit from another process (or
//! through a handle that does not share the signal) never notifies it: a
//! follower that must see those keeps [`Watch`](crate::Watch)'s change token
//! as its fallback, polled.
//!
//! Runtime-agnostic: std's `Mutex` and `Waker` alone, no async runtime, and
//! it builds for `wasm32`.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};

use crate::send::{MaybeSend, MaybeSync};

/// A store's commit signal: clones share one. See the module.
#[derive(Clone, Default)]
pub struct CommitSignal {
    shared: Arc<Mutex<Signal>>,
}

/// What the clones share: how many commits were signalled, and the tasks
/// waiting for the next, by waiter.
#[derive(Default)]
struct Signal {
    commits: u64,
    next_waiter: u64,
    waiting: BTreeMap<u64, Waker>,
}

fn lock(shared: &Mutex<Signal>) -> MutexGuard<'_, Signal> {
    // Nothing panics under the lock with the state half-changed.
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

impl CommitSignal {
    /// A signal no commit has notified yet.
    pub fn new() -> CommitSignal {
        CommitSignal::default()
    }

    /// A commit landed: every [`CommitWait`] armed before now resolves, and
    /// each task waiting on one is woken.
    pub fn notify(&self) {
        let woken = {
            let mut signal = lock(&self.shared);
            signal.commits = signal.commits.wrapping_add(1);
            std::mem::take(&mut signal.waiting)
        };
        for waker in woken.into_values() {
            waker.wake();
        }
    }

    /// The signal, held without keeping it: for a registry of the signals
    /// live handles share (one per file, say), which forgets one once no
    /// handle holds it.
    pub fn downgrade(&self) -> WeakCommitSignal {
        WeakCommitSignal {
            shared: Arc::downgrade(&self.shared),
        }
    }

    /// A wait armed now: it resolves at the first [`notify`](Self::notify)
    /// after this call.
    pub fn wait(&self) -> CommitWait {
        CommitWait {
            armed_at: lock(&self.shared).commits,
            shared: Arc::clone(&self.shared),
            waiter: None,
        }
    }
}

/// A [`CommitSignal`] held without keeping it ([`CommitSignal::downgrade`]).
#[derive(Clone, Default)]
pub struct WeakCommitSignal {
    shared: Weak<Mutex<Signal>>,
}

impl WeakCommitSignal {
    /// The signal, while a clone of it (or a wait on it) is still held.
    pub fn upgrade(&self) -> Option<CommitSignal> {
        self.shared.upgrade().map(|shared| CommitSignal { shared })
    }
}

/// A wait for the next commit after it was armed ([`CommitSignal::wait`],
/// [`CommitNotify::next_commit`]): a future that resolves once one lands.
/// Dropped, it leaves nothing behind.
pub struct CommitWait {
    shared: Arc<Mutex<Signal>>,
    armed_at: u64,
    /// Its place among the waiting, once it has been polled and found
    /// nothing.
    waiter: Option<u64>,
}

impl Future for CommitWait {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        let mut signal = lock(&this.shared);
        if signal.commits != this.armed_at {
            if let Some(waiter) = this.waiter.take() {
                signal.waiting.remove(&waiter);
            }
            return Poll::Ready(());
        }
        let waiter = match this.waiter {
            Some(waiter) => waiter,
            None => {
                let waiter = signal.next_waiter;
                signal.next_waiter = waiter.wrapping_add(1);
                this.waiter = Some(waiter);
                waiter
            }
        };
        signal.waiting.insert(waiter, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for CommitWait {
    fn drop(&mut self) {
        if let Some(waiter) = self.waiter.take() {
            lock(&self.shared).waiting.remove(&waiter);
        }
    }
}

/// Opt-in: a push when this store commits, through this handle or any
/// other in this process that shares its commits. Never for another
/// process's commit: [`Watch`](crate::Watch) is the fallback that sees
/// those.
pub trait CommitNotify: MaybeSend + MaybeSync {
    /// A wait armed now, resolved by the first successful commit after this
    /// call. Arm it before reading, so a commit between the read and the
    /// wait is not missed.
    fn next_commit(&self) -> CommitWait;
}

impl<T: CommitNotify + ?Sized> CommitNotify for Arc<T> {
    fn next_commit(&self) -> CommitWait {
        (**self).next_commit()
    }
}

#[cfg(test)]
#[path = "notify.test.rs"]
mod tests;
