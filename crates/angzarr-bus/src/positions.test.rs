//! The two trackers, run through the positions checks, and what only one of
//! them has to say.

use angzarr_ports::memory::MemoryStore;
use angzarr_proto::v1::{Cover, Uuid};

use super::*;
use crate::contract::positions::PositionsHarness;

struct InMemory;

impl PositionsHarness for InMemory {
    type Tracker<'h> = MemoryPositions;

    fn tracker(&self) -> MemoryPositions {
        MemoryPositions::new()
    }
}

#[derive(Default)]
struct InStore {
    store: MemoryStore,
}

impl PositionsHarness for InStore {
    type Tracker<'h> = DurablePositions<'h, MemoryStore>;

    fn tracker(&self) -> DurablePositions<'_, MemoryStore> {
        DurablePositions::new(&self.store)
    }
}

mod memory {
    use super::InMemory;
    crate::positions_contract!(#[tokio::test], InMemory);
}

mod durable {
    use super::InStore;
    crate::positions_contract!(#[tokio::test], InStore::default());
}

fn stream(edition: Option<&str>) -> Cover {
    Cover {
        domain: "tracked".to_string(),
        root: Some(Uuid::from_bytes([1; 16])),
        edition: edition.map(str::to_string),
    }
}

#[test]
fn a_memory_tracker_is_not_atomic_with_outputs_and_does_not_resume_and_a_durable_one_is_both() {
    assert!(!MemoryPositions::new().atomic_with_outputs());
    assert!(!MemoryPositions::new().resumes());
    let store = MemoryStore::default();
    let durable = DurablePositions::new(&store);
    assert!(durable.atomic_with_outputs());
    assert!(durable.resumes());
}

#[tokio::test]
async fn a_memory_tracker_tells_a_stream_from_the_same_stream_in_an_edition() {
    let tracker = MemoryPositions::new();
    tracker.record("feed", &stream(None), 4).await.unwrap();
    assert_eq!(tracker.load("feed", &stream(None)).await, Ok(4));
    assert_eq!(tracker.load("feed", &stream(Some("what-if"))).await, Ok(0));
}

#[tokio::test]
async fn a_serve_beginning_leaves_a_durable_tracker_alone() {
    let store = MemoryStore::default();
    let durable = DurablePositions::new(&store);
    durable.record("feed", &stream(None), 4).await.unwrap();
    durable.begin();
    assert_eq!(durable.load("feed", &stream(None)).await, Ok(4));
}

// ---------------------------------------------------------------- broken trackers

mod broken {
    use std::future::Future;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::task::Poll;

    use angzarr_ports::BoxFuture;

    use super::*;
    use crate::contract::positions as kit;

    /// What a tracker gets wrong.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Flaw {
        Honest,
        /// It records nothing.
        Forgets,
        /// It keys a mark by stream alone.
        IgnoresName,
        /// It keys a mark by subscription alone.
        IgnoresStream,
        /// A mark recorded as 0 is kept as it was.
        IgnoresZero,
        /// A serve beginning leaves it as it was, though it does not outlive it.
        KeepsAcrossServes,
        /// It says it outlives the serve, and loses its marks between trackers.
        ClaimsToResume,
        /// It says it is atomic with outputs, and does not resume.
        ClaimsAtomic,
    }

    /// A memory tracker with a flaw.
    struct Bent {
        inner: MemoryPositions,
        flaw: Flaw,
    }

    impl Positions for Bent {
        fn atomic_with_outputs(&self) -> bool {
            self.flaw == Flaw::ClaimsAtomic
        }

        fn resumes(&self) -> bool {
            self.flaw == Flaw::ClaimsToResume
        }

        fn begin(&self) {
            if self.flaw != Flaw::KeepsAcrossServes {
                self.inner.begin();
            }
        }

        fn load<'a>(
            &'a self,
            name: &'a str,
            stream: &'a Cover,
        ) -> BoxFuture<'a, Result<u64, StoreError>> {
            Box::pin(async move {
                match self.flaw {
                    Flaw::IgnoresName => self.inner.load("", stream).await,
                    Flaw::IgnoresStream => self.inner.load(name, &Cover::default()).await,
                    _ => self.inner.load(name, stream).await,
                }
            })
        }

        fn record<'a>(
            &'a self,
            name: &'a str,
            stream: &'a Cover,
            mark: u64,
        ) -> BoxFuture<'a, Result<(), StoreError>> {
            Box::pin(async move {
                match (self.flaw, mark) {
                    (Flaw::Forgets, _) | (Flaw::IgnoresZero, 0) => Ok(()),
                    (Flaw::IgnoresName, _) => self.inner.record("", stream, mark).await,
                    (Flaw::IgnoresStream, _) => {
                        self.inner.record(name, &Cover::default(), mark).await
                    }
                    _ => self.inner.record(name, stream, mark).await,
                }
            })
        }
    }

    struct Harness(Flaw);

    impl PositionsHarness for Harness {
        type Tracker<'h> = Bent;

        fn tracker(&self) -> Bent {
            Bent {
                inner: MemoryPositions::new(),
                flaw: self.0,
            }
        }
    }

    async fn panicked<F: Future<Output = ()>>(check: F) -> bool {
        let mut check = Box::pin(check);
        std::future::poll_fn(|cx| {
            match catch_unwind(AssertUnwindSafe(|| check.as_mut().poll(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(())) => Poll::Ready(false),
                Err(_) => Poll::Ready(true),
            }
        })
        .await
    }

    /// `check` must pass the honest tracker and fail one with `flaw`.
    async fn catches<F: Future<Output = ()>>(flaw: Flaw, check: impl Fn(Harness) -> F) {
        assert!(
            !panicked(check(Harness(Flaw::Honest))).await,
            "honest passes"
        );
        assert!(
            panicked(check(Harness(flaw))).await,
            "the flaw passes the check"
        );
    }

    #[tokio::test]
    async fn a_tracker_that_records_nothing_fails_the_first_check() {
        catches(
            Flaw::Forgets,
            kit::a_stream_starts_at_zero_and_holds_what_is_recorded,
        )
        .await;
    }

    #[tokio::test]
    async fn a_tracker_that_ignores_the_name_fails_the_per_subscription_check() {
        catches(
            Flaw::IgnoresName,
            kit::marks_are_per_subscription_and_per_stream,
        )
        .await;
    }

    #[tokio::test]
    async fn a_tracker_that_ignores_the_stream_fails_the_per_stream_check() {
        catches(
            Flaw::IgnoresStream,
            kit::marks_are_per_subscription_and_per_stream,
        )
        .await;
    }

    #[tokio::test]
    async fn a_tracker_that_ignores_zero_fails_the_forgotten_check() {
        catches(Flaw::IgnoresZero, kit::a_mark_recorded_as_zero_is_forgotten).await;
    }

    #[tokio::test]
    async fn a_tracker_that_says_it_resumes_and_loses_its_marks_fails_the_restart_check() {
        let check =
            kit::a_restart_resumes_where_it_outlives_the_serve_and_starts_over_where_it_does_not;
        catches(Flaw::ClaimsToResume, check).await;
    }

    #[tokio::test]
    async fn a_tracker_that_keeps_marks_across_serves_fails_the_begin_check() {
        let check = kit::a_serve_beginning_clears_a_tracker_that_does_not_outlive_it;
        catches(Flaw::KeepsAcrossServes, check).await;
    }

    #[tokio::test]
    async fn a_tracker_atomic_with_outputs_that_does_not_resume_fails_the_capability_check() {
        catches(
            Flaw::ClaimsAtomic,
            kit::only_a_tracker_in_the_store_is_atomic_with_outputs,
        )
        .await;
    }

    #[test]
    fn every_positions_check_has_a_broken_tracker_test() {
        let source = include_str!("positions.test.rs");
        for check in kit::CHECKS {
            assert!(
                source.contains(&format!("kit::{check}")),
                "{check} has no broken tracker"
            );
        }
    }
}
