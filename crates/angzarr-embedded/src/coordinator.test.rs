#[allow(unused_imports)]
use crate::toys::prelude::*;
mod dedup {
    //! Step 3: deduplication by `command_id` on the events, with its accepted
    //! costs. No claims, no fingerprints, no commands table.
    use crate::toys::prelude::*;
    use crate::v1::DeliveryOutcome;
    use crate::{Failure, Handled, Payload};

    use crate::toys::{
        at, command, cover, fact, handle, opened, payloads, registry, AccountCmd, AccountEvent,
        AccountFact, Edit,
    };

    fn deposit(n: u128, cents: i64) -> crate::v1::CommandBook {
        command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
    }

    #[test]
    fn a_retried_command_is_a_duplicate_answered_with_its_events() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        let first =
            handle::<crate::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();
        assert_eq!(first.delivery(), DeliveryOutcome::Applied);
        let retry =
            handle::<crate::toys::Account>(&store, &registry, deposit(3, 25), &at(21)).unwrap();
        assert_eq!(retry.delivery(), DeliveryOutcome::Duplicate);
        assert!(matches!(retry, Handled::Duplicate(..)));
        assert_eq!(
            retry.books(),
            first.books(),
            "the first delivery's events, re-read"
        );
        assert_eq!(
            payloads::<AccountEvent>(&retry.books()),
            [AccountEvent::Deposited(25)]
        );
        let page = &retry.books()[0].pages[0];
        assert_eq!(
            page.meta.as_ref().unwrap().recorded_at.unwrap().seconds,
            20,
            "nothing re-stamped"
        );
    }

    #[test]
    fn a_zero_event_decision_is_decided_again() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);

        // A sweep of an empty account withdraws nothing: applied, no events.
        let sweep = || command(cover("account", 1), AccountCmd::Sweep.to_any()).id(7);
        let first = handle::<crate::toys::Account>(&store, &registry, sweep(), &at(20)).unwrap();
        assert_eq!(first.delivery(), DeliveryOutcome::Applied);
        assert!(payloads::<AccountEvent>(&first.books()).is_empty());

        // Money arrives; the same sweep is redelivered.
        let payment = fact(
            cover("account", 1),
            AccountFact::PaymentReceived(50).to_any(),
            "pi_1",
        );
        handle::<crate::toys::Account>(&store, &registry, payment, &at(21)).unwrap();
        let again = handle::<crate::toys::Account>(&store, &registry, sweep(), &at(22)).unwrap();
        assert_eq!(
            again.delivery(),
            DeliveryOutcome::Applied,
            "the accepted cost: re-decided against the stream as it is now"
        );
        assert_eq!(
            payloads::<AccountEvent>(&again.books()),
            [AccountEvent::Withdrew(49)],
            "50 less the v2 fee"
        );
    }

    #[cfg(feature = "purge")]
    #[test]
    fn a_creating_command_redelivered_after_a_purge_creates_its_stream_again() {
        let registry = registry();
        let store = MemoryStore::new();
        let stream = cover("account", 1);
        let open = || command(stream.clone(), AccountCmd::Open.to_any()).id(5);
        handle::<crate::toys::Account>(&store, &registry, open(), &at(20)).unwrap();
        let retry = handle::<crate::toys::Account>(&store, &registry, open(), &at(21)).unwrap();
        assert_eq!(retry.delivery(), DeliveryOutcome::Duplicate);

        assert_eq!(store.purge(sref(&stream), 1).ready(), Ok(1));
        let again = handle::<crate::toys::Account>(&store, &registry, open(), &at(22)).unwrap();
        assert_eq!(
            again.delivery(),
            DeliveryOutcome::Applied,
            "the accepted cost: nothing left to find, so decided again"
        );
        assert_eq!(
            payloads::<AccountEvent>(&again.books()),
            [AccountEvent::Opened]
        );
        let slice = slice(&store, &stream);
        assert_eq!(
            slice.next_sequence, 3,
            "after the tombstone, which keeps the sequence"
        );
        assert_eq!(slice.last_recorded_at.unwrap().seconds, 22);
    }

    #[test]
    fn a_reused_command_id_with_another_payload_is_answered_with_the_first() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        handle::<crate::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();
        let reused =
            handle::<crate::toys::Account>(&store, &registry, deposit(3, 999), &at(21)).unwrap();
        assert_eq!(
            reused.delivery(),
            DeliveryOutcome::Duplicate,
            "the accepted cost: payloads are not fingerprinted"
        );
        assert_eq!(
            payloads::<AccountEvent>(&reused.books()),
            [AccountEvent::Deposited(25)]
        );
    }

    #[test]
    fn the_same_command_id_to_another_stream_is_another_command() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        opened(&store, &registry, 2, 100);

        handle::<crate::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();
        let other = command(cover("account", 2), AccountCmd::Deposit(25).to_any()).id(3);
        let applied = handle::<crate::toys::Account>(&store, &registry, other, &at(21)).unwrap();
        assert_eq!(applied.delivery(), DeliveryOutcome::Applied);
    }

    #[test]
    fn a_redelivered_fact_is_a_duplicate() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);
        let payment = || {
            fact(
                cover("account", 1),
                AccountFact::PaymentReceived(50).to_any(),
                "pi_1234",
            )
        };

        handle::<crate::toys::Account>(&store, &registry, payment(), &at(20)).unwrap();
        let again = handle::<crate::toys::Account>(&store, &registry, payment(), &at(21)).unwrap();
        assert_eq!(again.delivery(), DeliveryOutcome::Duplicate);
        let meta = again.books()[0].pages[0].meta.clone().unwrap();
        let Some(crate::v1::event_meta::Causation::Fact(cause)) = meta.causation else {
            panic!("a fact's cause: {meta:?}");
        };
        assert_eq!(
            cause.command_id,
            Some(crate::ids::fact_command_id("pi_1234")),
            "the id is on the event, where the lookup finds it"
        );
    }

    #[test]
    fn a_rejection_leaves_nothing_and_a_retry_runs_again() {
        let registry = registry();
        let store = MemoryStore::new();
        let early = || command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(4);

        let refused = handle::<crate::toys::Account>(&store, &registry, early(), &at(5));
        assert!(matches!(
            refused,
            Err(Failure::Rejected(r))
                if r.rejection::<crate::toys::Refused>() == Some(&crate::toys::Refused::NotOpen)
        ));
        assert!(streams(&store).is_empty());

        opened(&store, &registry, 1, 0);
        let retried = handle::<crate::toys::Account>(&store, &registry, early(), &at(20)).unwrap();
        assert_eq!(retried.delivery(), DeliveryOutcome::Applied);
    }

    /// A store that plays READ COMMITTED with a concurrent commit: it
    /// already holds the original delivery's events, but the first read
    /// made of it (whichever it is) sees the stream as it was before them;
    /// every later read, and every write, sees the store as it is.
    struct CommitLandsAfterFirstRead {
        store: MemoryStore,
        /// The stream's length before the original commit.
        before: u64,
        seen: std::sync::atomic::AtomicBool,
    }

    impl CommitLandsAfterFirstRead {
        fn first_read(&self) -> bool {
            !self.seen.swap(true, std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl crate::EventStore for CommitLandsAfterFirstRead {
        async fn write(
            &self,
            write: crate::Write<'_>,
        ) -> Result<crate::Written, crate::StoreError> {
            self.store.write(write).await
        }

        async fn read(
            &self,
            stream: crate::StreamRef<'_>,
            from: u64,
            to: Option<u64>,
        ) -> Result<crate::Slice, crate::StoreError> {
            let early = self.first_read();
            let mut slice = self.store.read(stream, from, to).await?;
            if early {
                slice.pages.retain(|p| p.sequence < self.before);
                slice.next_sequence = self.before;
            }
            Ok(slice)
        }

        async fn find_command(
            &self,
            stream: crate::StreamRef<'_>,
            command_id: angzarr_ports::Uuid,
        ) -> Result<Vec<crate::v1::EventPage>, crate::StoreError> {
            let early = self.first_read();
            let mut pages = self.store.find_command(stream, command_id).await?;
            if early {
                pages.retain(|p| p.sequence < self.before);
            }
            Ok(pages)
        }

        async fn covers(&self, domain: &str) -> Result<Vec<crate::v1::Cover>, crate::StoreError> {
            self.store.covers(domain).await
        }
    }

    impl crate::SnapshotStore for CommitLandsAfterFirstRead {
        async fn read_snapshot(
            &self,
            stream: crate::StreamRef<'_>,
        ) -> Result<Option<crate::v1::Snapshot>, crate::StoreError> {
            self.store.read_snapshot(stream).await
        }

        async fn write_snapshot(
            &self,
            stream: crate::StreamRef<'_>,
            snapshot: &crate::v1::Snapshot,
        ) -> Result<(), crate::StoreError> {
            self.store.write_snapshot(stream, snapshot).await
        }
    }

    impl crate::PositionStore for CommitLandsAfterFirstRead {
        async fn position(&self, key: crate::PositionKey<'_>) -> Result<u64, crate::StoreError> {
            self.store.position(key).await
        }
    }

    #[test]
    fn a_retry_racing_its_original_commit_is_never_applied_twice() {
        // The stream is loaded before the command is looked up, so an original
        // that commits between the retry's two reads is seen by the lookup.
        // (Looked up first, the retry would find nothing, then load the head
        // after the original's events and append again.)
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let before = pages(&store, &cover("account", 1)).len() as u64;
        handle::<crate::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();

        let log = crate::MemoryDeadLetters::new();
        let kernel = crate::Kernel::new(&registry).log_with(&log);
        let racing = CommitLandsAfterFirstRead {
            store: store.clone(),
            before,
            seen: std::sync::atomic::AtomicBool::new(false),
        };
        let retry = kernel
            .handle::<crate::toys::Account, _>(&racing, deposit(3, 25), &at(21))
            .ready()
            .unwrap();
        assert!(
            matches!(retry, Handled::Duplicate(..)),
            "found, not re-applied"
        );
        let deposits = pages(&store, &cover("account", 1)).len() as u64;
        assert_eq!(deposits, before + 1, "the deposit is on the stream once");
    }
}

mod pipeline {
    //! The normative order: validate → identify → load, then deduplicate →
    //! resolve → decode (and upcast) → decide or ingest. Each test pins one
    //! boundary by sending something that two adjacent steps would answer
    //! differently.
    use crate::toys::prelude::*;

    use super::super::{steps::Coordinator, Deduplicated};
    use crate::v1::DeliveryOutcome;
    use crate::{check, pack, Failure, KernelError, Malformed, Payload, Recovery};

    use crate::toys::{
        account, at, command, cover, handle, mismatch_of, opened, registry, Account, AccountCmd,
        Edit,
    };

    /// The staleness check's expected sequence and head, if `failure` is
    /// its refusal.
    fn mismatch(failure: &Failure<crate::Rejected>) -> Option<(u64, u64)> {
        match failure {
            Failure::Rejected(rejected) => mismatch_of(rejected),
            _ => None,
        }
    }

    fn deposit(n: u128) -> crate::v1::CommandBook {
        command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(n)
    }

    #[test]
    fn an_applied_retry_is_a_duplicate_whatever_its_expected_sequence() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        let at_head = || deposit(3).expecting(2);
        handle::<Account>(&store, &registry, at_head(), &at(20)).unwrap();
        handle::<Account>(&store, &registry, deposit(4), &at(21)).unwrap();

        // The stream is at 4 now; the retry still expects 2.
        let retry = handle::<Account>(&store, &registry, at_head(), &at(22)).unwrap();
        assert_eq!(
            retry.delivery(),
            DeliveryOutcome::Duplicate,
            "the retry's events are found before its sequence is checked"
        );

        // A new command expecting 2 is refused: the head is 4.
        let stale = handle::<Account>(&store, &registry, deposit(5).expecting(2), &at(23))
            .err()
            .unwrap();
        assert_eq!(mismatch(&stale), Some((2, 4)));
        assert_eq!(stale.recovery(), Recovery::Business);
    }

    #[test]
    fn a_malformed_retry_of_an_applied_command_is_still_malformed() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        handle::<Account>(&store, &registry, deposit(3), &at(20)).unwrap();

        let mut forged = deposit(3);
        forged.page_mut().correlation_id = Some(crate::v1::Uuid {
            value: vec![1; 3].into(),
        });
        let refused = handle::<Account>(&store, &registry, forged, &at(21))
            .err()
            .unwrap();
        assert_eq!(
            refused,
            Failure::Kernel(KernelError::Malformed(Malformed::BadCorrelationId {
                len: 3
            })),
            "refused before anything is looked up"
        );
        assert_eq!(refused.recovery(), Recovery::Invalid);
    }

    #[test]
    fn an_unknown_command_type_is_refused_at_decode() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let unknown = || command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 }));

        let refused = handle::<Account>(&store, &registry, unknown().id(8), &at(20))
            .err()
            .unwrap();
        assert!(
            matches!(
                &refused,
                Failure::Kernel(KernelError::Router(e))
                    if e.code == "ANGZARR_REASON_UNKNOWN_COMMAND_TYPE"
                        && e.extras.get("type_url").map(String::as_str)
                            == Some("/toy.account.v9.Bonus")
            ),
            "ANGZARR_REASON_UNKNOWN_COMMAND_TYPE, named by its type URL: {refused:?}"
        );
        // The router's own error on malformed input is a bug, INTERNAL (the
        // owner's ruling, 2026-10-29), never the sender's to fix.
        assert_eq!(refused.recovery(), Recovery::Bug);

        // Stale and undecodable: the staleness check comes first.
        let stale = handle::<Account>(&store, &registry, unknown().id(9).expecting(1), &at(21))
            .err()
            .unwrap();
        assert_eq!(mismatch(&stale), Some((1, 2)));

        // Applied under that id already: the duplicate is found first, the
        // payload never decoded (and never compared: a reused id goes
        // undetected).
        let dup = handle::<Account>(
            &store,
            &registry,
            command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 })).id(2),
            &at(22),
        )
        .unwrap();
        assert_eq!(dup.delivery(), DeliveryOutcome::Duplicate);
    }

    #[test]
    fn the_coordinator_runs_dedup_then_load_then_decode() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let mut coordinator = Coordinator::new(&store, &registry);

        // An applied command: step 3 answers it.
        let applied = check(deposit(2), registry.catalog())
            .unwrap()
            .identify(&at(30));
        assert!(matches!(
            coordinator.deduplicate(applied).unwrap(),
            Deduplicated::Duplicate { .. }
        ));

        // A new one goes on, decided at the head step 3 loaded: no
        // expected sequence is stamped onto it (decision 14.94).
        let novel = check(deposit(3), registry.catalog())
            .unwrap()
            .identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(novel).unwrap() else {
            panic!("never applied");
        };
        assert_eq!(novel.head(), 2, "the head as step 3 loaded it");
        assert_eq!(novel.command().expected_sequence(), None);
        let contextual = coordinator.load(novel).unwrap();
        assert_eq!(contextual.head(), 2);
        assert_eq!(
            contextual.snapshot().map(|s| s.sequence),
            Some(1),
            "the deposit's snapshot"
        );
        assert!(contextual.pages().is_empty(), "nothing after it");

        let prepared = contextual.decode::<Account>(&registry).unwrap();
        assert_eq!(prepared.stream().state().cents, 100);
    }

    #[test]
    fn the_coordinator_refuses_an_expected_sequence_not_the_head_at_load() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let mut coordinator = Coordinator::new(&store, &registry);
        for (expected, refused) in [(2u64, None), (1, Some((1, 2))), (3, Some((3, 2)))] {
            let identified = check(
                deposit(10 + u128::from(expected)).expecting(expected),
                registry.catalog(),
            )
            .unwrap()
            .identify(&at(30));
            let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
                panic!("never applied");
            };
            assert_eq!(novel.command().expected_sequence(), Some(expected));
            let loaded = coordinator.load(novel);
            assert_eq!(
                loaded.as_ref().err().and_then(mismatch),
                refused,
                "expecting {expected}"
            );
            assert_eq!(loaded.is_ok(), refused.is_none(), "expecting {expected}");
        }
    }

    #[test]
    fn the_coordinators_decision_commits_to_its_stream() {
        let registry = registry();
        let log = crate::MemoryDeadLetters::new();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let mut coordinator = Coordinator::new(&store, &registry);
        let identified = check(deposit(3), registry.catalog())
            .unwrap()
            .identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        let prepared = coordinator
            .load(novel)
            .unwrap()
            .decode::<Account>(&registry)
            .unwrap();
        assert_eq!(prepared.command().command_id(), &crate::toys::command_id(3));
        let pages = prepared.decide(&at(30), &registry, &log).unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].sequence, 2, "at the head");
        let mut stream = prepared.into_stream();
        assert_eq!(stream.commit(&pages, &registry), Ok(3));
        assert_eq!(stream.state().cents, 105);

        opened(&store, &registry, 2, 0);
        let sweep = check(
            command(cover("account", 2), AccountCmd::Sweep.to_any()).id(5),
            registry.catalog(),
        )
        .unwrap()
        .identify(&at(32));
        let mut empty = Coordinator::new(&store, &registry);
        let Deduplicated::Novel(novel) = empty.deduplicate(sweep).unwrap() else {
            panic!("never applied");
        };
        let prepared = empty
            .load(novel)
            .unwrap()
            .decode::<Account>(&registry)
            .unwrap();
        let pages = prepared.decide(&at(32), &registry, &log).unwrap();
        assert!(pages.is_empty(), "a sweep of nothing withdraws nothing");

        // A cover of another domain than the one decoded for.
        let mut wrong = Coordinator::new(&store, &registry);
        let identified = check(deposit(6), registry.catalog())
            .unwrap()
            .identify(&at(33));
        let Deduplicated::Novel(novel) = wrong.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        assert_eq!(
            wrong
                .load(novel)
                .unwrap()
                .decode::<crate::toys::Ledger>(&registry)
                .err(),
            Some(KernelError::Replay(crate::ReplayError::WrongDomain {
                found: "account".to_string(),
                expected: "ledger"
            }))
        );
    }
}

mod snapshot {
    //! Snapshots: a framework optimisation, never business data. Used only
    //! under the current rules version and state type.
    use crate::toys::prelude::*;

    use super::super::{steps::Coordinator, Deduplicated};
    use crate::v1::Snapshot;
    use crate::{check, pack, Payload, Stream};
    use coded_diagnostic::code;

    use crate::toys::{
        account, at, command, cover, opened, registry, root, set, shelf, Account, AccountCmd, Edit,
        Shelf,
    };

    /// A snapshot of an account at sequence 1 claiming `cents`: a poisoned one
    /// shows when it is used.
    fn poisoned(rules: &str, cents: i64) -> Snapshot {
        Snapshot {
            sequence: 1,
            state: Some(pack(&account::AccountState {
                open: true,
                cents,
                ..Default::default()
            })),
            rules_version: rules.to_string(),
        }
    }

    /// Load (steps 2 to 5) a deposit for account 1, expecting `expected` if
    /// given.
    fn load(store: &MemoryStore, expected: Option<u64>) -> (Option<u64>, i64) {
        let registry = registry();
        let mut book = command(cover("account", 1), AccountCmd::Deposit(1).to_any()).id(9);
        if let Some(sequence) = expected {
            book = book.expecting(sequence);
        }
        let mut coordinator = Coordinator::new(store, &registry);
        let identified = check(book, registry.catalog()).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        let contextual = coordinator.load(novel).unwrap();
        let used = contextual.snapshot().map(|s| s.sequence);
        let prepared = contextual.decode::<Account>(&registry).unwrap();
        (used, prepared.stream().state().cents)
    }

    #[test]
    fn a_snapshot_under_another_rules_version_is_discarded() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        store
            .write_snapshot(sref(&cover("account", 1)), &poisoned("v1", 999))
            .ready()
            .unwrap();
        assert_eq!(
            load(&store, None),
            (None, 100),
            "folded under v1, the current rules are v2: replayed from 0"
        );

        store
            .write_snapshot(sref(&cover("account", 1)), &poisoned("v2", 999))
            .ready()
            .unwrap();
        assert_eq!(
            load(&store, None),
            (Some(1), 999),
            "a current one is used (and here shows its poison)"
        );
    }

    #[test]
    fn a_snapshot_of_another_state_type_is_discarded() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let foreign = Snapshot {
            sequence: 1,
            state: Some(pack(&shelf::ShelfState::default())),
            rules_version: "v2".to_string(),
        };
        store
            .write_snapshot(sref(&cover("account", 1)), &foreign)
            .ready()
            .unwrap();
        assert_eq!(load(&store, None), (None, 100));
    }

    /// A snapshot at or past the head claims events the stream does not
    /// hold (one a purge left, or one written ahead of its events): not
    /// used, and the stream is read from its events. Used, the stream
    /// would resume past its head, its next events would not run on from
    /// it, and every later command on it would fail.
    #[test]
    fn a_snapshot_at_or_past_the_head_is_not_used() {
        use crate::toys::handle;
        let registry = registry();
        for sequence in [2, 5] {
            let store = MemoryStore::new();
            opened(&store, &registry, 1, 100);
            let ahead = Snapshot {
                sequence,
                ..poisoned("v2", 999)
            };
            store
                .write_snapshot(sref(&cover("account", 1)), &ahead)
                .ready()
                .unwrap();
            assert_eq!(
                load(&store, None),
                (None, 100),
                "a snapshot at {sequence}, the head at 2: read from the events"
            );
            for (n, head) in [(3, 3), (4, 4)] {
                let deposit = command(cover("account", 1), AccountCmd::Deposit(1).to_any()).id(n);
                let handled = handle::<Account>(&store, &registry, deposit, &at(20))
                    .unwrap_or_else(|e| panic!("a snapshot at {sequence}, command {n}: {e}"));
                assert_eq!(handled.books()[0].next_sequence, head);
            }
        }
    }

    /// The kernel writes the snapshot from the state the router returns
    /// with a command's events (decision 14.40), stamped with the last
    /// event's sequence and the domain's rules version, under the default
    /// policy: the newest only, each replacing the last. A decision with no
    /// events writes none.
    #[test]
    fn the_state_a_command_returns_is_written_as_the_snapshot() {
        use crate::toys::handle;
        let registry = registry();
        let store = MemoryStore::new();
        let stream = cover("account", 1);
        let stored = || store.read_snapshot(sref(&stream)).ready().unwrap();
        let state = |snapshot: &Snapshot| {
            let any = snapshot.state.as_ref().expect("a snapshot holds a state");
            assert_eq!(any.type_url, "/toy.account.v1.AccountState");
            <account::AccountState as prost::Message>::decode(&any.value[..]).unwrap()
        };
        opened(&store, &registry, 1, 100);

        let snapshot = stored().expect("written with the deposit");
        assert_eq!(snapshot.sequence, 1, "the last event's");
        assert_eq!(snapshot.rules_version, "v2", "the domain's current rules");
        assert_eq!(state(&snapshot).cents, 100);
        assert!(registry.catalog().snapshot_usable("account", &snapshot));

        let deposit = command(stream.clone(), AccountCmd::Deposit(5).to_any()).id(3);
        handle::<Account>(&store, &registry, deposit, &at(12)).unwrap();
        let snapshot = stored().unwrap();
        assert_eq!(snapshot.sequence, 2, "the newest replaces the last");
        assert_eq!(state(&snapshot).cents, 105);

        let sweep_nothing = command(cover("account", 2), AccountCmd::Sweep.to_any()).id(4);
        opened(&store, &registry, 2, 0);
        let before = store
            .read_snapshot(sref(&cover("account", 2)))
            .ready()
            .unwrap();
        handle::<Account>(&store, &registry, sweep_nothing, &at(13)).unwrap();
        assert_eq!(
            store
                .read_snapshot(sref(&cover("account", 2)))
                .ready()
                .unwrap(),
            before,
            "no events, no snapshot"
        );
    }

    /// The account as a build of it that takes no snapshots: the same
    /// events, folded the same way, into a state that keeps `State`'s
    /// defaults over the same anchor.
    #[derive(Clone, Debug, Default, PartialEq)]
    struct Unsnapshotted(account::AccountState);

    impl crate::State for Unsnapshotted {
        type Anchor = account::AccountState;
    }

    struct UnsnapshottedAccount;

    impl crate::Domain for UnsnapshottedAccount {
        type State = Unsnapshotted;
        type Command = AccountCmd;
        type Fact = crate::toys::AccountFact;
        type Event = crate::toys::AccountEvent;
        type Rejection = crate::toys::Refused;
        const NAME: &'static str = "account";
        const RULES_VERSION: &'static str = "v2";

        fn apply(
            state: &mut Unsnapshotted,
            event: &crate::toys::AccountEvent,
            page: &crate::PageContext<'_>,
        ) {
            <Account as crate::Domain>::apply(&mut state.0, event, page);
        }
    }

    /// A stream holding a snapshot an opted-in build of its domain wrote
    /// (the account's own, and one claiming 999 cents, which shows when it
    /// is used), loaded by a build that takes no snapshots: the snapshot is
    /// not offered, and the whole history is folded from sequence 0, on
    /// the typed load and on a command's alike. The opted-in build still
    /// starts from it.
    #[test]
    fn a_build_taking_no_snapshots_folds_the_whole_history_past_a_stored_one() {
        let store = MemoryStore::new();
        opened(&store, &registry(), 1, 100);
        let stream = cover("account", 1);
        assert!(
            store
                .read_snapshot(sref(&stream))
                .ready()
                .unwrap()
                .is_some(),
            "the opted-in build stored one"
        );
        store
            .write_snapshot(sref(&stream), &poisoned("v2", 999))
            .ready()
            .unwrap();
        assert_eq!(
            load(&store, None),
            (Some(1), 999),
            "the opted-in build uses it"
        );

        let mut plain = crate::Registry::new();
        plain
            .register::<UnsnapshottedAccount>(
                crate::Upcaster::new("account-upcaster", "account"),
                |table| table,
            )
            .unwrap();
        let (loaded, head) = crate::Kernel::new(&plain)
            .load::<UnsnapshottedAccount, _>(&store, &stream)
            .ready()
            .unwrap();
        assert_eq!(head, 2);
        assert_eq!(loaded.first(), 0, "replayed from its first event");
        assert_eq!(
            loaded.state().0.cents,
            100,
            "the history's, not the snapshot's"
        );
        assert!(loaded.state().0.open);

        let book = command(stream.clone(), AccountCmd::Deposit(1).to_any()).id(9);
        let mut coordinator = Coordinator::new(&store, &plain);
        let identified = check(book, plain.catalog()).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        let contextual = coordinator.load(novel).unwrap();
        assert_eq!(contextual.snapshot(), None, "not offered");
        assert_eq!(contextual.pages().len(), 2, "every page from 0");
    }

    /// The sequences of the snapshots `book`s write to account 1, one
    /// after each, handled by a kernel under `policy`, from an empty store
    /// or from what `store` holds already.
    fn written_under(
        policy: &crate::SnapshotPolicy,
        store: &MemoryStore,
        books: impl IntoIterator<Item = (u128, AccountCmd)>,
    ) -> Vec<Option<u64>> {
        let registry = registry();
        let log = crate::MemoryDeadLetters::new();
        let kernel = crate::Kernel::new(&registry)
            .log_with(&log)
            .on_retryable(crate::toys::no_retries())
            .snapshot_policy(policy);
        let stream = cover("account", 1);
        books
            .into_iter()
            .map(|(id, cmd)| {
                let book = command(stream.clone(), cmd.to_any()).id(id);
                kernel
                    .handle::<Account, _>(store, book, &at(10 + id as i64))
                    .ready()
                    .unwrap_or_else(|e| panic!("command {id}: {e}"));
                let snapshot = store.read_snapshot(sref(&stream)).ready().unwrap();
                snapshot.map(|s| s.sequence)
            })
            .collect()
    }

    /// An account opened, then deposited into five times: six events, 0 to
    /// 5, one a write.
    fn six_writes() -> Vec<(u128, AccountCmd)> {
        std::iter::once((1, AccountCmd::Open))
            .chain((2..=6).map(|id| (id, AccountCmd::Deposit(1))))
            .collect()
    }

    fn every(events: u64) -> std::num::NonZeroU64 {
        std::num::NonZeroU64::new(events).unwrap()
    }

    /// Under a policy of every three events, the snapshot a write's state
    /// offers is written once the stream has gained three events since the
    /// snapshot its load started from, or since its start; the others are
    /// not, and the one stored stays.
    #[test]
    fn a_snapshot_is_written_once_its_stream_gains_the_policys_events() {
        let written = written_under(
            &crate::SnapshotPolicy::every(every(3)),
            &MemoryStore::new(),
            six_writes(),
        );
        assert_eq!(
            written,
            [None, None, Some(2), Some(2), Some(2), Some(5)],
            "three events from the start (0 to 2), then three after 2"
        );
    }

    /// The kernel's default policy writes one at every write.
    #[test]
    fn the_default_policy_writes_a_snapshot_at_every_write() {
        let written = written_under(
            &crate::SnapshotPolicy::default(),
            &MemoryStore::new(),
            six_writes(),
        );
        assert_eq!(written, (0..6).map(Some).collect::<Vec<_>>());
    }

    /// A domain the policy names has its own frequency; the rest keep the
    /// policy's.
    #[test]
    fn a_domain_the_policy_names_is_snapshotted_at_its_own_frequency() {
        let account = crate::SnapshotPolicy::every(every(100)).domain("account", every(2));
        let written = written_under(&account, &MemoryStore::new(), six_writes());
        assert_eq!(written, [None, Some(1), Some(1), Some(3), Some(3), Some(5)]);

        let other = crate::SnapshotPolicy::every(every(2)).domain("shelf", every(100));
        let written = written_under(&other, &MemoryStore::new(), six_writes());
        assert_eq!(written, [None, Some(1), Some(1), Some(3), Some(3), Some(5)]);
    }

    /// A stored snapshot the load cannot use (under other rules) counts as
    /// none: the policy counts from the stream's start, so the first write
    /// past the policy's events replaces it.
    #[test]
    fn an_unusable_snapshot_counts_as_none() {
        let store = MemoryStore::new();
        let policy = crate::SnapshotPolicy::every(every(3));
        let opened = written_under(&policy, &store, six_writes().into_iter().take(2));
        assert_eq!(opened, [None, None]);
        store
            .write_snapshot(sref(&cover("account", 1)), &poisoned("v1", 999))
            .ready()
            .unwrap();
        let written = written_under(&policy, &store, [(3, AccountCmd::Deposit(1))]);
        assert_eq!(written, [Some(2)], "the poisoned one at 1 replaced");
    }

    #[test]
    fn a_stream_snapshots_under_the_current_rules_version() {
        let registry = registry();
        let mut stream = Stream::<Shelf>::new(root(3), None).unwrap();
        let log = crate::MemoryDeadLetters::new();
        stream
            .execute(
                command(cover("shelf", 3), set("a", 1)),
                &registry,
                &at(1),
                &log,
            )
            .ready()
            .unwrap();
        let snapshot = stream.snapshot(&registry).unwrap();
        assert_eq!(snapshot.sequence, 0);
        assert_eq!(snapshot.rules_version, "v1");
        assert_eq!(
            snapshot.state.as_ref().unwrap().type_url,
            "/toy.shelf.v1.ShelfState"
        );
        assert!(registry.catalog().snapshot_usable("shelf", &snapshot));
        assert!(Stream::<Shelf>::new(root(4), None)
            .unwrap()
            .snapshot(&registry)
            .is_none());
    }

    /// Ledger 1 after recording `cents`, executed in memory.
    fn recorded(cents: i64) -> Stream<crate::toys::Ledger> {
        use crate::MemoryDeadLetters;
        let registry = registry();
        let mut stream = Stream::<crate::toys::Ledger>::new(root(1), None).unwrap();
        let book = command(
            cover("ledger", 1),
            crate::toys::ledger::Record {
                source_sequence: 0,
                cents,
            }
            .to_any(),
        );
        stream
            .execute(book, &registry, &at(10), &MemoryDeadLetters::default())
            .ready()
            .unwrap();
        stream
    }

    #[test]
    fn a_domain_typed_state_snapshots_as_its_wire_form_and_resumes_from_it() {
        let registry = registry();
        let stream = recorded(40);

        let snapshot = stream.snapshot(&registry).unwrap();
        assert_eq!(
            snapshot.state.as_ref().unwrap().type_url,
            "/toy.ledger.v1.LedgerSnapshot"
        );
        let resumed = Stream::<crate::toys::Ledger>::resume(
            cover("ledger", 1),
            &snapshot,
            None,
            &[],
            &registry,
        )
        .unwrap();
        assert_eq!(resumed.state(), stream.state());
        assert_eq!(resumed.state().total, 40);
    }

    #[test]
    fn a_snapshot_whose_wire_form_is_not_a_valid_state_is_unreadable() {
        use crate::KernelError;
        use coded_diagnostic::Diagnostic;
        let snapshot = Snapshot {
            sequence: 0,
            state: Some(pack(&crate::toys::ledger::LedgerSnapshot {
                next_source: 1,
                total: -5,
            })),
            rules_version: "v1".to_string(),
        };
        let error = Stream::<crate::toys::Ledger>::resume(
            cover("ledger", 1),
            &snapshot,
            None,
            &[],
            &registry(),
        )
        .err()
        .expect("a negative total is not a ledger");
        let KernelError::Unreadable { reason, .. } = error else {
            panic!("expected an unreadable snapshot, got {error:?}");
        };
        assert_eq!(reason.code(), code!("payload-invalid"));
    }
}

mod upcasting {
    //! The kernel upcasts every incoming payload before dispatch, through
    //! the domain's upcaster port (decision 14.39): the router is handed the
    //! current type only.
    use std::sync::{Arc, Mutex};

    use crate::angzarr_router::error::CodedError;
    use crate::angzarr_router::router::CommandHandler;
    use crate::toys::prelude::*;
    use crate::toys::{
        account, account_upcaster, at, command, cover, handlers, payloads, Account, AccountCmd,
        AccountEvent, Edit,
    };
    use crate::{pack, Failure, KernelError, Payload, Registry, Upcaster};
    use angzarr_proto::v1 as pb;

    /// The account's router table with no upcaster of its own, recording
    /// the type of every command it is handed.
    pub(crate) struct Seen {
        inner: Box<dyn crate::angzarr_router::router::DynCommandHandler>,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl CommandHandler for Seen {
        fn name(&self) -> &str {
            self.inner.name()
        }
        fn command_types(&self) -> Vec<String> {
            self.inner.command_types()
        }
        async fn dispatch(
            &self,
            req: &crate::angzarr_router::hpb::HandleRequest,
        ) -> Result<crate::angzarr_router::hpb::HandleResponse, CodedError> {
            let sent = req
                .input
                .as_ref()
                .and_then(|book| book.page.as_ref())
                .and_then(|page| match &page.body {
                    Some(pb::command_page::Body::Command(c)) => c.payload.clone(),
                    Some(pb::command_page::Body::Fact(f)) => f.payload.clone(),
                    None => None,
                })
                .map(|payload| payload.type_url)
                .unwrap_or_default();
            self.seen.lock().unwrap().push(sent);
            self.inner.dispatch(req).await
        }
    }

    /// A registry whose account is [`Seen`], upcast through `upcaster`.
    fn seen_account(upcaster: Upcaster) -> (Registry, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let table = crate::route::table::<Account>(Arc::new(account::AccountState::default), false);
        let inner = handlers::<Account>()(table);
        let mut registry = Registry::new();
        registry
            .command_handler(
                "account",
                Seen {
                    inner: Box::new(inner),
                    seen: Arc::clone(&seen),
                },
                crate::toys::descriptor_of::<Account>(
                    crate::v1::ComponentKind::Aggregate,
                    "v2",
                    &[],
                ),
            )
            .unwrap();
        registry.upcast_with("account", upcaster).unwrap();
        (registry, seen)
    }

    fn any(
        store: &MemoryStore,
        registry: &Registry,
        book: crate::v1::CommandBook,
        seconds: i64,
    ) -> Result<crate::Handled, Failure<crate::Rejected>> {
        let log = crate::MemoryDeadLetters::new();
        crate::Kernel::new(registry)
            .log_with(&log)
            .on_retryable(crate::toys::no_retries())
            .handle_any(store, book, &at(seconds))
            .ready()
    }

    #[test]
    fn an_old_command_reaches_the_router_as_the_current_type() {
        let (registry, seen) = seen_account(account_upcaster());
        let store = MemoryStore::new();
        let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
        any(&store, &registry, open, 10).unwrap();

        let old = command(
            cover("account", 1),
            pack(&account::cmd::v0::Credit { euros: 2 }),
        )
        .id(2);
        let handled = any(&store, &registry, old, 11).unwrap();
        assert_eq!(
            payloads::<AccountEvent>(&handled.books()),
            [AccountEvent::Deposited(200)],
            "decided as the current type"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            [
                "/toy.account.cmd.v1.Open".to_string(),
                "/toy.account.cmd.v1.Deposit".to_string()
            ],
            "the router is handed the current type only"
        );
    }

    /// An upcaster sidecar that is down (decision 14.39, C-0681).
    struct Down;

    impl crate::UpcastPort for Down {
        fn upcast<'a>(
            &'a self,
            _: &'a crate::angzarr_router::hpb::UpcastRequest,
        ) -> crate::angzarr_router::send::BoxFuture<
            'a,
            Result<crate::angzarr_router::hpb::UpcastResponse, crate::UpcastError>,
        > {
            Box::pin(std::future::ready(Err(crate::UpcastError::Unavailable {
                cause: coded_diagnostic::Report::new("toy-sidecar-down"),
            })))
        }
    }

    /// A sidecar that cannot be reached delivers nothing that needs it, and
    /// its caller is answered to retry (C-0681).
    #[test]
    fn a_sidecar_that_is_down_is_retried_and_writes_nothing() {
        let (mut registry, seen) = seen_account(Upcaster::new("account-upcaster", "account"));
        registry.upcast_with("account", Down).unwrap();
        let store = MemoryStore::new();
        let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
        let failure = any(&store, &registry, open, 10).err().unwrap();
        assert!(
            matches!(
                &failure,
                Failure::Kernel(KernelError::Upcast {
                    error: crate::UpcastError::Unavailable { .. },
                    ..
                })
            ),
            "{failure:?}"
        );
        assert_eq!(failure.recovery(), crate::Recovery::Retry);
        assert!(seen.lock().unwrap().is_empty(), "nothing delivered");
        assert!(pages(&store, &cover("account", 1)).is_empty());
    }

    #[test]
    fn an_unknown_type_after_upcasting_names_the_type_sent_and_the_type_upcast() {
        let upcaster = Upcaster::new("account-upcaster", "account")
            .rule::<account::cmd::v0::Credit, account::v9::Bonus>(|c| {
                Ok(account::v9::Bonus { cents: c.euros })
            });
        let (registry, seen) = seen_account(upcaster);
        let store = MemoryStore::new();
        let old = command(
            cover("account", 1),
            pack(&account::cmd::v0::Credit { euros: 2 }),
        )
        .id(2);
        let refused = any(&store, &registry, old, 11).err().unwrap();
        let Failure::Kernel(KernelError::Router(error)) = &refused else {
            panic!("the router's refusal: {refused:?}");
        };
        assert_eq!(error.code, "ANGZARR_REASON_UNKNOWN_COMMAND_TYPE");
        let extra = |key: &str| error.extras.get(key).map(String::as_str);
        assert_eq!(
            extra("type_url"),
            Some("/toy.account.cmd.v0.Credit"),
            "as sent"
        );
        assert_eq!(
            extra("upcast_type_url"),
            Some("/toy.account.v9.Bonus"),
            "where the upcaster's chain ended"
        );
        assert_eq!(*seen.lock().unwrap(), ["/toy.account.v9.Bonus".to_string()]);

        // No rule applied: the two are the same.
        let (registry, _) = seen_account(Upcaster::new("account-upcaster", "account"));
        let bonus = command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 })).id(3);
        let refused = any(&store, &registry, bonus, 12).err().unwrap();
        let Failure::Kernel(KernelError::Router(error)) = &refused else {
            panic!("the router's refusal: {refused:?}");
        };
        let extra = |key: &str| error.extras.get(key).map(String::as_str);
        assert_eq!(extra("type_url"), Some("/toy.account.v9.Bonus"));
        assert_eq!(extra("upcast_type_url"), Some("/toy.account.v9.Bonus"));
    }

    /// The account's router table with no upcaster of its own, recording
    /// the type of every stored event it is handed.
    struct History {
        inner: Box<dyn crate::angzarr_router::router::DynCommandHandler>,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl CommandHandler for History {
        fn name(&self) -> &str {
            self.inner.name()
        }
        fn command_types(&self) -> Vec<String> {
            self.inner.command_types()
        }
        async fn dispatch(
            &self,
            req: &crate::angzarr_router::hpb::HandleRequest,
        ) -> Result<crate::angzarr_router::hpb::HandleResponse, CodedError> {
            let types = (req.history.iter())
                .flat_map(|book| &book.pages)
                .filter_map(crate::angzarr_router::page_event)
                .map(|payload| payload.type_url.clone());
            self.seen.lock().unwrap().extend(types);
            self.inner.dispatch(req).await
        }
    }

    /// A registry whose account is [`History`], a handler the kernel did
    /// not build (as one in another process is), upcast through
    /// `upcaster` as its sidecar; beside it the ledger.
    fn handled_account(upcaster: Upcaster) -> (Registry, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let table = crate::route::table::<Account>(Arc::new(account::AccountState::default), false);
        let inner = handlers::<Account>()(table);
        let mut registry = Registry::new();
        registry
            .command_handler(
                "account",
                History {
                    inner: Box::new(inner),
                    seen: Arc::clone(&seen),
                },
                crate::toys::descriptor_of::<Account>(
                    crate::v1::ComponentKind::Aggregate,
                    "v2",
                    &[],
                ),
            )
            .unwrap();
        registry.upcast_with("account", upcaster).unwrap();
        crate::toys::register::<crate::toys::Ledger>(
            &mut registry,
            crate::Upcaster::new("ledger-upcaster", "ledger"),
        )
        .unwrap();
        (registry, seen)
    }

    /// Account 1 as an older build wrote it: opened, then 2 euros credited
    /// under the old event type.
    fn old_history(store: &MemoryStore) {
        use crate::v1::{event_meta::Causation, EventMeta, EventPage};
        let page = |sequence: u64, payload| EventPage {
            body: Some(crate::v1::event_page::Body::Event(payload)),
            meta: Some(EventMeta {
                causation: Some(Causation::Command(crate::v1::CommandCause {
                    command_id: Some(crate::toys::command_id(u128::from(sequence) + 1)),
                })),
                correlation_id: Some(crate::toys::command_id(u128::from(sequence) + 1)),
                recorded_at: Some(crate::toys::instant(sequence as i64)),
                rules_version: "v2".to_string(),
            }),
            sequence,
        };
        let log = [
            page(0, pack(&account::Opened {})),
            page(1, pack(&account::v0::Credited { euros: 2 })),
        ];
        store
            .write(Write::append(sref(&cover("account", 1)), 0, &log))
            .ready()
            .unwrap();
    }

    /// A stream loaded for a command reaches its handler in current shapes,
    /// through the domain's sidecar, whoever built the handler (C-0584).
    #[test]
    fn a_handlers_stored_events_reach_it_as_the_current_types() {
        let (registry, seen) = handled_account(account_upcaster());
        let store = MemoryStore::new();
        old_history(&store);
        let deposit = command(cover("account", 1), AccountCmd::Deposit(1).to_any()).id(3);
        let handled = any(&store, &registry, deposit, 20);
        assert_eq!(
            *seen.lock().unwrap(),
            [
                "/toy.account.v1.Opened".to_string(),
                "/toy.account.v1.Deposited".to_string()
            ],
            "the handler is handed current types only"
        );
        assert_eq!(
            payloads::<AccountEvent>(&handled.unwrap().books()),
            [AccountEvent::Deposited(1)]
        );
    }

    /// A policy reading a stream's stored events is handed them in current
    /// shapes, through the source's sidecar, whoever built the source's
    /// handler (C-0586).
    #[test]
    fn a_policy_reads_a_handlers_stored_events_as_the_current_types() {
        use crate::toys::{ledger, ledger_root, no_retries, Bookkeeping};
        let (mut registry, _) = handled_account(account_upcaster());
        registry.policy(Bookkeeping).unwrap();
        let store = MemoryStore::new();
        old_history(&store);
        let log = crate::MemoryDeadLetters::new();
        crate::Kernel::new(&registry)
            .log_with(&log)
            .on_retryable(no_retries())
            .resume(&store, &at(30))
            .ready()
            .unwrap();
        let ledger = crate::v1::Cover {
            domain: "ledger".to_string(),
            root: Some(ledger_root()),
            edition: None,
        };
        let recorded: Vec<(u64, i64)> = pages(&store, &ledger)
            .iter()
            .map(|page| {
                let payload = crate::angzarr_router::page_event(page).unwrap();
                let recorded =
                    <ledger::Recorded as prost::Message>::decode(&payload.value[..]).unwrap();
                (recorded.source_sequence, recorded.cents)
            })
            .collect();
        assert_eq!(recorded, [(0, 0), (1, 200)], "the credit booked in cents");
    }
}

mod admission {
    //! Admission is the kernel's own (decision 14.42): deduplication and a
    //! process manager's start rule, by the input's type name after
    //! upcasting against the descriptor's `starting_types`.
    use std::sync::Arc;

    use crate::angzarr_router::error::CodedError;
    use crate::angzarr_router::router::CommandHandler;
    use crate::toys::prelude::*;
    use crate::toys::{
        at, command, cover, fact, handlers, payloads, Account, AccountCmd, AccountEvent,
        AccountFact, Edit,
    };
    use crate::v1::DeliveryOutcome;
    use crate::{DeadLetterStore, Failure, Handled, KernelError, Payload, Registry};
    use angzarr_proto::v1 as pb;

    /// The account's table, as a process manager: its descriptor (in
    /// [`registry`]) has it started only by `Open`.
    struct AsProcessManager(Box<dyn crate::angzarr_router::router::DynCommandHandler>);

    impl CommandHandler for AsProcessManager {
        fn name(&self) -> &str {
            self.0.name()
        }
        fn kind(&self) -> pb::ComponentKind {
            pb::ComponentKind::ProcessManager
        }
        fn command_types(&self) -> Vec<String> {
            self.0.command_types()
        }
        async fn dispatch(
            &self,
            req: &crate::angzarr_router::hpb::HandleRequest,
        ) -> Result<crate::angzarr_router::hpb::HandleResponse, CodedError> {
            self.0.dispatch(req).await
        }
    }

    fn registry() -> Registry {
        let table = crate::route::table::<Account>(
            Arc::new(crate::toys::account::AccountState::default),
            true,
        );
        let inner = handlers::<Account>()(table);
        let mut registry = Registry::new();
        registry
            .command_handler(
                "account",
                AsProcessManager(Box::new(inner)),
                crate::toys::descriptor_of::<Account>(
                    pb::ComponentKind::ProcessManager,
                    "v2",
                    &["toy.account.cmd.v1.Open"],
                ),
            )
            .unwrap();
        registry
    }

    fn any(
        store: &MemoryStore,
        registry: &Registry,
        book: crate::v1::CommandBook,
        seconds: i64,
    ) -> Result<Handled, Failure<crate::Rejected>> {
        let log = crate::MemoryDeadLetters::new();
        crate::Kernel::new(registry)
            .log_with(&log)
            .on_retryable(crate::toys::no_retries())
            .handle_any(store, book, &at(seconds))
            .ready()
    }

    #[test]
    fn a_non_starting_command_for_a_pm_with_no_instance_is_refused_by_the_kernel() {
        let registry = registry();
        let store = MemoryStore::new();
        let stream = cover("account", 1);

        let deposit = command(stream.clone(), AccountCmd::Deposit(5).to_any()).id(2);
        let refused = any(&store, &registry, deposit, 10).err().unwrap();
        let Failure::Kernel(KernelError::Router(error)) = &refused else {
            panic!("the framework's key, never a typed refusal: {refused:?}");
        };
        assert_eq!(error.code, "ANGZARR_REASON_PM_NO_INSTANCE");
        assert_eq!(
            error.grpc,
            crate::angzarr_router::error::GrpcCode::FailedPrecondition
        );
        assert_eq!(refused.recovery(), crate::Recovery::Business);
        assert!(pages(&store, &stream).is_empty(), "nothing written");

        // A non-starting fact is dead-lettered (decision 14.69, C-0593),
        // never refused: its sender answered FACT_DEAD_LETTERED, the
        // letter kept.
        let paid = fact(
            stream.clone(),
            AccountFact::PaymentReceived(5).to_any(),
            "pi_1",
        );
        let failure = any(&store, &registry, paid, 11).err().unwrap();
        assert!(
            matches!(
                failure,
                Failure::Kernel(KernelError::FactDeadLettered { .. })
            ),
            "{failure:?}"
        );
        let kept = store.dead_letters(sref(&stream)).ready().unwrap();
        let whys: Vec<_> = kept
            .iter()
            .filter_map(|l| match &l.letter.why {
                Some(crate::v1::angzarr_dead_letter::Why::Reason(r)) => Some(r.code.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(whys, ["ANGZARR_REASON_PM_NO_INSTANCE"]);
        assert!(pages(&store, &stream).is_empty(), "nothing written");

        // A starting type starts it; then any input is dispatched.
        let open = || command(stream.clone(), AccountCmd::Open.to_any()).id(1);
        any(&store, &registry, open(), 12).unwrap();
        let deposit = command(stream.clone(), AccountCmd::Deposit(5).to_any()).id(3);
        let handled = any(&store, &registry, deposit, 13).unwrap();
        assert_eq!(
            payloads::<AccountEvent>(&handled.books()),
            [AccountEvent::Deposited(5)]
        );

        // The kernel deduplicates.
        let retry = any(&store, &registry, open(), 14).unwrap();
        assert_eq!(retry.delivery(), DeliveryOutcome::Duplicate);
    }

    /// A fact's causation is not admission's to read (decision 14.69): a
    /// policy's fact names its trigger in another stream, and is admitted
    /// as any input is, by the start rule alone.
    #[test]
    fn a_reactions_fact_is_admitted_whatever_stream_its_causation_names() {
        use super::super::{admit, Admitted};
        use crate::v1::{reaction_delivery, ReactionDelivery};
        let registry = registry();
        for trigger in [1, 2] {
            let trigger = crate::v1::EventRef {
                stream: Some(cover("account", trigger)),
                sequence: 0,
            };
            let delivery = ReactionDelivery {
                cover: Some(cover("account", 1)),
                body: Some(reaction_delivery::Body::Fact(
                    AccountFact::PaymentReceived(5).to_any(),
                )),
                command_id: crate::ids::policy_command_id(&trigger, "refunds", 0).ok(),
                correlation_id: Some(crate::toys::correlation(1)),
                causation: Some(trigger),
                component: "refunds".to_string(),
            };
            let fact = crate::check::check_reaction(delivery, registry.catalog())
                .unwrap()
                .identify(&at(1));
            let current = Ok(fact.payload().clone());
            assert_eq!(
                admit(&registry, &fact, &current, Vec::new(), true).unwrap(),
                Admitted::Dispatch
            );
        }
    }

    #[test]
    fn a_start_rule_on_a_payload_that_did_not_upcast_is_the_upcasts_failure() {
        use super::super::{admit, Admitted};
        use crate::{check, KernelError, UpcastError};
        let registry = registry();
        let deposit = command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(2);
        let identified = check(deposit, registry.catalog()).unwrap().identify(&at(1));
        let failed: Result<crate::Any, UpcastError> = Err(UpcastError::Rule {
            source: "toy.account.cmd.v1.Deposit".to_string(),
            error: coded_diagnostic::Report::new("toy-broken"),
        });
        let refused = admit(&registry, &identified, &failed, Vec::new(), false)
            .err()
            .unwrap();
        assert!(
            matches!(refused, Failure::Kernel(KernelError::Upcast { .. })),
            "{refused:?}"
        );
        // An instance exists: no start rule, so the type is not read here.
        assert_eq!(
            admit(&registry, &identified, &failed, Vec::new(), true).unwrap(),
            Admitted::Dispatch
        );
        // An aggregate has no start rule.
        let aggregate = crate::toys::registry();
        let identified = check(
            command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(2),
            aggregate.catalog(),
        )
        .unwrap()
        .identify(&at(1));
        assert_eq!(
            admit(&aggregate, &identified, &failed, Vec::new(), false).unwrap(),
            Admitted::Dispatch
        );
    }
}

mod rules_version {
    //! The rules version an answer reports (command_handler.proto,
    //! `CommandHandlerResponse.rules_version`): stamped in full, and refused
    //! as a handler bug when it is not the registered one (C-0558).
    use crate::angzarr_router::error::CodedError;
    use crate::toys::prelude::*;
    use crate::toys::{at, command, cover, Edit};
    use crate::{Failure, Recovery, Registry};
    use angzarr_proto::v1 as pb;

    /// A hand-written handler answering every command with one event,
    /// reporting `reported`, and a state of type `state` if any.
    struct Reporting {
        reported: &'static str,
        state: Option<&'static str>,
    }

    impl crate::angzarr_router::router::CommandHandler for Reporting {
        fn name(&self) -> &str {
            "order"
        }

        fn command_types(&self) -> Vec<String> {
            Vec::new()
        }

        async fn dispatch(
            &self,
            _: &crate::angzarr_router::hpb::HandleRequest,
        ) -> Result<crate::angzarr_router::hpb::HandleResponse, CodedError> {
            Ok(crate::angzarr_router::hpb::HandleResponse {
                events: vec![crate::Any {
                    type_url: "/orders.v1.ItemAdded".to_string(),
                    value: Vec::new(),
                }],
                state: self.state.map(|type_url| crate::Any {
                    type_url: type_url.to_string(),
                    value: Vec::new(),
                }),
                rules_version: self.reported.to_string(),
            })
        }
    }

    /// `order`, registered at "1.4.2+build.7" with its state
    /// `orders.v1.OrderState`, answering `reported`.
    fn order(reported: &'static str) -> Registry {
        stating(reported, None)
    }

    /// [`order`], its answer carrying a state of type `state`.
    fn stating(reported: &'static str, state: Option<&'static str>) -> Registry {
        let mut registry = Registry::new();
        registry
            .command_handler(
                "order",
                Reporting { reported, state },
                pb::ComponentDescriptor {
                    name: "order".to_string(),
                    rules_version: "1.4.2+build.7".to_string(),
                    kind: Some(pb::component_descriptor::Kind::Aggregate(
                        pb::AggregateDescriptor {
                            domain: "order".to_string(),
                            state: Some(pb::StateDescriptor {
                                type_name: "orders.v1.OrderState".to_string(),
                            }),
                        },
                    )),
                },
            )
            .unwrap();
        registry
    }

    fn add_item() -> crate::v1::CommandBook {
        command(
            cover("order", 1),
            crate::Any {
                type_url: "/orders.v1.AddItem".to_string(),
                value: Vec::new(),
            },
        )
        .id(1)
    }

    /// C-0518: the version reported is stamped on every event, in full.
    #[test]
    fn every_event_records_the_reported_version_in_full() {
        let registry = order("1.4.2+build.7");
        let store = MemoryStore::new();
        let log = crate::MemoryDeadLetters::new();
        crate::Kernel::new(&registry)
            .log_with(&log)
            .handle_any(&store, add_item(), &at(10))
            .ready()
            .expect("applied");
        let written = pages(&store, &cover("order", 1));
        assert_eq!(
            written[0].meta.as_ref().map(|m| m.rules_version.as_str()),
            Some("1.4.2+build.7")
        );
    }

    /// C-0558: an answer reporting another version than the registered one
    /// (an empty one included) is a handler bug, refused INTERNAL, and
    /// nothing is persisted; it is dead-lettered, as every handler bug is.
    #[test]
    fn an_answer_reporting_another_version_is_refused_and_persists_nothing() {
        for reported in ["1.5.0+build.9", "1.4.2+build.8", ""] {
            let registry = order(reported);
            let store = MemoryStore::new();
            let log = crate::MemoryDeadLetters::new();
            let failure = crate::Kernel::new(&registry)
                .log_with(&log)
                .handle_any(&store, add_item(), &at(10))
                .ready()
                .err()
                .unwrap_or_else(|| panic!("{reported:?} is refused"));
            assert_eq!(failure.recovery(), Recovery::Bug, "{reported:?}");
            let Failure::Kernel(error) = &failure else {
                panic!("a kernel error: {failure:?}");
            };
            let coded = error.as_coded().expect("a spec key");
            assert_eq!(coded.code, "ANGZARR_REASON_RULES_VERSION_MISMATCH");
            assert_eq!(coded.grpc, crate::angzarr_router::error::GrpcCode::Internal);
            assert_eq!(
                coded.extras.get("registered").map(String::as_str),
                Some("1.4.2+build.7")
            );
            assert_eq!(
                coded.extras.get("reported").map(String::as_str),
                Some(reported)
            );
            assert_eq!(
                coded.extras.get("component").map(String::as_str),
                Some("order")
            );
            assert!(
                pages(&store, &cover("order", 1)).is_empty(),
                "nothing persisted"
            );
            assert_eq!(log.letters().len(), 1, "dead-lettered: {:?}", log.lines());
        }
    }

    /// C-0818: an answer whose state is of another type than the registered
    /// one is a handler bug, refused INTERNAL, nothing persisted and no
    /// snapshot stored; it is dead-lettered. The registered type, under any
    /// type URL prefix, is kept.
    #[test]
    fn an_answer_stating_another_state_type_is_refused_and_persists_nothing() {
        let registry = stating("1.4.2+build.7", Some("/orders.v1.LegacyOrderState"));
        let store = MemoryStore::new();
        let log = crate::MemoryDeadLetters::new();
        let failure = crate::Kernel::new(&registry)
            .log_with(&log)
            .handle_any(&store, add_item(), &at(10))
            .ready()
            .err()
            .expect("refused");
        let Failure::Kernel(error) = &failure else {
            panic!("a kernel error: {failure:?}");
        };
        let coded = error.as_coded().expect("a spec key");
        assert_eq!(coded.code, "ANGZARR_REASON_STATE_TYPE_MISMATCH");
        assert_eq!(coded.grpc, crate::angzarr_router::error::GrpcCode::Internal);
        assert_eq!(
            coded.extras.get("registered").map(String::as_str),
            Some("orders.v1.OrderState")
        );
        assert_eq!(
            coded.extras.get("state_type").map(String::as_str),
            Some("orders.v1.LegacyOrderState")
        );
        assert_eq!(failure.recovery(), Recovery::Bug);
        assert!(pages(&store, &cover("order", 1)).is_empty());
        assert_eq!(
            store
                .read_snapshot(sref(&cover("order", 1)))
                .ready()
                .unwrap(),
            None
        );
        assert_eq!(log.letters().len(), 1, "dead-lettered: {:?}", log.lines());

        let registry = stating(
            "1.4.2+build.7",
            Some("type.googleapis.com/orders.v1.OrderState"),
        );
        let store = MemoryStore::new();
        crate::Kernel::new(&registry)
            .handle_any(&store, add_item(), &at(10))
            .ready()
            .expect("its own state type");
        assert_eq!(pages(&store, &cover("order", 1)).len(), 1);
    }
}

/// Decision 14.107's per-stream forks, as a loaded history shows them:
/// main is always its own; an edition stream is forked once it holds its
/// fork page (an `EditionCreated` meta-event) or a snapshot (which never
/// falls through), and reads as main's, with neither, until then (C-0633).
mod forks {
    use crate::coordinator::History;
    use crate::toys::{cover, cover_in, fork_page};
    use crate::v1::{EventPage, Snapshot};

    fn page(type_url: &str) -> EventPage {
        EventPage {
            body: Some(crate::v1::event_page::Body::Event(prost_types::Any {
                type_url: type_url.to_string(),
                value: Vec::new(),
            })),
            ..EventPage::default()
        }
    }

    #[test]
    fn a_stream_is_forked_by_its_fork_page_or_a_snapshot_and_main_always_is() {
        let edition = cover_in("account", 1, "what-if");
        let main = cover("account", 1);
        let as_mains = History {
            pages: vec![page("/toy.account.v1.Opened")],
            ..History::default()
        };
        assert!(as_mains.forked_in(&main));
        assert!(History::default().forked_in(&main));
        assert!(!as_mains.forked_in(&edition));
        assert!(!History::default().forked_in(&edition));
        let created = History {
            pages: vec![page("/toy.account.v1.Opened"), fork_page(1)],
            ..History::default()
        };
        assert!(created.forked_in(&edition));
        let snapshotted = History {
            snapshot: Some(Snapshot::default()),
            ..History::default()
        };
        assert!(snapshotted.forked_in(&edition));
        let untyped = History {
            pages: vec![EventPage::default()],
            ..History::default()
        };
        assert!(!untyped.forked_in(&edition));
    }
}
