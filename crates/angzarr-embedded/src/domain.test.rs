#[allow(unused_imports)]
use crate::toys::prelude::*;
mod ingest {
    //! Facts: ingested, never decided and never refused, with a read-only
    //! context.
    use crate::toys::prelude::*;
    use crate::v1::CommandBook;
    use crate::{ids, Payload};

    use crate::toys::{
        account, at, command, cover, fact, handle, opened, payloads, registry, Account, AccountCmd,
        AccountEvent, AccountFact,
    };

    fn payment(cents: i64, external_id: &str) -> CommandBook {
        fact(
            cover("account", 1),
            AccountFact::PaymentReceived(cents).to_any(),
            external_id,
        )
    }

    #[test]
    fn ingest_gets_a_read_only_context() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);

        let handled = handle::<Account>(&store, &registry, payment(70, "pi_7"), &at(40)).unwrap();
        assert_eq!(
            payloads::<AccountEvent>(&handled.books()),
            [AccountEvent::Received(account::Received {
                cents: 70,
                external_id: "pi_7".to_string(),
                recorded_seconds: 40,
                rules_version: "v2".to_string(),
                matched: true,
            })],
            "the context's recorded_at, source and rules version, copied by the toy"
        );
        let meta = handled.books()[0].pages[0].meta.clone().unwrap();
        let Some(crate::v1::event_meta::Causation::Fact(cause)) = meta.causation else {
            panic!("a fact's cause: {:?}", meta.causation);
        };
        assert_eq!(cause.command_id, Some(ids::fact_command_id("pi_7")));
        assert_eq!(cause.origin.unwrap().external_id, "pi_7");
    }

    #[test]
    fn a_fact_is_never_refused() {
        let registry = registry();
        let store = MemoryStore::new();

        // The account was never opened: a Deposit command would be refused.
        let handled = handle::<Account>(&store, &registry, payment(70, "pi_8"), &at(40)).unwrap();
        let [AccountEvent::Received(received)] = &payloads::<AccountEvent>(&handled.books())[..]
        else {
            panic!("one event");
        };
        assert!(!received.matched, "recorded as unmatched");
        assert!(
            handle::<Account>(
                &store,
                &registry,
                command(cover("account", 1), AccountCmd::Deposit(70).to_any()),
                &at(41)
            )
            .is_err(),
            "where the command is refused"
        );
    }

    #[test]
    fn a_fact_is_stamped_at_the_head() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);
        let handled = handle::<Account>(&store, &registry, payment(1, "pi_9"), &at(40)).unwrap();
        assert_eq!(handled.books()[0].pages[0].sequence, 2);
    }
}

mod tx {
    //! What a decide reads through its `Tx`: a probe domain records every
    //! accessor into its event, reached as a policy's target so it has a
    //! causation too.
    use crate::toys::prelude::*;

    use std::convert::Infallible;

    use crate::toys::{Toy, ToyCtx};
    use crate::v1::Uuid;
    use crate::{
        ids, pack, Decoder, Domain, Kernel, MemoryDeadLetters, Payload, PayloadError, Policy,
        Registry, State, Upcaster,
    };

    use crate::toys::{at, command, cover, opened, Account, AccountCmd, Edit};

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ProbeState {
        #[prost(uint32, tag = "1")]
        pub bumps: u32,
    }

    impl prost::Name for ProbeState {
        const NAME: &'static str = "ProbeState";
        const PACKAGE: &'static str = "toy.probe.v1";
    }

    /// What decide saw.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Probed {
        #[prost(string, tag = "1")]
        pub rules_version: String,
        #[prost(string, tag = "2")]
        pub domain: String,
        #[prost(int64, tag = "3")]
        pub recorded_seconds: i64,
        #[prost(bytes = "vec", tag = "4")]
        pub command_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "5")]
        pub correlation_id: Vec<u8>,
        #[prost(uint64, optional, tag = "6")]
        pub causation: Option<u64>,
        #[prost(uint64, tag = "7")]
        pub first: u64,
        #[prost(uint64, tag = "8")]
        pub next: u64,
        #[prost(uint64, tag = "9")]
        pub emitted: u64,
        #[prost(uint32, tag = "10")]
        pub bumps_seen: u32,
        #[prost(uint32, tag = "11")]
        pub bumps_committed: u32,
        #[prost(uint64, tag = "13")]
        pub emitted_before: u64,
    }

    impl prost::Name for Probed {
        const NAME: &'static str = "Probed";
        const PACKAGE: &'static str = "toy.probe.v1";
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Bumped {}

    impl prost::Name for Bumped {
        const NAME: &'static str = "Bumped";
        const PACKAGE: &'static str = "toy.probe.v1";
    }

    /// The probe's one command.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Probe {}

    impl prost::Name for Probe {
        const NAME: &'static str = "Probe";
        const PACKAGE: &'static str = "toy.probe.cmd.v1";
    }

    impl State for ProbeState {
        type Anchor = Self;

        fn snapshot(&self) -> Option<Self> {
            Some(self.clone())
        }

        fn restore(&mut self, anchor: Self) -> Result<(), coded_diagnostic::Report> {
            *self = anchor;
            Ok(())
        }
    }

    #[derive(Clone, PartialEq, Debug)]
    pub enum ProbeEvent {
        Bumped,
        Probed(Probed),
    }

    impl Payload for ProbeEvent {
        fn to_any(&self) -> crate::Any {
            match self {
                ProbeEvent::Bumped => pack(&Bumped {}),
                ProbeEvent::Probed(p) => pack(p),
            }
        }

        fn from_any(any: &crate::Any) -> Result<Self, PayloadError> {
            Decoder::new()
                .on::<Bumped>(|_| ProbeEvent::Bumped)
                .on::<Probed>(ProbeEvent::Probed)
                .decode(any)
        }

        fn type_urls() -> Vec<String> {
            vec![
                "/toy.probe.v1.Bumped".to_string(),
                "/toy.probe.v1.Probed".to_string(),
            ]
        }
    }

    impl Payload for Probe {
        fn to_any(&self) -> crate::Any {
            pack(self)
        }

        fn from_any(any: &crate::Any) -> Result<Self, PayloadError> {
            Decoder::new().on::<Probe>(|p| p).decode(any)
        }

        fn type_urls() -> Vec<String> {
            vec!["/toy.probe.cmd.v1.Probe".to_string()]
        }
    }

    pub struct ProbeDomain;

    impl Domain for ProbeDomain {
        type State = ProbeState;
        type Command = Probe;
        type Fact = Infallible;
        type Event = ProbeEvent;
        type Rejection = Infallible;
        const RULES_VERSION: &'static str = "p7";

        const NAME: &'static str = "probe";

        fn apply(s: &mut ProbeState, e: &ProbeEvent, _: &crate::PageContext<'_>) {
            if let ProbeEvent::Bumped = e {
                s.bumps += 1;
            }
        }
    }

    impl Toy for ProbeDomain {
        fn decide(
            _: &Probe,
            state: &ProbeState,
            ctx: &ToyCtx<'_, Self>,
        ) -> Result<Vec<ProbeEvent>, Infallible> {
            let probed = Probed {
                rules_version: ctx.rules_version.to_string(),
                domain: ctx.stream.domain.clone(),
                recorded_seconds: ctx.recorded_at.seconds,
                command_id: ctx.command_id.value.to_vec(),
                correlation_id: ctx.correlation_id.value.to_vec(),
                causation: None,
                first: ctx.head,
                next: ctx.head + 1,
                emitted: 1,
                bumps_seen: state.bumps + 1,
                bumps_committed: state.bumps,
                emitted_before: 0,
            };
            Ok(vec![ProbeEvent::Bumped, ProbeEvent::Probed(probed)])
        }
    }

    fn probe_root() -> Uuid {
        ids::compute_root("probe", "one").unwrap()
    }

    /// Every account event → the probe.
    struct Probing;

    impl Policy for Probing {
        const NAME: &'static str = "probing";
        type Source = Account;
        type Target = ProbeDomain;

        fn on_event(
            &self,
            _: &crate::Any,
            _: &crate::markup::PolicyContext,
        ) -> Result<Option<crate::markup::Commands<ProbeDomain>>, crate::markup::Report> {
            Ok(Some(
                crate::markup::Commands::none().send(probe_root(), &Probe {}),
            ))
        }
    }

    fn registry() -> Registry {
        let mut registry = Registry::new();
        registry
            .register::<Account>(
                Upcaster::new("u", "account"),
                crate::toys::handlers::<crate::toys::Account>(),
            )
            .unwrap();
        registry
            .register::<ProbeDomain>(
                Upcaster::new("u", "probe"),
                crate::toys::handlers::<ProbeDomain>(),
            )
            .unwrap();
        registry
    }

    fn probes(books: &[crate::v1::EventBook]) -> Vec<Probed> {
        let probe: Vec<_> = books
            .iter()
            .filter(|b| b.cover.as_ref().is_some_and(|c| c.domain == "probe"))
            .cloned()
            .collect();
        crate::toys::payloads::<ProbeEvent>(&probe)
            .into_iter()
            .filter_map(|e| match e {
                ProbeEvent::Probed(p) => Some(p),
                ProbeEvent::Bumped => None,
            })
            .collect()
    }

    #[test]
    fn decide_reads_the_rules_version_the_stream_the_instant_and_the_ids() {
        let plain = registry();
        let mut registry = registry();
        registry.policy(Probing).unwrap();
        let log = MemoryDeadLetters::new();
        let kernel = Kernel::new(&registry).log_with(&log);
        let store = MemoryStore::new();
        opened(&store, &plain, 1, 0);
        let book = command(cover("account", 1), AccountCmd::Deposit(5).to_any())
            .id(3)
            .correlated(6);
        let handled = kernel
            .handle::<Account, _>(&store, book, &at(50))
            .ready()
            .unwrap();
        let trigger = crate::v1::EventRef {
            stream: Some(cover("account", 1)),
            sequence: 2,
        };
        assert_eq!(
            probes(&handled.books()),
            [Probed {
                rules_version: "p7".to_string(),
                domain: "probe".to_string(),
                recorded_seconds: 50,
                command_id: ids::policy_command_id(&trigger, "probing", 0)
                    .unwrap()
                    .value
                    .to_vec(),
                correlation_id: crate::toys::correlation(6).value.to_vec(),
                causation: None,
                first: 0,
                next: 1,
                emitted: 1,
                bumps_seen: 1,
                bumps_committed: 0,
                emitted_before: 0,
            }]
        );

        // Again: the probe stream is at 2 now, and decide sees its state.
        let book = command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(4);
        let handled = kernel
            .handle::<Account, _>(&store, book, &at(51))
            .ready()
            .unwrap();
        let probed = &probes(&handled.books())[0];
        assert_eq!((probed.first, probed.next), (2, 3));
        assert_eq!((probed.bumps_committed, probed.bumps_seen), (1, 2));
        assert_eq!(
            probed.correlation_id,
            crate::toys::command_id(4).value.to_vec()
        );
    }

    /// Each probe event → the ledger: the probe emits two per command, so
    /// this sets off two sibling steps, two deep.
    struct ProbeToLedger;

    impl Policy for ProbeToLedger {
        const NAME: &'static str = "probe-to-ledger";
        type Source = ProbeDomain;
        type Target = crate::toys::Ledger;

        fn on_event(
            &self,
            _: &crate::Any,
            ctx: &crate::markup::PolicyContext,
        ) -> Result<Option<crate::markup::Commands<crate::toys::Ledger>>, crate::markup::Report>
        {
            let record = crate::toys::ledger::Record {
                source_sequence: ctx.trigger().sequence,
                cents: 1,
            };
            Ok(Some(
                crate::markup::Commands::none().send(crate::toys::ledger_root(), &record),
            ))
        }
    }

    /// A chain two steps deep, each step committed: within the limit it
    /// settles; one shallower, the step past it fails with the path to it,
    /// and the steps before it stand.
    #[test]
    fn a_two_deep_chain_keeps_the_path_to_the_step_running_and_its_limit() {
        let plain = registry();
        let mut registry = registry();
        registry.policy(Probing).unwrap();
        registry.policy(ProbeToLedger).unwrap();
        registry
            .register::<crate::toys::Ledger>(
                Upcaster::new("u", "ledger"),
                crate::toys::handlers::<crate::toys::Ledger>(),
            )
            .unwrap();
        let log = MemoryDeadLetters::new();
        let store = MemoryStore::new();
        opened(&store, &plain, 1, 0);
        let deposit = || command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(3);

        let kernel = Kernel::new(&registry).log_with(&log).limit(2);
        kernel
            .handle::<Account, _>(&store, deposit(), &at(50))
            .ready()
            .unwrap();
        let probe = crate::v1::Cover {
            domain: "probe".to_string(),
            root: Some(probe_root()),
            edition: None,
        };
        assert_eq!(
            pages(&store, &probe).len(),
            2,
            "the probe step's two events"
        );
        let ledger = crate::v1::Cover {
            domain: "ledger".to_string(),
            root: Some(crate::toys::ledger_root()),
            edition: None,
        };
        assert_eq!(
            pages(&store, &ledger).len(),
            2,
            "then the ledger, once per probe event"
        );

        let store = MemoryStore::new();
        opened(&store, &plain, 1, 0);
        let shallow = Kernel::new(&registry).log_with(&log).limit(1);
        let failure = shallow
            .handle::<Account, _>(&store, deposit(), &at(50))
            .ready()
            .err()
            .unwrap();
        assert!(matches!(
            &failure,
            crate::Failure::Downstream {
                path,
                error: crate::StepError::Kernel(crate::KernelError::ChainTooDeep {
                    limit: 1
                }),
                ..
            } if path.len() == 2
                && path[0].stream.domain == "probe"
                && path[1].stream.domain == "ledger"
                && path[1].cause.sequence == 0
        ));
        assert_eq!(
            streams(&store).len(),
            2,
            "the account's and the probe's commits stand; no ledger step ran"
        );
        assert_eq!(shallow.registry().catalog().domains().count(), 3);
    }
}
