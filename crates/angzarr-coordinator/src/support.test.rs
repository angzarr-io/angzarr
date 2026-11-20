//! Test support: a minimal executor, so no test needs a runtime (every
//! future here completes without waiting on anything outside it).

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Drive `future` to completion on this thread. A future that is still
/// pending after being polled (it waits on something no test provides) is a
/// test bug and panics.
pub fn block_on<T>(future: impl Future<Output = T>) -> T {
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("a future under test is pending: nothing wakes it"),
    }
}

// The gate's fixtures: an application's gate over a token table, an audit
// sink that records everything it is given, and endpoints.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use angzarr_proto::v1::{command_page, CommandBook, CommandPage, Cover, Uuid};

use crate::{
    Allowlist, Arrival, AuditSink, BodyKind, Credential, Denied, Gate, Principal, Reach, ReadOp,
    Served, Unauthenticated,
};

/// The type URL of the fact the table's agents send.
pub const FACT: &str = "type.example/Joined";
/// The type URL of the command the table's agents may send.
pub const COMMAND: &str = "type.example/Deal";

/// The wire Uuid of 16 bytes of `n`.
pub fn uuid(n: u8) -> Uuid {
    Uuid {
        value: vec![n; 16].into(),
    }
}

/// Stream `n` of `domain`.
pub fn cover(domain: &str, n: u8) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(uuid(n)),
        edition: None,
    }
}

/// A command (`kind` Command) or fact (`kind` Fact) of `type_url`, command
/// id `id`, addressed to stream `root` of `domain`. (The payload's `Any` is
/// filled by `Default` and a field write: no direct prost-types.)
pub fn book(domain: &str, root: u8, kind: BodyKind, type_url: &str, id: u8) -> CommandBook {
    let body = match kind {
        BodyKind::Command => {
            let mut command = angzarr_proto::v1::Command::default();
            command
                .payload
                .get_or_insert_with(Default::default)
                .type_url = type_url.to_string();
            command_page::Body::Command(command)
        }
        BodyKind::Fact => {
            let mut fact = angzarr_proto::v1::Fact::default();
            fact.payload.get_or_insert_with(Default::default).type_url = type_url.to_string();
            command_page::Body::Fact(fact)
        }
    };
    CommandBook {
        cover: Some(cover(domain, root)),
        page: Some(CommandPage {
            body: Some(body),
            command_id: Some(uuid(id)),
            correlation_id: None,
        }),
    }
}

/// An identity of the test application: its name and the roots it owns.
#[derive(Clone, Debug)]
pub struct Agent {
    pub name: &'static str,
    pub roots: Vec<u8>,
    pub revoked: Arc<AtomicBool>,
}

struct Entry {
    token: &'static str,
    agent: Agent,
    allowlist: Allowlist,
    framework: bool,
}

/// An application's gate over a token table: it compares with
/// [`Credential::secret_matches`], owns the roots its agent lists, and
/// stamps the agent's first root as the correlation (the "sender", from the
/// identity and never the payload).
pub struct TestGate {
    entries: Vec<Entry>,
    /// How many times `identify` ran.
    pub identified: AtomicUsize,
    /// The agent names `stamp` ran for.
    pub stamps: Mutex<Vec<&'static str>>,
    /// The (domain, root byte) of every `owns` asked.
    pub owns_asked: Mutex<Vec<(String, u8)>>,
    /// A stamp that redirects the command to root 9.
    pub redirect: AtomicBool,
}

impl TestGate {
    /// Three tokens: `tok-fact` (a fact-only agent owning root 1), `tok-both`
    /// (facts and commands, roots 1 and 2, reads events and correlated) and
    /// `tok-fw` (a framework component owning root 1).
    pub fn new() -> TestGate {
        let entry = |token, name, roots: Vec<u8>, allowlist, framework| Entry {
            token,
            agent: Agent {
                name,
                roots,
                revoked: Arc::new(AtomicBool::new(false)),
            },
            allowlist,
            framework,
        };
        TestGate {
            entries: vec![
                entry(
                    "tok-fact",
                    "fact-only",
                    vec![1],
                    Allowlist::none().send(BodyKind::Fact, FACT),
                    false,
                ),
                entry(
                    "tok-both",
                    "both",
                    vec![1, 2],
                    Allowlist::none()
                        .send(BodyKind::Fact, FACT)
                        .send(BodyKind::Command, COMMAND)
                        .read(ReadOp::Events)
                        .read(ReadOp::Correlated),
                    false,
                ),
                entry(
                    "tok-fw",
                    "framework",
                    vec![1],
                    Allowlist::none()
                        .send(BodyKind::Fact, FACT)
                        .send(BodyKind::Command, COMMAND),
                    true,
                ),
            ],
            identified: AtomicUsize::new(0),
            stamps: Mutex::new(Vec::new()),
            owns_asked: Mutex::new(Vec::new()),
            redirect: AtomicBool::new(false),
        }
    }

    /// The principal `token` identifies as, as `identify` builds it.
    pub fn principal(&self, token: &str) -> Principal<Agent> {
        let entry = self
            .entries
            .iter()
            .find(|entry| entry.token == token)
            .expect("a token of the table");
        entry.principal()
    }

    /// Revoke the agent `token` names: `identify` refuses it from now on,
    /// and `still_valid` is false.
    pub fn revoke(&self, token: &str) {
        let entry = self.entries.iter().find(|entry| entry.token == token);
        entry
            .expect("a token of the table")
            .agent
            .revoked
            .store(true, Ordering::SeqCst);
    }
}

impl Entry {
    fn principal(&self) -> Principal<Agent> {
        let principal = Principal::new(self.agent.clone(), self.allowlist.clone());
        #[cfg(feature = "commands")]
        if self.framework {
            return principal.framework();
        }
        let _ = self.framework;
        principal
    }
}

impl Gate for TestGate {
    type Identity = Agent;

    fn identify<'a>(
        &'a self,
        arrival: Arrival,
        credential: Option<&'a Credential>,
    ) -> crate::angzarr_ports::BoxFuture<'a, Result<Principal<Agent>, Unauthenticated>> {
        let _ = arrival;
        self.identified.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let credential = credential.ok_or(Unauthenticated)?;
            let entry = self
                .entries
                .iter()
                .find(|entry| credential.secret_matches(entry.token.as_bytes()))
                .ok_or(Unauthenticated)?;
            if entry.agent.revoked.load(Ordering::SeqCst) {
                return Err(Unauthenticated);
            }
            Ok(entry.principal())
        })
    }

    fn owns(&self, identity: &Agent, cover: &Cover) -> bool {
        let root = cover
            .root
            .as_ref()
            .and_then(|root| root.value.first().copied())
            .unwrap_or(0);
        self.owns_asked
            .lock()
            .unwrap()
            .push((cover.domain.clone(), root));
        identity.roots.contains(&root)
    }

    fn stamp(&self, identity: &Agent, mut book: CommandBook) -> CommandBook {
        self.stamps.lock().unwrap().push(identity.name);
        if let Some(page) = book.page.as_mut() {
            page.correlation_id = Some(uuid(identity.roots[0]));
        }
        if self.redirect.load(Ordering::SeqCst) {
            if let Some(cover) = book.cover.as_mut() {
                cover.root = Some(uuid(9));
            }
        }
        book
    }

    fn still_valid<'a>(&'a self, identity: &'a Agent) -> crate::angzarr_ports::BoxFuture<'a, bool> {
        Box::pin(async move { !identity.revoked.load(Ordering::SeqCst) })
    }
}

/// An audit sink that keeps everything it is given.
#[derive(Default)]
pub struct Sink {
    pub denials: Mutex<Vec<(&'static str, Arrival, Denied)>>,
    pub unauthenticated: Mutex<Vec<Arrival>>,
}

impl Sink {
    /// The reasons recorded, in order.
    pub fn reasons(&self) -> Vec<&'static str> {
        let denials = self.denials.lock().unwrap();
        denials
            .iter()
            .map(|(_, _, denied)| denied.reason())
            .collect()
    }

    /// Everything recorded, as one debug string.
    pub fn text(&self) -> String {
        format!(
            "{:?} {:?}",
            self.denials.lock().unwrap(),
            self.unauthenticated.lock().unwrap()
        )
    }
}

impl AuditSink<Agent> for Sink {
    fn denied(&self, principal: &Agent, arrival: Arrival, denied: &Denied) {
        self.denials
            .lock()
            .unwrap()
            .push((principal.name, arrival, denied.clone()));
    }

    fn unauthenticated(&self, arrival: Arrival) {
        self.unauthenticated.lock().unwrap().push(arrival);
    }
}

/// What serves domains: `table` to loopback, `lobby` to the network.
pub fn served() -> BTreeMap<String, Reach> {
    [
        ("table".to_string(), Reach::Loopback),
        ("lobby".to_string(), Reach::Ip),
    ]
    .into()
}

/// A served-by-map reads half, for the stream checks.
pub struct Table(pub BTreeMap<String, Reach>);

impl Served for Table {
    fn reach(&self, domain: &str) -> Reach {
        self.0.reach(domain)
    }
}

pub fn arc<T>(value: T) -> Arc<T> {
    Arc::new(value)
}

#[cfg(feature = "commands")]
pub use endpoint::{Bare, Endpoint, Failure};

/// The endpoints and their own failure type.
#[cfg(feature = "commands")]
mod endpoint {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    use angzarr_coordinator_base::{Answered, Code, Coded, Refusal};
    use angzarr_ports::BoxFuture;
    use angzarr_proto::v1::{CommandBook, EventBook, ReactionDelivery};

    use super::served;
    use crate::{Caller, Commands, Reach, Served};

    /// An endpoint's own failure: its native unknown-domain, apart from a
    /// coded refusal.
    #[derive(Clone, Debug, PartialEq)]
    pub enum Failure {
        UnknownDomain(String),
        Coded(Coded),
    }

    impl Refusal for Failure {
        fn coded(&self) -> Coded {
            match self {
                Failure::UnknownDomain(domain) => Coded::new(
                    Code::NotFound,
                    "unknown-domain",
                    [("domain".to_string(), domain.clone())],
                ),
                Failure::Coded(coded) => coded.clone(),
            }
        }
    }

    impl From<Coded> for Failure {
        fn from(coded: Coded) -> Failure {
            Failure::Coded(coded)
        }
    }

    /// An endpoint that applies a command id once (the second is a
    /// duplicate), takes `deliver`, and records what it was given.
    pub struct Endpoint {
        pub reach: std::collections::BTreeMap<String, Reach>,
        pub handled: Mutex<Vec<(CommandBook, Caller)>>,
        pub delivered: Mutex<Vec<ReactionDelivery>>,
        seen: Mutex<BTreeSet<Vec<u8>>>,
    }

    impl Endpoint {
        pub fn new() -> Endpoint {
            Endpoint::serving(served())
        }

        pub fn serving(reach: std::collections::BTreeMap<String, Reach>) -> Endpoint {
            Endpoint {
                reach,
                handled: Mutex::new(Vec::new()),
                delivered: Mutex::new(Vec::new()),
                seen: Mutex::new(BTreeSet::new()),
            }
        }
    }

    impl Served for Endpoint {
        fn reach(&self, domain: &str) -> Reach {
            self.reach.reach(domain)
        }
    }

    impl Commands for Endpoint {
        type Failure = Failure;

        fn unknown_domain(&self, domain: &str) -> Failure {
            Failure::UnknownDomain(domain.to_string())
        }

        fn handle<'a>(
            &'a self,
            book: CommandBook,
            caller: Caller,
        ) -> BoxFuture<'a, Result<Answered, Failure>> {
            Box::pin(async move {
                let id = book
                    .page
                    .as_ref()
                    .and_then(|page| page.command_id.as_ref())
                    .map(|id| id.value.to_vec())
                    .unwrap_or_default();
                let events = EventBook {
                    cover: book.cover.clone(),
                    ..EventBook::default()
                };
                self.handled.lock().unwrap().push((book, caller));
                if self.seen.lock().unwrap().insert(id) {
                    Ok(Answered::applied(events))
                } else {
                    Ok(Answered::duplicate(events))
                }
            })
        }

        fn deliver<'a>(
            &'a self,
            delivery: ReactionDelivery,
        ) -> BoxFuture<'a, Result<Answered, Failure>> {
            Box::pin(async move {
                let events = EventBook {
                    cover: delivery.cover.clone(),
                    ..EventBook::default()
                };
                self.delivered.lock().unwrap().push(delivery);
                Ok(Answered::applied(events))
            })
        }
    }

    /// An endpoint that implements `handle` alone: `deliver`, `handle_async`
    /// and `handle_sync` are the defaults.
    pub struct Bare(pub Endpoint);

    impl Served for Bare {
        fn reach(&self, domain: &str) -> Reach {
            self.0.reach(domain)
        }
    }

    impl Commands for Bare {
        type Failure = Failure;

        fn unknown_domain(&self, domain: &str) -> Failure {
            self.0.unknown_domain(domain)
        }

        fn handle<'a>(
            &'a self,
            book: CommandBook,
            caller: Caller,
        ) -> BoxFuture<'a, Result<Answered, Failure>> {
            self.0.handle(book, caller)
        }
    }
}
