//! `Subscribe` over a real Unix socket: one stream, caught up from the
//! subscriber's resume point and then followed live, as a subscriber that
//! has only a socket path and a token sees it. Live is pushed by the
//! store's commit signal; a commit the push cannot see (another process's)
//! is found by the change token's poll. Each test names its follow policy,
//! so which of the two delivered a page is never in doubt.

#![cfg(feature = "subscribe")]

mod counter;
mod support;

use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use angzarr_coordinator::{Coordinator, Reach, Served};
use angzarr_coordinator_grpc::{
    error_info, Edge, Follow, Follows, Listener, Queries, Running, ServedReads, Server,
};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{BoxFuture, ChangeToken, CommitWait, StoreError};
use angzarr_proto::v1::{Cover, EventBook, Uuid};
use angzarr_proto_transport_distributed::v1::event_stream_service_client::EventStreamServiceClient;
use angzarr_proto_transport_distributed::v1::{
    subscribe_request, CorrelationFrom, StreamFrom, SubscribeRequest,
};
use angzarr_reads::{Query, ReadError, Reads};
use angzarr_test_dir::TestDir;
use counter::{bearing, commands, counted, cover, Counter, Tokens, DOMAIN, READER, SEVEN, WRITER};
use tonic::transport::Channel;
use tonic::{Code, Status, Streaming};

/// Longer than any test: a policy whose poll never comes round in one.
const NEVER: Duration = Duration::from_secs(3600);
/// How long a test waits for what must come.
const PATIENCE: Duration = Duration::from_secs(10);
/// How long a test watches for what must not come.
const QUIET: Duration = Duration::from_millis(1500);

fn policy(poll: Duration, revalidate_every: u32, buffer: usize) -> Follow {
    Follow {
        poll,
        revalidate_every: NonZeroU32::new(revalidate_every).unwrap(),
        buffer: NonZeroUsize::new(buffer).unwrap(),
    }
}

/// Pushed only: the poll never comes round.
fn pushed() -> Follow {
    policy(NEVER, 1000, 16)
}

/// A read half whose store does not push: its followers poll alone (what a
/// commit from another process looks like to this one).
struct Unpushed<R>(R);

impl<R: Served> Served for Unpushed<R> {
    fn reach(&self, domain: &str) -> Reach {
        self.0.reach(domain)
    }
}

impl<R: Queries> Queries for Unpushed<R> {
    fn events<'a>(&'a self, query: &'a Query) -> BoxFuture<'a, Result<Vec<EventBook>, ReadError>> {
        self.0.events(query)
    }
}

impl<R: Follows> Follows for Unpushed<R> {
    fn next_commit(&self) -> Option<CommitWait> {
        None
    }

    fn change_token(&self) -> BoxFuture<'_, Result<ChangeToken, ReadError>> {
        self.0.change_token()
    }
}

/// A read half whose change token fails once `failing` is set.
struct Failing<R> {
    reads: R,
    failing: Arc<AtomicBool>,
}

impl<R: Served> Served for Failing<R> {
    fn reach(&self, domain: &str) -> Reach {
        self.reads.reach(domain)
    }
}

impl<R: Queries> Queries for Failing<R> {
    fn events<'a>(&'a self, query: &'a Query) -> BoxFuture<'a, Result<Vec<EventBook>, ReadError>> {
        self.reads.events(query)
    }
}

impl<R: Follows> Follows for Failing<R> {
    fn next_commit(&self) -> Option<CommitWait> {
        self.reads.next_commit()
    }

    fn change_token(&self) -> BoxFuture<'_, Result<ChangeToken, ReadError>> {
        if self.failing.load(Ordering::SeqCst) {
            let failure = ReadError::from(StoreError::malformed_stream(cover(1)));
            return Box::pin(async move { Err(failure) });
        }
        self.reads.change_token()
    }
}

/// A served, followable counter: its store, its gate, and a channel to it.
struct Following {
    store: Arc<MemoryStore>,
    tokens: Tokens,
    channel: Channel,
    _running: Running,
    _dir: TestDir,
}

/// Serve the counter on `listener` at `path`, its reads wrapped by `wrap`,
/// followed by `follow`.
async fn serve_as<R, W>(
    dir: TestDir,
    path: PathBuf,
    listener: Listener,
    follow: Follow,
    wrap: W,
) -> Following
where
    R: Follows + 'static,
    W: FnOnce(ServedReads<Arc<MemoryStore>>) -> R,
{
    let store = Arc::new(MemoryStore::new());
    let served: BTreeMap<String, Reach> = [(DOMAIN.to_string(), Reach::Loopback)].into();
    let commands = Counter::new(Arc::clone(&store), served.clone());
    let reads = wrap(ServedReads::new(Reads::new(Arc::clone(&store)), served));
    let tokens = Tokens::default();
    let coordinator =
        Coordinator::new(commands, reads, tokens.clone()).with_audit(Arc::new(tokens.clone()));
    let edge = Edge::new(Arc::new(coordinator)).with_follow(follow);
    let running = Server::builder()
        .add_edge(edge.clone())
        .add_event_stream(edge)
        .listen(listener)
        .bind()
        .await
        .expect("binds");
    let channel = support::uds(&path).await;
    Following {
        store,
        tokens,
        channel,
        _running: running,
        _dir: dir,
    }
}

async fn serve_wrapped<R, W>(name: &str, follow: Follow, wrap: W) -> Following
where
    R: Follows + 'static,
    W: FnOnce(ServedReads<Arc<MemoryStore>>) -> R,
{
    let dir = TestDir::new(name);
    let path = support::socket_in(&dir, "s.sock");
    serve_as(dir, path.clone(), Listener::uds(&path), follow, wrap).await
}

async fn serve(name: &str, follow: Follow) -> Following {
    serve_wrapped(name, follow, |reads| reads).await
}

impl Following {
    /// `n` facts appended to stream `stream`, each its own write.
    async fn write(&self, stream: u8, n: usize, tag: &str) {
        for i in 0..n {
            commands(&self.channel)
                .handle(bearing(counted(stream, &format!("{tag}-{i}")), WRITER))
                .await
                .expect("applied");
        }
    }

    async fn subscribe(
        &self,
        token: &str,
        request: SubscribeRequest,
    ) -> Result<Streaming<EventBook>, Status> {
        subscribe_on(&self.channel, token, request).await
    }
}

async fn subscribe_on(
    channel: &Channel,
    token: &str,
    request: SubscribeRequest,
) -> Result<Streaming<EventBook>, Status> {
    let mut client = EventStreamServiceClient::new(channel.clone());
    Ok(client
        .subscribe(bearing(request, token))
        .await?
        .into_inner())
}

/// A subscription to `stream` from `from`.
fn from(stream: u8, from: u64) -> SubscribeRequest {
    from_cover(Some(cover(stream)), from)
}

fn from_cover(cover: Option<Cover>, from_sequence: u64) -> SubscribeRequest {
    SubscribeRequest {
        target: Some(subscribe_request::Target::Stream(StreamFrom {
            cover,
            from_sequence,
        })),
    }
}

/// The next `count` sequences delivered, each book checked to be the
/// stream's; a test fails if they do not come in time.
async fn sequences(books: &mut Streaming<EventBook>, count: usize) -> Vec<u64> {
    let mut got = Vec::new();
    while got.len() < count {
        let book = tokio::time::timeout(PATIENCE, books.message())
            .await
            .unwrap_or_else(|_| panic!("only {got:?} came in time"))
            .expect("no error")
            .expect("the stream is still open");
        assert_eq!(book.cover.as_ref().map(|c| c.domain.as_str()), Some(DOMAIN));
        assert!(!book.pages.is_empty(), "no empty book is sent");
        got.extend(book.pages.iter().map(|page| page.sequence));
    }
    got
}

/// Nothing is delivered for `QUIET`.
async fn quiet(books: &mut Streaming<EventBook>, what: &str) {
    if let Ok(got) = tokio::time::timeout(QUIET, books.message()).await {
        panic!("{what}: {got:?} came");
    }
}

/// The stream's next item, an error, in time.
async fn ended_with(books: &mut Streaming<EventBook>) -> Status {
    match tokio::time::timeout(PATIENCE, books.message()).await {
        Ok(Err(status)) => status,
        Ok(Ok(got)) => panic!("not ended: {got:?}"),
        Err(_) => panic!("not ended in time"),
    }
}

/// The reason a refusal is keyed by.
fn reason(status: &Status) -> String {
    error_info(status).expect("keyed").reason
}

// ------------------------------------------------- catch-up, then live

/// Why: a subscriber resuming from a position gets every page from it
/// (inclusive), none before it, then each page as it commits, with no gap
/// or repeat at the seam; the push delivers it, since this policy's poll
/// never comes round.
#[tokio::test]
async fn catch_up_from_the_resume_point_then_live_pages_as_they_commit() {
    let served = serve("sub-catch-up", pushed()).await;
    served.write(1, 3, "before").await;
    served.write(2, 1, "elsewhere").await;
    let mut books = served.subscribe(WRITER, from(1, 1)).await.unwrap();
    assert_eq!(sequences(&mut books, 2).await, vec![1, 2]);
    served.write(1, 2, "after").await;
    assert_eq!(sequences(&mut books, 2).await, vec![3, 4]);
    served.write(2, 1, "elsewhere-again").await;
    quiet(&mut books, "another stream's commit").await;
}

/// Why: a subscription that starts before its stream exists waits for it,
/// and its first page comes by push, not after a poll.
#[tokio::test]
async fn a_subscription_to_an_empty_stream_gets_its_first_page_when_it_commits() {
    let served = serve("sub-empty", pushed()).await;
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    quiet(&mut books, "nothing committed").await;
    served.write(1, 1, "first").await;
    assert_eq!(sequences(&mut books, 1).await, vec![0]);
}

/// Why: a resume point past the head is a position still owed: nothing
/// below it comes, the pages from it do.
#[tokio::test]
async fn a_resume_point_past_the_head_waits_for_its_page() {
    let served = serve("sub-ahead", pushed()).await;
    served.write(1, 1, "a").await;
    let mut books = served.subscribe(WRITER, from(1, 3)).await.unwrap();
    served.write(1, 2, "b").await;
    quiet(&mut books, "pages below the resume point").await;
    served.write(1, 1, "c").await;
    assert_eq!(sequences(&mut books, 1).await, vec![3]);
}

/// Why: the race between the catch-up and live is where a gap or a repeat
/// would hide: writes landing all through the subscription's start are
/// delivered once each, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_racing_the_catch_up_are_delivered_once_each_in_order() {
    let served = Arc::new(serve("sub-race", pushed()).await);
    served.write(1, 5, "seed").await;
    let writer = {
        let served = Arc::clone(&served);
        tokio::spawn(async move { served.write(1, 40, "racing").await })
    };
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    let got = sequences(&mut books, 45).await;
    writer.await.unwrap();
    assert_eq!(got, (0..45).collect::<Vec<_>>());
    quiet(&mut books, "a repeat").await;
}

// ------------------------------------------------ the fallback: the poll

/// Why: a commit the push cannot see (another process's) is still
/// delivered: the change token's poll finds it.
#[tokio::test]
async fn a_commit_the_push_cannot_see_is_found_by_the_poll() {
    let served = serve_wrapped(
        "sub-poll",
        policy(Duration::from_millis(50), 1000, 16),
        Unpushed,
    )
    .await;
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    served.write(1, 2, "unpushed").await;
    assert_eq!(sequences(&mut books, 2).await, vec![0, 1]);
    served.write(1, 1, "again").await;
    assert_eq!(sequences(&mut books, 1).await, vec![2]);
}

/// Why: with neither a push nor a poll that comes round, nothing wakes the
/// subscription: what the poll test delivers, the poll delivered.
#[tokio::test]
async fn with_no_push_and_no_poll_nothing_is_delivered_live() {
    let served = serve_wrapped("sub-nothing", pushed(), Unpushed).await;
    served.write(1, 1, "caught-up").await;
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    assert_eq!(
        sequences(&mut books, 1).await,
        vec![0],
        "the catch-up still reads"
    );
    served.write(1, 1, "unseen").await;
    quiet(&mut books, "a commit with nothing to wake on").await;
}

/// Why: the poll is the policy's: the default poll comes round well within
/// a quiet spell, so a test relying on a never-polling policy really is
/// relying on the policy it set.
#[tokio::test]
async fn the_default_policy_polls() {
    let served = serve_wrapped("sub-default", Follow::default(), Unpushed).await;
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    served.write(1, 1, "polled").await;
    assert_eq!(sequences(&mut books, 1).await, vec![0]);
}

// -------------------------------------------- the gate, every few wakes

/// Why: a subscription outlives the request that opened it, so the gate is
/// asked again as it runs: once revoked, the subscriber's stream ends
/// UNAUTHENTICATED.
#[tokio::test]
async fn a_revoked_subscriber_is_cut_unauthenticated() {
    let served = serve("sub-revoked", policy(Duration::from_millis(20), 2, 16)).await;
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    served.write(1, 1, "seen").await;
    assert_eq!(sequences(&mut books, 1).await, vec![0]);
    served.tokens.revoke("writer");
    assert_eq!(ended_with(&mut books).await.code(), Code::Unauthenticated);
    let ended = tokio::time::timeout(PATIENCE, books.message()).await;
    assert!(
        matches!(ended, Ok(Ok(None))),
        "and nothing after: {ended:?}"
    );
}

/// Why: the gate is asked every few wakes, not on every one: with a
/// policy of every thousandth wake, a revocation is not noticed in a quiet
/// spell of a few dozen.
#[tokio::test]
async fn the_gate_is_asked_only_every_few_wakes() {
    let served = serve("sub-few-wakes", policy(Duration::from_millis(20), 1000, 16)).await;
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    served.tokens.revoke("writer");
    quiet(&mut books, "a revocation before its wake").await;
}

/// Why: a revoked caller cannot open a subscription at all.
#[tokio::test]
async fn a_revoked_caller_cannot_subscribe() {
    let served = serve("sub-revoked-first", pushed()).await;
    served.tokens.revoke("writer");
    let status = served.subscribe(WRITER, from(1, 0)).await.unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
}

/// Why: a credential the gate does not know is refused at the call.
#[tokio::test]
async fn an_unknown_credential_cannot_subscribe() {
    let served = serve("sub-unknown", pushed()).await;
    let status = served.subscribe("nobody", from(1, 0)).await.unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
}

/// Why: a stream the caller may not follow (no follow read allowed, or a
/// stream it does not own) is indistinguishable from an absent stream that
/// never grows: the call is accepted and nothing is delivered, and the
/// real reason is the audit's alone.
#[tokio::test]
async fn a_stream_the_caller_may_not_follow_answers_as_an_absent_one() {
    let served = serve("sub-denied", policy(Duration::from_millis(20), 1, 16)).await;
    served.write(1, 2, "hidden").await;
    let mut unfollowable = served.subscribe(READER, from(1, 0)).await.unwrap();
    let mut unowned = served.subscribe(SEVEN, from(1, 0)).await.unwrap();
    served.write(1, 1, "still-hidden").await;
    quiet(&mut unfollowable, "a reader with no follow read").await;
    quiet(&mut unowned, "a stream not owned").await;
    let denied = served.tokens.denied();
    assert!(
        denied.iter().any(|d| d.contains("SelectionNotAllowed")),
        "{denied:?}"
    );
    assert!(denied.iter().any(|d| d.contains("NotOwner")), "{denied:?}");
    // Its own stream it follows.
    let mut owned = served.subscribe(SEVEN, from(7, 0)).await.unwrap();
    commands(&served.channel)
        .handle(bearing(counted(7, "owned"), SEVEN))
        .await
        .unwrap();
    assert_eq!(sequences(&mut owned, 1).await, vec![0]);
}

/// Why: an unfollowable subscription is still the gate's: revoked, it ends
/// as any other does.
#[tokio::test]
async fn an_unfollowable_subscription_is_still_cut_when_revoked() {
    let served = serve(
        "sub-denied-revoked",
        policy(Duration::from_millis(20), 1, 16),
    )
    .await;
    let mut books = served.subscribe(SEVEN, from(1, 0)).await.unwrap();
    served.tokens.revoke("seven");
    assert_eq!(ended_with(&mut books).await.code(), Code::Unauthenticated);
}

// ------------------------------------------------------------ refusals

/// Why: what the spec refuses is refused at the call, keyed as the reads
/// engine keys it: no target, a cover with no domain, no root or a root
/// that is not 16 bytes, an empty edition name; an unknown edition is
/// NOT_FOUND; a correlation subscription is not served.
#[tokio::test]
async fn malformed_subscriptions_are_refused_at_the_call() {
    let served = serve("sub-refused", pushed()).await;
    let refused = |request| served.subscribe(WRITER, request);

    let none = refused(SubscribeRequest { target: None })
        .await
        .unwrap_err();
    assert_eq!(none.code(), Code::InvalidArgument);
    assert_eq!(reason(&none), "angzarr-reads-no-target");

    let no_cover = refused(from_cover(None, 0)).await.unwrap_err();
    assert_eq!(reason(&no_cover), "angzarr-reads-no-domain");

    let no_domain = Cover {
        domain: String::new(),
        ..cover(1)
    };
    let status = refused(from_cover(Some(no_domain), 0)).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(reason(&status), "angzarr-reads-no-domain");

    let no_root = Cover {
        root: None,
        ..cover(1)
    };
    let status = refused(from_cover(Some(no_root), 0)).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(reason(&status), "angzarr-reads-bad-root");

    let short_root = Cover {
        root: Some(Uuid {
            value: vec![1; 3].into(),
        }),
        ..cover(1)
    };
    let status = refused(from_cover(Some(short_root), 0)).await.unwrap_err();
    assert_eq!(reason(&status), "angzarr-reads-bad-root");

    let empty_edition = Cover {
        edition: Some(String::new()),
        ..cover(1)
    };
    let status = refused(from_cover(Some(empty_edition), 0))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(reason(&status), "angzarr-reads-empty-edition");

    let unknown_edition = Cover {
        edition: Some("nope".to_string()),
        ..cover(1)
    };
    let status = refused(from_cover(Some(unknown_edition), 0))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);

    let correlation = SubscribeRequest {
        target: Some(subscribe_request::Target::Correlation(
            CorrelationFrom::default(),
        )),
    };
    let status = refused(correlation).await.unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented);
}

// --------------------------------------- cut, resumed; slow; gone; failed

/// Why: a connection closed under a subscription (the listener's age cap)
/// ends it; the subscriber resumes from the last sequence it received + 1
/// on a new connection, and loses nothing and repeats nothing.
#[tokio::test]
async fn a_subscription_cut_by_the_connection_age_resumes_from_its_position() {
    let dir = TestDir::new("sub-cut");
    let path = support::socket_in(&dir, "s.sock");
    let listener = Listener::uds(&path)
        .max_connection_age(Duration::from_millis(300))
        .max_connection_age_grace(Duration::from_millis(100));
    let served = serve_as(dir, path.clone(), listener, pushed(), |reads| reads).await;
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    served.write(1, 2, "before-the-cut").await;
    let mut got = sequences(&mut books, 2).await;
    let cut = tokio::time::timeout(PATIENCE, async {
        while let Ok(Some(book)) = books.message().await {
            got.extend(book.pages.iter().map(|page| page.sequence));
        }
    })
    .await;
    assert!(cut.is_ok(), "the connection's age cuts the stream");
    let resume = got.last().unwrap() + 1;
    let channel = support::uds(&path).await;
    let mut resumed = subscribe_on(&channel, WRITER, from(1, resume))
        .await
        .unwrap();
    commands(&channel)
        .handle(bearing(counted(1, "after-the-cut"), WRITER))
        .await
        .unwrap();
    let missing = (3 - resume) as usize;
    got.extend(sequences(&mut resumed, missing).await);
    assert_eq!(got, vec![0, 1, 2]);
}

/// Why: a subscriber that does not read is held to the policy's buffer,
/// and loses nothing: the subscription reads no further until it takes a
/// book, and then every page comes, in order.
#[tokio::test]
async fn a_slow_subscriber_loses_nothing() {
    let served = serve("sub-slow", policy(NEVER, 1000, 1)).await;
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    served.write(1, 30, "unread").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(sequences(&mut books, 30).await, (0..30).collect::<Vec<_>>());
}

/// Why: a subscriber that leaves ends its subscription, and nothing of it
/// is left holding the store.
#[tokio::test]
async fn a_subscriber_that_leaves_ends_its_subscription() {
    let served = serve("sub-gone", pushed()).await;
    let baseline = Arc::strong_count(&served.store);
    let books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    drop(books);
    let released = tokio::time::timeout(PATIENCE, async {
        while Arc::strong_count(&served.store) > baseline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(released.is_ok(), "the subscription let go of the store");
}

/// Why: a read that fails mid-stream is told as the stream's last item;
/// the subscriber resumes from its position on a new subscription.
#[tokio::test]
async fn a_read_that_fails_live_ends_the_stream_with_its_status() {
    let failing = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&failing);
    let served = serve_wrapped("sub-failed", pushed(), move |reads| Failing {
        reads,
        failing: flag,
    })
    .await;
    let mut books = served.subscribe(WRITER, from(1, 0)).await.unwrap();
    served.write(1, 1, "read").await;
    assert_eq!(sequences(&mut books, 1).await, vec![0]);
    failing.store(true, Ordering::SeqCst);
    served.write(1, 1, "unread").await;
    let status = ended_with(&mut books).await;
    assert_ne!(status.code(), Code::Ok);
    let ended = tokio::time::timeout(PATIENCE, books.message()).await;
    assert!(
        matches!(ended, Ok(Ok(None))),
        "and nothing after: {ended:?}"
    );
}

/// Why: a read that fails at the start is the call's refusal.
#[tokio::test]
async fn a_read_that_fails_at_the_start_is_refused_at_the_call() {
    let served = serve_wrapped("sub-failed-first", pushed(), |reads| Failing {
        reads,
        failing: Arc::new(AtomicBool::new(true)),
    })
    .await;
    let status = served.subscribe(WRITER, from(1, 0)).await.unwrap_err();
    assert_ne!(status.code(), Code::Ok);
}
