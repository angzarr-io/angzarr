//! `EventStreamService`: `Subscribe` to one stream (`StreamFrom`) through
//! the gate as a follow read: catch-up through the GetEvents read path from
//! the resume point, then live, woken by the store's commit push and, as
//! the fallback for commits the push cannot see, its change token polled.
//! `CorrelationFrom` is UNIMPLEMENTED. The crate's header has the behaviour.

use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::slice::from_ref;
use std::sync::Arc;
use std::time::Duration;

use angzarr_coordinator::{Coordinator, Gate, ReadOp, Session, Unauthenticated};
use angzarr_ports::{
    BoxFuture, ChangeToken, CommitNotify, CommitWait, MaybeSend, MaybeSync, Watch,
};
use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::{Cover, EventBook};
use angzarr_proto_transport_distributed::v1::event_stream_service_server::EventStreamService;
use angzarr_proto_transport_distributed::v1::{subscribe_request, StreamFrom, SubscribeRequest};
use angzarr_reads::{Invalid, Query, ReadError, ReadPorts, Select};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::edge::{unimplemented, Presented};
use crate::{status_of, Edge, Queries, ServedReads};

/// A read half that can be followed: the reads, the store's commit push
/// (when it has one) and its change token.
pub trait Follows: Queries {
    /// A wait armed now, resolved by the store's next commit in this
    /// process; `None` for a store that does not push (its followers poll
    /// the change token alone).
    fn next_commit(&self) -> Option<CommitWait>;

    /// The store's change token: what a follower polls to see a commit the
    /// push cannot (another process's).
    fn change_token(&self) -> BoxFuture<'_, Result<ChangeToken, ReadError>>;
}

impl<S> Follows for ServedReads<S>
where
    S: ReadPorts + Watch + CommitNotify + MaybeSend + MaybeSync,
{
    fn next_commit(&self) -> Option<CommitWait> {
        Some(self.reads.store().next_commit())
    }

    fn change_token(&self) -> BoxFuture<'_, Result<ChangeToken, ReadError>> {
        Box::pin(async move { Ok(Watch::change_token(self.reads.store()).await?) })
    }
}

/// How a subscription follows its stream once caught up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Follow {
    /// How often the store's change token is polled: the latency of a
    /// commit the push cannot see.
    pub poll: Duration,
    /// How many wakes (a push or a poll) between two of the gate's
    /// `still_valid` checks.
    pub revalidate_every: NonZeroU32,
    /// How many books the server holds for one subscriber; when they are
    /// all unsent, the subscription reads no further until one is taken.
    pub buffer: NonZeroUsize,
}

impl Follow {
    /// A poll every half second, the gate asked every fourth wake (at
    /// least every two seconds), sixteen books held.
    pub const DEFAULT: Follow = Follow {
        poll: Duration::from_millis(500),
        revalidate_every: NonZeroU32::new(4).expect("non-zero"),
        buffer: NonZeroUsize::new(16).expect("non-zero"),
    };
}

impl Default for Follow {
    fn default() -> Follow {
        Follow::DEFAULT
    }
}

type Books =
    Pin<Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<EventBook, Status>> + Send>>;

type Sender = mpsc::Sender<Result<EventBook, Status>>;

/// A `StreamFrom`'s cover, validated as the reads engine validates a
/// stream's (before any store call): a domain, a root of 16 bytes, an
/// edition that is unset or named.
pub(crate) fn followed_cover(cover: Option<Cover>) -> Result<Cover, Invalid> {
    let _ = (cover, Uuid::nil(), Invalid::NoDomain);
    todo!("resolute-sleet: the followed cover")
}

/// One subscription's position: the stream, and the next sequence it owes
/// the subscriber (the watermark: what is below it was delivered).
#[derive(Debug, PartialEq)]
pub(crate) struct Position {
    pub(crate) cover: Cover,
    pub(crate) next: u64,
}

impl Position {
    /// The read from the watermark: every page at or above it.
    pub(crate) fn query(&self) -> Query {
        let _ = (&self.cover, Select::All);
        todo!("resolute-sleet: the read from the watermark")
    }

    /// `book` as the subscriber is owed it: its pages below the watermark
    /// dropped (delivered already), none when nothing is left; the
    /// watermark moved past its last page.
    pub(crate) fn fresh(&mut self, mut book: EventBook) -> Option<EventBook> {
        let _ = (&mut self.next, &mut book);
        todo!("resolute-sleet: the watermark")
    }
}

/// What woke a waiting subscription.
#[derive(Debug, PartialEq)]
enum Woke {
    /// The store pushed a commit.
    Pushed,
    /// The poll came round.
    Polled,
    /// The subscriber is gone.
    Gone,
}

/// The first of: the subscriber leaving, the push, the poll.
async fn woken(push: Option<&mut CommitWait>, tx: &Sender, poll: Duration) -> Woke {
    let _ = (push, tx, poll, Woke::Pushed, Woke::Polled);
    todo!("resolute-sleet: what wakes a subscription")
}

/// A subscription that is followed: its position, and what it read last
/// (the push armed and the change token taken before that read).
struct Follower {
    position: Position,
    push: Option<CommitWait>,
    token: ChangeToken,
}

impl Follower {
    /// Arm the push and take the token, then read from the watermark: a
    /// commit after the arming is either in this read or resolves the push
    /// (and changes the token), so none is missed.
    async fn read<R: Follows>(
        reads: &R,
        mut position: Position,
    ) -> Result<(Follower, Vec<EventBook>), ReadError> {
        let push = reads.next_commit();
        let token = reads.change_token().await?;
        let books = reads.events(&position.query()).await?;
        let books = books
            .into_iter()
            .filter_map(|book| position.fresh(book))
            .collect();
        Ok((
            Follower {
                position,
                push,
                token,
            },
            books,
        ))
    }
}

/// What a subscription serves: its follower, or nothing for a stream the
/// caller may not follow (an absent stream that never grows).
type Followed = Option<Follower>;

/// Send `books`, in order; false once the subscriber is gone.
async fn send(tx: &Sender, books: Vec<EventBook>) -> bool {
    for book in books {
        if tx.send(Ok(book)).await.is_err() {
            return false;
        }
    }
    true
}

/// Wait until there is something new to read (true), or the subscription
/// ends (false: the subscriber left, or was revoked and told so). Every
/// `follow.revalidate_every`th wake asks the gate again.
async fn wait_for_news<C, R: Follows, G: Gate>(
    session: &Session<'_, C, R, G>,
    reads: &R,
    followed: &mut Followed,
    wakes: &mut u32,
    follow: Follow,
    tx: &Sender,
) -> Result<bool, ReadError> {
    loop {
        let push = followed.as_mut().and_then(|f| f.push.as_mut());
        let woke = woken(push, tx, follow.poll).await;
        if woke == Woke::Gone {
            return Ok(false);
        }
        *wakes = wakes.wrapping_add(1);
        if wakes.is_multiple_of(follow.revalidate_every.get()) && !session.still_valid().await {
            let _ = tx.send(Err(status_of(&Unauthenticated.coded()))).await;
            return Ok(false);
        }
        let Some(follower) = followed.as_ref() else {
            continue;
        };
        if woke == Woke::Pushed || reads.change_token().await? != follower.token {
            return Ok(true);
        }
    }
}

/// The live half: send what the catch-up read, then wait, read again and
/// send, until the subscriber leaves, is revoked, or a read fails (sent as
/// the stream's last item; the subscriber resumes from its position).
async fn serve<C, R: Follows, G: Gate>(
    session: Session<'_, C, R, G>,
    reads: &R,
    mut followed: Followed,
    caught_up: Vec<EventBook>,
    follow: Follow,
    tx: Sender,
) {
    let mut wakes = 0;
    let mut books = caught_up;
    loop {
        if !send(&tx, books).await {
            return;
        }
        let ended =
            match wait_for_news(&session, reads, &mut followed, &mut wakes, follow, &tx).await {
                Ok(news) => !news,
                Err(failure) => {
                    let _ = tx.send(Err(status_of(&failure))).await;
                    return;
                }
            };
        if ended {
            return;
        }
        let Some(follower) = followed.take() else {
            return;
        };
        match Follower::read(reads, follower.position).await {
            Ok((next, read)) => {
                followed = Some(next);
                books = read;
            }
            Err(failure) => {
                let _ = tx.send(Err(status_of(&failure))).await;
                return;
            }
        }
    }
}

/// The served half of a subscription, on a task of its own.
fn spawn_serve<C, R, G>(
    coordinator: Arc<Coordinator<C, R, G>>,
    arrival: angzarr_coordinator::Arrival,
    principal: angzarr_coordinator::Principal<G::Identity>,
    followed: Followed,
    caught_up: Vec<EventBook>,
    follow: Follow,
    tx: Sender,
) where
    C: Send + Sync + 'static,
    R: Follows + 'static,
    G: Gate + 'static,
    G::Identity: 'static,
{
    tokio::spawn(async move {
        let session = coordinator.open_as(arrival, principal);
        serve(
            session,
            coordinator.reads(),
            followed,
            caught_up,
            follow,
            tx,
        )
        .await;
    });
}

#[tonic::async_trait]
impl<C, R, G> EventStreamService for Edge<C, R, G>
where
    C: Send + Sync + 'static,
    R: Follows + 'static,
    G: Gate + 'static,
    G::Identity: 'static,
{
    type SubscribeStream = Books;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Books>, Status> {
        let presented = Presented::of(&request)?;
        let session = self.session(&presented).await?;
        if !session.still_valid().await {
            return Err(status_of(&Unauthenticated.coded()));
        }
        let StreamFrom {
            cover,
            from_sequence,
        } = match request.into_inner().target {
            Some(subscribe_request::Target::Stream(from)) => from,
            Some(subscribe_request::Target::Correlation(_)) => {
                return Err(unimplemented("subscribe_correlation"))
            }
            None => return Err(status_of(&ReadError::from(Invalid::NoTarget))),
        };
        let cover =
            followed_cover(cover).map_err(|invalid| status_of(&ReadError::from(invalid)))?;
        let position = Position {
            cover,
            next: from_sequence,
        };
        // The first catch-up is read here, so what refuses it (an unknown
        // edition) is the call's status, not the stream's.
        let (followed, caught_up) =
            match session.authorize_streams(ReadOp::Follow, from_ref(&position.cover)) {
                Ok(()) => {
                    let (follower, books) = Follower::read(self.coordinator.reads(), position)
                        .await
                        .map_err(|failure| status_of(&failure))?;
                    (Some(follower), books)
                }
                Err(_denied) => (None, Vec::new()),
            };
        let (tx, rx) = mpsc::channel(self.follow.buffer.get());
        spawn_serve(
            Arc::clone(&self.coordinator),
            presented.arrival,
            session.into_principal(),
            followed,
            caught_up,
            self.follow,
            tx,
        );
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

#[cfg(test)]
#[path = "subscribe.test.rs"]
mod tests;
