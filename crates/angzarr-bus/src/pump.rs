//! The pump: a pull-shaped transport served as the push contract. It asks
//! its feed for a delivery only while a slot is free (the credit's
//! instances), calls the sink with it, and settles it by the call's
//! answer. Nothing waits in a queue: an unsettled delivery is held only by
//! its own call, so the buffer is the credit and never a copy of the store.

use std::future::poll_fn;
use std::task::Poll;

use angzarr_ports::BoxFuture;

use super::transport::{Feed, Flow, Next, Opening, PullTransport, Sink, Transport};

/// A [`PullTransport`] as a [`Transport`]. See the module.
pub struct Pumped<P> {
    pull: P,
}

impl<P> Pumped<P> {
    /// `pull`, pumped.
    pub fn new(pull: P) -> Pumped<P> {
        Pumped { pull }
    }

    /// The pull transport.
    pub fn inner(&self) -> &P {
        &self.pull
    }
}

impl<P: PullTransport> Transport for Pumped<P>
where
    P::Error: Send,
    P::Token: Send,
{
    type Error = P::Error;

    async fn run<'a, K: Sink>(&'a self, opening: Opening<'a>, sink: &'a K) -> Result<(), P::Error> {
        let slots = opening.credit.instances().get();
        let feed = self.pull.open(opening).await?;
        pump(&feed, sink, slots).await
    }
}

/// How many deliveries or idle answers one poll of the pump may take before
/// it yields: a feed that is always ready (a held delivery with no delay,
/// redelivered at once) must not keep the runtime from its other work.
const BURST: usize = 32;

/// A feed's next answer, being awaited.
type Asked<'f, F> = BoxFuture<'f, Result<Next<<F as Feed>::Token>, <F as Feed>::Error>>;

/// Pump `feed` into `sink` with `slots` calls at most.
async fn pump<F, K>(feed: &F, sink: &K, slots: usize) -> Result<(), F::Error>
where
    F: Feed,
    F::Token: Send,
    F::Error: Send,
    K: Sink,
{
    let mut calls: Vec<BoxFuture<'_, Result<(), F::Error>>> = Vec::new();
    let mut next: Option<Asked<'_, F>> = None;
    let mut idle: Option<BoxFuture<'_, Flow>> = None;
    poll_fn(|cx| {
        let mut taken = 0;
        loop {
            taken += 1;
            if taken > BURST {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let mut failed = None;
            calls.retain_mut(|call| match call.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => false,
                Poll::Ready(Err(error)) => {
                    failed = Some(error);
                    false
                }
                Poll::Pending => true,
            });
            if let Some(error) = failed {
                return Poll::Ready(Err(error));
            }
            if let Some(waiting) = idle.as_mut() {
                match waiting.as_mut().poll(cx) {
                    Poll::Ready(Flow::Stop) => return Poll::Ready(Ok(())),
                    Poll::Ready(Flow::Continue) => idle = None,
                    Poll::Pending => {}
                }
            }
            if next.is_none() && calls.len() < slots {
                next = Some(Box::pin(feed.next()));
            }
            let Some(asked) = next.as_mut() else {
                return Poll::Pending;
            };
            match asked.as_mut().poll(cx) {
                Poll::Ready(Ok(Next::Delivery(token, delivery))) => {
                    next = None;
                    calls.push(Box::pin(async move {
                        let settle = sink.deliver(*delivery).await;
                        feed.settle(token, settle).await
                    }));
                }
                Poll::Ready(Ok(Next::Idle)) => {
                    next = None;
                    if idle.is_none() {
                        idle = Some(Box::pin(sink.idle()));
                    }
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
    })
    .await
}
