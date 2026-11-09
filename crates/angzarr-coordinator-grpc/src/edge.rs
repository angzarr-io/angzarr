//! [`Edge`]: a gate-checked coordinator, served.

use std::sync::Arc;

use angzarr_coordinator::{Coordinator, Credential, Gate, Session, Unimplemented};
use tonic::{Request, Status};

use crate::{arrival_of, bearer, status, Arrival};

/// A [`Coordinator`] (commands, reads and gate) as the tonic services of
/// this crate. The arrival of a request is its listener's declaration and
/// the credential is its `authorization` bearer: nothing of either is held
/// here.
pub struct Edge<C, R, G: Gate> {
    pub(crate) coordinator: Arc<Coordinator<C, R, G>>,
}

impl<C, R, G: Gate> Edge<C, R, G> {
    /// `coordinator`, served.
    pub fn new(coordinator: Arc<Coordinator<C, R, G>>) -> Edge<C, R, G> {
        Edge { coordinator }
    }

    /// The caller's session: the gate is asked to identify what the request
    /// presented, on every request, so a revoked credential stops working at
    /// once. A malformed `authorization` header is UNAUTHENTICATED before the
    /// gate is asked; a missing one is the gate's to judge; a credential the
    /// gate refuses is UNAUTHENTICATED.
    pub(crate) async fn session<'a>(
        &'a self,
        presented: &'a Presented,
    ) -> Result<Session<'a, C, R, G>, Status> {
        self.coordinator
            .open(presented.arrival, presented.credential.as_ref())
            .await
            .map_err(|unauthenticated| status(&unauthenticated.coded()))
    }
}

/// What a request presented: where its listener says it arrived from, and
/// its bearer as the gate's credential. Taken before the message is, so the
/// request's metadata is read once and never logged.
pub(crate) struct Presented {
    arrival: Arrival,
    credential: Option<Credential>,
}

impl Presented {
    /// Read `request`'s arrival and bearer.
    pub(crate) fn of<T>(request: &Request<T>) -> Result<Presented, Status> {
        let credential = bearer(request.metadata())?
            .map(|bearer| Credential::bearer(bearer.secret().as_bytes()));
        Ok(Presented {
            arrival: arrival_of(request),
            credential,
        })
    }
}

/// The RPC this crate does not serve, as a typed status.
pub(crate) fn unimplemented(operation: &'static str) -> Status {
    status(&Unimplemented { operation }.coded())
}

impl<C, R, G: Gate> Clone for Edge<C, R, G> {
    fn clone(&self) -> Self {
        Edge {
            coordinator: Arc::clone(&self.coordinator),
        }
    }
}

#[cfg(all(feature = "commands", feature = "reads"))]
impl crate::Server {
    /// Serve `edge` as the command-handler coordinator service and the
    /// event-query service, on every listener.
    pub fn add_edge<C, R, G>(self, edge: Edge<C, R, G>) -> crate::Server
    where
        C: angzarr_coordinator::Commands + angzarr_coordinator::Served + 'static,
        R: crate::Queries + 'static,
        G: Gate + 'static,
    {
        use angzarr_proto_transport_distributed::v1::command_handler_coordinator_service_server::CommandHandlerCoordinatorServiceServer;
        use angzarr_proto_transport_distributed::v1::event_query_service_server::EventQueryServiceServer;
        self.add_service(CommandHandlerCoordinatorServiceServer::new(edge.clone()))
            .add_service(EventQueryServiceServer::new(edge))
    }
}
