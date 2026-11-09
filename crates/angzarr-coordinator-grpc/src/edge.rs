//! [`Edge`]: a gate-checked coordinator, served.

use std::sync::Arc;

use angzarr_coordinator::{Coordinator, Gate};

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
