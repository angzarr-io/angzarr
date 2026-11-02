//! A tonic client and server stub for every service of the distributed
//! transport (decision 14.64). Each row below names a service's generated
//! server trait and its methods, so a renamed RPC fails the build; the test
//! then checks the rows against the package's descriptor set, so a service
//! or method the spec adds fails the test.

use std::collections::BTreeMap;

use angzarr_proto_transport_distributed::v1;
use prost::Message;
use tonic::server::NamedService;

/// One row per service: `Trait, its server and client modules, the server
/// and client types { rust_method => "SpecMethod", ... }`. Each expands to
/// a check that the server trait has the methods and the client type
/// exists, and yields the server's spec name with its methods' spec names.
macro_rules! services {
    ($($svc:ident, $server:ident::$server_ty:ident, $client:ident::$client_ty:ident {
        $($method:ident => $rpc:literal),+ $(,)?
    })+) => {{
        let mut rows = BTreeMap::<String, Vec<&'static str>>::new();
        $({
            #[allow(dead_code)]
            fn named<T: v1::$server::$svc>() {
                $(let _ = T::$method;)+
            }
            let _ = std::any::type_name::<v1::$client::$client_ty<()>>();
            let name = <v1::$server::$server_ty<()> as NamedService>::NAME;
            rows.insert(name.to_string(), vec![$($rpc),+]);
        })+
        rows
    }};
}

#[test]
fn every_distributed_service_has_a_generated_client_and_server() {
    let rows = services! {
        CommandHandlerCoordinatorService, command_handler_coordinator_service_server::CommandHandlerCoordinatorServiceServer, command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient {
            handle => "Handle", handle_async => "HandleAsync", handle_sync => "HandleSync",
        }
        PolicyCoordinatorService, policy_coordinator_service_server::PolicyCoordinatorServiceServer, policy_coordinator_service_client::PolicyCoordinatorServiceClient {
            execute => "Execute",
        }
        ProjectorCoordinatorService, projector_coordinator_service_server::ProjectorCoordinatorServiceServer, projector_coordinator_service_client::ProjectorCoordinatorServiceClient {
            handle_sync => "HandleSync", handle_async => "HandleAsync",
        }
        EventQueryService, event_query_service_server::EventQueryServiceServer, event_query_service_client::EventQueryServiceClient {
            get_event_book => "GetEventBook", get_events => "GetEvents",
            get_correlated_events => "GetCorrelatedEvents", get_aggregate_roots => "GetAggregateRoots",
        }
        EventStreamService, event_stream_service_server::EventStreamServiceServer, event_stream_service_client::EventStreamServiceClient {
            subscribe => "Subscribe",
        }
        EditionAdminService, edition_admin_service_server::EditionAdminServiceServer, edition_admin_service_client::EditionAdminServiceClient {
            fork_stream => "ForkStream", list_editions => "ListEditions", delete_edition => "DeleteEdition",
        }
        DlqAdminService, dlq_admin_service_server::DlqAdminServiceServer, dlq_admin_service_client::DlqAdminServiceClient {
            list_dead_letters => "ListDeadLetters", get_dead_letter => "GetDeadLetter",
            delete_dead_letter => "DeleteDeadLetter", replay_dead_letter => "ReplayDeadLetter",
        }
    };

    let set = prost_types::FileDescriptorSet::decode(
        angzarr_proto_transport_distributed::FILE_DESCRIPTOR_SET,
    )
    .unwrap();
    let mut spec = BTreeMap::<String, Vec<&str>>::new();
    for file in set
        .file
        .iter()
        .filter(|f| f.package().starts_with("io.angzarr"))
    {
        for service in &file.service {
            let methods = service.method.iter().map(|m| m.name()).collect();
            spec.insert(format!("{}.{}", file.package(), service.name()), methods);
        }
    }
    assert_eq!(rows, spec);
}

#[test]
fn a_service_is_reached_under_the_distributed_package() {
    assert_eq!(
        <v1::command_handler_coordinator_service_server::CommandHandlerCoordinatorServiceServer<()> as NamedService>::NAME,
        "io.angzarr.transport.distributed.v1.CommandHandlerCoordinatorService"
    );
    assert_eq!(
        <v1::dlq_admin_service_server::DlqAdminServiceServer<()> as NamedService>::NAME,
        "io.angzarr.transport.distributed.v1.DlqAdminService"
    );
}

#[test]
fn the_transport_imports_core_and_sererr_only() {
    let set = prost_types::FileDescriptorSet::decode(
        angzarr_proto_transport_distributed::FILE_DESCRIPTOR_SET,
    )
    .unwrap();
    for file in set
        .file
        .iter()
        .filter(|f| f.package() == "io.angzarr.transport.distributed.v1")
    {
        for import in &file.dependency {
            assert!(
                import.starts_with("io/angzarr/v1/")
                    || import.starts_with("io/angzarr/transport/distributed/v1/")
                    || import.starts_with("google/")
                    || import == "sererr/v1/sererr.proto",
                "{} imports {import}",
                file.name()
            );
        }
    }
}
