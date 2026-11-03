//! A tonic client and server stub for every service of the component
//! contract (decision 14.64). Each row below names a service's generated
//! server trait and its methods, so a renamed RPC fails the build; the test
//! then checks the rows against the package's descriptor set, so a service
//! or method the spec adds fails the test.

use std::collections::BTreeMap;

use angzarr_proto_component::v1;
use prost::{Message, Name};
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
fn every_component_service_has_a_generated_client_and_server() {
    let rows = services! {
        CommandHandlerService, command_handler_service_server::CommandHandlerServiceServer, command_handler_service_client::CommandHandlerServiceClient {
            handle => "Handle",
        }
        ComponentService, component_service_server::ComponentServiceServer, component_service_client::ComponentServiceClient {
            describe => "Describe",
        }
        PolicyService, policy_service_server::PolicyServiceServer, policy_service_client::PolicyServiceClient {
            react => "React", translate => "Translate",
        }
        ProjectorService, projector_service_server::ProjectorServiceServer, projector_service_client::ProjectorServiceClient {
            project => "Project",
        }
        UpcasterService, upcaster_service_server::UpcasterServiceServer, upcaster_service_client::UpcasterServiceClient {
            upcast => "Upcast",
        }
    };

    let set = prost_types::FileDescriptorSet::decode(angzarr_proto_component::FILE_DESCRIPTOR_SET)
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
fn a_service_is_reached_under_the_component_package() {
    assert_eq!(
        <v1::command_handler_service_server::CommandHandlerServiceServer<()> as NamedService>::NAME,
        "io.angzarr.component.v1.CommandHandlerService"
    );
    assert_eq!(
        <v1::component_service_server::ComponentServiceServer<()> as NamedService>::NAME,
        "io.angzarr.component.v1.ComponentService"
    );
}

#[test]
fn describe_takes_the_packages_empty_request_and_core_answers() {
    assert_eq!(
        v1::DescribeRequest::type_url(),
        "/io.angzarr.component.v1.DescribeRequest"
    );
    // An empty message encodes as nothing.
    assert!(v1::DescribeRequest::default().encode_to_vec().is_empty());
}

#[test]
fn the_rpcs_take_and_answer_the_handler_layers_messages() {
    // Decision 14.78: the services are retyped onto the handler layer;
    // Describe alone answers core's descriptor.
    let set = prost_types::FileDescriptorSet::decode(angzarr_proto_component::FILE_DESCRIPTOR_SET)
        .unwrap();
    let mut found = BTreeMap::new();
    for file in set
        .file
        .iter()
        .filter(|f| f.package() == "io.angzarr.component.v1")
    {
        for service in &file.service {
            for m in &service.method {
                found.insert(
                    format!("{}.{}", service.name(), m.name()),
                    (m.input_type().to_string(), m.output_type().to_string()),
                );
            }
        }
    }
    let h = |m: &str| format!(".io.angzarr.handler.v1.{m}");
    let expected = BTreeMap::from([
        (
            "CommandHandlerService.Handle".to_string(),
            (h("HandleRequest"), h("HandleResponse")),
        ),
        (
            "ComponentService.Describe".to_string(),
            (
                ".io.angzarr.component.v1.DescribeRequest".to_string(),
                ".io.angzarr.v1.ComponentDescriptor".to_string(),
            ),
        ),
        (
            "PolicyService.React".to_string(),
            (h("ReactRequest"), h("ReactResponse")),
        ),
        (
            "PolicyService.Translate".to_string(),
            (h("TranslateRequest"), h("TranslateResponse")),
        ),
        (
            "ProjectorService.Project".to_string(),
            (h("ProjectRequest"), h("ProjectResponse")),
        ),
        (
            "UpcasterService.Upcast".to_string(),
            (h("UpcastRequest"), h("UpcastResponse")),
        ),
    ]);
    assert_eq!(found, expected);
}

#[test]
fn the_contract_imports_only_the_handler_layer_and_core() {
    let set = prost_types::FileDescriptorSet::decode(angzarr_proto_component::FILE_DESCRIPTOR_SET)
        .unwrap();
    let ours: Vec<_> = set
        .file
        .iter()
        .filter(|f| f.package() == "io.angzarr.component.v1")
        .collect();
    assert_eq!(ours.len(), 5, "the five services' files");
    for file in ours {
        for import in &file.dependency {
            assert!(
                import.starts_with("io/angzarr/handler/v1/")
                    || import.starts_with("io/angzarr/v1/")
                    || import.starts_with("google/protobuf/"),
                "{} imports {import}",
                file.name()
            );
        }
    }
}
