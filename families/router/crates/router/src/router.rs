//! Router: the one component of one business process (owner ruling: one
//! coordinator, one component, one business process). A component is one
//! aggregate, policy, process manager or projector. It upcasts nothing: the
//! coordinator upcasts every payload and event through the component's
//! upcaster sidecar before the router sees it (decision 14.39).
//!
//! [`RouterBuilder`] takes the component and validates the configuration at
//! build time: no component is ANGZARR_REASON_NO_HANDLERS_REGISTERED (C-0060); a second
//! component, of any kind, is ANGZARR_REASON_DUPLICATE_COMPONENT, naming the
//! one registered and the one refused (C-0580).
//! The built [`Router`] dispatches to that component, on the handler
//! layer's messages (decision 14.78):
//!
//! - `Handle`, a command or a fact (decision 14.98), to the domain: an
//!   aggregate, or a process manager, which is a domain (decision 14.31)
//!   served the same way, once the coordinator has admitted it (decision
//!   14.42);
//! - `React`, one trigger event, and `Translate`, one refusal of a command
//!   it sent, to the policy;
//! - `Project`, one stream's run, to the projector.
//!
//! The component may be registered boxed (`Box<dyn DynCommandHandler>` and so
//! on). Transport (gRPC servers), health and readiness stay with the host.

use std::future::Future;

use crate::domain::DomainDispatch;
use crate::error::{AngzarrReason, CodedError};
use crate::hpb;
use crate::pb;
use crate::policy::PolicyDispatch;
use crate::projector::ProjectorDispatch;
use crate::send::{BoxFuture, MaybeSend, MaybeSync};
use angzarr_proto::v1::reason_args;

/// A domain (an aggregate or a process manager; decision 14.31) as the
/// router sees it, whatever its state type.
///
/// Kernel-facing (owner ruling 2026-10-30): the coordinator awaits
/// [`dispatch`](Self::dispatch). [`DomainDispatch`] implements it by
/// running the business code's synchronous functions and answering at
/// once (a ready future); a hand-written domain (one in another process,
/// say) awaits its I/O. Business code is never async. Hold one boxed as
/// `Box<dyn DynCommandHandler>`, its object-safe twin.
pub trait CommandHandler: MaybeSend + MaybeSync {
    /// The component's name.
    fn name(&self) -> &str;
    /// AGGREGATE or PROCESS_MANAGER.
    fn kind(&self) -> pb::ComponentKind {
        pb::ComponentKind::Aggregate
    }
    /// The fully-qualified command types it handles.
    fn command_types(&self) -> Vec<String>;
    /// `CommandHandlerService.Handle`: a command or a fact (decision
    /// 14.98), answered success only (decision 14.95).
    fn dispatch(
        &self,
        req: &hpb::HandleRequest,
    ) -> impl Future<Output = Result<hpb::HandleResponse, CodedError>> + MaybeSend;
}

/// [`CommandHandler`], object-safe: its future boxed. Every
/// `CommandHandler` is one, and `dyn DynCommandHandler` (boxed, too) is a
/// `CommandHandler` again, so a router or a coordinator holds any domain
/// as `Box<dyn DynCommandHandler>`.
pub trait DynCommandHandler: MaybeSend + MaybeSync {
    /// [`CommandHandler::name`].
    fn name(&self) -> &str;
    /// [`CommandHandler::kind`].
    fn kind(&self) -> pb::ComponentKind;
    /// [`CommandHandler::command_types`].
    fn command_types(&self) -> Vec<String>;
    /// [`CommandHandler::dispatch`], boxed.
    fn dispatch<'a>(
        &'a self,
        req: &'a hpb::HandleRequest,
    ) -> BoxFuture<'a, Result<hpb::HandleResponse, CodedError>>;
}

impl<T: CommandHandler> DynCommandHandler for T {
    fn name(&self) -> &str {
        CommandHandler::name(self)
    }
    fn kind(&self) -> pb::ComponentKind {
        CommandHandler::kind(self)
    }
    fn command_types(&self) -> Vec<String> {
        CommandHandler::command_types(self)
    }
    fn dispatch<'a>(
        &'a self,
        req: &'a hpb::HandleRequest,
    ) -> BoxFuture<'a, Result<hpb::HandleResponse, CodedError>> {
        Box::pin(CommandHandler::dispatch(self, req))
    }
}

impl CommandHandler for dyn DynCommandHandler + '_ {
    fn name(&self) -> &str {
        DynCommandHandler::name(self)
    }
    fn kind(&self) -> pb::ComponentKind {
        DynCommandHandler::kind(self)
    }
    fn command_types(&self) -> Vec<String> {
        DynCommandHandler::command_types(self)
    }
    async fn dispatch(&self, req: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        DynCommandHandler::dispatch(self, req).await
    }
}

impl<T: CommandHandler + ?Sized> CommandHandler for Box<T> {
    fn name(&self) -> &str {
        (**self).name()
    }
    fn kind(&self) -> pb::ComponentKind {
        (**self).kind()
    }
    fn command_types(&self) -> Vec<String> {
        (**self).command_types()
    }
    fn dispatch(
        &self,
        req: &hpb::HandleRequest,
    ) -> impl Future<Output = Result<hpb::HandleResponse, CodedError>> + MaybeSend {
        (**self).dispatch(req)
    }
}

impl<S> CommandHandler for DomainDispatch<S> {
    fn name(&self) -> &str {
        DomainDispatch::name(self)
    }
    fn kind(&self) -> pb::ComponentKind {
        DomainDispatch::kind(self)
    }
    fn command_types(&self) -> Vec<String> {
        DomainDispatch::command_types(self)
    }
    /// [`DomainDispatch::dispatch`]'s answer, ready.
    fn dispatch(
        &self,
        req: &hpb::HandleRequest,
    ) -> impl Future<Output = Result<hpb::HandleResponse, CodedError>> + MaybeSend {
        std::future::ready(DomainDispatch::dispatch(self, req))
    }
}

/// A policy as a runtime holds it (decisions 14.41, 14.42, 14.78): a
/// [`PolicyDispatch`], or one written by hand (a policy in another process,
/// behind its `PolicyService`). It answers its reactions and its refusals'
/// translations; the runtime addresses, stamps and checks its outputs and
/// delivers its refusal facts. It carries no descriptor (nor does any
/// component): descriptors are generated by angzarr-cli.
///
/// Kernel-facing, as [`CommandHandler`] is: its calls are awaited, a
/// [`PolicyDispatch`] answering at once. Held boxed as
/// `Box<dyn DynPolicyComponent>`.
pub trait PolicyComponent: MaybeSend + MaybeSync {
    /// The component's name.
    fn name(&self) -> &str;
    /// The one domain whose events it reacts to, and to which its refusal
    /// facts go.
    fn input_domain(&self) -> &str;
    /// `PolicyService.React`: the outputs for one trigger event.
    fn react(
        &self,
        req: &hpb::ReactRequest,
    ) -> impl Future<Output = Result<hpb::ReactResponse, CodedError>> + MaybeSend;
    /// `PolicyService.Translate`: one refusal of a command it sent, as a
    /// fact of its own source.
    fn translate(
        &self,
        req: &hpb::TranslateRequest,
    ) -> impl Future<Output = Result<hpb::TranslateResponse, CodedError>> + MaybeSend;
}

/// [`PolicyComponent`], object-safe: its futures boxed. Every
/// `PolicyComponent` is one, and `dyn DynPolicyComponent` is a
/// `PolicyComponent` again.
pub trait DynPolicyComponent: MaybeSend + MaybeSync {
    /// [`PolicyComponent::name`].
    fn name(&self) -> &str;
    /// [`PolicyComponent::input_domain`].
    fn input_domain(&self) -> &str;
    /// [`PolicyComponent::react`], boxed.
    fn react<'a>(
        &'a self,
        req: &'a hpb::ReactRequest,
    ) -> BoxFuture<'a, Result<hpb::ReactResponse, CodedError>>;
    /// [`PolicyComponent::translate`], boxed.
    fn translate<'a>(
        &'a self,
        req: &'a hpb::TranslateRequest,
    ) -> BoxFuture<'a, Result<hpb::TranslateResponse, CodedError>>;
}

impl<T: PolicyComponent> DynPolicyComponent for T {
    fn name(&self) -> &str {
        PolicyComponent::name(self)
    }
    fn input_domain(&self) -> &str {
        PolicyComponent::input_domain(self)
    }
    fn react<'a>(
        &'a self,
        req: &'a hpb::ReactRequest,
    ) -> BoxFuture<'a, Result<hpb::ReactResponse, CodedError>> {
        Box::pin(PolicyComponent::react(self, req))
    }
    fn translate<'a>(
        &'a self,
        req: &'a hpb::TranslateRequest,
    ) -> BoxFuture<'a, Result<hpb::TranslateResponse, CodedError>> {
        Box::pin(PolicyComponent::translate(self, req))
    }
}

impl PolicyComponent for dyn DynPolicyComponent + '_ {
    fn name(&self) -> &str {
        DynPolicyComponent::name(self)
    }
    fn input_domain(&self) -> &str {
        DynPolicyComponent::input_domain(self)
    }
    async fn react(&self, req: &hpb::ReactRequest) -> Result<hpb::ReactResponse, CodedError> {
        DynPolicyComponent::react(self, req).await
    }
    async fn translate(
        &self,
        req: &hpb::TranslateRequest,
    ) -> Result<hpb::TranslateResponse, CodedError> {
        DynPolicyComponent::translate(self, req).await
    }
}

impl<T: PolicyComponent + ?Sized> PolicyComponent for Box<T> {
    fn name(&self) -> &str {
        (**self).name()
    }
    fn input_domain(&self) -> &str {
        (**self).input_domain()
    }
    fn react(
        &self,
        req: &hpb::ReactRequest,
    ) -> impl Future<Output = Result<hpb::ReactResponse, CodedError>> + MaybeSend {
        (**self).react(req)
    }
    fn translate(
        &self,
        req: &hpb::TranslateRequest,
    ) -> impl Future<Output = Result<hpb::TranslateResponse, CodedError>> + MaybeSend {
        (**self).translate(req)
    }
}

impl PolicyComponent for PolicyDispatch {
    fn name(&self) -> &str {
        PolicyDispatch::name(self)
    }
    fn input_domain(&self) -> &str {
        PolicyDispatch::input_domain(self)
    }
    /// [`PolicyDispatch::react`]'s answer, ready.
    fn react(
        &self,
        req: &hpb::ReactRequest,
    ) -> impl Future<Output = Result<hpb::ReactResponse, CodedError>> + MaybeSend {
        std::future::ready(PolicyDispatch::react(self, req))
    }
    /// [`PolicyDispatch::translate`]'s answer, ready.
    fn translate(
        &self,
        req: &hpb::TranslateRequest,
    ) -> impl Future<Output = Result<hpb::TranslateResponse, CodedError>> + MaybeSend {
        std::future::ready(PolicyDispatch::translate(self, req))
    }
}

/// A projector as the router sees it, whatever its projection type.
///
/// Kernel-facing, as [`CommandHandler`] is: its dispatch is awaited, a
/// [`ProjectorDispatch`] answering at once. Held boxed as
/// `Box<dyn DynProjectorHandler>`.
pub trait ProjectorHandler: MaybeSend + MaybeSync {
    /// The component's name.
    fn name(&self) -> &str;
    /// `ProjectorService.Project`: one stream's run (decision 14.78).
    fn dispatch(
        &self,
        req: &hpb::ProjectRequest,
    ) -> impl Future<Output = Result<hpb::ProjectResponse, CodedError>> + MaybeSend;
}

/// [`ProjectorHandler`], object-safe: its future boxed. Every
/// `ProjectorHandler` is one, and `dyn DynProjectorHandler` is a
/// `ProjectorHandler` again.
pub trait DynProjectorHandler: MaybeSend + MaybeSync {
    /// [`ProjectorHandler::name`].
    fn name(&self) -> &str;
    /// [`ProjectorHandler::dispatch`], boxed.
    fn dispatch<'a>(
        &'a self,
        req: &'a hpb::ProjectRequest,
    ) -> BoxFuture<'a, Result<hpb::ProjectResponse, CodedError>>;
}

impl<T: ProjectorHandler> DynProjectorHandler for T {
    fn name(&self) -> &str {
        ProjectorHandler::name(self)
    }
    fn dispatch<'a>(
        &'a self,
        req: &'a hpb::ProjectRequest,
    ) -> BoxFuture<'a, Result<hpb::ProjectResponse, CodedError>> {
        Box::pin(ProjectorHandler::dispatch(self, req))
    }
}

impl ProjectorHandler for dyn DynProjectorHandler + '_ {
    fn name(&self) -> &str {
        DynProjectorHandler::name(self)
    }
    async fn dispatch(
        &self,
        req: &hpb::ProjectRequest,
    ) -> Result<hpb::ProjectResponse, CodedError> {
        DynProjectorHandler::dispatch(self, req).await
    }
}

impl<T: ProjectorHandler + ?Sized> ProjectorHandler for Box<T> {
    fn name(&self) -> &str {
        (**self).name()
    }
    fn dispatch(
        &self,
        req: &hpb::ProjectRequest,
    ) -> impl Future<Output = Result<hpb::ProjectResponse, CodedError>> + MaybeSend {
        (**self).dispatch(req)
    }
}

impl<P> ProjectorHandler for ProjectorDispatch<P> {
    fn name(&self) -> &str {
        ProjectorDispatch::name(self)
    }
    /// [`ProjectorDispatch::dispatch`]'s answer, ready.
    fn dispatch(
        &self,
        req: &hpb::ProjectRequest,
    ) -> impl Future<Output = Result<hpb::ProjectResponse, CodedError>> + MaybeSend {
        std::future::ready(ProjectorDispatch::dispatch(self, req))
    }
}

/// The one component a router hosts.
enum Component {
    Aggregate(Box<dyn DynCommandHandler>),
    Policy(PolicyDispatch),
    Projector(Box<dyn DynProjectorHandler>),
}

impl Component {
    fn name(&self) -> &str {
        match self {
            Component::Aggregate(c) => DynCommandHandler::name(&**c),
            Component::Policy(c) => c.name(),
            Component::Projector(c) => DynProjectorHandler::name(&**c),
        }
    }

    /// The refusal of a second component, `refused`, beside this one
    /// (ANGZARR_REASON_DUPLICATE_COMPONENT, C-0580 and C-0670): a
    /// misconfiguration, INTERNAL, naming this component as `registered`
    /// and the refused one's name as `refused` (decisions 14.87 and 14.88).
    fn refuse_another(&self, refused: &str) -> CodedError {
        CodedError::keyed(
            AngzarrReason::DuplicateComponent,
            [
                (reason_args::REGISTERED.to_string(), self.name().to_string()),
                (reason_args::REFUSED.to_string(), refused.to_string()),
            ],
        )
    }
}

/// Collects the one component of a [`Router`], and the descriptor it
/// answers Describe with; [`RouterBuilder::build`] validates the
/// configuration.
#[derive(Default)]
pub struct RouterBuilder {
    components: Vec<Component>,
    descriptor: Option<pb::ComponentDescriptor>,
}

impl RouterBuilder {
    /// An empty configuration.
    pub fn new() -> Self {
        RouterBuilder::default()
    }

    /// Hosts an aggregate (command handler).
    pub fn aggregate(mut self, aggregate: impl CommandHandler + 'static) -> Self {
        self.components
            .push(Component::Aggregate(Box::new(aggregate)));
        self
    }

    /// Hosts a policy.
    pub fn policy(mut self, policy: PolicyDispatch) -> Self {
        self.components.push(Component::Policy(policy));
        self
    }

    /// Hosts a process manager: a domain (decision 14.31), hosted as an
    /// aggregate is ([`DomainDispatch::process_manager`]).
    pub fn process_manager(self, pm: impl CommandHandler + 'static) -> Self {
        self.aggregate(pm)
    }

    /// Hosts a projector.
    pub fn projector(mut self, projector: impl ProjectorHandler + 'static) -> Self {
        self.components
            .push(Component::Projector(Box::new(projector)));
        self
    }

    /// The component's descriptor (types.proto, ComponentDescriptor), as
    /// angzarr-cli generated it at build time: what the business process
    /// answers `ComponentService.Describe` with, unchanged. The coordinator
    /// validates it (decision 14.42); the router builds and checks none.
    pub fn descriptor(mut self, descriptor: pb::ComponentDescriptor) -> Self {
        self.descriptor = Some(descriptor);
        self
    }

    /// Validates the configuration and builds the router.
    pub fn build(mut self) -> Result<Router, CodedError> {
        if self.components.is_empty() {
            return Err(CodedError::keyed(AngzarrReason::NoHandlersRegistered, []));
        }
        let component = self.components.remove(0);
        if let Some(second) = self.components.first() {
            return Err(component.refuse_another(second.name()));
        }
        Ok(Router {
            component,
            descriptor: self.descriptor,
        })
    }
}

/// The one component of one business process, ready to dispatch. Dispatching
/// a kind the router does not host is ANGZARR_REASON_NO_COMPONENT_OF_KIND (UNIMPLEMENTED).
pub struct Router {
    component: Component,
    descriptor: Option<pb::ComponentDescriptor>,
}

fn wrong_kind() -> CodedError {
    CodedError::keyed(AngzarrReason::NoComponentOfKind, [])
}

impl Router {
    /// The refusal of another component, `refused`, registered beside
    /// this one (ANGZARR_REASON_DUPLICATE_COMPONENT naming this one as
    /// `registered`; C-0580), for hosts that register components one at a
    /// time (the C ABI).
    pub fn refuse_another_component(&self, refused: &str) -> CodedError {
        self.component.refuse_another(refused)
    }

    /// The descriptor `ComponentService.Describe` answers: the one the
    /// router was given ([`RouterBuilder::descriptor`]), unchanged; `None`
    /// when it was given none.
    pub fn component_descriptor(&self) -> Option<&pb::ComponentDescriptor> {
        self.descriptor.as_ref()
    }

    /// `Handle`: a command or a fact, with the domain (an aggregate or a
    /// process manager; decision 14.98).
    pub async fn dispatch_command(
        &self,
        req: &hpb::HandleRequest,
    ) -> Result<hpb::HandleResponse, CodedError> {
        match &self.component {
            Component::Aggregate(aggregate) => DynCommandHandler::dispatch(&**aggregate, req).await,
            _ => Err(wrong_kind()),
        }
    }

    /// `React`: the policy's outputs for one trigger event. The coordinator
    /// delivers only its input domain's events; the router guards no
    /// envelope (decision 14.42).
    pub async fn dispatch_react(
        &self,
        req: &hpb::ReactRequest,
    ) -> Result<hpb::ReactResponse, CodedError> {
        match &self.component {
            Component::Policy(policy) => PolicyComponent::react(policy, req).await,
            _ => Err(wrong_kind()),
        }
    }

    /// `Translate`: the policy's one refusal callback on a refusal of a
    /// command it sent (decision 14.89).
    pub async fn dispatch_translate(
        &self,
        req: &hpb::TranslateRequest,
    ) -> Result<hpb::TranslateResponse, CodedError> {
        match &self.component {
            Component::Policy(policy) => PolicyComponent::translate(policy, req).await,
            _ => Err(wrong_kind()),
        }
    }

    /// `Project`: the projector over one stream's run.
    pub async fn dispatch_project(
        &self,
        req: &hpb::ProjectRequest,
    ) -> Result<hpb::ProjectResponse, CodedError> {
        match &self.component {
            Component::Projector(projector) => {
                DynProjectorHandler::dispatch(&**projector, req).await
            }
            _ => Err(wrong_kind()),
        }
    }
}

#[cfg(test)]
#[path = "router.test.rs"]
mod router_tests;
