//! The Rust emitter: a package's domain layer for marked-up components on
//! angzarr-embedded ([`markup_package`]).
//!
//! Every domain type is a plain Rust type with its wire conversions: into the
//! prost message infallibly, back out fallibly with angzarr-embedded's `WireError`
//! naming the field. Message fields are required (the wire's `Option` is
//! gone), proto3 `optional` stays an `Option`, and enums are closed: the
//! zero value (`*_UNSPECIFIED`) and any number without a variant are refused.
//! Enums convert by number, so nothing here depends on how prost names
//! variants. A oneof is a required choice of its arms (`<Message><Oneof>`),
//! a nested type is named after its enclosing messages, `Timestamp`,
//! `Duration` and angzarr's `UUID` are their own types on both sides (a
//! UUID declaring `id_type` its newtype), and another package's type is
//! that package's domain type, at its layer's path. A type derives
//! `Default` where every field has one.

use angzarr_diagnostic::code;
use proc_macro2::{Span, TokenStream};
use prost_reflect::{EnumDescriptor, FieldDescriptor, Kind, MessageDescriptor};
use quote::{format_ident, quote};

use crate::Paths;
use crate::common::{
    choice_name, enum_name, message_name, output_name, refusal_choice_name, refusal_key,
    refusal_reason, screaming, snake, upper_camel, wire_name,
};
use crate::model::{DURATION, Package, TIMESTAMP, UUID};

/// The runtime crate the generated code targets, as its Rust identifier:
/// every path into it, the macros crate's name (`<RUNTIME>_macros`) and the
/// prose naming it in generated docs are built from this one name.
pub(crate) const RUNTIME: &str = "angzarr_embedded";

/// The runtime crate's identifier.
pub(crate) fn runtime() -> syn::Ident {
    format_ident!("{RUNTIME}")
}

/// The runtime's markup macros crate's identifier.
pub(crate) fn runtime_macros() -> syn::Ident {
    format_ident!("{RUNTIME}_macros")
}

/// `::<runtime>`, the runtime's crate root: the API the generated code is
/// written against, its kernel, `markup` and re-exported diagnostics.
pub(crate) fn root() -> TokenStream {
    let runtime = runtime();
    quote!(::#runtime)
}

/// The markup domain layer for one package: its domain types with their
/// wire conversions, a `DomainMessage` impl for every input, event and
/// refusal of its components, and each component's command and event enums
/// with their `Payload` impls. No `State` impl: the markup generates it.
/// The package whose layer is being written, and where every package's
/// code is.
struct Layer<'a> {
    package: &'a str,
    paths: &'a Paths,
}

impl Layer<'_> {
    /// The module path of prost's types in `message`'s own module, for a
    /// type declared in it: its package's wire path, then a module per
    /// enclosing message (`place_order::gift`), as prost nests them.
    fn wire_module(&self, message: &MessageDescriptor) -> TokenStream {
        let wire = &self.paths.wire[message.package_name()];
        let mut modules: Vec<syn::Ident> = Vec::new();
        let mut parent = message.parent_message();
        while let Some(p) = parent {
            modules.push(ident(&snake(p.name())));
            parent = p.parent_message();
        }
        modules.reverse();
        quote!(#wire #(::#modules)*)
    }

    /// `message`'s prost type.
    fn wire_type(&self, message: &MessageDescriptor) -> TokenStream {
        let module = self.wire_module(message);
        let name = ident(message.name());
        quote!(#module::#name)
    }

    /// The prost choice of `oneof`: `<message module>::<Oneof>`.
    fn wire_choice(&self, oneof: &prost_reflect::OneofDescriptor) -> TokenStream {
        let message = oneof.parent_message();
        let module = self.wire_module(message);
        let inner = ident(&snake(message.name()));
        let name = ident(&upper_camel(oneof.name()));
        quote!(#module::#inner::#name)
    }

    /// The domain type named `local` of `package`: bare in its own layer,
    /// else at that package's domain path.
    fn domain(&self, package: &str, local: &str) -> TokenStream {
        let name = ident(local);
        if package == self.package {
            quote!(#name)
        } else {
            let path = &self.paths.domain[package];
            quote!(#path::#name)
        }
    }
}

pub(crate) fn markup_package(package: &Package, name: &str, paths: &Paths) -> String {
    let at = Layer {
        package: name,
        paths,
    };
    let wire = &paths.wire[name];
    let mut items = TokenStream::new();
    for e in &package.enums {
        items.extend(enumeration(e));
    }
    items.extend(id_types(package));
    for message in &package.messages {
        items.extend(structure(message, &at));
    }
    items.extend(handler_context());
    let mut refusals: Vec<&MessageDescriptor> = Vec::new();
    for component in &package.components {
        for refusal in component.inputs.iter().flat_map(|i| &i.refuses) {
            if !refusals.contains(&refusal) {
                refusals.push(refusal);
            }
        }
        // A projector decides nothing: no command or event enum.
        if component.kind == crate::model::ComponentKind::Projector {
            continue;
        }
        let commands: Vec<MessageDescriptor> = component
            .inputs
            .iter()
            .filter(|i| !i.fact)
            .map(|i| i.message.clone())
            .collect();
        items.extend(plain_enum(
            &format_ident!("{}Command", component.name),
            &commands,
            &format!(" A command of {}.", component.name),
        ));
        items.extend(output_enum(component));
        items.extend(fact_enum(component));
        items.extend(refusal_enum(component));
        items.extend(plain_enum(
            &format_ident!("{}Event", component.name),
            &component.events,
            &format!(
                " An event of {}, for a decision recording any number of them.",
                component.name
            ),
        ));
    }
    for refusal in refusals {
        items.extend(refusal_diagnostic(refusal));
    }
    for component in &package.components {
        for input in component.inputs.iter().filter(|i| i.refuses.len() > 1) {
            items.extend(refusal_choice(input));
        }
    }
    items.extend(wire_settings(&package.messages, &at));
    items.extend(domain_messages(package, &at));
    for component in package
        .components
        .iter()
        .filter(|c| c.kind.is_domain() && c.events.is_empty())
    {
        items.extend(no_events_message(component, wire));
    }
    let file: syn::File = syn::parse2(items).expect("the emitter writes valid Rust");
    let mut out = prettyplease::unparse(&file);
    for interface in &package.interfaces {
        out.push('\n');
        out.push_str(interface);
    }
    out
}

/// The `DomainMessage` impl of every aggregate's and process manager's
/// anchor, and every input, refusal and event of the package's
/// components, each once.
fn domain_messages(package: &Package, at: &Layer) -> TokenStream {
    let mut items = TokenStream::new();
    let mut messages: Vec<&MessageDescriptor> = Vec::new();
    for component in &package.components {
        let declared = component
            .kind
            .is_domain()
            .then_some(&component.anchor)
            .into_iter()
            .chain(
                component
                    .inputs
                    .iter()
                    .flat_map(|i| std::iter::once(&i.message).chain(&i.refuses)),
            )
            .chain(&component.events);
        for message in declared {
            if !messages.contains(&message) {
                messages.push(message);
            }
        }
    }
    let root = root();
    for message in messages {
        let name = ident(&message_name(message));
        let w = at.wire_type(message);
        items.extend(quote! {
            impl #root::markup::DomainMessage for #name {
                type Wire = #w;

                fn to_wire(&self) -> #w {
                    #w::from(self.clone())
                }

                fn from_wire(wire: #w) -> Result<Self, #root::WireError> {
                    #name::try_from(wire)
                }
            }
        });
    }
    items
}

/// The event type of a domain with no events (`{Component}Event`, an enum
/// with no value) as a `DomainMessage`, which the markup's `Emit::of` takes
/// of nothing else: the adapter's empty decision is `Emit::of(None::<..>)`
/// of it. Its wire type is the domain's state message, a prost message
/// with a type URL no event of the domain has; nothing decodes to it.
fn no_events_message(component: &crate::model::Component, wire: &syn::Path) -> TokenStream {
    let root = root();
    let events = format_ident!("{}Event", component.name);
    let state = ident(component.anchor.name());
    let anchor = component.anchor.full_name();
    quote! {
        impl #root::markup::DomainMessage for #events {
            type Wire = #wire::#state;

            fn to_wire(&self) -> #wire::#state {
                match *self {}
            }

            fn from_wire(_wire: #wire::#state) -> Result<Self, #root::WireError> {
                Err(#root::WireError::Missing { message: #anchor, field: "event" })
            }
        }
    }
}

/// The prost settings the domain layer is generated for, checked where the
/// wire types are compiled: a `bytes` field is a `Vec<u8>` (prost's
/// default) and a map a `BTreeMap` (`btree_map(["."])`), as
/// `angzarr_build::wire` builds them. A prost crate built otherwise fails
/// to compile with the named error, `angzarr-codegen-wire-bytes` or
/// `angzarr-codegen-wire-map`, at the field, instead of a type mismatch
/// deep in a conversion. Nothing is emitted for messages holding neither.
fn wire_settings(messages: &[MessageDescriptor], at: &Layer) -> TokenStream {
    let (mut bytes, mut maps) = (false, false);
    let mut checks = Vec::new();
    for message in messages {
        let w = at.wire_type(message);
        let mut fields = Vec::new();
        // A oneof's arms are in prost's choice, not fields of the message.
        for field in message
            .fields()
            .filter(|f| f.containing_oneof().is_none_or(|o| o.is_synthetic()))
        {
            let f = ident(field.name());
            if field.is_map() {
                maps = true;
                fields.push(quote!(wire_settings::map(&wire.#f);));
            } else if field.kind() == Kind::Bytes {
                bytes = true;
                // A list's or an `optional`'s each value.
                let several =
                    field.is_list() || field.containing_oneof().is_some_and(|o| o.is_synthetic());
                fields.push(if several {
                    quote!(wire.#f.iter().for_each(wire_settings::bytes);)
                } else {
                    quote!(wire_settings::bytes(&wire.#f);)
                });
            }
        }
        if !fields.is_empty() {
            checks.push(quote! {
                const _: fn(&#w) = |wire| {
                    #(#fields)*
                };
            });
        }
    }
    if checks.is_empty() {
        return TokenStream::new();
    }
    // The compile errors' codes are this crate's, named by the package
    // that generates them (code!), written into the generated source as
    // literals.
    let bytes = bytes.then(|| {
        let message = concat!(
            code!("wire-bytes"),
            ": a bytes field of the prost types is `{Self}`; the domain layer is generated for `Vec<u8>`"
        );
        quote! {
            #[diagnostic::on_unimplemented(
                message = #message,
                label = "not `Vec<u8>`",
                note = "build the prost types with angzarr_build::wire, or prost-build with its default bytes"
            )]
            pub trait Bytes {}

            impl Bytes for Vec<u8> {}

            pub fn bytes<T: Bytes>(_: &T) {}
        }
    });
    let maps = maps.then(|| {
        let message = concat!(
            code!("wire-map"),
            ": a map field of the prost types is `{Self}`; the domain layer is generated for `BTreeMap`"
        );
        quote! {
            #[diagnostic::on_unimplemented(
                message = #message,
                label = "not a `BTreeMap`",
                note = "build the prost types with angzarr_build::wire, or prost-build with btree_map([\".\"])"
            )]
            pub trait Map {}

            impl<K, V> Map for ::std::collections::BTreeMap<K, V> {}

            pub fn map<T: Map>(_: &T) {}
        }
    });
    quote! {
        /// The prost settings this layer is generated for, checked against
        /// the wire types: bytes as `Vec<u8>`, maps as `BTreeMap`.
        mod wire_settings {
            #bytes
            #maps
        }

        #(#checks)*
    }
}

/// What the user's handlers read and fail with, under the domain layer's
/// names, so a user file imports only `crate::domain`: the runtime's
/// contexts and its `Failed`, `Unrefusable`, `BusinessStatus` and
/// `RetryableStatus` re-exported, `Refusal`, the status a refusal
/// message declares, and `Bug`, a defect: a diagnostic key and its args,
/// never text (decision 14.37).
fn handler_context() -> TokenStream {
    let root = root();
    quote! {
        /// What a command handler reads besides its command and state: the
        /// instant its events are recorded at, and the router's context of
        /// the input (its stream, sequences, resolution, conflict and
        /// identity) as `ctx.command`.
        pub use #root::markup::Context;

        /// What a policy reaction reads: its trigger, the trigger's
        /// correlation and when it was recorded.
        pub use #root::markup::PolicyContext;

        /// The refused command's context, whole, as its target's handler
        /// was handed it: what a fact's continuation step reads.
        pub use #root::markup::CommandContext;

        /// The refused answer a policy's refusal callback reads besides
        /// the refused command and the refusal: the status it travelled as,
        /// the delivery, the trigger and its metadata.
        pub use #root::markup::Refused;

        /// What a projector's handler reads besides the event: the stream
        /// it is from (its source domain and root), its sequence there and
        /// when it was recorded.
        pub use #root::markup::ProjectionContext;

        /// What a process manager rolls back with (decision 14.74): each
        /// step it completes paired, in code, with the undo that reverses
        /// it, and the rollback built from them.
        pub use #root::markup::Compensation;

        /// What a command handler fails with, one of four classes:
        /// `Failed::Rejected`, one of the command's typed refusals under
        /// a [`BusinessStatus`] (the one the refusal message declares);
        /// `Failed::immediate(&bug)`, ABORTED under the
        /// [`Bug`]'s key, "re-read and re-decide", back to a caller at
        /// once and re-run as a policy's step; `Failed::Bug`, a [`Bug`],
        /// dead-lettered at once; or `Failed::retryable_bug(status, &bug)`,
        /// a [`Bug`] under the [`RetryableStatus`] the handler picks,
        /// retried with capped backoff, then dead-lettered.
        /// `Err(refusal)?` and `Err(bug)?` both convert. A command that
        /// refuses nothing fails with `Failed<Unrefusable>`.
        ///
        /// The retryable statuses are not interchangeable: `Unavailable`
        /// means resend (the dependency is down for now);
        /// `DeadlineExceeded` means resend only if the call is idempotent,
        /// since the work may have happened (a command is, by its
        /// `command_id`); `ResourceExhausted` means wait longer before
        /// resending (a quota or a capacity limit was hit).
        pub use #root::markup::{BusinessStatus, Failed, RetryableStatus, Unrefusable};

        /// Why an aggregate's or a process manager's `restore` refuses a
        /// snapshot: a diagnostic key and its args, never text.
        pub use #root::markup::Report;

        /// A typed refusal message: the status it is refused under, the
        /// `(refusal_status)` its message declares (there is no default).
        /// Every refusal message of the domain layer implements it; a
        /// generated adapter refuses under it, and a hand-written handler
        /// reads it for the same status.
        pub trait Refusal {
            /// The business status the refusal is refused with.
            const REFUSAL_STATUS: BusinessStatus;
        }

        /// A bug: a defect, never a business answer. The other class a
        /// handler fails with besides a typed refusal. A command handler
        /// fails with it as
        /// `Failed::Bug` (`Err(Bug::new("key").arg(..))?`), a policy as its
        /// `Err`, since a policy reacts to events the application's own
        /// domains recorded. A diagnostic key and its args, never text; it
        /// goes out as ROUTER_REASON_UNHANDLED_HANDLER_ERROR, the key its
        /// `cause`, and the runtime alerts it and dead-letters the input.
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct Bug {
            pub key: &'static str,
            pub args: Vec<(&'static str, String)>,
        }

        impl Bug {
            /// A bug keyed `key`, with no args yet.
            pub fn new(key: &'static str) -> Bug {
                Bug { key, args: Vec::new() }
            }

            /// This bug with the arg `name` set to `value`.
            pub fn arg(mut self, name: &'static str, value: impl ToString) -> Bug {
                self.args.push((name, value.to_string()));
                self
            }
        }

        impl ::std::fmt::Display for Bug {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.key)
            }
        }

        impl ::std::error::Error for Bug {}

        impl ::angzarr_embedded::angzarr_diagnostic::Diagnostic for Bug {
            fn code(&self) -> &str {
                self.key
            }

            fn args(&self) -> Vec<(&str, ::angzarr_embedded::angzarr_diagnostic::Arg)> {
                self.args
                    .iter()
                    .map(|(name, value)| (*name, value.as_str().into()))
                    .collect()
            }
        }

        /// A command handler's bug, as it fails: `Failed::Bug`.
        impl ::core::convert::From<Bug> for Failed<Unrefusable> {
            fn from(bug: Bug) -> Self {
                Failed::bug(&bug)
            }
        }

        /// A fact handler's failure: the router's `Bug`, under this bug's
        /// key and args (a fact is never refused, so a bug is its one
        /// failure).
        impl ::core::convert::From<Bug> for #root::markup::Bug {
            fn from(bug: Bug) -> Self {
                #root::markup::Bug::coded(
                    bug.key,
                    bug.args.into_iter().map(|(name, value)| (name.to_string(), value)),
                )
            }
        }
    }
}

/// A typed refusal as a diagnostic (decision 14.50): its code
/// [`refusal_key`] of the message's full name and, when it declares a
/// nested `Reason` enum as its `reason` field, the reason's wire name; its
/// args the fields that are set, each by its proto name (see
/// [`field_arg`]).
fn refusal_diagnostic(message: &MessageDescriptor) -> TokenStream {
    let name = ident(message.name());
    let code = match refusal_reason(message) {
        Some(e) => {
            let ty = ident(&enum_name(&e));
            let arms = e.values().filter(|v| v.number() != 0).map(|v| {
                let variant = ident(&camel(wire_name(&e, v.name())));
                let key = refusal_key(message.full_name(), Some(wire_name(&e, v.name())));
                quote!(#ty::#variant => #key)
            });
            quote!(match self.reason { #(#arms),* })
        }
        _ => {
            let key = refusal_key(message.full_name(), None);
            quote!(#key)
        }
    };
    let args: Vec<TokenStream> = message.fields().filter_map(|f| field_arg(&f)).collect();
    let args = if args.is_empty() {
        quote!(Vec::new())
    } else {
        quote! {
            let mut args = Vec::new();
            #(#args)*
            args
        }
    };
    let status = business_status(crate::model::message_status(message));
    let status = quote! {
        impl Refusal for #name {
            const REFUSAL_STATUS: BusinessStatus = BusinessStatus::#status;
        }

        impl #name {
            /// `failed`, its refusal under the status this message declares
            /// ([`Refusal::REFUSAL_STATUS`]), whatever status it was given;
            /// any other failure unchanged. What the generated adapter
            /// refuses with.
            pub fn with_declared_status(failed: Failed<#name>) -> Failed<#name> {
                match failed {
                    Failed::Rejected { rejection, .. } => Failed::Rejected {
                        rejection,
                        status: <#name as Refusal>::REFUSAL_STATUS,
                    },
                    other => other,
                }
            }
        }
    };
    quote! {
        #status

        /// A bug of the handler refusing with this, as it fails:
        /// `Failed::Bug`.
        impl ::core::convert::From<Bug> for Failed<#name> {
            fn from(bug: Bug) -> Self {
                Failed::bug(&bug)
            }
        }

        impl ::std::fmt::Display for #name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(::angzarr_embedded::angzarr_diagnostic::Diagnostic::code(self))
            }
        }

        impl ::angzarr_embedded::angzarr_diagnostic::Diagnostic for #name {
            fn code(&self) -> &str {
                #code
            }

            fn args(&self) -> Vec<(&str, ::angzarr_embedded::angzarr_diagnostic::Arg)> {
                #args
            }
        }
    }
}

/// A choice's `with_declared_status`: its refusal under the status the
/// chosen type declares ([`Refusal::REFUSAL_STATUS`]).
fn choice_status(name: &syn::Ident, input: &crate::model::Input) -> TokenStream {
    let arms = input.refuses.iter().map(|r| {
        let ty = ident(r.name());
        quote!(#name::#ty(_) => <#ty as Refusal>::REFUSAL_STATUS)
    });
    quote! {
        impl #name {
            /// `failed`, its refusal under the status the chosen type
            /// declares ([`Refusal::REFUSAL_STATUS`]), whatever status it
            /// was given; any other failure unchanged. What the generated
            /// adapter refuses with.
            pub fn with_declared_status(failed: Failed<#name>) -> Failed<#name> {
                match failed {
                    Failed::Rejected { rejection, .. } => {
                        let status = match &rejection { #(#arms),* };
                        Failed::Rejected { rejection, status }
                    }
                    other => other,
                }
            }
        }
    }
}

/// What a command naming several `refuses` types refuses with: one of
/// them, each `From` its type, its diagnostic the chosen one's, and
/// `widen`, which hands the adapter's markup its own choice.
fn refusal_choice(input: &crate::model::Input) -> TokenStream {
    let name = format_ident!("{}", refusal_choice_name(&input.message));
    let types: Vec<syn::Ident> = input.refuses.iter().map(|r| ident(r.name())).collect();
    let doc = format!(
        " What {} refuses with: one of its refuses types.",
        input.message.name()
    );
    let coded = choice_status(&name, input);
    quote! {
        #coded

        #[doc = #doc]
        #[derive(Clone, Debug, PartialEq)]
        pub enum #name { #(#types(#types)),* }

        #(
            impl From<#types> for #name {
                fn from(refusal: #types) -> Self {
                    #name::#types(refusal)
                }
            }
        )*

        impl #name {
            /// `failed`, its refusal converted by the chosen type's own
            /// `Into`: what the generated adapter hands the markup.
            pub fn widen<Y>(failed: Failed<#name>) -> Failed<Y>
            where
                #(#types: Into<Y>,)*
            {
                match failed {
                    Failed::Rejected { rejection, status } => Failed::Rejected {
                        rejection: match rejection {
                            #(#name::#types(refusal) => refusal.into(),)*
                        },
                        status,
                    },
                    Failed::Immediate(report) => Failed::Immediate(report),
                    Failed::Bug(report) => Failed::Bug(report),
                    Failed::RetryableBug { status, report } => Failed::RetryableBug { status, report },
                }
            }
        }

        /// A bug of the handler refusing with this, as it fails:
        /// `Failed::Bug`.
        impl ::core::convert::From<Bug> for Failed<#name> {
            fn from(bug: Bug) -> Self {
                Failed::bug(&bug)
            }
        }

        impl ::std::fmt::Display for #name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                match self {
                    #(#name::#types(refusal) => ::std::fmt::Display::fmt(refusal, f),)*
                }
            }
        }

        impl ::angzarr_embedded::angzarr_diagnostic::Diagnostic for #name {
            fn code(&self) -> &str {
                match self {
                    #(#name::#types(refusal) => ::angzarr_embedded::angzarr_diagnostic::Diagnostic::code(refusal),)*
                }
            }

            fn args(&self) -> Vec<(&str, ::angzarr_embedded::angzarr_diagnostic::Arg)> {
                match self {
                    #(#name::#types(refusal) => ::angzarr_embedded::angzarr_diagnostic::Diagnostic::args(refusal),)*
                }
            }
        }
    }
}

/// The `BusinessStatus` variant of a `refusal_status` number (a
/// `google.rpc.Code`, decision 14.52): a business-fatal status, or ABORTED
/// (decision 14.111); unmarked, or a number outside that set,
/// FAILED_PRECONDITION.
fn business_status(code: i32) -> syn::Ident {
    format_ident!(
        "{}",
        match code {
            3 => "InvalidArgument",
            5 => "NotFound",
            6 => "AlreadyExists",
            7 => "PermissionDenied",
            10 => "Aborted",
            11 => "OutOfRange",
            _ => "FailedPrecondition",
        }
    )
}

/// The statement pushing `field` onto a refusal's args when it is set:
/// a proto3 `optional` one when it is `Some`, any other always. An
/// integer is its number, a string itself, an enum its value's wire
/// name ([`wire_name`]), bytes their lower-case hex, a bool or a float
/// its Rust text. A message, list or map field is not an arg (`None`).
fn field_arg(field: &FieldDescriptor) -> Option<TokenStream> {
    if field.is_list()
        || field.is_map()
        || field.containing_oneof().is_some_and(|o| !o.is_synthetic())
    {
        return None;
    }
    let arg = quote!(::angzarr_embedded::angzarr_diagnostic::Arg);
    let value = match field.kind() {
        Kind::Message(_) => return None,
        Kind::Enum(e) => {
            let ty = ident(&enum_name(&e));
            let arms = e.values().filter(|v| v.number() != 0).map(|v| {
                let variant = ident(&camel(wire_name(&e, v.name())));
                let wire = wire_name(&e, v.name());
                quote!(#ty::#variant => #wire)
            });
            quote!(#arg::from(match value { #(#arms),* }))
        }
        Kind::String => quote!(#arg::from(value.as_str())),
        Kind::Bytes => quote! {
            #arg::from(value.iter().map(|b| format!("{b:02x}")).collect::<String>())
        },
        Kind::Bool | Kind::Float | Kind::Double => quote!(#arg::from(value.to_string())),
        _ => quote!(#arg::from(*value)),
    };
    let name = ident(field.name());
    let proto = field.name();
    Some(
        if field.containing_oneof().is_some_and(|o| o.is_synthetic()) {
            quote! {
                if let Some(value) = &self.#name {
                    args.push((#proto, #value));
                }
            }
        } else {
            quote! {
                let value = &self.#name;
                args.push((#proto, #value));
            }
        },
    )
}

/// An enum of `messages`, one variant each, named as its type, with a
/// `From` of each type and its `Payload`: the domain's one nameable payload
/// set, which the markup takes (`command = ..`, `event = ..`) instead of
/// writing its own. Its `Payload` stands on markup's public functions, so
/// writing the enum by hand is writing the same code.
fn plain_enum(name: &syn::Ident, messages: &[MessageDescriptor], doc: &str) -> TokenStream {
    let variants: Vec<syn::Ident> = messages.iter().map(|m| ident(m.name())).collect();
    let froms = variants.iter().map(|v| {
        quote! {
            impl From<#v> for #name {
                fn from(value: #v) -> Self {
                    #name::#v(value)
                }
            }
        }
    });
    let payload = payload(name, &variants);
    quote! {
        #[doc = #doc]
        #[derive(Clone, Debug, PartialEq)]
        pub enum #name { #(#variants(#variants)),* }

        #(#froms)*

        #payload
    }
}

/// What a policy sends `component` (`<Component>Output`): one variant per
/// input, command or fact, each `From` its type. The policy's generated
/// adapter sends each in the arm the input declares (decision 14.69).
fn output_enum(component: &crate::model::Component) -> TokenStream {
    let name = format_ident!("{}", output_name(component));
    let variants: Vec<syn::Ident> = component
        .inputs
        .iter()
        .map(|i| ident(i.message.name()))
        .collect();
    let doc = format!(
        " What a policy sends {}: its commands and facts, each in its arm. As a \
         `markup::CommandTarget`, its domain and commands: the destination a \
         process manager's compensation step names (decision 14.74).",
        component.name
    );
    let root = root();
    let domain = &component.domain;
    let commands: Vec<String> = component
        .inputs
        .iter()
        .filter(|i| !i.fact)
        .map(|i| i.message.full_name().to_string())
        .collect();
    quote! {
        #[doc = #doc]
        #[derive(Clone, Debug, PartialEq)]
        pub enum #name { #(#variants(#variants)),* }

        impl #root::markup::CommandTarget for #name {
            const DOMAIN: &'static str = #domain;

            fn command_types() -> Vec<String> {
                vec![#(#commands.to_string()),*]
            }
        }

        #(
            impl From<#variants> for #name {
                fn from(value: #variants) -> Self {
                    #name::#variants(value)
                }
            }
        )*
    }
}

/// A domain's facts as one choice (`<Domain>Fact`), when it takes any: one
/// variant per fact, each `From` its type; what a policy's translating
/// callback answers this domain, its source, with. It is a
/// `markup::Translation` for any source ingesting each of its facts, so the
/// markup packs whichever it holds.
fn fact_enum(component: &crate::model::Component) -> TokenStream {
    let Some(name) = crate::scaffold::fact_choice(component) else {
        return quote!();
    };
    let name = format_ident!("{name}");
    let facts: Vec<syn::Ident> = component
        .inputs
        .iter()
        .filter(|i| i.fact)
        .map(|i| ident(i.message.name()))
        .collect();
    let root = root();
    let m = quote!(#root::markup);
    let doc = format!(
        " A fact of {}: what a policy sourced from it translates a refusal into.",
        component.name
    );
    quote! {
        #[doc = #doc]
        #[derive(Clone, Debug, PartialEq)]
        pub enum #name { #(#facts(#facts)),* }

        #(
            impl From<#facts> for #name {
                fn from(value: #facts) -> Self {
                    #name::#facts(value)
                }
            }
        )*

        impl<S> #m::Translation<S> for #name
        where
            S: #(#m::Ingests<#facts>)+*,
        {
            fn fact(self) -> #root::Any {
                match self {
                    #(#name::#facts(value) => #m::pack_message(&value),)*
                }
            }
        }
    }
}

/// A domain's typed refusals as one choice (`<Component>Refusal`), when
/// any command of it names one: one arm per refusal message its commands
/// name (typed only, decision 14.95), each once, each `From` its type. What
/// a policy targeting the domain is handed, with the command refused, by
/// its one refusal callback: matching it is exhaustive, so an arm missing
/// from the callback does not compile. [`Self::read`] reads both from the
/// refused answer.
fn refusal_enum(component: &crate::model::Component) -> TokenStream {
    let Some(name) = crate::scaffold::refusal_choice(component) else {
        return quote!();
    };
    let name = format_ident!("{name}");
    let mut refusals: Vec<MessageDescriptor> = Vec::new();
    for refusal in component
        .inputs
        .iter()
        .filter(|i| !i.fact)
        .flat_map(|i| &i.refuses)
    {
        if !refusals.contains(refusal) {
            refusals.push(refusal.clone());
        }
    }
    let doc = format!(
        " A typed refusal of {}'s commands: one arm per refusal message they name. \
         What a policy sending {} is handed by its one refusal callback.",
        component.name, component.name
    );
    let choice = plain_enum(&name, &refusals, &doc);
    let command = format_ident!("{}Command", component.name);
    let root = root();
    quote! {
        #choice

        impl #name {
            /// The command refused and the refusal it was refused with,
            /// read from the answer a policy's refusal callback is handed.
            /// A bug when the answer holds none: the kernel hands the
            /// callback only a typed refusal of a command the domain decides.
            pub fn read(refused: &Refused) -> Result<(#command, #name), Bug> {
                use #root::Payload as _;
                let command = #command::from_any(refused.command_payload())
                    .map_err(|_| Bug::new("refused-command-unknown"))?;
                let object = refused
                    .refusal_object()
                    .ok_or_else(|| Bug::new("refusal-without-object"))?;
                let refusal = #name::from_any(object)
                    .map_err(|_| Bug::new("refusal-unknown"))?;
                Ok((command, refusal))
            }
        }
    }
}

/// The `Payload` of the enum `name` of `variants`: each variant packed as
/// its message, each type read back by its type URL, any other refused as
/// the kernel's unknown type, and the types listed.
fn payload(name: &syn::Ident, variants: &[syn::Ident]) -> TokenStream {
    let root = root();
    let m = quote!(#root::markup);
    let packing = if variants.is_empty() {
        quote!(match *self {})
    } else {
        quote!(match self { #(#name::#variants(value) => #m::pack_message(value),)* })
    };
    quote! {
        impl #root::Payload for #name {
            fn to_any(&self) -> #root::Any {
                #packing
            }

            fn from_any(any: &#root::Any) -> Result<Self, #root::PayloadError> {
                #(
                    if let Some(value) = #m::decode_message::<#variants>(any)? {
                        return Ok(#name::#variants(value));
                    }
                )*
                Err(#root::PayloadError::UnknownType {
                    type_url: any.type_url.clone(),
                })
            }

            fn type_urls() -> Vec<String> {
                [#(#m::type_url_of::<#variants>()),*].to_vec()
            }
        }
    }
}

fn enumeration(e: &EnumDescriptor) -> TokenStream {
    let name = ident(&enum_name(e));
    let prefix = format!("{}_", screaming(e.name()));
    let values: Vec<(syn::Ident, i32)> = e
        .values()
        .filter(|v| v.number() != 0)
        .map(|v| {
            let bare = v.name().strip_prefix(&prefix).unwrap_or(v.name());
            (ident(&camel(bare)), v.number())
        })
        .collect();
    let variants = values.iter().map(|(v, _)| v);
    let into = values.iter().map(|(v, n)| quote!(#name::#v => #n));
    let from = values.iter().map(|(v, n)| quote!(#n => Ok(#name::#v)));
    quote! {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum #name { #(#variants),* }

        impl From<#name> for i32 {
            fn from(value: #name) -> i32 {
                match value { #(#into),* }
            }
        }

        impl TryFrom<i32> for #name {
            type Error = i32;

            fn try_from(value: i32) -> Result<Self, i32> {
                match value {
                    #(#from,)*
                    other => Err(other),
                }
            }
        }
    }
}

/// Each id newtype the package's messages declare (`id_type` on a UUID
/// field), once: angzarr-proto's `Uuid` under the name, with its
/// conversions both ways and its text (canonical hyphenated, as `Uuid`'s).
fn id_types(package: &Package) -> TokenStream {
    let root = root();
    let uuid = quote!(#root::v1::Uuid);
    let mut seen: Vec<String> = Vec::new();
    let mut items = TokenStream::new();
    for field in package.messages.iter().flat_map(|m| m.fields()) {
        let Some(id) = crate::options::id_type(&field) else {
            continue;
        };
        if seen.contains(&id) {
            continue;
        }
        let doc = format!(" An id: a UUID, as `{}` holds it.", field.full_name());
        let name = ident(&id);
        items.extend(quote! {
            #[doc = #doc]
            #[derive(Clone, Debug, Default, PartialEq)]
            pub struct #name(pub #uuid);

            impl From<#uuid> for #name {
                fn from(value: #uuid) -> Self {
                    #name(value)
                }
            }

            impl From<#name> for #uuid {
                fn from(value: #name) -> Self {
                    value.0
                }
            }

            impl ::std::fmt::Display for #name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    ::std::fmt::Display::fmt(&self.0, f)
                }
            }

            impl ::std::str::FromStr for #name {
                type Err = #root::ids::UuidError;

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    s.parse().map(#name)
                }
            }
        });
        seen.push(id);
    }
    items
}

/// Whether `message`'s domain type has a `Default` ([`defaultable`]).
pub(crate) fn has_default(message: &MessageDescriptor) -> bool {
    defaultable(message, &mut Vec::new())
}

/// Whether every field of `message` has a `Default`, so its domain type
/// derives one: not a singular enum (closed: its zero is no value), nor a
/// oneof's choice, nor a singular message without one. `visiting` breaks
/// a cycle of singular messages, which has none.
fn defaultable(message: &MessageDescriptor, visiting: &mut Vec<String>) -> bool {
    if visiting.iter().any(|v| v == message.full_name()) {
        return false;
    }
    visiting.push(message.full_name().to_string());
    let all = message.fields().all(|f| {
        if f.is_list() || f.is_map() {
            return true;
        }
        match f.containing_oneof() {
            Some(o) if o.is_synthetic() => return true,
            Some(_) => return false,
            None => {}
        }
        match f.kind() {
            Kind::Enum(_) => false,
            // Timestamp, Duration and UUID, held as they are, are scalars
            // all through: defaultable, as every other message of such
            // fields is.
            Kind::Message(m) => defaultable(&m, visiting),
            _ => true,
        }
    });
    visiting.pop();
    all
}

fn structure(message: &MessageDescriptor, at: &Layer) -> TokenStream {
    let root = root();
    let name = ident(&message_name(message));
    let wire = at.wire_type(message);
    let full = message.full_name();
    let mut fields: Vec<Field> = Vec::new();
    let mut choices = TokenStream::new();
    for f in message.fields() {
        match f.containing_oneof() {
            Some(oneof) if !oneof.is_synthetic() => {
                if fields.iter().any(|known| known.proto_name == oneof.name()) {
                    continue;
                }
                let (field, choice) = Field::choice(&oneof, at);
                fields.push(field);
                choices.extend(choice);
            }
            _ => fields.push(Field::of(&f, at)),
        }
    }
    let decls = fields.iter().map(|f| {
        let (n, ty) = (&f.name, f.shape.domain_type());
        quote!(pub #n: #ty)
    });
    let to = fields.iter().map(|f| {
        let n = &f.name;
        let value = f.shape.encoded(quote!(value.#n));
        quote!(#n: #value)
    });
    let from = fields.iter().map(|f| {
        let n = &f.name;
        let value = f.shape.decoded(quote!(value.#n), full, &f.proto_name);
        quote!(#n: #value)
    });
    let param = if fields.is_empty() {
        quote!(_value)
    } else {
        quote!(value)
    };
    let derives = if defaultable(message, &mut Vec::new()) {
        quote!(#[derive(Clone, Debug, Default, PartialEq)])
    } else {
        quote!(#[derive(Clone, Debug, PartialEq)])
    };
    quote! {
        #derives
        pub struct #name { #(#decls),* }

        impl From<#name> for #wire {
            fn from(#param: #name) -> Self {
                #wire { #(#to),* }
            }
        }

        impl TryFrom<#wire> for #name {
            type Error = #root::WireError;

            fn try_from(#param: #wire) -> Result<Self, Self::Error> {
                Ok(#name { #(#from),* })
            }
        }

        #choices
    }
}

/// One field: its Rust name, its proto name (for errors) and its shape.
struct Field {
    name: syn::Ident,
    proto_name: String,
    shape: Shape,
}

impl Field {
    fn of(field: &FieldDescriptor, at: &Layer) -> Field {
        let shape = if field.is_map() {
            let Kind::Message(entry) = field.kind() else {
                unreachable!("a map field's kind is its entry message")
            };
            Shape::Map(
                Elem::of(&entry.map_entry_key_field(), at).domain_type(),
                Elem::of(&entry.map_entry_value_field(), at),
            )
        } else if field.is_list() {
            Shape::Repeated(Elem::of(field, at))
        } else if field.containing_oneof().is_some_and(|o| o.is_synthetic()) {
            Shape::Optional(Elem::of(field, at))
        } else {
            Shape::Single(Elem::of(field, at))
        };
        Field {
            name: ident(field.name()),
            proto_name: field.name().to_string(),
            shape,
        }
    }

    /// The field holding `oneof`'s choice, and the choice: an enum of a
    /// variant per arm, named after the message and the oneof.
    fn choice(oneof: &prost_reflect::OneofDescriptor, at: &Layer) -> (Field, TokenStream) {
        let name = ident(&choice_name(oneof));
        let arms: Vec<ChoiceArm> = oneof
            .fields()
            .map(|f| ChoiceArm {
                variant: ident(&upper_camel(f.name())),
                proto_name: f.name().to_string(),
                elem: Elem::of(&f, at),
            })
            .collect();
        let variants = arms.iter().map(|a| {
            let (v, ty) = (&a.variant, a.elem.domain_type());
            quote!(#v(#ty))
        });
        let doc = format!(" The choice of `{}`: one of its arms.", oneof.full_name());
        let declaration = quote! {
            #[doc = #doc]
            #[derive(Clone, Debug, PartialEq)]
            pub enum #name { #(#variants),* }
        };
        let field = Field {
            name: ident(oneof.name()),
            proto_name: oneof.name().to_string(),
            shape: Shape::Choice {
                domain: name,
                wire: at.wire_choice(oneof),
                arms,
            },
        };
        (field, declaration)
    }
}

/// One arm of a oneof: its variant, in the domain choice and prost's, its
/// proto name and its value.
struct ChoiceArm {
    variant: syn::Ident,
    proto_name: String,
    elem: Elem,
}

/// What one value of a field is.
enum Elem {
    /// A scalar: the same Rust type on both sides.
    Scalar(TokenStream),
    /// A message held as its own type on both sides: angzarr-embedded's
    /// `Timestamp`, prost's `Duration` or angzarr-proto's `Uuid`.
    AsIs(TokenStream),
    /// An id newtype of angzarr-proto's `Uuid` (`id_type`).
    Id(TokenStream),
    /// A domain enum; `i32` on the wire.
    Enum(TokenStream),
    /// A domain message, and its wire type.
    Message(TokenStream, TokenStream),
}

impl Elem {
    fn of(field: &FieldDescriptor, at: &Layer) -> Elem {
        let root = root();
        match field.kind() {
            Kind::Message(m) if m.full_name() == TIMESTAMP => Elem::AsIs(quote!(#root::Timestamp)),
            Kind::Message(m) if m.full_name() == DURATION => {
                Elem::AsIs(quote!(::prost_types::Duration))
            }
            Kind::Message(m) if m.full_name() == UUID => match crate::options::id_type(field) {
                Some(id) => Elem::Id(at.domain(field.parent_message().package_name(), &id)),
                None => Elem::AsIs(quote!(#root::v1::Uuid)),
            },
            Kind::Message(m) => Elem::Message(
                at.domain(m.package_name(), &message_name(&m)),
                at.wire_type(&m),
            ),
            Kind::Enum(e) => Elem::Enum(at.domain(e.package_name(), &enum_name(&e))),
            Kind::Double => Elem::Scalar(quote!(f64)),
            Kind::Float => Elem::Scalar(quote!(f32)),
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Elem::Scalar(quote!(i32)),
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Elem::Scalar(quote!(i64)),
            Kind::Uint32 | Kind::Fixed32 => Elem::Scalar(quote!(u32)),
            Kind::Uint64 | Kind::Fixed64 => Elem::Scalar(quote!(u64)),
            Kind::Bool => Elem::Scalar(quote!(bool)),
            Kind::String => Elem::Scalar(quote!(String)),
            Kind::Bytes => Elem::Scalar(quote!(Vec<u8>)),
        }
    }

    fn domain_type(&self) -> TokenStream {
        match self {
            Elem::Scalar(ty) | Elem::AsIs(ty) | Elem::Id(ty) | Elem::Enum(ty) => ty.clone(),
            Elem::Message(ty, _) => ty.clone(),
        }
    }

    /// The function taking one domain value to its wire value, or `None`
    /// where the two are one type.
    fn wire_fn(&self) -> Option<TokenStream> {
        let root = root();
        match self {
            Elem::Scalar(_) | Elem::AsIs(_) => None,
            Elem::Id(_) => Some(quote!(#root::v1::Uuid::from)),
            Elem::Enum(_) => Some(quote!(i32::from)),
            Elem::Message(_, w) => Some(quote!(#w::from)),
        }
    }

    /// `value`, one wire value, as its domain value (`?` refusing what the
    /// domain type cannot hold), `field` naming it in a refusal.
    fn decoded_one(&self, value: TokenStream, message: &str, field: &str) -> TokenStream {
        let root = root();
        match self {
            Elem::Scalar(_) | Elem::AsIs(_) => value,
            Elem::Id(id) => quote!(#id::from(#value)),
            Elem::Enum(e) => quote! {
                #e::try_from(#value).map_err(|value| #root::WireError::UnknownEnum {
                    message: #message, field: #field, value
                })?
            },
            Elem::Message(d, _) => quote!(#d::try_from(#value)?),
        }
    }
}

/// How a field holds its values.
enum Shape {
    /// One value. A message is required: the wire's absence is refused.
    Single(Elem),
    /// proto3 `optional`: `Option` on both sides.
    Optional(Elem),
    Repeated(Elem),
    /// A map, by its key's Rust type.
    Map(TokenStream, Elem),
    /// A oneof: the domain choice, prost's, and the arms. Required: an
    /// unset oneof is refused.
    Choice {
        domain: syn::Ident,
        wire: TokenStream,
        arms: Vec<ChoiceArm>,
    },
}

impl Shape {
    fn domain_type(&self) -> TokenStream {
        match self {
            Shape::Single(e) => e.domain_type(),
            Shape::Optional(e) => {
                let t = e.domain_type();
                quote!(Option<#t>)
            }
            Shape::Repeated(e) => {
                let t = e.domain_type();
                quote!(Vec<#t>)
            }
            Shape::Map(k, e) => {
                let t = e.domain_type();
                quote!(::std::collections::BTreeMap<#k, #t>)
            }
            Shape::Choice { domain, .. } => quote!(#domain),
        }
    }

    /// `value`, a domain value of this shape, as its wire value.
    fn encoded(&self, value: TokenStream) -> TokenStream {
        match self {
            Shape::Single(e @ (Elem::Scalar(_) | Elem::Enum(_))) => match e.wire_fn() {
                Some(f) => quote!(#f(#value)),
                None => value,
            },
            Shape::Single(e) => match e.wire_fn() {
                Some(f) => quote!(Some(#f(#value))),
                None => quote!(Some(#value)),
            },
            Shape::Optional(e) => match e.wire_fn() {
                Some(f) => quote!(#value.map(#f)),
                None => value,
            },
            Shape::Repeated(e) => match e.wire_fn() {
                Some(f) => quote!(#value.into_iter().map(#f).collect()),
                None => value,
            },
            Shape::Map(_, e) => match e.wire_fn() {
                Some(f) => quote!(#value.into_iter().map(|(k, v)| (k, #f(v))).collect()),
                None => value,
            },
            Shape::Choice { domain, wire, arms } => {
                let arms = arms.iter().map(|a| {
                    let v = &a.variant;
                    let inner = match a.elem.wire_fn() {
                        Some(f) => quote!(#f(v)),
                        None => quote!(v),
                    };
                    quote!(#domain::#v(v) => #wire::#v(#inner))
                });
                quote!(Some(match #value { #(#arms),* }))
            }
        }
    }

    /// `value`, a wire value of this shape, as its domain value, refusing
    /// what the domain type cannot hold.
    fn decoded(&self, value: TokenStream, message: &str, field: &str) -> TokenStream {
        let root = root();
        let missing = quote! {
            #root::WireError::Missing { message: #message, field: #field }
        };
        let unknown = quote! {
            |value| #root::WireError::UnknownEnum { message: #message, field: #field, value }
        };
        let one = |e: &Elem, v: TokenStream| e.decoded_one(v, message, field);
        match self {
            Shape::Single(e @ (Elem::Scalar(_) | Elem::Enum(_))) => one(e, value),
            Shape::Single(e) => one(e, quote!(#value.ok_or(#missing)?)),
            Shape::Optional(Elem::Scalar(_) | Elem::AsIs(_))
            | Shape::Repeated(Elem::Scalar(_) | Elem::AsIs(_))
            | Shape::Map(_, Elem::Scalar(_) | Elem::AsIs(_)) => value,
            Shape::Optional(Elem::Id(id)) => quote!(#value.map(#id::from)),
            Shape::Optional(Elem::Enum(e)) => {
                quote!(#value.map(|v| #e::try_from(v).map_err(#unknown)).transpose()?)
            }
            Shape::Optional(Elem::Message(d, _)) => quote!(#value.map(#d::try_from).transpose()?),
            Shape::Repeated(Elem::Id(id)) => quote!(#value.into_iter().map(#id::from).collect()),
            Shape::Repeated(Elem::Enum(e)) => quote! {
                #value
                    .into_iter()
                    .map(|v| #e::try_from(v).map_err(#unknown))
                    .collect::<Result<Vec<_>, _>>()?
            },
            Shape::Repeated(Elem::Message(d, _)) => quote! {
                #value.into_iter().map(#d::try_from).collect::<Result<Vec<_>, _>>()?
            },
            Shape::Map(_, Elem::Id(id)) => {
                quote!(#value.into_iter().map(|(k, v)| (k, #id::from(v))).collect())
            }
            Shape::Map(_, Elem::Enum(e)) => quote! {
                #value
                    .into_iter()
                    .map(|(k, v)| #e::try_from(v).map(|v| (k, v)).map_err(#unknown))
                    .collect::<Result<::std::collections::BTreeMap<_, _>, _>>()?
            },
            Shape::Map(_, Elem::Message(d, _)) => quote! {
                #value
                    .into_iter()
                    .map(|(k, v)| #d::try_from(v).map(|v| (k, v)))
                    .collect::<Result<::std::collections::BTreeMap<_, _>, _>>()?
            },
            Shape::Choice { domain, wire, arms } => {
                let arms = arms.iter().map(|a| {
                    let v = &a.variant;
                    let inner = a.elem.decoded_one(quote!(v), message, &a.proto_name);
                    quote!(#wire::#v(v) => #domain::#v(#inner))
                });
                quote!(match #value.ok_or(#missing)? { #(#arms),* })
            }
        }
    }
}

/// `name` as an identifier, raw where it is a Rust keyword (`type`).
pub(crate) fn ident(name: &str) -> syn::Ident {
    syn::parse_str::<syn::Ident>(name)
        .unwrap_or_else(|_| syn::Ident::new_raw(name, Span::call_site()))
}

/// `SHIPPED_LATE` as `ShippedLate`.
fn camel(screaming: &str) -> String {
    screaming
        .split('_')
        .filter(|w| !w.is_empty())
        .map(|w| {
            let lower = w.to_ascii_lowercase();
            let mut chars = lower.chars();
            chars
                .next()
                .map(|c| c.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

/// The modules of the prost types of `files`' packages (`<package>.rs` in
/// `OUT_DIR`), nested as the packages are (`io::angzarr::examples::v1`) so
/// prost's `super::` paths resolve, each package's module carrying the
/// allowance its output needs (its oneofs nest past the complexity gate's
/// threshold): what a crate root `include!`s as its wire types, no
/// allowance of its own. angzarr's own package is angzarr-proto's, never
/// here.
pub(crate) fn wire_modules(packages: &[String]) -> String {
    /// One module of the tree: its children by name, and the package whose
    /// types it holds, when one ends here.
    #[derive(Default)]
    struct Node {
        children: std::collections::BTreeMap<String, Node>,
        package: Option<String>,
    }
    fn tokens(name: &str, node: &Node) -> TokenStream {
        let ident = format_ident!("{name}");
        let children = node.children.iter().map(|(n, child)| tokens(n, child));
        let (allow, include) = match &node.package {
            Some(package) => {
                let file = format!("/{package}.rs");
                (
                    quote!(#[allow(clippy::excessive_nesting)]),
                    quote!(include!(concat!(env!("OUT_DIR"), #file));),
                )
            }
            None => (quote!(), quote!()),
        };
        quote! {
            #allow
            pub mod #ident {
                #include
                #(#children)*
            }
        }
    }
    let mut root = Node::default();
    for package in packages {
        let mut node = &mut root;
        for part in package.split('.') {
            node = node.children.entry(part.to_string()).or_default();
        }
        node.package = Some(package.clone());
    }
    let modules = root.children.iter().map(|(n, child)| tokens(n, child));
    let file: syn::File = syn::parse2(quote!(#(#modules)*)).expect("the emitter writes valid Rust");
    format!(
        "// @generated by angzarr-codegen. Do not edit: every build rewrites it.\n\n{}",
        prettyplease::unparse(&file)
    )
}
