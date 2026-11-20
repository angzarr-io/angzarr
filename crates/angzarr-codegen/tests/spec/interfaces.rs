//! Whens and thens on the Rust the generator writes for the handlers: the
//! interfaces (one trait per component, in the domain layer) and the
//! adapters (each handler's attribute and signature), read as syntax. The
//! scenarios say "the handler interfaces are generated"; the Rust is the
//! one language bound here (Python's are the Python generator's).

use cucumber::{then, when};

use crate::protos::full;
use crate::world::Generator;

/// Tokens as text without spacing.
fn text(tokens: impl quote::ToTokens) -> String {
    tokens.to_token_stream().to_string().replace(' ', "")
}

/// `Name` of a full or scenario name (`order.OrderPlaced`).
fn short(name: &str) -> &str {
    name.rsplit_once('.').map_or(name, |(_, last)| last)
}

/// `RecordFundsHeld` as `record_funds_held`.
fn snake(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// The domain layer of every package, as one file.
fn domain(w: &mut Generator) -> syn::File {
    let source = w.interfaces().domain.clone();
    syn::parse_file(&source).unwrap_or_else(|e| panic!("{e}:\n{source}"))
}

/// The interface of the component anchored on `name`: its trait,
/// `<name>Interface`.
fn interface(file: &syn::File, name: &str) -> syn::ItemTrait {
    let trait_name = format!("{name}Interface");
    file.items
        .iter()
        .find_map(|item| match item {
            syn::Item::Trait(t) if t.ident == trait_name => Some(t.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no trait {name}"))
}

/// The methods of a trait.
fn trait_methods(t: &syn::ItemTrait) -> Vec<&syn::TraitItemFn> {
    t.items
        .iter()
        .filter_map(|i| match i {
            syn::TraitItem::Fn(f) => Some(f),
            _ => None,
        })
        .collect()
}

/// The variants of the enum `name`, by name.
fn variants(file: &syn::File, name: &str) -> Vec<String> {
    file.items
        .iter()
        .find_map(|item| match item {
            syn::Item::Enum(e) if e.ident == name => {
                Some(e.variants.iter().map(|v| v.ident.to_string()).collect())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no enum {name}"))
}

/// The method `name` of any trait of the domain layer.
fn handler(file: &syn::File, name: &str) -> syn::TraitItemFn {
    file.items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Trait(t) => Some(t),
            _ => None,
        })
        .flat_map(trait_methods)
        .find(|f| f.sig.ident == name)
        .cloned()
        .unwrap_or_else(|| panic!("no handler {name}"))
}

/// The items of the generated module `adapters`.
fn adapter_items(w: &mut Generator) -> Vec<syn::Item> {
    let source = w.interfaces().adapters.clone();
    let file = syn::parse_file(&source).unwrap_or_else(|e| panic!("{e}:\n{source}"));
    file.items
        .into_iter()
        .find_map(|item| match item {
            syn::Item::Mod(m) if m.ident == "adapters" => m.content.map(|(_, items)| items),
            _ => None,
        })
        .expect("the adapters module")
}

/// The marked-up (inherent) impl of the component `name` in the adapters.
fn adapter(w: &mut Generator, name: &str) -> syn::ItemImpl {
    adapter_items(w)
        .into_iter()
        .find_map(|item| match item {
            syn::Item::Impl(i) if i.trait_.is_none() && text(&i.self_ty) == name => Some(i),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no adapter of {name}"))
}

/// The adapter method that handles `message`, with its attribute `(name,
/// args)`: the first method marked for it.
fn adapter_handler(w: &mut Generator, message: &str) -> (syn::ImplItemFn, String, String) {
    let items = adapter_items(w);
    for item in items {
        let syn::Item::Impl(imp) = item else { continue };
        for f in imp.items {
            let syn::ImplItem::Fn(f) = f else { continue };
            for attr in &f.attrs {
                let name = text(attr.path());
                let args = match &attr.meta {
                    syn::Meta::List(list) => text(&list.tokens),
                    _ => String::new(),
                };
                let handles = name == "handles_cmd" || name == "handles_fact";
                if handles && args.split(',').next() == Some(message) {
                    return (f.clone(), name, args);
                }
            }
        }
    }
    panic!("no handler of {message} in the adapters");
}

#[when(
    regex = r#"^(?:the handler interfaces are generated(?: again)?|angzarr-cli scaffolds the component)$"#
)]
fn interfaces_generated(w: &mut Generator) {
    w.generate_interfaces();
}

#[when(
    regex = r#"^"([^"]+)"'s option is changed to \((command|fact)\) with the same component and emits$"#
)]
fn option_changed(w: &mut Generator, message: String, option: String) {
    w.protos
        .message(&full(&message))
        .input
        .as_mut()
        .expect("an input")
        .fact = option == "fact";
}

#[when(regex = r#"^the (\w+) handler refuses with "([^"]+)"$"#)]
fn handler_refuses(w: &mut Generator, _handler: String, refusal: String) {
    w.last_refusal = Some(full(&refusal));
}

// ---- C-0612, C-0861: the policy's one refusal callback

/// The policy `name`'s interface, and its refusal callbacks.
fn callbacks(w: &mut Generator, name: &str) -> Vec<syn::TraitItemFn> {
    let anchor = w.named(name);
    let file = domain(w);
    let t = interface(&file, short(&anchor));
    trait_methods(&t)
        .into_iter()
        .filter(|f| f.sig.ident == "refused")
        .cloned()
        .collect()
}

/// The argument types of a method, after its receiver.
fn parameters(f: &syn::TraitItemFn) -> Vec<(String, String)> {
    f.sig
        .inputs
        .iter()
        .filter_map(|input| match input {
            syn::FnArg::Typed(t) => Some((text(&t.pat), text(&t.ty))),
            syn::FnArg::Receiver(_) => None,
        })
        .collect()
}

#[then(
    regex = r#"^"([\w-]+)" must implement one refusal callback taking the refused command and the refusal$"#
)]
fn one_callback(w: &mut Generator, policy: String) {
    let found = callbacks(w, &policy);
    let [callback] = found.as_slice() else {
        panic!("{} refusal callbacks, not one", found.len());
    };
    let names: Vec<String> = parameters(callback).into_iter().map(|(n, _)| n).collect();
    assert_eq!(&names[..2], ["command", "refusal"], "{names:?}");
}

#[then(regex = r#"^the refusal is a choice of exactly "([^"]+)" and "([^"]+)"$"#)]
fn refusal_choice(w: &mut Generator, first: String, second: String) {
    let policy = w.last_component.clone().expect("a policy");
    let callback = callbacks(w, &policy).remove(0);
    let (_, choice) = parameters(&callback)[1].clone();
    let mut arms = variants(&domain(w), &choice);
    arms.sort();
    let mut wanted = vec![short(&first).to_string(), short(&second).to_string()];
    wanted.sort();
    assert_eq!(arms, wanted);
}

#[then(regex = r#"^the callback answers a fact of "([\w-]+)" or a Bug$"#)]
fn answers_a_fact(w: &mut Generator, domain_name: String) {
    let policy = w.last_component.clone().expect("a policy");
    let callback = callbacks(w, &policy).remove(0);
    let returns = text(&callback.sig.output);
    // The source's facts: those of its domain's component.
    let source = w
        .protos
        .messages
        .iter()
        .find(|(_, m)| {
            m.component
                .as_ref()
                .is_some_and(|c| c.domain.as_deref() == Some(domain_name.as_str()))
        })
        .map(|(name, _)| short(name).to_string())
        .expect("the source's component");
    assert_eq!(returns, format!("->Result<{source}Fact,Bug>"));
    let facts = variants(&domain(w), &format!("{source}Fact"));
    assert!(!facts.is_empty(), "{source}Fact holds no fact");
}

#[then("the callback cannot decline the refusal")]
fn cannot_decline(w: &mut Generator) {
    let policy = w.last_component.clone().expect("a policy");
    let callback = callbacks(w, &policy).remove(0);
    assert!(
        !text(&callback.sig.output).contains("Option"),
        "{}",
        text(&callback.sig.output)
    );
    // The markup's callback answers a fact, never an Option of one.
    let imp = adapter(w, short(&w.named(&policy)));
    let markers: Vec<String> = imp
        .items
        .iter()
        .filter_map(|i| match i {
            syn::ImplItem::Fn(f) => Some(f),
            _ => None,
        })
        .flat_map(|f| f.attrs.iter().map(|a| text(a.path())))
        .collect();
    assert_eq!(markers.iter().filter(|m| *m == "on_refused").count(), 1);
}

#[then(regex = r#"^"([\w-]+)" is generated with no refusal callback$"#)]
fn no_callback(w: &mut Generator, policy: String) {
    assert!(callbacks(w, &policy).is_empty());
    let imp = adapter(w, short(&w.named(&policy)));
    assert!(!text(&imp).contains("on_refused"));
}

// ---- C-0625, C-0745: a refusal is typed and refused under its status

#[then(regex = r#"^the (\w+) handler returns either its events or an? (\w+) refusal$"#)]
fn returns_events_or_refusal(w: &mut Generator, message: String, refusal: String) {
    let file = domain(w);
    let f = handler(&file, &snake(&message));
    let returns = text(&f.sig.output);
    assert!(returns.contains("Vec<"), "{returns}");
    assert!(
        returns.ends_with(&format!("Failed<{refusal}>>")),
        "{returns}"
    );
    let (_, _, args) = adapter_handler(w, &message);
    assert!(args.contains(&format!("refuses({refusal})")), "{args}");
}

#[then(
    regex = r#"^the client library packs the (\w+) returned into the refusal's gRPC status details as an Any$"#
)]
fn packed_as_any(w: &mut Generator, refusal: String) {
    // A typed refusal travels as a `DomainMessage` of its wire message,
    // which the macro packs when the handler lists it in `refuses(..)`.
    let source = w.interfaces().domain.clone();
    let packed = format!("DomainMessage for {refusal} {{\n    type Wire = crate::wire::{refusal};");
    assert!(source.contains(&packed), "{source}");
}

/// `INVALID_ARGUMENT` as `InvalidArgument`.
fn camel(screaming: &str) -> String {
    screaming
        .split('_')
        .map(|w| w[..1].to_string() + &w[1..].to_lowercase())
        .collect()
}

#[then(regex = r#"^the command is refused (\w+) with reason ANGZARR_REASON_COMMAND_REFUSED$"#)]
fn refused_under(w: &mut Generator, status: String) {
    let refusal = short(&w.last_refusal.clone().expect("a refusal")).to_string();
    let flat = w.interfaces().domain.split_whitespace().collect::<String>();
    let declared = format!(
        "implRefusalfor{refusal}{{constREFUSAL_STATUS:BusinessStatus=BusinessStatus::{};}}",
        camel(&status)
    );
    assert!(flat.contains(&declared), "{declared}");
    // The adapter refuses under it.
    let adapters = w
        .interfaces()
        .adapters
        .split_whitespace()
        .collect::<String>();
    assert!(
        adapters.contains(&format!(".map_err({refusal}::with_declared_status)")),
        "{adapters}"
    );
}

#[then(regex = r#"^the refusal's error info carries refusal_type "([^"]+)"$"#)]
fn carries_refusal_type(w: &mut Generator, refusal: String) {
    // The typed refusal is the object of the message of that name: the
    // wire type of its domain type is the message the type URL names.
    let source = w.interfaces().domain.clone();
    let name = short(&refusal);
    assert!(
        source.contains(&format!("type Wire = crate::wire::{name};")),
        "{source}"
    );
}

#[then(regex = r#"^the status details carry the "([^"]+)" object$"#)]
fn carries_object(w: &mut Generator, refusal: String) {
    let flat = w
        .interfaces()
        .adapters
        .split_whitespace()
        .collect::<String>();
    assert!(
        flat.contains(&format!("refuses({})", short(&refusal))),
        "{flat}"
    );
}

// ---- C-0648, C-0864: the attribute and the signature follow the option

/// The handler of the input declared last, its attribute and its signature.
fn last_handler(w: &mut Generator) -> (syn::ImplItemFn, String) {
    let message = short(&w.last_input.clone().expect("an input")).to_string();
    let (f, attribute, _) = adapter_handler(w, &message);
    (f, attribute)
}

#[then(regex = r#"^its handler (?:for "([^"]+)" )?is marked #\[(handles_cmd|handles_fact)\]$"#)]
fn marked(w: &mut Generator, _message: String, attribute: String) {
    let (_, found) = last_handler(w);
    assert_eq!(found, attribute);
}

#[then(regex = r#"^its generated signature returns (.+)$"#)]
fn signature_returns(w: &mut Generator, returns: String) {
    let (f, _) = last_handler(w);
    let actual = text(&f.sig.output)
        .trim_start_matches("->")
        .replace("::angzarr_embedded::markup::", "");
    let wanted = returns.replace(' ', "");
    match wanted.split_once("Refuses<..>") {
        // A command's failure is its typed refusals, or, naming none, the
        // `Failed<Unrefusable>` of a command that cannot refuse.
        Some((head, tail)) => {
            assert!(actual.starts_with(head), "{actual}");
            assert!(
                actual.contains("Refuses<") || actual.contains("Unrefusable"),
                "{actual}"
            );
            assert!(actual.ends_with(tail), "{actual}");
        }
        None => assert_eq!(actual, wanted),
    }
}

// ---- C-0794: a policy sends each output in the arm its destination declares

#[then(regex = r#"^the reaction of "([\w-]+)" to "([^"]+)" may return "([^"]+)"$"#)]
fn reaction_may_return(w: &mut Generator, policy: String, event: String, output: String) {
    let anchor = w.named(&policy);
    let file = domain(w);
    let t = interface(&file, short(&anchor));
    let reaction = trait_methods(&t)
        .into_iter()
        .find(|f| f.sig.ident == snake(short(&event)))
        .expect("the reaction")
        .clone();
    let returns = text(&reaction.sig.output);
    assert!(returns.starts_with("->Result<Vec<(Vec<u8>,"), "{returns}");
    let target = returns
        .trim_start_matches("->Result<Vec<(Vec<u8>,")
        .trim_end_matches(")>,Bug>")
        .to_string();
    assert!(
        variants(&file, &target).contains(&short(&output).to_string()),
        "{target} does not hold {output}"
    );
}

#[then(
    regex = r#"^the generated code of "([\w-]+)" sends "([^"]+)" in the Output's (command|fact) arm$"#
)]
fn sends_in_arm(w: &mut Generator, policy: String, output: String, arm: String) {
    let imp = adapter(w, short(&w.named(&policy)));
    let code = text(&imp).replace(['{', '}'], "");
    let call = if arm == "fact" { "fact" } else { "to" };
    let wanted = format!("::{}(output)=>commands.{call}(root,output)", short(&output));
    assert!(code.contains(&wanted), "{wanted}:\n{code}");
}

// ---- C-0797: a handler returns a list of its component's events

#[then(regex = r#"^the (\w+) handler returns a list of the component's events$"#)]
fn returns_a_list(w: &mut Generator, message: String) {
    let file = domain(w);
    let f = handler(&file, &snake(&message));
    let returns = text(&f.sig.output);
    assert!(
        returns.contains("Vec<") && returns.contains("Event>"),
        "{returns}"
    );
}

#[then("it may return an empty list")]
fn may_be_empty(_w: &mut Generator) {
    // The list is a `Vec`, which holds none as well as one or many.
}
