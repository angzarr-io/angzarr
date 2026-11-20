//! The language-neutral rules every backend shares: how a proto name is
//! cased and localised, and how a typed refusal is keyed (decision 14.50)
//! The Rust and the
//! Python backends both depend on this module; neither depends on the
//! other.

use prost_reflect::{EnumDescriptor, Kind, MessageDescriptor};

/// The diagnostic key of a typed refusal (decision 14.50): its message's
/// full name, then the reason's wire name when it has one (its `Reason`
/// value's name without the enum's prefix), `.` and `_` as `-`, all
/// lower-case: `refusals.v1.BetRefused` refusing `REASON_ALREADY_BET` is
/// `refusals-v1-betrefused-already-bet`. The generated `Diagnostic::code`
/// is this; a hand-written refusal reproduces it with this function.
pub fn refusal_key(full_name: &str, reason: Option<&str>) -> String {
    let key = match reason {
        Some(reason) => format!("{full_name}-{reason}"),
        None => full_name.to_string(),
    };
    key.replace(['.', '_'], "-").to_lowercase()
}

/// A typed refusal's rejection codes (decision 14.50): the nested `Reason`
/// enum it declares as its singular `reason` field, which its diagnostic
/// key reads; `None` for a refusal without one.
pub(crate) fn refusal_reason(message: &MessageDescriptor) -> Option<EnumDescriptor> {
    let field = message
        .get_field_by_name("reason")
        .filter(|f| !f.is_list())?;
    match field.kind() {
        Kind::Enum(e) if e.name() == "Reason" && e.parent_message().as_ref() == Some(message) => {
            Some(e)
        }
        _ => None,
    }
}

/// The arm a policy sends one of its target's inputs in (decision 14.69):
/// a Fact for a type its destination's `(command).fact` declares one, a
/// Command otherwise. The generated code reads it; business code never
/// picks it (decision 14.73).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Arm {
    Command,
    Fact,
}

impl Arm {
    /// The arm `input` is sent in.
    pub(crate) fn of(input: &crate::model::Input) -> Arm {
        if input.fact { Arm::Fact } else { Arm::Command }
    }
}

/// The name of what a policy sends `target` in its domain layer
/// (`<Target>Output`): each of its inputs, commands and facts alike, as
/// `PolicyReaction.outputs` carries them (decision 14.70).
pub(crate) fn output_name(target: &crate::model::Component) -> String {
    format!("{}Output", target.name)
}

/// The commands of `target` whose refusal reaches a policy sending to it
/// (decisions 14.89, 14.95): each that declares a `refuses` type. A command
/// naming none has no typed refusal for the policy's one callback to read,
/// and a fact is never refused.
pub(crate) fn handed_refusals(
    target: &crate::model::Component,
) -> impl Iterator<Item = &crate::model::Input> {
    target
        .inputs
        .iter()
        .filter(|i| !i.fact && !i.refuses.is_empty())
}

/// The name of the choice a command refusing several types refuses with
/// (`<Command>Refuses`) in the domain layer, which no message or enum may
/// take.
pub(crate) fn refusal_choice_name(command: &MessageDescriptor) -> String {
    format!("{}Refuses", command.name())
}

/// An enum value's wire name: its name without the enum's prefix
/// (`REASON_ALREADY_BET` of `Reason` as `ALREADY_BET`), what ErrorInfo
/// carries and an arg holds.
pub(crate) fn wire_name<'a>(e: &EnumDescriptor, value: &'a str) -> &'a str {
    let prefix = format!("{}_", screaming(e.name()));
    value.strip_prefix(&prefix).unwrap_or(value)
}

/// A type's plain name: its full name without `package`, its enclosing
/// messages' names, then its own (`SeatOutcome.Outcome` as
/// `SeatOutcomeOutcome`).
pub(crate) fn local_name(full: &str, package: &str) -> String {
    full.strip_prefix(package)
        .map_or(full, |rest| rest.trim_start_matches('.'))
        .replace('.', "")
}

/// A message's domain name: its [`local_name`] (`PlaceOrder.Gift` as
/// `PlaceOrderGift`).
pub(crate) fn message_name(m: &MessageDescriptor) -> String {
    local_name(m.full_name(), m.package_name())
}

/// The domain name of a oneof's choice: its message's domain name, then
/// the oneof's name in upper camel case (`PlaceOrder.payment` as
/// `PlaceOrderPayment`).
pub(crate) fn choice_name(oneof: &prost_reflect::OneofDescriptor) -> String {
    format!(
        "{}{}",
        message_name(oneof.parent_message()),
        upper_camel(oneof.name())
    )
}

/// `deferred_until` as `DeferredUntil`, as prost names a oneof and its
/// arms.
pub(crate) fn upper_camel(snake: &str) -> String {
    snake
        .split('_')
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut chars = w.chars();
            chars
                .next()
                .map(|c| c.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

/// An enum's domain name: its [`local_name`].
pub(crate) fn enum_name(e: &EnumDescriptor) -> String {
    local_name(e.full_name(), e.package_name())
}

/// `BuyInWallet` as `buy_in_wallet`: a word starts at an uppercase letter
/// after a lowercase one or a digit, or before a lowercase one in a run of
/// uppercase (`HTTPServer` as `http_server`).
pub(crate) fn snake(camel: &str) -> String {
    let chars: Vec<char> = camel.chars().collect();
    let mut out = String::new();
    for (i, c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            let before = chars[i - 1];
            let after = chars.get(i + 1);
            if before.is_ascii_lowercase()
                || before.is_ascii_digit()
                || (before.is_ascii_uppercase() && after.is_some_and(char::is_ascii_lowercase))
            {
                out.push('_');
            }
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// `CartState` as `CART_STATE`.
pub(crate) fn screaming(camel: &str) -> String {
    let mut out = String::new();
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

/// `text` wrapped at `width` columns, on spaces: prose for a generated
/// comment or docstring.
pub(crate) fn wrap(text: &str, width: usize) -> String {
    let mut lines: Vec<String> = vec![String::new()];
    for word in text.split(' ') {
        let line = lines.last_mut().expect("a line");
        if !line.is_empty() && line.len() + 1 + word.len() > width {
            lines.push(word.to_string());
        } else {
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }
    }
    lines.join("\n")
}
