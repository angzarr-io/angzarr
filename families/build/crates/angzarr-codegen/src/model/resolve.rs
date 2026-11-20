//! The resolver: the rules the declarations as a whole must keep
//! (component-options.md section 4 and the failure list), held once, for
//! the lint, the descriptors and every generator alike. A rule that finds
//! a fault refuses at the first, in the order they are written here; the
//! spec fixes no order between rules, so a declaration breaking two is
//! refused for the earlier.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    ComponentKind, Declared, Input, Markup, MarkupError, MessageDescriptor, strings_field, values,
};

/// The framework's own refusal, raised by the coordinator and listed in no
/// command's `refuses` (decision 14.94).
const FRAMEWORK_REFUSAL: &str = "io.angzarr.v1.ExpectedSequenceMismatch";

/// A message carrying both `(command)` and `(fact)`, refused: a message is
/// one or the other (decision 14.91).
pub(super) fn one_option(
    markup: &Markup<'_>,
    declaring: &[MessageDescriptor],
) -> Result<(), MarkupError> {
    for message in declaring {
        let command = !values(message, markup.command.as_ref()).is_empty();
        let fact = !values(message, markup.fact.as_ref()).is_empty();
        if command && fact {
            return Err(MarkupError::CommandAndFact {
                message: message.full_name().to_string(),
            });
        }
    }
    Ok(())
}

/// Every rule over the declared components, `declaring` the messages of the
/// whole set and `files` those of the files generated.
pub(super) fn check(
    markup: &Markup<'_>,
    declaring: &[MessageDescriptor],
    files: &[MessageDescriptor],
    declared: &[Declared],
) -> Result<(), MarkupError> {
    folding(markup, declaring)?;
    let owners = markup.owners(declaring);
    for d in declared {
        names_an_event(d)?;
        one_source(d)?;
        starts_from_its_inputs(d)?;
        has_a_source(&owners, d)?;
        source_takes_a_fact(declared, d)?;
    }
    refusal_statuses(markup, declaring, files, declared)
}

/// An event folded by two stateful components, and a domain override on an
/// event whose owner is in the set.
fn folding(markup: &Markup<'_>, declaring: &[MessageDescriptor]) -> Result<(), MarkupError> {
    let stateful = markup.domains();
    for message in declaring {
        let consumers: Vec<(String, String)> = markup
            .consumers(message)
            .iter()
            .map(|c| {
                (
                    super::string_field(c, "component"),
                    super::string_field(c, "domain"),
                )
            })
            .collect();
        let mut folders: Vec<&str> = Vec::new();
        for (component, _) in &consumers {
            if stateful.contains_key(component) && !folders.contains(&component.as_str()) {
                folders.push(component);
            }
        }
        if let [first, second, ..] = folders[..] {
            return Err(MarkupError::EventFoldedTwice {
                event: message.full_name().to_string(),
                first: first.to_string(),
                second: second.to_string(),
            });
        }
        if !folders.is_empty()
            && let Some((component, domain)) = consumers.iter().find(|(_, d)| !d.is_empty())
        {
            return Err(MarkupError::OverrideOnOwned {
                event: message.full_name().to_string(),
                component: component.clone(),
                domain: domain.clone(),
            });
        }
    }
    Ok(())
}

/// A policy or projector reacts to or folds the events it names: with none
/// it would do nothing.
fn names_an_event(d: &Declared) -> Result<(), MarkupError> {
    let c = &d.component;
    if matches!(c.kind, ComponentKind::Policy | ComponentKind::Projector) && c.events.is_empty() {
        return Err(MarkupError::NoEvent {
            component: c.anchor.full_name().to_string(),
        });
    }
    Ok(())
}

/// A policy translates one source domain.
fn one_source(d: &Declared) -> Result<(), MarkupError> {
    let c = &d.component;
    if c.kind == ComponentKind::Policy
        && let [first, second, ..] = &c.sources[..]
    {
        return Err(MarkupError::PolicySources {
            policy: c.anchor.full_name().to_string(),
            first: first.clone(),
            second: second.clone(),
        });
    }
    Ok(())
}

/// Each starting type of a process manager is one of its own commands or
/// facts.
fn starts_from_its_inputs(d: &Declared) -> Result<(), MarkupError> {
    let c = &d.component;
    for starting in &c.starting_types {
        if c.input(starting).is_none() {
            return Err(MarkupError::StartingTypeNotInput {
                component: c.anchor.full_name().to_string(),
                starting_type: starting.clone(),
            });
        }
    }
    Ok(())
}

/// Each event a policy or projector names has a source: an owner in the
/// set, else the domain its entry gives.
fn has_a_source(owners: &BTreeMap<String, String>, d: &Declared) -> Result<(), MarkupError> {
    if d.component.kind.is_domain() {
        return Ok(());
    }
    for (event, domain) in &d.entries {
        if domain.is_empty() && !owners.contains_key(event.full_name()) {
            return Err(MarkupError::EventWithoutSource {
                component: d.component.anchor.full_name().to_string(),
                event: event.full_name().to_string(),
            });
        }
    }
    Ok(())
}

/// A policy whose target has a command that can refuse needs a source with
/// a fact to translate the refusal into (when both are in the set).
fn source_takes_a_fact(declared: &[Declared], d: &Declared) -> Result<(), MarkupError> {
    let c = &d.component;
    if c.kind != ComponentKind::Policy {
        return Ok(());
    }
    let owner = |domain: &str| {
        declared
            .iter()
            .map(|d| &d.component)
            .find(|o| o.kind.is_domain() && o.domain == domain)
    };
    let (Some(target), Some(source)) = (owner(&c.output_domain), owner(&c.input_domain)) else {
        return Ok(());
    };
    let can_refuse = |i: &Input| !i.fact && !i.refuses.is_empty();
    if target.inputs.iter().any(can_refuse) && !source.inputs.iter().any(|i| i.fact) {
        return Err(MarkupError::SourceTakesNoFact {
            policy: c.anchor.full_name().to_string(),
            domain: c.input_domain.clone(),
        });
    }
    Ok(())
}

/// Every refusal the generated commands name declares its status, and only
/// a refusal does.
fn refusal_statuses(
    markup: &Markup<'_>,
    declaring: &[MessageDescriptor],
    files: &[MessageDescriptor],
    declared: &[Declared],
) -> Result<(), MarkupError> {
    if markup.refusal_status.is_none() {
        return Ok(());
    }
    for refusal in declared
        .iter()
        .flat_map(|d| &d.component.inputs)
        .flat_map(|i| &i.refuses)
    {
        if markup.status(refusal).is_none_or(|status| status == 0) {
            return Err(MarkupError::RefusalStatusMissing {
                refusal: refusal.full_name().to_string(),
            });
        }
    }
    let refused: BTreeSet<String> = declaring
        .iter()
        .flat_map(|m| values(m, markup.command.as_ref()))
        .flat_map(|command| strings_field(&command, "refuses"))
        .collect();
    for message in files {
        let name = message.full_name();
        if markup.status(message).is_some() && name != FRAMEWORK_REFUSAL && !refused.contains(name)
        {
            return Err(MarkupError::RefusalStatusMisplaced {
                message: name.to_string(),
            });
        }
    }
    Ok(())
}
