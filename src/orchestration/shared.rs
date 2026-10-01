//! Shared orchestration helpers used by saga and process manager flows.

use crate::orchestration::correlation::ANGZARR_UUID_NAMESPACE;
use crate::proto::{CommandBook, Cover, EventBook};

/// Derive a stable aggregate root UUID from a workflow correlation id.
///
/// Extension trait (project rule: id/proto helpers are trait methods, not
/// free functions) implementing decision **D-11 (O7)**: every
/// `correlation → root` site MUST agree on the derivation, otherwise a
/// rejection notification stamped with one root can never reach the PM
/// state persisted under a different root.
///
/// Derivation rule (identical at every call site):
/// - If `self` already parses as a UUID it passes through **unchanged**.
///   Existing UUID-keyed workflows keep their historical root, so no
///   already-persisted PM state is orphaned.
/// - Otherwise (a friendly / non-UUID id — which the router deliberately
///   still accepts, rather than rejecting as a client-contract break) the
///   root is `UUIDv5(ANGZARR_UUID_NAMESPACE, id)`. Pre-D-11 every non-UUID
///   id collapsed to the NIL uuid, so *all* friendly-id workflows shared a
///   single root and rejection notifications routed to the wrong, shared
///   aggregate.
///
/// The namespace is the project's canonical deterministic-UUID namespace
/// (`ANGZARR_UUID_NAMESPACE` = `UUIDv5(NAMESPACE_DNS, "angzarr.dev")`),
/// reused rather than re-hardcoded so the whole framework derives ids from
/// one fixed namespace.
pub trait CorrelationRootExt {
    /// The PM / provenance aggregate root for this correlation id.
    fn correlation_root(&self) -> uuid::Uuid;
}

impl CorrelationRootExt for str {
    fn correlation_root(&self) -> uuid::Uuid {
        match uuid::Uuid::parse_str(self) {
            // Already a UUID: pass through so existing roots are stable.
            Ok(already_uuid) => already_uuid,
            // Friendly id: derive a deterministic, per-id root.
            Err(_) => uuid::Uuid::new_v5(&ANGZARR_UUID_NAMESPACE, self.as_bytes()),
        }
    }
}

/// Backfill `correlation_id` on a cover only when it is currently empty.
///
/// Shared by [`fill_correlation_id`] (commands) and
/// [`fill_fact_correlation_id`] (facts). An explicitly-set correlation is
/// never overwritten — a saga/PM may deliberately route a command or fact
/// into a different workflow context.
fn fill_cover_correlation(cover: &mut Option<Cover>, correlation_id: &str) {
    if let Some(cover) = cover {
        if cover.correlation_id.is_empty() {
            cover.correlation_id = correlation_id.to_string();
        }
    }
}

/// Ensure correlation_id is set on all command covers.
///
/// Fills in the correlation_id on any command whose cover has an empty one.
pub fn fill_correlation_id(commands: &mut [CommandBook], correlation_id: &str) {
    for command in commands.iter_mut() {
        fill_cover_correlation(&mut command.cover, correlation_id);
    }
}

/// Ensure correlation_id is set on all injected fact (event) covers.
///
/// O10: commands emitted by a saga/PM get the workflow correlation_id
/// backfilled, but injected FACTS did not. Downstream process managers skip
/// events whose correlation_id is empty (empty correlation ⇒ no PM trigger),
/// so a fact injected without the workflow correlation silently fails to
/// advance any correlated PM. Mirrors [`fill_correlation_id`] for the fact
/// path.
pub fn fill_fact_correlation_id(facts: &mut [EventBook], correlation_id: &str) {
    for fact in facts.iter_mut() {
        fill_cover_correlation(&mut fact.cover, correlation_id);
    }
}

/// Response metadata key carrying CONTINUE-mode reaction errors from a
/// saga/PM coordinator to the aggregate that cascaded into it: a binary
/// `CommandResponse` whose only populated field is `reaction_errors`.
pub const REACTION_ERRORS_METADATA: &str = "angzarr-reaction-errors-bin";

/// Attach reaction errors to a coordinator response (no-op when empty).
pub fn attach_reaction_errors<T>(
    response: &mut tonic::Response<T>,
    reaction_errors: Vec<crate::proto::CascadeReactionError>,
) {
    use prost::Message;
    if reaction_errors.is_empty() {
        return;
    }
    let encoded = crate::proto::CommandResponse {
        reaction_errors,
        ..Default::default()
    }
    .encode_to_vec();
    response.metadata_mut().insert_bin(
        REACTION_ERRORS_METADATA,
        tonic::metadata::MetadataValue::from_bytes(&encoded),
    );
}

/// Reaction errors a coordinator response carries (empty when none, or when
/// the metadata is unreadable).
pub fn read_reaction_errors(
    metadata: &tonic::metadata::MetadataMap,
) -> Vec<crate::proto::CascadeReactionError> {
    use prost::Message;
    metadata
        .get_bin(REACTION_ERRORS_METADATA)
        .and_then(|value| value.to_bytes().ok())
        .and_then(|bytes| crate::proto::CommandResponse::decode(bytes.as_ref()).ok())
        .map(|response| response.reaction_errors)
        .unwrap_or_default()
}

/// A saga/PM command that could not be delivered.
#[derive(Debug, Clone)]
pub(crate) struct UndeliveredCommand {
    pub command: CommandBook,
    pub code: tonic::Code,
    pub reason: String,
}

impl UndeliveredCommand {
    /// The `CommandResponse.reaction_errors` entry for this failure.
    pub(crate) fn reaction_error(&self, component: &str) -> crate::proto::CascadeReactionError {
        use crate::proto::command_page;
        let command_type = self
            .command
            .pages
            .first()
            .and_then(|page| match &page.payload {
                Some(command_page::Payload::Command(any)) => {
                    Some(crate::proto_ext::type_url::fqn(&any.type_url).to_string())
                }
                _ => None,
            })
            .unwrap_or_default();
        crate::proto::CascadeReactionError {
            component: component.to_string(),
            target: self.command.cover.clone(),
            command_type,
            code: self.code as i32,
            message: self.reason.clone(),
        }
    }
}

/// The Compensate marker for events a delivered command produced: a fact
/// for the command's target aggregate listing those sequences.
///
/// The marker's external id is derived from the component and the
/// compensated sequences, so a redelivered compensation is deduplicated by
/// the target's fact idempotency. Returns `None` when the command produced
/// no events (nothing to compensate).
pub(crate) fn compensate_marker(
    executed: &EventBook,
    component: &str,
    reason: &str,
) -> Option<EventBook> {
    use crate::proto::{
        event_page, page_header::SequenceType, Compensate, EventPage, ExternalDeferredSequence,
        PageHeader,
    };
    use crate::proto_ext::EventPageExt;
    use prost::Message;

    let cover = executed.cover.clone()?;
    let sequences: Vec<u32> = executed.pages.iter().map(|p| p.sequence_num()).collect();
    if sequences.is_empty() {
        return None;
    }
    let external_id = format!(
        "compensate:{component}:{}:{}:{}",
        cover.domain,
        crate::proto_ext::CoverExt::root_id_hex(&cover).unwrap_or_default(),
        sequences
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    let marker = Compensate {
        sequences,
        reason: reason.to_string(),
        command_type: String::new(),
    };
    Some(EventBook {
        cover: Some(cover),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::ExternalDeferred(ExternalDeferredSequence {
                    external_id,
                    description: format!("{component}: cascade compensation"),
                })),
            }),
            payload: Some(event_page::Payload::Event(prost_types::Any {
                type_url: crate::proto_ext::type_url::COMPENSATE.to_string(),
                value: marker.encode_to_vec(),
            })),
            ..Default::default()
        }],
        ..Default::default()
    })
}

/// Write Compensate markers to the targets of the commands a saga/PM already
/// delivered in this invocation (CASCADE_ERROR_COMPENSATE).
///
/// Markers are persisted without the target's fact handler. Returns a
/// description of every marker that could not be written.
pub(crate) async fn write_compensate_markers(
    fact_executor: Option<&dyn super::FactExecutor>,
    executed: &[EventBook],
    component: &str,
    reason: &str,
) -> Vec<String> {
    let markers: Vec<EventBook> = executed
        .iter()
        .filter_map(|events| compensate_marker(events, component, reason))
        .collect();
    let Some(fact_executor) = fact_executor else {
        return markers
            .iter()
            .map(|m| {
                format!(
                    "{}: no fact executor to write the Compensate marker",
                    m.cover.as_ref().map(|c| c.domain.as_str()).unwrap_or("?")
                )
            })
            .collect();
    };
    let mut failures = Vec::new();
    for marker in markers {
        let domain = marker
            .cover
            .as_ref()
            .map(|c| c.domain.clone())
            .unwrap_or_default();
        let delivery = super::FactDelivery {
            sync_mode: crate::proto::SyncMode::Async,
            skip_handler: true,
        };
        if let Err(e) = fact_executor.inject(marker, delivery).await {
            failures.push(format!("{domain}: {e}"));
        }
    }
    failures
}

/// Apply the caller's cascade error mode to a finished delivery.
///
/// FAIL_FAST and COMPENSATE fail the orchestration (COMPENSATE first writes
/// Compensate markers to the targets of the commands already delivered);
/// CONTINUE succeeds and returns one reaction error per undelivered command;
/// DEAD_LETTER and bus-driven delivery succeed (failures are dead-lettered).
pub(crate) async fn settle_delivery(
    policy: super::command::DeliveryPolicy,
    component: &str,
    undelivered: &[UndeliveredCommand],
    executed: &[EventBook],
    fact_executor: Option<&dyn super::FactExecutor>,
) -> Result<Vec<crate::proto::CascadeReactionError>, crate::bus::BusError> {
    if undelivered.is_empty() {
        return Ok(Vec::new());
    }
    let detail = undelivered
        .iter()
        .map(|u| {
            format!(
                "{}: {}",
                crate::proto_ext::CoverExt::domain(&u.command),
                u.reason
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    match policy {
        super::command::DeliveryPolicy::Continue => Ok(undelivered
            .iter()
            .map(|u| u.reaction_error(component))
            .collect()),
        super::command::DeliveryPolicy::Background | super::command::DeliveryPolicy::DeadLetter => {
            Ok(Vec::new())
        }
        super::command::DeliveryPolicy::FailFast | super::command::DeliveryPolicy::Compensate => {
            let mut message = format!("{component}: undeliverable commands: {detail}");
            if policy == super::command::DeliveryPolicy::Compensate {
                let failures =
                    write_compensate_markers(fact_executor, executed, component, &detail).await;
                if !failures.is_empty() {
                    message.push_str(&format!(
                        "; Compensate markers not written: {}",
                        failures.join("; ")
                    ));
                }
            }
            Err(crate::bus::BusError::Grpc(tonic::Status::aborted(message)))
        }
    }
}

#[cfg(test)]
#[path = "shared.test.rs"]
mod tests;
