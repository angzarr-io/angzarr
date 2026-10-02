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

/// What a saga/PM orchestration reports to a synchronous caller.
#[derive(Debug, Clone, Default)]
pub struct ReactionReport {
    /// CONTINUE-mode failures.
    pub reaction_errors: Vec<crate::proto::CascadeReactionError>,
    /// Commands their targets executed, with the events they produced.
    pub executed: Vec<ExecutedCommand>,
}

/// A saga/PM command its target executed successfully in this invocation,
/// with the events it produced (if any).
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutedCommand {
    /// The command as delivered (stamped provenance).
    pub command: CommandBook,
    /// The events the target persisted for it.
    pub events: Option<EventBook>,
}

/// Response metadata key carrying the reaction commands a saga/PM coordinator
/// executed in a CASCADE_ERROR_COMPENSATE request, so the aggregate that
/// cascaded into it can compensate them when another reaction fails.
pub const EXECUTED_REACTIONS_METADATA: &str = "angzarr-executed-reactions-bin";

/// Attach `executed` to a coordinator response (no-op when empty).
///
/// Encoded as a `SagaResponse` whose `commands[i]` produced `events[i]`
/// (an empty book when it produced none). Payload bytes are dropped: a
/// Compensate needs only each command's cover, provenance and type, and the
/// sequences of its events.
pub fn attach_executed_reactions<T>(
    response: &mut tonic::Response<T>,
    executed: &[ExecutedCommand],
) {
    use prost::Message;
    if executed.is_empty() {
        return;
    }
    let mut commands = Vec::with_capacity(executed.len());
    let mut events = Vec::with_capacity(executed.len());
    for item in executed {
        let mut command = item.command.clone();
        for page in &mut command.pages {
            if let Some(crate::proto::command_page::Payload::Command(any)) = page.payload.as_mut() {
                any.value.clear();
            }
        }
        commands.push(command);
        let mut produced = item.events.clone().unwrap_or_default();
        produced.snapshot = None;
        for page in &mut produced.pages {
            page.payload = None;
        }
        events.push(produced);
    }
    let encoded = crate::proto::SagaResponse { commands, events }.encode_to_vec();
    response.metadata_mut().insert_bin(
        EXECUTED_REACTIONS_METADATA,
        tonic::metadata::MetadataValue::from_bytes(&encoded),
    );
}

/// The executed reaction commands a coordinator response reports (empty when
/// none, or when the metadata is unreadable).
pub fn read_executed_reactions(metadata: &tonic::metadata::MetadataMap) -> Vec<ExecutedCommand> {
    use prost::Message;
    let Some(response) = metadata
        .get_bin(EXECUTED_REACTIONS_METADATA)
        .and_then(|value| value.to_bytes().ok())
        .and_then(|bytes| crate::proto::SagaResponse::decode(bytes.as_ref()).ok())
    else {
        return Vec::new();
    };
    let mut events = response.events.into_iter();
    response
        .commands
        .into_iter()
        .map(|command| ExecutedCommand {
            command,
            events: events.next().filter(|book| !book.pages.is_empty()),
        })
        .collect()
}

/// Record the RejectionNotification of a rejected deferred command in the
/// coordinator's compensation outbox and attempt its delivery to the
/// command's source. A command without deferred provenance has no source to
/// notify. Only a failure to record is an error: the caller must not
/// acknowledge its trigger.
pub async fn record_rejection(
    outbox: Option<&std::sync::Arc<super::outbox::Outbox>>,
    command: &CommandBook,
    reason: &str,
    code: &str,
) -> Result<(), super::outbox::OutboxError> {
    let Some(envelope) = super::compensation::rejection_envelope(command, reason, code) else {
        tracing::warn!(
            domain = %crate::proto_ext::CoverExt::domain(command),
            "rejected command carries no deferred provenance; no source to notify"
        );
        return Ok(());
    };
    let Some(outbox) = outbox else {
        tracing::error!(
            domain = %crate::proto_ext::CoverExt::domain(command),
            reason,
            "rejected command's notification dropped: no compensation outbox is wired"
        );
        return Ok(());
    };
    outbox
        .submit(super::outbox::OutboxEntry::notification(envelope)?)
        .await
        .map(|_| ())
}

/// Record one Compensate notification per executed command (addressed to
/// the command's target, `reason` being the failure that aborted the
/// request) in the coordinator's compensation outbox and attempt their
/// delivery. Returns a description of every notification that could not be
/// recorded.
pub async fn record_compensations(
    outbox: Option<&std::sync::Arc<super::outbox::Outbox>>,
    executed: &[ExecutedCommand],
    reason: &str,
) -> Vec<String> {
    let mut failures = Vec::new();
    for item in executed {
        let domain = crate::proto_ext::CoverExt::domain(&item.command).to_string();
        let Some(outbox) = outbox else {
            failures.push(format!("{domain}: no compensation outbox is wired"));
            continue;
        };
        let envelope =
            super::compensation::compensate_envelope(&item.command, item.events.as_ref(), reason);
        let recorded = match super::outbox::OutboxEntry::notification(envelope) {
            Ok(entry) => outbox.submit(entry).await.map(|_| ()),
            Err(e) => Err(e),
        };
        if let Err(e) = recorded {
            failures.push(format!("{domain}: {e}"));
        }
    }
    failures
}

/// Apply the caller's cascade error mode to a finished delivery.
///
/// FAIL_FAST and COMPENSATE fail the orchestration (COMPENSATE first records
/// a Compensate notification for every command already executed);
/// CONTINUE succeeds and returns one reaction error per undelivered command;
/// DEAD_LETTER and bus-driven delivery succeed (failures are dead-lettered).
pub(crate) async fn settle_delivery(
    policy: super::command::DeliveryPolicy,
    component: &str,
    undelivered: &[UndeliveredCommand],
    executed: &[ExecutedCommand],
    outbox: Option<&std::sync::Arc<super::outbox::Outbox>>,
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
                let failures = record_compensations(outbox, executed, &detail).await;
                if !failures.is_empty() {
                    message.push_str(&format!(
                        "; Compensate notifications not recorded: {}",
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
