//! The kernel's checks of a policy's answer (decision 14.42): each output
//! addressed to the target domain in the trigger's edition, rooted, never
//! the trigger's own stream; and each stamped as a `ReactionDelivery`.

use angzarr_router::hpb;

use crate::toys::{cover, cover_in, ledger, ledger_root};
use crate::v1::{reaction_delivery, Cover, EventRef};

use super::{answered, check_outputs, Refused};

fn record() -> crate::Any {
    crate::pack(&ledger::Record {
        source_sequence: 0,
        cents: 1,
    })
}

fn to_ledger(body: hpb::output::Body) -> hpb::Output {
    hpb::Output {
        root: Some(ledger_root()),
        body: Some(body),
    }
}

fn ledger_in(edition: Option<&str>) -> Cover {
    Cover {
        domain: "ledger".to_string(),
        root: Some(ledger_root()),
        edition: edition.map(str::to_string),
    }
}

#[test]
fn an_output_is_addressed_to_the_target_in_its_triggers_edition() {
    let trigger = cover_in("account", 1, "what-if");
    let checked = check_outputs(
        "ledger",
        &trigger,
        vec![
            to_ledger(hpb::output::Body::Command(record())),
            to_ledger(hpb::output::Body::Fact(record())),
        ],
    )
    .unwrap();
    assert_eq!(
        checked,
        [
            (
                ledger_in(Some("what-if")),
                reaction_delivery::Body::Command(record())
            ),
            (
                ledger_in(Some("what-if")),
                reaction_delivery::Body::Fact(record())
            ),
        ]
    );
    let main = check_outputs(
        "ledger",
        &cover("account", 1),
        vec![to_ledger(hpb::output::Body::Command(record()))],
    )
    .unwrap();
    assert_eq!(main[0].0, ledger_in(None));
}

#[test]
fn an_output_with_no_root_is_the_policys_bug_naming_its_domain_and_type() {
    for root in [
        None,
        Some(crate::v1::Uuid {
            value: vec![1; 15].into(),
        }),
    ] {
        let refused = check_outputs(
            "ledger",
            &cover("account", 1),
            vec![hpb::Output {
                root: root.clone(),
                body: Some(hpb::output::Body::Command(record())),
            }],
        )
        .unwrap_err();
        assert_eq!(
            refused.code, "ANGZARR_REASON_POLICY_COMMAND_NO_ROOT",
            "{root:?}"
        );
        assert_eq!(
            refused.grpc,
            crate::angzarr_router::error::GrpcCode::Internal
        );
        let extra = |key: &str| refused.extras.get(key).map(String::as_str);
        assert_eq!(extra("domain"), Some("ledger"));
        assert_eq!(extra("command_type"), Some("toy.ledger.cmd.v1.Record"));
    }
}

#[test]
fn an_output_with_no_body_is_the_policys_bug() {
    let refused = check_outputs(
        "ledger",
        &cover("account", 1),
        vec![hpb::Output {
            root: Some(ledger_root()),
            body: None,
        }],
    )
    .unwrap_err();
    assert_eq!(refused.code, "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR");
    assert!(
        refused
            .extras
            .get("cause")
            .is_some_and(|c| c.ends_with("policy-output-no-body")),
        "{refused:?}"
    );
}

#[test]
fn an_output_never_addresses_its_triggers_own_stream() {
    let trigger = Cover {
        domain: "ledger".to_string(),
        root: Some(ledger_root()),
        edition: Some("what-if".to_string()),
    };
    let refused = check_outputs(
        "ledger",
        &trigger,
        vec![to_ledger(hpb::output::Body::Fact(record()))],
    )
    .unwrap_err();
    assert_eq!(refused.code, "ANGZARR_REASON_POLICY_TARGETS_ITS_TRIGGER");
    let extra = |key: &str| refused.extras.get(key).map(String::as_str);
    assert_eq!(
        extra("stream"),
        Some(format!("ledger/{}@what-if", ledger_root()).as_str())
    );
    assert_eq!(extra("command_type"), Some("toy.ledger.cmd.v1.Record"));
    // Its own domain at another root is another stream.
    let elsewhere = check_outputs(
        "ledger",
        &trigger,
        vec![hpb::Output {
            root: Some(crate::toys::root(7)),
            body: Some(hpb::output::Body::Command(record())),
        }],
    );
    assert!(elsewhere.is_ok(), "{elsewhere:?}");
}

#[test]
fn one_bad_output_refuses_the_whole_answer() {
    let refused = check_outputs(
        "ledger",
        &cover("account", 1),
        vec![
            to_ledger(hpb::output::Body::Command(record())),
            hpb::Output {
                root: None,
                body: Some(hpb::output::Body::Command(record())),
            },
        ],
    );
    assert!(refused.is_err());
}

#[test]
fn each_output_is_stamped_its_reaction_identity_commands_and_facts_in_one_index_space() {
    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 4,
    };
    let correlation = crate::toys::correlation(9);
    let orders = answered(
        "bookkeeping",
        &trigger,
        &correlation,
        vec![
            (ledger_in(None), reaction_delivery::Body::Command(record())),
            (ledger_in(None), reaction_delivery::Body::Fact(record())),
        ],
    );
    assert_eq!(orders.len(), 2);
    for (index, order) in orders.iter().enumerate() {
        let delivery = &order.delivery;
        assert_eq!(
            delivery.command_id,
            crate::ids::policy_command_id(&trigger, "bookkeeping", index as u32).ok()
        );
        assert_eq!(delivery.correlation_id, Some(correlation.clone()));
        assert_eq!(delivery.causation, Some(trigger.clone()));
        assert_eq!(delivery.component, "bookkeeping");
        assert_eq!(order.trigger(), &trigger);
        assert_eq!(order.cover(), &ledger_in(None));
    }
    assert_eq!(orders[0].arm(), crate::Arm::Command);
    assert_eq!(orders[1].arm(), crate::Arm::Fact);
}

#[test]
fn a_refusal_reads_back_its_command_its_typed_object_and_its_status() {
    use crate::angzarr_router::error::BusinessStatus;
    let delivery = crate::v1::ReactionDelivery {
        cover: Some(ledger_in(None)),
        body: Some(reaction_delivery::Body::Command(record())),
        ..crate::v1::ReactionDelivery::default()
    };
    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    let refused = Refused::new(
        delivery.clone(),
        BusinessStatus::OutOfRange,
        crate::pack(&crate::toys::ToyRefusal {
            code: "toy-ledger-too-large".to_string(),
        }),
        trigger.clone(),
        crate::v1::EventMeta::default(),
    );
    assert_eq!(refused.command_payload(), &record());
    assert_eq!(refused.delivery(), &delivery);
    assert_eq!(refused.status(), Some(BusinessStatus::OutOfRange));
    assert_eq!(
        refused.refusal_object(),
        Some(&crate::pack(&crate::toys::ToyRefusal {
            code: "toy-ledger-too-large".to_string(),
        }))
    );
    assert_eq!(refused.trigger(), &trigger);
    let report = super::no_refusal_handler(&refused);
    assert_eq!(
        angzarr_diagnostic::Diagnostic::code(&report),
        "ANGZARR_REASON_NO_REFUSAL_HANDLER"
    );
}

/// What [`Witness`] was handed: each trigger's whole metadata.
static WITNESSED: std::sync::Mutex<Vec<crate::v1::EventMeta>> = std::sync::Mutex::new(Vec::new());

/// A policy that sends nothing and keeps the metadata of each event it is
/// handed.
struct Witness;

impl crate::Policy for Witness {
    const NAME: &'static str = "witness";
    type Source = crate::toys::Account;
    type Target = crate::toys::Ledger;

    fn on_event(
        &self,
        _event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<crate::toys::Ledger>>, crate::markup::Report> {
        WITNESSED.lock().unwrap().push(ctx.meta().clone());
        Ok(None)
    }
}

#[test]
fn a_policy_is_handed_its_triggers_whole_metadata() {
    use crate::testing::Harness;
    use crate::toys::{cover, Account, AccountCmd, AccountEvent};
    let mut registry = crate::toys::registry();
    registry.policy(Witness).unwrap();
    let mut account = Harness::<Account>::new(registry, cover("account", 9));
    account.given([AccountEvent::Opened]);
    account.when(AccountCmd::Deposit(5)).events();
    let stored = account.pages()[1].meta.clone().unwrap();
    assert!(stored.causation.is_some() && !stored.rules_version.is_empty());
    assert!(WITNESSED.lock().unwrap().contains(&stored), "{stored:?}");
}
