//! The handler-contract tier's `compensation.feature` (angzarr decision
//! 14.74), vendored in `handler-contract/`, run against the markup's
//! compensation tooling ([`Compensation`]): the shipment process manager of
//! the tier's `fixture/compensation.proto`, its messages written here as
//! prost declares them, its destinations as their stand-ins declare them
//! (the inventory takes ReleaseStock as a fact and RestockItems as a
//! command; the payment domain RefundPayment as a fact).

use std::fmt;

use angzarr_embedded::markup::{
    CommandTarget, Compensation, DomainMessage, EventSource, Ingests, TypeName, UndoTargetsCommand,
};
use angzarr_embedded::WireError;
use cucumber::{given, then, when};

/// The fixture's package.
const PACKAGE: &str = "test.compensation";

/// A fixture message: its prost declaration under `test.compensation`, a
/// domain message of its own wire type.
macro_rules! message {
    ($name:ident { $($field:ident: $tag:literal),* }) => {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub(crate) struct $name {
            $(#[prost(string, tag = $tag)] pub(crate) $field: String,)*
        }

        impl ::prost::Name for $name {
            const NAME: &'static str = stringify!($name);
            const PACKAGE: &'static str = PACKAGE;
        }

        impl DomainMessage for $name {
            type Wire = $name;

            fn to_wire(&self) -> $name {
                self.clone()
            }

            fn from_wire(wire: $name) -> Result<Self, WireError> {
                Ok(wire)
            }
        }
    };
}

message!(StockReserved {
    reservation_id: "1"
});
message!(PaymentCharged { charge_id: "1" });
message!(ReleaseStock {
    reservation_id: "1"
});
message!(RestockItems {
    reservation_id: "1"
});
message!(RefundPayment { charge_id: "1" });

/// The shipment's state: the journal, folded.
#[derive(Clone, Debug, Default)]
pub(crate) struct ShipmentState {
    reservation_id: String,
    charge_id: String,
    failed: bool,
}

/// The shipment's events, as a scenario names them.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Event {
    StockReserved,
    PaymentCharged,
    ShipmentFailed(String),
    StockReleaseDue,
    RefundDue,
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::ShipmentFailed(reason) => write!(f, "ShipmentFailed {reason:?}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl From<StockReserved> for Event {
    fn from(_: StockReserved) -> Event {
        Event::StockReserved
    }
}

impl From<PaymentCharged> for Event {
    fn from(_: PaymentCharged) -> Event {
        Event::PaymentCharged
    }
}

/// The inventory, as its stand-in declares it: RestockItems a command,
/// ReleaseStock a fact.
pub(crate) struct Inventory;

impl CommandTarget for Inventory {
    const DOMAIN: &'static str = "inventory";

    fn command_types() -> Vec<String> {
        vec![TypeName::of::<RestockItems>().full_name()]
    }
}

impl EventSource for Inventory {
    const DOMAIN: &'static str = "inventory";

    fn event_types() -> Vec<String> {
        Vec::new()
    }
}

impl Ingests<ReleaseStock> for Inventory {}

/// The payment domain: RefundPayment a fact.
pub(crate) struct Payment;

impl CommandTarget for Payment {
    const DOMAIN: &'static str = "payment";

    fn command_types() -> Vec<String> {
        Vec::new()
    }
}

impl EventSource for Payment {
    const DOMAIN: &'static str = "payment";

    fn event_types() -> Vec<String> {
        Vec::new()
    }
}

impl Ingests<RefundPayment> for Payment {}

/// One scenario: the shipment's state, which undo "reserve stock" becomes,
/// its compensation once registered (or registration's refusal), and what
/// it recorded.
#[derive(Default, cucumber::World)]
pub(crate) struct Shipment {
    state: ShipmentState,
    /// "reserve stock"'s undo becomes RestockItems, a command, not
    /// ReleaseStock.
    restocks: bool,
    registered: Option<Result<Compensation<ShipmentState, Event>, UndoTargetsCommand>>,
    recorded: Vec<Event>,
}

impl fmt::Debug for Shipment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shipment")
            .field("state", &self.state)
            .field("recorded", &self.recorded)
            .finish()
    }
}

/// The shipment's steps, in order, with "reserve stock"'s undo becoming
/// RestockItems where `restocks`, else ReleaseStock; refused at
/// registration where an undo becomes a command.
fn steps(restocks: bool) -> Result<Compensation<ShipmentState, Event>, UndoTargetsCommand> {
    let compensation = Compensation::new(
        |_: &ShipmentState, reason: String| Event::ShipmentFailed(reason),
        |state: &ShipmentState| state.failed,
    );
    let reserved = |state: &ShipmentState| {
        (!state.reservation_id.is_empty()).then(|| StockReserved {
            reservation_id: state.reservation_id.clone(),
        })
    };
    let release = |_: &StockReserved| Event::StockReleaseDue;
    let compensation = if restocks {
        compensation.step::<Inventory, RestockItems, _>("reserve stock", reserved, release)
    } else {
        compensation.step::<Inventory, ReleaseStock, _>("reserve stock", reserved, release)
    }?;
    compensation.step::<Payment, RefundPayment, _>(
        "charge payment",
        |state: &ShipmentState| {
            (!state.charge_id.is_empty()).then(|| PaymentCharged {
                charge_id: state.charge_id.clone(),
            })
        },
        |_: &PaymentCharged| Event::RefundDue,
    )
}

impl Shipment {
    /// Its steps' registration, on first use (the shipment starting).
    fn registration(&mut self) -> &Result<Compensation<ShipmentState, Event>, UndoTargetsCommand> {
        let restocks = self.restocks;
        self.registered.get_or_insert_with(|| steps(restocks))
    }

    /// Its compensation, registered.
    fn compensation(&mut self) -> &Compensation<ShipmentState, Event> {
        match self.registration() {
            Ok(compensation) => compensation,
            Err(refused) => panic!("registration refused: {refused}"),
        }
    }

    /// The step `name` completed in the state.
    fn complete(&mut self, name: &str) {
        match name {
            "reserve stock" => self.state.reservation_id = "r1".to_string(),
            "charge payment" => self.state.charge_id = "c1".to_string(),
            other => panic!("no step {other:?}"),
        }
    }
}

#[given("the shipment process manager's steps, registered at startup")]
fn registered(_: &mut Shipment) {}

#[given(regex = r#"^"reserve stock" is registered with an undo that becomes "([^"]+)"$"#)]
fn undo_becomes(w: &mut Shipment, target: String) {
    w.restocks = match target.as_str() {
        "test.compensation.RestockItems" => true,
        "test.compensation.ReleaseStock" => false,
        other => panic!("no fixture type {other}"),
    };
}

#[given(regex = r#"^the shipment has completed "([^"]+)"(?: and "([^"]+)")?$"#)]
fn completed(w: &mut Shipment, first: String, second: String) {
    w.complete(&first);
    if !second.is_empty() {
        w.complete(&second);
    }
}

#[given("the shipment has completed no step")]
fn completed_none(_: &mut Shipment) {}

#[given(regex = r#"^the shipment has completed "([^"]+)" and rolled back$"#)]
fn rolled_back(w: &mut Shipment, done: String) {
    w.complete(&done);
    w.state.failed = true;
}

#[when(regex = r#"^it rolls back because "([^"]+)"$"#)]
fn rolls_back(w: &mut Shipment, reason: String) {
    let state = w.state.clone();
    w.recorded = w.compensation().rollback(&state, reason);
}

#[when(regex = r#"^the reply completing "charge payment" arrives$"#)]
fn reply(w: &mut Shipment) {
    let state = w.state.clone();
    let charged = PaymentCharged {
        charge_id: "c1".to_string(),
    };
    w.recorded = w.compensation().reply(&state, charged);
}

#[when("the shipment process manager starts")]
fn starts(w: &mut Shipment) {
    let _ = w.registration();
}

/// `listed`, "A, B and C" or "A and B" or "A", as each event's text.
fn listed(listed: &str) -> Vec<String> {
    let (rest, last) = listed.rsplit_once(" and ").unwrap_or(("", listed));
    rest.split(", ")
        .filter(|s| !s.is_empty())
        .chain(std::iter::once(last))
        .map(str::to_string)
        .collect()
}

#[then(regex = r"^it records (.+?)(?:, in that order| alone)$")]
fn records(w: &mut Shipment, events: String) {
    let recorded: Vec<String> = w.recorded.iter().map(Event::to_string).collect();
    assert_eq!(recorded, listed(&events));
}

#[then("registration succeeds")]
fn registration_succeeds(w: &mut Shipment) {
    if let Err(refused) = w.registration() {
        panic!("registration refused: {refused}");
    }
}

#[then(
    regex = r#"^registration is refused, naming the step "([^"]+)" and its undo's target, the command "([^"]+)"$"#
)]
fn registration_refused(w: &mut Shipment, name: String, command: String) {
    let Err(refused) = w.registration() else {
        panic!("registration succeeded: {w:?}");
    };
    assert_eq!(
        (refused.step, refused.command.as_str()),
        (name.as_str(), command.as_str())
    );
}
