//! Every domain type converts to its wire type and back unchanged, through
//! prost's bytes too; and a wire value the domain cannot hold is refused,
//! naming the field.

use angzarr_embedded::markup::DomainMessage;
use angzarr_embedded::v1::Uuid;
use angzarr_embedded::{Timestamp, WireError};
use proptest::prelude::*;
use prost::Message;

use crate::domain::catalog::{CatalogState, Item, ItemListed, ListItem, ListingId};
use crate::domain::common::Money;
use crate::domain::order::{
    CustomerId, OrderPlaced, OrderState, PlaceOrder, PlaceOrderGift, PlaceOrderGiftWrap,
    PlaceOrderPayment, PlaceOrderSpeed,
};
use crate::wire::shapes::order::v1 as order_wire;

fn uuid() -> impl Strategy<Value = Uuid> {
    any::<[u8; 16]>().prop_map(Uuid::from_bytes)
}

fn timestamp() -> impl Strategy<Value = Timestamp> {
    (any::<i64>(), any::<i32>()).prop_map(|(seconds, nanos)| Timestamp { seconds, nanos })
}

fn duration() -> impl Strategy<Value = prost_types::Duration> {
    (any::<i64>(), any::<i32>()).prop_map(|(seconds, nanos)| prost_types::Duration { seconds, nanos })
}

fn money() -> impl Strategy<Value = Money> {
    (any::<i64>(), ".*").prop_map(|(cents, currency)| Money { cents, currency })
}

fn item() -> impl Strategy<Value = Item> {
    (".*", money(), proptest::option::of(".*")).prop_map(|(sku, price, note)| Item {
        sku,
        price,
        note,
    })
}

fn speed() -> impl Strategy<Value = PlaceOrderSpeed> {
    prop_oneof![Just(PlaceOrderSpeed::Slow), Just(PlaceOrderSpeed::Fast)]
}

fn gift() -> impl Strategy<Value = PlaceOrderGift> {
    (".*", ".*").prop_map(|(code, paper)| PlaceOrderGift {
        code,
        wrap: PlaceOrderGiftWrap { paper },
    })
}

fn payment() -> impl Strategy<Value = PlaceOrderPayment> {
    prop_oneof![
        ".*".prop_map(PlaceOrderPayment::Card),
        money().prop_map(PlaceOrderPayment::Credit),
        gift().prop_map(PlaceOrderPayment::Gift),
        speed().prop_map(PlaceOrderPayment::Speed),
        timestamp().prop_map(PlaceOrderPayment::DeferredUntil),
    ]
}

prop_compose! {
    fn place_order()(
        items in proptest::collection::vec(item(), 0..4),
        order in uuid(),
        customer in proptest::option::of(uuid().prop_map(CustomerId)),
        related in proptest::collection::vec(uuid(), 0..3),
        wait in duration(),
        grace in proptest::option::of(duration()),
        payment in payment(),
        pace in speed(),
    ) -> PlaceOrder {
        PlaceOrder { items, order, customer, related, wait, grace, payment, pace }
    }
}

prop_compose! {
    fn order_state()(
        placed_at in proptest::option::of(timestamp()),
        skus in proptest::collection::vec(".*", 0..3),
        counts in proptest::collection::btree_map(".*", any::<u32>(), 0..3),
        customer in proptest::option::of(uuid().prop_map(CustomerId)),
    ) -> OrderState {
        OrderState { placed_at, skus, counts, customer }
    }
}

prop_compose! {
    fn order_placed()(
        items in proptest::collection::vec(item(), 0..3),
        customer in proptest::option::of(uuid().prop_map(CustomerId)),
        wait in duration(),
        at in proptest::option::of(timestamp()),
    ) -> OrderPlaced {
        OrderPlaced { items, customer, wait, at }
    }
}

prop_compose! {
    fn catalog_state()(
        items in proptest::collection::vec(item(), 0..3),
        stock in proptest::collection::btree_map(".*", any::<u32>(), 0..3),
    ) -> CatalogState {
        CatalogState { items, stock }
    }
}

/// `value` to its wire type, through prost's bytes, and back.
fn through_the_wire<D>(value: &D) -> Result<D, WireError>
where
    D: DomainMessage,
    D::Wire: Message + Default,
{
    let bytes = value.to_wire().encode_to_vec();
    let wire = D::Wire::decode(bytes.as_slice()).expect("prost reads its own bytes");
    D::from_wire(wire)
}

proptest! {
    #[test]
    fn place_order_round_trips(value in place_order()) {
        prop_assert_eq!(through_the_wire(&value), Ok(value));
    }

    #[test]
    fn order_state_round_trips(value in order_state()) {
        prop_assert_eq!(through_the_wire(&value), Ok(value));
    }

    #[test]
    fn order_placed_round_trips(value in order_placed()) {
        prop_assert_eq!(through_the_wire(&value), Ok(value));
    }

    #[test]
    fn catalog_state_round_trips(value in catalog_state()) {
        prop_assert_eq!(through_the_wire(&value), Ok(value));
    }

    #[test]
    fn list_item_and_item_listed_round_trip(item in item(), listing in uuid()) {
        let list = ListItem { item: item.clone(), listing: ListingId(listing.clone()) };
        prop_assert_eq!(through_the_wire(&list), Ok(list));
        let listed = ItemListed { item, listing: ListingId(listing) };
        prop_assert_eq!(through_the_wire(&listed), Ok(listed));
    }
}

/// An unset oneof, a required UUID, a required message of another
/// package's, and an enum number with no value are each refused, naming
/// the field.
#[test]
fn what_the_domain_cannot_hold_is_refused() {
    let whole = || {
        order_wire::PlaceOrder::from(PlaceOrder {
            items: Vec::new(),
            order: Uuid::from_bytes([1; 16]),
            customer: None,
            related: Vec::new(),
            wait: prost_types::Duration::default(),
            grace: None,
            payment: PlaceOrderPayment::Card("c".to_string()),
            pace: PlaceOrderSpeed::Fast,
        })
    };
    let missing = |field| WireError::Missing {
        message: "shapes.order.v1.PlaceOrder",
        field,
    };
    let refused = |wire| PlaceOrder::try_from(wire).unwrap_err();
    assert_eq!(
        refused(order_wire::PlaceOrder {
            payment: None,
            ..whole()
        }),
        missing("payment")
    );
    assert_eq!(
        refused(order_wire::PlaceOrder {
            order: None,
            ..whole()
        }),
        missing("order")
    );
    assert_eq!(
        refused(order_wire::PlaceOrder {
            pace: 7,
            ..whole()
        }),
        WireError::UnknownEnum {
            message: "shapes.order.v1.PlaceOrder",
            field: "pace",
            value: 7
        }
    );
    assert_eq!(
        refused(order_wire::PlaceOrder {
            payment: Some(order_wire::place_order::Payment::Speed(0)),
            ..whole()
        }),
        WireError::UnknownEnum {
            message: "shapes.order.v1.PlaceOrder",
            field: "speed",
            value: 0
        }
    );
    let item = crate::wire::shapes::catalog::v1::Item {
        sku: "s".to_string(),
        price: None,
        note: None,
    };
    assert_eq!(
        Item::try_from(item).unwrap_err(),
        WireError::Missing {
            message: "shapes.catalog.v1.Item",
            field: "price"
        }
    );
}

/// An anchor's `Default` is the empty state, and an id newtype's text
/// is its UUID's.
#[test]
fn a_state_defaults_empty_and_an_id_reads_as_its_uuid() {
    assert_eq!(
        OrderState::default(),
        OrderState {
            placed_at: None,
            skus: Vec::new(),
            counts: Default::default(),
            customer: None
        }
    );
    let text = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    let id: CustomerId = text.parse().unwrap();
    assert_eq!(id.to_string(), text);
    assert_eq!(Uuid::from(id.clone()), id.0);
}
