//! The generated adapters' snapshots, through the scaffold of the day: the
//! orders' implementation, whose host config entry sets
//! `scaffold.snapshots` (`angzarr.yaml`), overrides its interface's
//! `snapshot` and `restore`; the catalog's keeps their defaults and takes
//! none. Each is registered as its anchor either way.

#[cfg(test)]
mod tests {
    use angzarr_embedded::markup::DomainMessage;
    use angzarr_embedded::v1::{Snapshot, Uuid};
    use angzarr_embedded::{NotSnapshotted, Registry, State, pack};

    use crate::adapters;
    use crate::catalog_state_impl::CatalogStateImpl;
    use crate::domain::catalog::{CatalogState, Item};
    use crate::domain::common::Money;
    use crate::domain::order::{CustomerId, OrderState};
    use crate::order_state_impl::OrderStateImpl;
    use crate::wire::shapes::order::v1 as order_wire;

    fn item(sku: &str) -> Item {
        Item {
            sku: sku.to_string(),
            price: Money {
                cents: 5,
                currency: "EUR".to_string(),
            },
            note: None,
        }
    }

    /// The orders after one order of two of `a` and one of `b`.
    fn placed() -> adapters::OrderState {
        adapters::OrderState(OrderStateImpl {
            state: OrderState {
                placed_at: Some(angzarr_embedded::Timestamp {
                    seconds: 3,
                    nanos: 0,
                }),
                skus: ["a", "b", "a"].map(String::from).to_vec(),
                counts: [("a".to_string(), 2), ("b".to_string(), 1)].into(),
                customer: Some(CustomerId(Uuid::from_bytes([7; 16]))),
            },
        })
    }

    /// The catalog after listing one `a`.
    fn listed() -> adapters::CatalogState {
        adapters::CatalogState(CatalogStateImpl {
            state: CatalogState {
                items: vec![item("a")],
                stock: [("a".to_string(), 1)].into(),
            },
        })
    }

    /// The registry `register()` fills.
    fn registered() -> Registry {
        let mut registry = Registry::new();
        crate::register(&mut registry).expect("it registers");
        registry
    }

    /// Each aggregate is registered as its anchor, whether it takes
    /// snapshots or not.
    #[test]
    fn each_aggregate_is_registered_as_its_anchor() {
        let registry = registered();
        let state = |domain: &str| registry.catalog().state(domain).map(|s| s.type_name);
        assert_eq!(state("orders").as_deref(), Some("shapes.order.v1.OrderState"));
        assert_eq!(
            state("catalog").as_deref(),
            Some("shapes.catalog.v1.CatalogState")
        );
    }

    /// The scaffold the configuration names offers the orders' state as
    /// its anchor's wire type, and restores it.
    #[test]
    fn the_configured_scaffold_snapshots_its_state_as_the_anchors_wire_type() {
        let orders = placed();
        let snapshot: order_wire::OrderState = State::snapshot(&orders).expect("it offers one");
        assert_eq!(snapshot.skus, ["a", "b", "a"]);
        assert_eq!(snapshot.counts.get("a"), Some(&2));
        assert_eq!(
            snapshot.customer.as_ref().map(|c| c.value.to_vec()),
            Some(vec![7; 16])
        );
        let mut onto = placed();
        onto.0.state.skus.clear();
        State::restore(&mut onto, snapshot).expect("it restores");
        assert_eq!(onto.state, orders.state);
    }

    /// The catalog keeps its interface's defaults: it offers no snapshot,
    /// and refuses to restore one. A wire value its anchor's domain type
    /// cannot hold (an item without its required price) is refused first,
    /// naming the field; either way the state is kept.
    #[test]
    fn the_unconfigured_scaffold_takes_no_snapshot() {
        let mut catalog = listed();
        assert_eq!(State::snapshot(&catalog), None);
        let before = catalog.state.clone();
        let wire = catalog.state.to_wire();
        assert_eq!(
            State::restore(&mut catalog, wire.clone()),
            Err(angzarr_embedded::markup::Report::of(&NotSnapshotted {
                anchor: "shapes.catalog.v1.CatalogState".to_string()
            }))
        );
        let mut priceless = wire;
        priceless.items[0].price = None;
        let refused = State::restore(&mut catalog, priceless).expect_err("no price");
        assert!(format!("{refused:?}").contains("price"), "{refused:?}");
        assert_eq!(catalog.state, before);
    }

    /// Registered by register(), the orders' snapshot is usable at their
    /// rules version, and one of another state type is not.
    #[test]
    fn a_registered_domains_snapshot_of_its_anchor_is_usable() {
        let registry = registered();
        let catalog = registry.catalog();
        let snapshot = |state| Snapshot {
            sequence: 1,
            state: Some(state),
            rules_version: catalog.rules_version("orders").expect("registered"),
        };
        let orders = snapshot(pack(&State::snapshot(&placed()).expect("it offers one")));
        assert!(catalog.snapshot_usable("orders", &orders));
        let other = snapshot(pack(&listed().state.to_wire()));
        assert!(!catalog.snapshot_usable("orders", &other));
    }
}
