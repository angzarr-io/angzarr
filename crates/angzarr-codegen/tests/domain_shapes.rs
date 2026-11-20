//! The domain layer of every shape beyond scalars, lists and maps (the
//! `shapes` fixtures): another package's types, referenced through its own
//! layer; angzarr's `UUID` and its id newtypes; `google.protobuf.Duration`;
//! oneofs and nested types; `Default` where every field has one; and a
//! component whose anchor is its state. Read back as Rust items; whether it
//! compiles and round-trips is angzarr-build's `tests/local` shapes-app.

mod common;

use angzarr_codegen::{
    CodegenError, Completion, Existing, Generated, Notes, ProtoError, ScaffoldConfig, ScaffoldFile,
    UseError, adapters, adapters_with, complete, markup_domain, markup_domain_with, scaffold,
    scaffold_with,
};
use common::*;

const SHAPES: &[&str] = &[
    "shapes/common/v1/common.proto",
    "shapes/catalog/v1/catalog.proto",
    "shapes/order/v1/order.proto",
];
const WIRE: &[(&str, &str)] = &[
    ("shapes.common.v1", "crate::wire::shapes::common::v1"),
    ("shapes.catalog.v1", "crate::wire::shapes::catalog::v1"),
    ("shapes.order.v1", "crate::wire::shapes::order::v1"),
];
const DOMAIN: &[(&str, &str)] = &[
    ("shapes.common.v1", "crate::domain::common"),
    ("shapes.catalog.v1", "crate::domain::catalog"),
    ("shapes.order.v1", "crate::domain::order"),
];

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");

/// `actual` is the golden file at `path` (rewritten under `ANGZARR_BLESS`).
fn golden(path: &str, actual: &str) {
    let path = format!("{GOLDEN}/{path}");
    if std::env::var_os("ANGZARR_BLESS").is_some() {
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("no {path}"));
    assert_eq!(actual, expected, "{path}");
}

fn layers() -> Vec<Generated> {
    markup_domain_with(&descriptors(SHAPES), SHAPES, WIRE, DOMAIN).unwrap()
}

fn source(package: &str) -> String {
    let layers = layers();
    layers
        .iter()
        .find(|g| g.package == package)
        .unwrap_or_else(|| panic!("no layer {package}: {layers:#?}"))
        .source
        .clone()
}

fn layer(package: &str) -> syn::File {
    syn::parse_file(&source(package)).expect("generated source parses")
}

fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
    items
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

fn item_struct<'a>(file: &'a syn::File, name: &str) -> Option<&'a syn::ItemStruct> {
    file.items.iter().find_map(|i| match i {
        syn::Item::Struct(s) if s.ident == name => Some(s),
        _ => None,
    })
}

fn fields(file: &syn::File, name: &str) -> Vec<(String, String)> {
    item_struct(file, name)
        .unwrap_or_else(|| panic!("no struct {name}"))
        .fields
        .iter()
        .map(|f| {
            let name = f.ident.as_ref().map(|i| i.to_string()).unwrap_or_default();
            (name, text(&f.ty))
        })
        .collect()
}

fn variants(file: &syn::File, name: &str) -> Vec<(String, String)> {
    file.items
        .iter()
        .find_map(|i| match i {
            syn::Item::Enum(e) if e.ident == name => Some(e),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no enum {name}"))
        .variants
        .iter()
        .map(|v| (v.ident.to_string(), text(&v.fields)))
        .collect()
}

/// `(trait, type)` of every impl in the file.
fn impls(file: &syn::File) -> Vec<(String, String)> {
    file.items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Impl(imp) => Some((
                imp.trait_
                    .as_ref()
                    .map(|(path, _)| text(path))
                    .unwrap_or_default(),
                text(&imp.self_ty),
            )),
            _ => None,
        })
        .collect()
}

/// The derives of the struct or enum `name`, as text.
fn derives(file: &syn::File, name: &str) -> String {
    let attrs = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Struct(s) if s.ident == name => Some(&s.attrs),
            syn::Item::Enum(e) if e.ident == name => Some(&e.attrs),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no type {name}"));
    attrs
        .iter()
        .filter(|a| a.path().is_ident("derive"))
        .map(text)
        .collect()
}

/// The names of the file's structs, enums and traits.
fn names(file: &syn::File) -> Vec<String> {
    file.items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Struct(s) => Some(s.ident.to_string()),
            syn::Item::Enum(e) => Some(e.ident.to_string()),
            syn::Item::Trait(t) => Some(t.ident.to_string()),
            _ => None,
        })
        .collect()
}

fn flat(source: &str) -> String {
    source.split_whitespace().collect()
}

// region another package's types

/// Each package's types are in its own layer, once: a package of types
/// alone (no component) gets a layer of them.
#[test]
fn test_each_package_has_its_own_layer_and_its_own_types() {
    let mut packages: Vec<String> = layers().into_iter().map(|g| g.package).collect();
    packages.sort();
    assert_eq!(
        packages,
        ["shapes.catalog.v1", "shapes.common.v1", "shapes.order.v1"]
    );
    assert!(item_struct(&layer("shapes.common.v1"), "Money").is_some());
    let order = layer("shapes.order.v1");
    for foreign in ["Item", "Money"] {
        assert!(
            item_struct(&order, foreign).is_none(),
            "{foreign} is another package's, not duplicated: {:?}",
            names(&order)
        );
    }
}

/// A field of another package's type is its own layer's domain type,
/// converted to and from that package's prost type.
#[test]
fn test_another_packages_type_is_referenced_through_its_layer() {
    let order = layer("shapes.order.v1");
    assert!(
        fields(&order, "PlaceOrder").contains(&(
            "items".to_string(),
            "Vec<crate::domain::catalog::Item>".to_string()
        )),
        "{:?}",
        fields(&order, "PlaceOrder")
    );
    let catalog = layer("shapes.catalog.v1");
    assert!(
        fields(&catalog, "Item").contains(&(
            "price".to_string(),
            "crate::domain::common::Money".to_string()
        )),
        "{:?}",
        fields(&catalog, "Item")
    );
    let source = source("shapes.order.v1");
    for expected in [
        "crate::wire::shapes::catalog::v1::Item::from",
        "crate::domain::catalog::Item::try_from",
    ] {
        assert!(
            flat(&source).contains(expected),
            "no {expected} in:\n{source}"
        );
    }
}

/// A package whose types another's reach, without a path for its domain
/// layer, is refused: nothing is duplicated into the package reaching it.
#[test]
fn test_another_packages_type_without_a_domain_path_is_refused() {
    assert_eq!(
        markup_domain(&descriptors(SHAPES), SHAPES, WIRE),
        Err(CodegenError::Use(UseError::NoDomainPath {
            package: "shapes.common.v1".to_string(),
            message: "shapes.catalog.v1.Item".to_string(),
        }))
    );
}

/// Another package's message held as a map's values, or its enum, is
/// another package's type too: without a domain path, refused.
#[test]
fn test_another_packages_map_value_or_enum_without_a_domain_path_is_refused() {
    let wire = [
        ("foreign.other.v1", "crate::wire::other"),
        ("foreign.map.v1", "crate::wire::map"),
        ("foreign.enum.v1", "crate::wire::en"),
    ];
    for (file, holder) in [
        ("foreign/map/v1/map.proto", "foreign.map.v1.Holder"),
        ("foreign/enum/v1/enum.proto", "foreign.enum.v1.Holder"),
    ] {
        let package = holder.trim_end_matches(".Holder");
        assert_eq!(
            markup_domain_with(
                &descriptors(&[file]),
                &[file],
                &wire,
                &[(package, "crate::d")]
            ),
            Err(CodegenError::Use(UseError::NoDomainPath {
                package: "foreign.other.v1".to_string(),
                message: holder.to_string(),
            })),
            "{file}"
        );
    }
}

// endregion another package's types

// region UUID

/// An id newtype's name is a name of its layer: one a message has taken is
/// refused.
#[test]
fn test_an_id_type_taking_a_messages_name_is_refused() {
    const FILE: &str = "bad/v1/id_name.proto";
    assert_eq!(
        markup_domain(
            &descriptors(&[FILE]),
            &[FILE],
            &[("bad.v1", "::bad_proto::v1")]
        ),
        Err(CodegenError::Proto(ProtoError::NameTaken {
            name: "Ticket".to_string(),
            first: "bad.v1.Ticket".to_string(),
            second: "bad.v1.Ticket.id".to_string(),
        }))
    );
}

/// angzarr's `UUID` is angzarr-proto's own type on both sides: required
/// when singular, a `Vec` when repeated; no domain type is generated for
/// it.
#[test]
fn test_a_uuid_is_angzarrs_own_type() {
    let order = layer("shapes.order.v1");
    let place = fields(&order, "PlaceOrder");
    for (field, ty) in [
        ("order", "::angzarr_embedded::v1::Uuid"),
        ("related", "Vec<::angzarr_embedded::v1::Uuid>"),
    ] {
        assert!(
            place.contains(&(field.to_string(), ty.to_string())),
            "{field}: {place:?}"
        );
    }
    assert!(!names(&order).iter().any(|n| n == "UUID" || n == "Uuid"));
    let source = source("shapes.order.v1");
    assert!(
        flat(&source)
            .contains("WireError::Missing{message:\"shapes.order.v1.PlaceOrder\",field:\"order\""),
        "{source}"
    );
}

/// A UUID field declaring `id_type` holds that newtype, declared once per
/// package with its conversions and its text.
#[test]
fn test_a_uuid_with_an_id_type_is_its_newtype() {
    let order = layer("shapes.order.v1");
    assert!(
        fields(&order, "PlaceOrder")
            .contains(&("customer".to_string(), "Option<CustomerId>".to_string()))
    );
    assert_eq!(
        fields(&order, "CustomerId"),
        pairs(&[("", "::angzarr_embedded::v1::Uuid")])
    );
    let count = names(&order).iter().filter(|n| *n == "CustomerId").count();
    assert_eq!(count, 1, "declared once: {:?}", names(&order));
    let impls = impls(&order);
    for (t, ty) in [
        ("From<::angzarr_embedded::v1::Uuid>", "CustomerId"),
        ("From<CustomerId>", "::angzarr_embedded::v1::Uuid"),
        ("::std::fmt::Display", "CustomerId"),
        ("::std::str::FromStr", "CustomerId"),
    ] {
        assert!(
            impls.contains(&(t.to_string(), ty.to_string())),
            "{t} for {ty}: {impls:?}"
        );
    }
    let catalog = layer("shapes.catalog.v1");
    assert!(
        fields(&catalog, "ListItem").contains(&("listing".to_string(), "ListingId".to_string()))
    );
}

// endregion UUID

/// `google.protobuf.Duration` is prost's type on both sides, required when
/// singular, as `Timestamp` is angzarr-embedded's.
#[test]
fn test_a_duration_is_prosts_type() {
    let place = fields(&layer("shapes.order.v1"), "PlaceOrder");
    for (field, ty) in [
        ("wait", "::prost_types::Duration"),
        ("grace", "Option<::prost_types::Duration>"),
    ] {
        assert!(
            place.contains(&(field.to_string(), ty.to_string())),
            "{field}: {place:?}"
        );
    }
}

// region oneofs and nested types

/// A oneof is one field of a generated choice, named after the message and
/// the oneof, one variant per arm holding the arm's domain type, in the
/// place of its first arm.
#[test]
fn test_a_oneof_is_a_choice_of_its_arms() {
    let order = layer("shapes.order.v1");
    let place = fields(&order, "PlaceOrder");
    let names: Vec<&str> = place.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        [
            "items", "order", "customer", "related", "wait", "grace", "payment", "pace"
        ]
    );
    assert!(place.contains(&("payment".to_string(), "PlaceOrderPayment".to_string())));
    assert_eq!(
        variants(&order, "PlaceOrderPayment"),
        pairs(&[
            ("Card", "(String)"),
            ("Credit", "(crate::domain::common::Money)"),
            ("Gift", "(PlaceOrderGift)"),
            ("Speed", "(PlaceOrderSpeed)"),
            ("DeferredUntil", "(::angzarr_embedded::Timestamp)"),
        ])
    );
}

/// A oneof's arm is converted through prost's own choice
/// (`<message module>::<Oneof>`), and an unset oneof is refused as
/// missing, as a message field is.
#[test]
fn test_a_oneof_converts_through_prosts_choice_and_is_required() {
    let source = source("shapes.order.v1");
    for expected in [
        "crate::wire::shapes::order::v1::place_order::Payment::Card(",
        "crate::wire::shapes::order::v1::place_order::Payment::Credit(",
        "WireError::Missing{message:\"shapes.order.v1.PlaceOrder\",field:\"payment\"",
    ] {
        assert!(
            flat(&source).contains(expected),
            "no `{expected}` in:\n{source}"
        );
    }
}

/// A nested message is named after its enclosing messages, as a nested
/// enum is, and converts to and from prost's type in the enclosing
/// message's module.
#[test]
fn test_a_nested_message_is_named_after_its_enclosing_messages() {
    let order = layer("shapes.order.v1");
    assert_eq!(
        fields(&order, "PlaceOrderGift"),
        pairs(&[("code", "String"), ("wrap", "PlaceOrderGiftWrap")])
    );
    assert!(item_struct(&order, "PlaceOrderGiftWrap").is_some());
    let impls = impls(&order);
    assert!(
        impls.contains(&(
            "From<PlaceOrderGiftWrap>".to_string(),
            "crate::wire::shapes::order::v1::place_order::gift::Wrap".to_string()
        )),
        "{impls:?}"
    );
    assert_eq!(
        variants(&order, "PlaceOrderSpeed"),
        pairs(&[("Slow", ""), ("Fast", "")])
    );
}

/// A choice taking the name of a type of its package is refused, naming
/// both: nothing is renamed silently.
#[test]
fn test_a_choice_taking_a_types_name_is_refused() {
    const FILE: &str = "bad/v1/oneof_name.proto";
    assert_eq!(
        markup_domain(
            &descriptors(&[FILE]),
            &[FILE],
            &[("bad.v1", "::bad_proto::v1")]
        ),
        Err(CodegenError::Proto(ProtoError::NameTaken {
            name: "PokeShape".to_string(),
            first: "bad.v1.PokeShape".to_string(),
            second: "bad.v1.Poke.shape".to_string(),
        }))
    );
}

/// A refusal's args are its fields, never a oneof's arms: the choice is
/// not one value.
#[test]
fn test_a_refusals_oneof_arms_are_not_its_args() {
    let catalog = flat(&source("shapes.catalog.v1"));
    assert!(catalog.contains(r#"args.push(("kind","#), "{catalog}");
    for arm in ["sku", "count"] {
        assert!(
            !catalog.contains(&format!(r#"args.push(("{arm}","#)),
            "{arm}: {catalog}"
        );
    }
}

// endregion oneofs and nested types

/// `Default` is derived for every domain type all of whose fields have
/// one; a required enum or a oneof has none, nor has a type holding one.
#[test]
fn test_default_is_derived_where_every_field_has_one() {
    let order = layer("shapes.order.v1");
    for defaulted in ["OrderPlaced", "OrderState", "PlaceOrderGift", "CustomerId"] {
        assert!(
            derives(&order, defaulted).contains("Default"),
            "{defaulted}: {}",
            derives(&order, defaulted)
        );
    }
    for plain in [
        "PlaceOrder",
        "PlaceOrderPayment",
        "PlaceOrderSpeed",
        "Label",
        "Shipment",
    ] {
        assert!(
            !derives(&order, plain).contains("Default"),
            "{plain}: {}",
            derives(&order, plain)
        );
    }
    assert!(derives(&layer("shapes.common.v1"), "Money").contains("Default"));
}

// region anchors and snapshots

/// The gauge fixture: an aggregate whose anchor's domain type has no
/// `Default`, and a process manager.
const GAUGE: &[&str] = &["gauge/v1/gauge.proto"];

/// An aggregate's anchor is its state: a domain type of its layer (its
/// `Default` the empty state), a `DomainMessage` like an input or event,
/// and its component's interface is `<Anchor>Interface`, so the two never
/// collide.
#[test]
fn test_an_anchor_is_a_domain_type_beside_its_interface() {
    let order = layer("shapes.order.v1");
    assert_eq!(
        fields(&order, "OrderState"),
        pairs(&[
            ("placed_at", "Option<::angzarr_embedded::Timestamp>"),
            ("skus", "Vec<String>"),
            ("counts", "::std::collections::BTreeMap<String,u32>"),
            ("customer", "Option<CustomerId>"),
        ])
    );
    assert!(derives(&order, "OrderState").contains("Default"));
    assert!(impls(&order).contains(&(
        "::angzarr_embedded::markup::DomainMessage".to_string(),
        "OrderState".to_string()
    )));
    assert!(
        names(&order).contains(&"OrderStateInterface".to_string()),
        "{:?}",
        names(&order)
    );
}

/// Every component's interface is `<Anchor>Interface`, a policy's and a
/// projector's too, and no anchor's name is a trait.
#[test]
fn test_every_components_interface_is_its_anchors_name_and_interface() {
    let generated = markup_domain(&descriptors(BUYIN), BUYIN, BUYIN_WIRE).unwrap();
    let file = syn::parse_file(&generated[0].source).unwrap();
    let traits: Vec<String> = file
        .items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Trait(t) if t.ident.to_string().ends_with("Interface") => {
                Some(t.ident.to_string())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        traits,
        [
            "WalletStateInterface",
            "BuyInStateInterface",
            "BuyInWalletPolicyInterface",
            "WalletBuyInPolicyInterface",
        ]
    );
    let structs = names(&file);
    assert!(structs.contains(&"WalletState".to_string()), "{structs:?}");
    assert!(structs.contains(&"BuyInState".to_string()), "{structs:?}");
}

/// An aggregate's adapter delegates to `<Anchor>Interface`, and its
/// scaffold implements that interface on a state of the anchor's domain
/// type.
#[test]
fn test_an_aggregates_adapter_and_scaffold_name_its_interface() {
    let set = descriptors(SHAPES);
    let source = adapters(&set, SHAPES, &[]).unwrap();
    assert!(
        flat(&source).contains("crate::domain::OrderStateInterface::place_order"),
        "{source}"
    );
    let files = scaffold(&set, SHAPES).unwrap();
    let order = order_impl(&files);
    assert!(
        order
            .source
            .contains("impl OrderStateInterface for OrderStateImpl {"),
        "{}",
        order.source
    );
    assert!(
        order.source.contains("pub state: OrderState,"),
        "{}",
        order.source
    );
}

/// The interface of an aggregate or a process manager ends with its
/// snapshots over the anchor's domain type, `snapshot` and `restore`, which
/// it does not require: their defaults take none, `restore`'s refusing the
/// snapshot under angzarr-embedded's `NotSnapshotted`.
#[test]
fn test_an_interface_offers_snapshots_over_the_anchors_domain_type() {
    let order = layer("shapes.order.v1");
    let interface = order
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Trait(t) if t.ident == "OrderStateInterface" => Some(t),
            _ => None,
        })
        .expect("the interface");
    let methods: Vec<(String, Option<String>)> = interface
        .items
        .iter()
        .filter_map(|i| match i {
            syn::TraitItem::Fn(f) => Some((text(&f.sig), f.default.as_ref().map(text))),
            _ => None,
        })
        .collect();
    assert_eq!(
        methods,
        [
            (
                "fnplace_order(&self,cmd:&PlaceOrder,ctx:&Context,)->Result<Vec<OrderStateEvent>,Failed<OrderRefused>>"
                    .to_string(),
                None
            ),
            (
                "fnapply_order_placed(&mutself,event:&OrderPlaced)".to_string(),
                None
            ),
            (
                "fnsnapshot(&self)->Option<OrderState>".to_string(),
                Some("{None}".to_string())
            ),
            (
                "fnrestore(&mutself,state:OrderState)->Result<(),Report>".to_string(),
                Some(
                    "{let_=state;Err(Report::of(&::angzarr_embedded::NotSnapshotted::of::<\
                     <OrderStateas::angzarr_embedded::markup::DomainMessage>::Wire,>()))}"
                        .to_string()
                )
            ),
        ]
    );
    let doc = "`snapshot` and `restore` are its snapshots, its state as the anchor's domain \
               type, which angzarr-embedded stores as the anchor's wire type and resumes a \
               fresh implementation from (`markup::Snapshots`). Their defaults take none; \
               overriding both opts in.";
    let source = source("shapes.order.v1");
    assert!(
        flat(&source.replace("///", "")).contains(&flat(doc)),
        "{source}"
    );
}

/// An aggregate's adapter implements angzarr-embedded's `markup::Snapshots`
/// over the anchor's domain type, delegating both methods to its
/// interface, so it takes snapshots exactly when the implementation
/// overrides them; the markup takes no argument for it.
#[test]
fn test_an_aggregates_adapter_takes_its_snapshots_through_its_interface() {
    let set = descriptors(SHAPES);
    let file = parse_adapters(&adapters(&set, SHAPES, &[]).unwrap());
    let marked = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Impl(imp)
                if text(&imp.self_ty) == "OrderState"
                    && imp.attrs.iter().any(|a| a.path().is_ident("aggregate")) =>
            {
                Some(imp)
            }
            _ => None,
        })
        .expect("the aggregate");
    assert_eq!(
        impl_markup(marked),
        (
            "aggregate".to_string(),
            "domain=\"orders\",command=OrderStateCommand,event=OrderStateEvent".to_string()
        )
    );
    let snapshots = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Impl(imp)
                if text(&imp.self_ty) == "OrderState"
                    && imp.trait_.as_ref().is_some_and(|(path, _)| {
                        text(path) == "::angzarr_embedded::markup::Snapshots"
                    }) =>
            {
                Some(imp)
            }
            _ => None,
        })
        .expect("the adapter's Snapshots");
    let anchor = snapshots.items.iter().find_map(|i| match i {
        syn::ImplItem::Type(t) => Some(text(&t.ty)),
        _ => None,
    });
    assert_eq!(anchor.as_deref(), Some("crate::domain::OrderState"));
    let bodies: Vec<(String, String)> = methods(snapshots)
        .into_iter()
        .map(|f| (text(&f.sig), text(&f.block)))
        .collect();
    assert_eq!(
        bodies,
        [
            (
                "fnsnapshot(&self)->Option<crate::domain::OrderState>".to_string(),
                "{crate::domain::OrderStateInterface::snapshot(&self.0)}".to_string()
            ),
            (
                "fnrestore(&mutself,anchor:crate::domain::OrderState,)->Result<(),::angzarr_embedded::markup::Report>"
                    .to_string(),
                "{crate::domain::OrderStateInterface::restore(&mutself.0,anchor)}".to_string()
            ),
        ]
    );
}

/// The orders' implementation as the scaffold writes it.
fn order_impl(files: &[ScaffoldFile]) -> &ScaffoldFile {
    files
        .iter()
        .find(|f| f.path == "order_state_impl.rs")
        .expect("the order's implementation")
}

/// The scaffold configuration naming the orders.
fn orders() -> ScaffoldConfig {
    ScaffoldConfig {
        snapshots: vec!["orders".to_string()],
    }
}

/// The snapshot methods, as the scaffold's files name them.
fn snapshot_methods() -> Vec<String> {
    ["snapshot", "restore"].map(String::from).to_vec()
}

/// An implementation the configuration names is written taking snapshots:
/// `snapshot` and `restore` over the state it holds, the anchor's domain
/// type, cloned and assigned, both selected. One it does not name keeps
/// the interface's defaults: neither is written, both offered.
#[test]
fn test_the_configured_scaffold_takes_snapshots_of_the_state_it_holds() {
    let set = descriptors(SHAPES);
    let files = scaffold_with(&set, SHAPES, &[], &[], &orders()).unwrap();
    let order = order_impl(&files);
    assert!(
        order.source.ends_with(
            "    fn snapshot(&self) -> Option<OrderState> {\n        Some(self.state.clone())\n    }\n\n    \
             fn restore(&mut self, state: OrderState) -> Result<(), Report> {\n        \
             self.state = state;\n        Ok(())\n    }\n}\n"
        ),
        "{}",
        order.source
    );
    assert!(
        order.source.contains(
            "//! `--missing` adds the required methods it lacks, each `todo!()` but\n\
             //! `snapshot` and `restore`, which take snapshots of `state`.\n"
        ),
        "{}",
        order.source
    );
    assert_eq!(order.optional, snapshot_methods());
    assert_eq!(order.selected, snapshot_methods());
    let catalog = files
        .iter()
        .find(|f| f.path == "catalog_state_impl.rs")
        .expect("the catalog's implementation");
    assert!(
        !catalog.source.contains("fn snapshot"),
        "{}",
        catalog.source
    );
    assert!(!catalog.source.contains("fn restore"), "{}", catalog.source);
    assert_eq!(catalog.optional, snapshot_methods());
    assert!(catalog.selected.is_empty());
    let plain = scaffold(&set, SHAPES).unwrap();
    assert_eq!(order_impl(&plain).source, catalog_like(&order.source));
}

/// `source` without its snapshot methods and their note: what an
/// implementation the configuration does not name is.
fn catalog_like(source: &str) -> String {
    let start = source.find("\n    fn snapshot(").expect("a snapshot");
    format!("{}}}\n", &source[..start]).replace(
        " each `todo!()` but\n//! `snapshot` and `restore`, which take snapshots of `state`.",
        " each `todo!()`.",
    )
}

/// A configuration taking snapshots of a component the protos declare as
/// a policy is refused, naming it: it holds no state. One the protos do not
/// declare is another crate's, and changes nothing here.
#[test]
fn test_a_configuration_taking_snapshots_of_a_policy_is_refused() {
    let set = descriptors(BUYIN);
    let policy = ScaffoldConfig {
        snapshots: vec!["wallet".to_string(), "policy-buyin-wallet".to_string()],
    };
    assert_eq!(
        scaffold_with(&set, BUYIN, &[], &[], &policy),
        Err(CodegenError::Use(UseError::ScaffoldNoComponent {
            component: "policy-buyin-wallet".to_string()
        })),
    );
    let elsewhere = ScaffoldConfig {
        snapshots: vec!["nowhere".to_string()],
    };
    assert_eq!(
        scaffold_with(&set, BUYIN, &[], &[], &elsewhere).unwrap(),
        scaffold(&set, BUYIN).unwrap()
    );
    let config = ScaffoldConfig {
        snapshots: vec!["wallet".to_string(), "pmg-buyin".to_string()],
    };
    let files = scaffold_with(&set, BUYIN, &[], &[], &config).unwrap();
    let selected: Vec<(&str, usize)> = files
        .iter()
        .filter(|f| f.path.ends_with("_impl.rs"))
        .map(|f| (f.path.as_str(), f.selected.len()))
        .collect();
    assert_eq!(
        selected,
        [
            ("wallet_state_impl.rs", 2),
            ("buy_in_state_impl.rs", 2),
            ("buy_in_wallet_policy_impl.rs", 0),
            ("wallet_buy_in_policy_impl.rs", 0),
        ]
    );
}

/// An anchor whose domain type has no `Default` is held as its wire type,
/// at its package's wire path, and its configured snapshot methods are
/// written `todo!()`, as every other: only the user knows its conversion.
/// A process manager's anchor that has one is held and cloned as an
/// aggregate's is.
#[test]
fn test_a_wire_held_state_is_scaffolded_with_todo_snapshots() {
    let set = descriptors(GAUGE);
    let config = ScaffoldConfig {
        snapshots: vec!["gauge".to_string(), "pmg-reading".to_string()],
    };
    let files = scaffold_with(&set, GAUGE, &[("gauge.v1", "::gauge_proto")], &[], &config).unwrap();
    let gauge = files
        .iter()
        .find(|f| f.path == "gauge_state_impl.rs")
        .expect("the gauge's implementation");
    assert!(
        gauge.source.contains(
            "/// The state the `apply_` methods fold: the anchor's wire type, as its\n\
             /// domain type has no `Default`.\n#[derive(Clone, Debug, Default)]\n\
             pub struct GaugeStateImpl {\n    pub state: ::gauge_proto::GaugeState,\n}\n"
        ),
        "{}",
        gauge.source
    );
    assert!(
        gauge.source.ends_with(
            "    fn snapshot(&self) -> Option<GaugeState> {\n        todo!()\n    }\n\n    \
             fn restore(&mut self, _state: GaugeState) -> Result<(), Report> {\n        \
             todo!()\n    }\n}\n"
        ),
        "{}",
        gauge.source
    );
    assert!(
        gauge
            .source
            .contains("//! `--missing` adds the required methods it lacks, each `todo!()`.\n"),
        "{}",
        gauge.source
    );
    let reading = files
        .iter()
        .find(|f| f.path == "reading_state_impl.rs")
        .expect("the process manager's implementation");
    assert!(
        reading.source.contains("pub state: ReadingState,")
            && reading
                .source
                .contains("        Some(self.state.clone())\n"),
        "{}",
        reading.source
    );
    let layer = markup_domain(&set, GAUGE, &[("gauge.v1", "::gauge_proto")]).unwrap();
    let file = syn::parse_file(&layer[0].source).unwrap();
    assert!(!derives(&file, "GaugeState").contains("Default"));
}

/// `--missing` adds the selected two to an implementation written before
/// them, as the scaffold writes them, kept or completed alike; present,
/// nothing is added and nothing is obsolete. Unselected, they are offered.
#[test]
fn test_complete_adds_the_selected_snapshot_methods() {
    let files = scaffold_with(&descriptors(SHAPES), SHAPES, &[], &[], &orders()).unwrap();
    let fresh = order_impl(&files);
    let mine = catalog_like(&fresh.source);
    for mode in [Existing::Keep, Existing::Complete] {
        let completed = complete(fresh, &mine, mode).unwrap();
        assert_eq!(
            completed.completed.as_deref().map(|c| c.replace(
                " each `todo!()`.",
                " each `todo!()` but\n//! `snapshot` and `restore`, which take snapshots of `state`."
            )),
            Some(fresh.source.clone()),
            "{mode:?}"
        );
        assert_eq!(completed.notes, Notes::default(), "{mode:?}");
    }
    let whole = complete(fresh, &fresh.source, Existing::Complete).unwrap();
    assert_eq!(whole, Completion::default());
    let plain = scaffold(&descriptors(SHAPES), SHAPES).unwrap();
    let kept = complete(order_impl(&plain), &mine, Existing::Complete).unwrap();
    assert_eq!(kept.completed, None);
    assert_eq!(kept.notes.optional, snapshot_methods());
    assert!(kept.notes.lacks.is_empty(), "{:?}", kept.notes);
}

/// A method of no message in the user's implementation that the interface
/// does not have is reported obsolete, as a handler of a message the protos
/// dropped is; its snapshot methods, which the interface offers, are not.
#[test]
fn test_complete_reports_a_method_of_no_message_the_interface_lacks() {
    let files = scaffold_with(&descriptors(SHAPES), SHAPES, &[], &[], &orders()).unwrap();
    let fresh = order_impl(&files);
    let mine = fresh.source.replace(
        "    fn snapshot(&self)",
        "    fn checksum(&self) -> u32 {\n        0\n    }\n\n    fn snapshot(&self)",
    );
    assert_ne!(mine, fresh.source);
    let plain = scaffold(&descriptors(SHAPES), SHAPES).unwrap();
    for fresh in [fresh, order_impl(&plain)] {
        let kept = complete(fresh, &mine, Existing::Keep).unwrap();
        assert_eq!(kept.notes.obsolete, ["checksum"]);
        assert!(kept.notes.lacks.is_empty(), "{:?}", kept.notes);
        assert!(kept.notes.optional.is_empty(), "{:?}", kept.notes);
    }
}

// endregion anchors and snapshots

// region golden

/// Each shapes package's layer is its golden file: the text angzarr-build's
/// `tests/local` shapes-app compiles and round-trips through prost's bytes,
/// and anchors-app builds its adapters and aggregates on.
#[test]
fn test_each_packages_layer_is_its_golden_file() {
    for generated in layers() {
        golden(
            &format!("shapes/{}.domain.rs", generated.package),
            &generated.source,
        );
    }
}

// endregion golden

// region components of several packages

/// Components in two packages, each package's layer at its own path: each
/// package's adapters are a module of their own reading that package's
/// layer, re-exported as `adapters::<Component>` (what `register()`
/// names). Whether it compiles is angzarr-build's `tests/local`
/// anchors-app.
#[test]
fn test_each_packages_adapters_read_that_packages_layer() {
    let source = adapters_with(&descriptors(SHAPES), SHAPES, &[], DOMAIN).unwrap();
    golden("shapes.adapters.rs", &source);
}

/// One domain path for every package (or none given) is the one module
/// `adapters` reading `crate::domain`, as `adapters` writes it.
#[test]
fn test_adapters_of_one_layer_are_one_module() {
    let set = descriptors(SHAPES);
    let one = [
        ("shapes.common.v1", "crate::domain"),
        ("shapes.catalog.v1", "crate::domain"),
        ("shapes.order.v1", "crate::domain"),
    ];
    let plain = adapters(&set, SHAPES, &[]).unwrap();
    assert_eq!(adapters_with(&set, SHAPES, &[], &one).unwrap(), plain);
    assert_eq!(adapters_with(&set, SHAPES, &[], &[]).unwrap(), plain);
    // A path that is not a Rust path is none given.
    let unparsed = [
        ("shapes.catalog.v1", "not a path"),
        ("shapes.order.v1", "::"),
    ];
    assert_eq!(adapters_with(&set, SHAPES, &[], &unparsed).unwrap(), plain);
}

/// Each component's scaffold imports its own package's layer.
#[test]
fn test_each_components_scaffold_imports_its_packages_layer() {
    let files = scaffold_with(
        &descriptors(SHAPES),
        SHAPES,
        WIRE,
        DOMAIN,
        &ScaffoldConfig::default(),
    )
    .unwrap();
    for f in files.iter().filter(|f| f.path.ends_with("_impl.rs")) {
        golden(&format!("rust/shapes/{}", f.path), &f.source);
    }
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        [
            "catalog_state.rs",
            "catalog_state_impl.rs",
            "order_state.rs",
            "order_state_impl.rs"
        ]
    );
}

/// A scaffold imports its package's layer at its domain path, and holds
/// the anchor's domain type (a wire-held state, at its package's wire
/// path, is the gauge's: `test_a_wire_held_state_is_scaffolded_with_todo_snapshots`);
/// with no paths given, `crate::wire` and `crate::domain`, as `scaffold`
/// writes it.
#[test]
fn test_a_scaffolds_wire_state_is_at_its_packages_wire_path() {
    let set = descriptors(BUYIN);
    let files = scaffold_with(
        &set,
        BUYIN,
        BUYIN_WIRE,
        &[("buyin.v1", "crate::buyin")],
        &ScaffoldConfig::default(),
    )
    .unwrap();
    let wallet = files
        .iter()
        .find(|f| f.path == "wallet_state_impl.rs")
        .expect("the wallet's implementation");
    assert!(
        wallet.source.contains("use crate::buyin::*;\n"),
        "{}",
        wallet.source
    );
    assert!(
        wallet.source.contains("pub state: WalletState,"),
        "{}",
        wallet.source
    );
    let plain = scaffold(&set, BUYIN).unwrap();
    assert_eq!(
        scaffold_with(&set, BUYIN, &[], &[], &ScaffoldConfig::default()).unwrap(),
        plain
    );
    // A path that is not a Rust path is none given.
    let unparsed = [("buyin.v1", "not a path")];
    assert_eq!(
        scaffold_with(
            &set,
            BUYIN,
            &unparsed,
            &unparsed,
            &ScaffoldConfig::default()
        )
        .unwrap(),
        plain
    );
}

// endregion components of several packages
