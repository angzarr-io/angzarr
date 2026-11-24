//! The crates angzarr-embedded re-exports at its root, so an application
//! pins none of them itself and names each through it: the very crates the
//! kernel is built on, not copies.

#[test]
fn angzarr_proto_is_the_kernels_own() {
    let _: Option<::angzarr_embedded::angzarr_proto::v1::Cover> = None::<angzarr_proto::v1::Cover>;
    let _: Option<::angzarr_embedded::angzarr_proto::v1::Cover> =
        None::<::angzarr_embedded::v1::Cover>;
}

#[test]
fn angzarr_ports_and_angzarr_diagnostic_are_the_kernels_own() {
    let _: Option<::angzarr_embedded::angzarr_ports::StoreError> =
        None::<::angzarr_embedded::StoreError>;
    assert_eq!(
        ::angzarr_embedded::angzarr_ports::EN_US,
        angzarr_ports::EN_US
    );
    let _: Option<Box<dyn ::angzarr_embedded::coded_diagnostic::Diagnostic>> =
        None::<Box<dyn coded_diagnostic::Diagnostic>>;
}

#[test]
fn the_well_known_types_are_prost_types_own() {
    let _: Option<::angzarr_embedded::Any> = None::<prost_types::Any>;
    let _: Option<::angzarr_embedded::Timestamp> = None::<prost_types::Timestamp>;
    let _: Option<::angzarr_embedded::Duration> = None::<prost_types::Duration>;
}

/// With the `sqlite` feature (on in this crate's tests).
#[test]
fn angzarr_store_sqlite_is_the_workspaces_own() {
    let _: Option<::angzarr_embedded::angzarr_store_sqlite::SqliteStore> =
        None::<angzarr_store_sqlite::SqliteStore>;
    assert_eq!(
        ::angzarr_embedded::angzarr_store_sqlite::EN_US,
        angzarr_store_sqlite::EN_US
    );
}
