use super::*;

#[test]
fn internal_means_workspace_crate_or_path_target() {
    let mut m = Model::default();
    m.workspace_crates.insert("router".into());
    assert!(m.is_internal("router"));
    assert!(m.is_internal("router::saga"));
    assert!(m.is_internal("path:bindings/go"));
    assert!(!m.is_internal("prost::Message"));
    assert!(!m.is_internal("router_ffi"));
}
