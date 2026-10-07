use std::path::PathBuf;

use super::*;
use crate::model::{Edge, Model, Scope};

fn edge(from: &str, to: &str, scope: Scope) -> Edge {
    Edge {
        from: from.into(),
        to: to.into(),
        scope,
        file: PathBuf::from("src/lib.rs"),
        line: 1,
        via: to.into(),
    }
}

fn model(edges: Vec<Edge>) -> Model {
    let mut m = Model::default();
    for e in &edges {
        m.modules
            .insert(e.from.clone(), PathBuf::from("src/lib.rs"));
    }
    m.workspace_crates = ["core_crate", "ffi"].map(String::from).into();
    m.edges = edges;
    m
}

fn names(file: &RulesFile, model: &Model) -> Vec<(String, String)> {
    check(file, model)
        .iter()
        .map(|v| (v.rule.name.clone(), v.edge.to.clone()))
        .collect()
}

#[test]
fn double_star_matches_zero_or_more_segments() {
    assert!(matches("a::**", "a"));
    assert!(matches("a::**", "a::b::c"));
    assert!(matches("**::c", "a::b::c"));
    assert!(matches("**", "anything::at::all"));
    assert!(!matches("a::**", "ab"));
    assert!(!matches("a::**", "b::a"));
}

#[test]
fn single_star_matches_within_one_segment() {
    assert!(matches("a::*", "a::b"));
    assert!(!matches("a::*", "a"));
    assert!(!matches("a::*", "a::b::c"));
    assert!(matches("a::test#*", "a::test#saga"));
    assert!(matches("a::*_x", "a::y_x"));
    assert!(!matches("a::*_x", "a::y_z"));
    assert!(matches("a::x*y", "a::xy"));
    assert!(!matches("a::x*y", "a::x"));
}

#[test]
fn exact_patterns_match_only_that_module() {
    assert!(matches("a::b", "a::b"));
    assert!(!matches("a::b", "a::b::c"));
    assert!(!matches("a::b", "a"));
    assert!(!matches("a", "a::b"));
}

#[test]
fn path_patterns_only_match_path_targets() {
    assert!(matches("path:bindings/**", "path:bindings/go/x.h"));
    assert!(!matches("path:bindings/**", "path:crates/router"));
    assert!(!matches("path:bindings/**", "bindings"));
    assert!(!matches("bindings::**", "path:bindings/go"));
}

#[test]
fn parse_requires_allow_or_deny() {
    let err = parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['a']\n").unwrap_err();
    assert!(err.contains("neither `allow` nor `deny`"), "{err}");
}

#[test]
fn parse_rejects_empty_from_and_unknown_fields() {
    let err = parse("[[rule]]\nname='r'\nwhy='w'\nfrom=[]\ndeny=['x']\n").unwrap_err();
    assert!(err.contains("empty `from`"), "{err}");
    assert!(parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['a']\ndeny=['x']\nextra=1\n").is_err());
}

#[test]
fn parse_reads_scope_allow_and_deny() {
    let f = parse(
        "[[rule]]\nname='r'\nwhy='w'\nfrom=['a::**']\nscope='lib'\nallow=['b']\ndeny=['c']\n",
    )
    .unwrap();
    let r = &f.rules[0];
    assert_eq!(r.scope, ScopeSel::Lib);
    assert_eq!(r.allow.as_deref(), Some(&["b".to_string()][..]));
    assert_eq!(r.deny, vec!["c".to_string()]);
    assert_eq!(
        parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['a']\ndeny=['c']\n")
            .unwrap()
            .rules[0]
            .scope,
        ScopeSel::All
    );
}

#[test]
fn load_reports_the_file_on_error() {
    let err = load(std::path::Path::new("/nonexistent/archlint.toml")).unwrap_err();
    assert!(err.starts_with("/nonexistent/archlint.toml:"), "{err}");
}

#[test]
fn deny_flags_internal_and_external_targets() {
    let f = parse("[[rule]]\nname='no-ffi'\nwhy='w'\nfrom=['core_crate::**']\ndeny=['ffi::**','std::ffi::**']\n").unwrap();
    let m = model(vec![
        edge("core_crate::a", "ffi", Scope::Lib),
        edge("core_crate::a", "std::ffi::c_void", Scope::Lib),
        edge("core_crate::a", "std::collections::HashMap", Scope::Lib),
        edge("ffi", "core_crate", Scope::Lib),
    ]);
    assert_eq!(
        names(&f, &m),
        vec![
            ("no-ffi".to_string(), "ffi".to_string()),
            ("no-ffi".to_string(), "std::ffi::c_void".to_string()),
        ]
    );
}

#[test]
fn allow_constrains_only_workspace_targets() {
    let f = parse("[[rule]]\nname='leaf'\nwhy='w'\nfrom=['core_crate::error']\nallow=['core_crate::proto::**']\n").unwrap();
    let m = model(vec![
        edge("core_crate::error", "core_crate::proto::v1", Scope::Lib),
        edge("core_crate::error", "prost::Message", Scope::Lib),
        edge("core_crate::error", "core_crate::saga", Scope::Lib),
        edge("core_crate::error", "path:bindings/x", Scope::Lib),
    ]);
    assert_eq!(
        names(&f, &m),
        vec![
            ("leaf".to_string(), "core_crate::saga".to_string()),
            ("leaf".to_string(), "path:bindings/x".to_string()),
        ]
    );
}

#[test]
fn deny_wins_over_allow() {
    let f = parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['core_crate']\nallow=['ffi::**']\ndeny=['ffi::inner']\n").unwrap();
    let m = model(vec![
        edge("core_crate", "ffi", Scope::Lib),
        edge("core_crate", "ffi::inner", Scope::Lib),
    ]);
    assert_eq!(
        names(&f, &m),
        vec![("r".to_string(), "ffi::inner".to_string())]
    );
}

#[test]
fn self_edges_never_violate() {
    let f = parse(
        "[[rule]]\nname='r'\nwhy='w'\nfrom=['core_crate::**']\nallow=[]\ndeny=['core_crate::**']\n",
    )
    .unwrap();
    let m = model(vec![edge("core_crate::a", "core_crate::a", Scope::Lib)]);
    assert!(check(&f, &m).is_empty());
}

#[test]
fn scope_selects_which_edges_a_rule_sees() {
    let lib =
        parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['core_crate']\nscope='lib'\ndeny=['ffi']\n")
            .unwrap();
    let test =
        parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['core_crate']\nscope='test'\ndeny=['ffi']\n")
            .unwrap();
    let all = parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['core_crate']\ndeny=['ffi']\n").unwrap();
    let m_lib = model(vec![edge("core_crate", "ffi", Scope::Lib)]);
    let m_test = model(vec![edge("core_crate", "ffi", Scope::Test)]);
    assert_eq!(check(&lib, &m_lib).len(), 1);
    assert_eq!(check(&lib, &m_test).len(), 0);
    assert_eq!(check(&test, &m_lib).len(), 0);
    assert_eq!(check(&test, &m_test).len(), 1);
    assert_eq!(check(&all, &m_lib).len(), 1);
    assert_eq!(check(&all, &m_test).len(), 1);
}

#[test]
fn rules_only_see_edges_from_their_modules() {
    let f = parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['core_crate::a']\ndeny=['ffi']\n").unwrap();
    let m = model(vec![edge("core_crate::b", "ffi", Scope::Lib)]);
    assert!(check(&f, &m).is_empty());
}

#[test]
fn unmatched_from_reports_stale_patterns() {
    let f = parse(
        "[[rule]]\nname='r'\nwhy='w'\nfrom=['core_crate::a','core_crate::gone::**']\ndeny=['x']\n",
    )
    .unwrap();
    let m = model(vec![edge("core_crate::a", "ffi", Scope::Lib)]);
    assert_eq!(unmatched_from(&f, &m), vec![("r", "core_crate::gone::**")]);
}

#[test]
fn self_patterns_stand_for_the_source_module() {
    assert_eq!(expand_self("self", "a::b"), "a::b");
    assert_eq!(expand_self("self::**", "a::b"), "a::b::**");
    assert_eq!(expand_self("selfish::x", "a::b"), "selfish::x");
    assert_eq!(expand_self("x::self", "a::b"), "x::self");
    let f =
        parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['core_crate::*']\nallow=['self::**']\n").unwrap();
    let m = model(vec![
        edge("core_crate::a", "core_crate::a::inner", Scope::Lib),
        edge("core_crate::a", "core_crate::b", Scope::Lib),
        edge("core_crate::b", "core_crate::b::x::y", Scope::Lib),
    ]);
    assert_eq!(
        names(&f, &m),
        vec![("r".to_string(), "core_crate::b".to_string())]
    );
    let f = parse("[[rule]]\nname='r'\nwhy='w'\nfrom=['core_crate::*']\ndeny=['self::secret']\n")
        .unwrap();
    let m = model(vec![
        edge("core_crate::a", "core_crate::a::secret", Scope::Lib),
        edge("core_crate::a", "core_crate::b::secret", Scope::Lib),
    ]);
    assert_eq!(
        names(&f, &m),
        vec![("r".to_string(), "core_crate::a::secret".to_string())]
    );
}
